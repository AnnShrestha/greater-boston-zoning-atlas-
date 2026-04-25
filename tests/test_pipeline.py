"""
tests/test_pipeline.py — Unit tests for transform and QA/QC logic.

These tests run without a live database (no RDS required).
They validate geometry repair, CRS handling, column derivation, and QA thresholds.

Run:
    pip install pytest
    pytest tests/ -v
"""

import uuid
import pytest
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, MultiPolygon
from shapely.validation import make_valid

# Patch config before importing ETL modules so tests don't require env vars
import config
config.DRY_RUN = True

from etl.transform import transform, _standardise_columns, _repair_geometries
from etl.qaqc import (
    check_feature_count,
    check_geometry_validity,
    check_crs,
    check_small_features,
    check_null_rates,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_gdf(n: int = 5, crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    """Return a minimal GeoDataFrame with n valid polygon features."""
    polys = [
        Polygon([(i, 0), (i + 1, 0), (i + 1, 1), (i, 1)])
        for i in range(n)
    ]
    return gpd.GeoDataFrame(
        {"zone_id": [str(i) for i in range(n)], "muni_name": [f"Town {i}" for i in range(n)]},
        geometry=polys,
        crs=crs,
    )


# ── Transform tests ───────────────────────────────────────────────────────────

class TestColumnStandardisation:
    def test_lowercase(self):
        gdf = _make_gdf()
        gdf.columns = ["Zone_ID", "Muni Name", "geometry"]
        gdf = _standardise_columns(gdf)
        assert list(gdf.columns) == ["zone_id", "muni_name", "geometry"]

    def test_spaces_to_underscores(self):
        gdf = _make_gdf()
        gdf.columns = ["zone id", "muni-name", "geometry"]
        gdf = _standardise_columns(gdf)
        assert "zone_id" in gdf.columns
        assert "muni_name" in gdf.columns


class TestGeometryRepair:
    def test_valid_geometries_unchanged(self):
        gdf = _make_gdf()
        repaired = _repair_geometries(gdf)
        assert len(repaired) == len(gdf)
        assert repaired.geometry.is_valid.all()

    def test_invalid_geometry_repaired(self):
        # Self-intersecting "bowtie" polygon — invalid by OGC rules
        bowtie = Polygon([(0, 0), (2, 2), (2, 0), (0, 2)])
        gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[bowtie], crs="EPSG:4326")
        assert not gdf.geometry.is_valid.all()
        repaired = _repair_geometries(gdf)
        assert repaired.geometry.is_valid.all()


class TestTransformPipeline:
    def test_output_crs(self):
        gdf = _make_gdf(crs="EPSG:4326")
        result = transform(gdf, run_id=str(uuid.uuid4()))
        assert str(result.crs) == config.PROJECT_CRS

    def test_area_field_added(self):
        gdf = _make_gdf(crs="EPSG:4326")
        result = transform(gdf)
        assert "area_m2" in result.columns
        assert "area_acres" in result.columns
        assert (result["area_m2"] > 0).all()

    def test_provenance_fields_added(self):
        run_id = str(uuid.uuid4())
        gdf = _make_gdf(crs="EPSG:4326")
        result = transform(gdf, run_id=run_id)
        assert "etl_run_id" in result.columns
        assert "etl_loaded_at" in result.columns
        assert (result["etl_run_id"] == run_id).all()

    def test_null_geometry_rows_dropped(self):
        gdf = _make_gdf(n=3)
        gdf.loc[0, "geometry"] = None
        result = transform(gdf)
        assert len(result) == 2


# ── QA/QC check tests ─────────────────────────────────────────────────────────

class TestQAChecks:
    def test_feature_count_pass(self):
        gdf = _make_gdf(n=config.QA_MIN_FEATURE_COUNT)
        result = check_feature_count(gdf)
        assert result.passed

    def test_feature_count_fail(self):
        gdf = _make_gdf(n=10)
        result = check_feature_count(gdf)
        assert not result.passed
        assert result.critical

    def test_geometry_validity_pass(self):
        gdf = _make_gdf()
        result = check_geometry_validity(gdf)
        assert result.passed

    def test_crs_check_pass(self):
        gdf = _make_gdf(crs="EPSG:4326")
        gdf = transform(gdf)
        result = check_crs(gdf)
        assert result.passed

    def test_crs_check_fail(self):
        gdf = _make_gdf(crs="EPSG:4326")  # Not PROJECT_CRS — not yet reprojected
        result = check_crs(gdf)
        assert not result.passed
        assert result.critical

    def test_small_features_flag(self):
        # Very small polygon (10 cm²) should trigger warning
        tiny = Polygon([(0, 0), (0.001, 0), (0.001, 0.001), (0, 0.001)])
        gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[tiny], crs=config.PROJECT_CRS)
        gdf["area_m2"] = gdf.geometry.area
        result = check_small_features(gdf)
        assert not result.passed
        assert not result.critical  # Warning only

    def test_null_rate_pass(self):
        gdf = _make_gdf()
        results = check_null_rates(gdf)
        assert all(r.passed for r in results)

    def test_null_rate_fail(self):
        gdf = _make_gdf(n=20)
        # Set 50% of zone_id to null — above QA_MAX_NULL_RATE
        gdf.loc[:9, "zone_id"] = None
        results = check_null_rates(gdf)
        zone_check = next(r for r in results if "zone_id" in r.check_name)
        assert not zone_check.passed
