"""
etl/transform.py — Clean, validate, and enrich the MAPC Zoning Atlas data.

Steps:
  1. Validate and repair geometries
  2. Reproject to project CRS (EPSG:26986, NAD83 / Massachusetts Mainland)
  3. Standardise column names (lowercase, underscores)
  4. Add derived fields: area_m2, area_acres, centroid coordinates
  5. Add ETL provenance fields: etl_run_id, etl_timestamp

CRS note: MAPC source is WGS84 (EPSG:4326). We store in EPSG:26986 for accurate
          area/distance queries. GeoJSON exports (for web maps) should be
          reprojected back to EPSG:4326 at query time via ST_Transform.
"""

# ── Dependencies ──────────────────────────────────────────────────────────────
import logging
import uuid
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from shapely.validation import make_valid

import config

logger = logging.getLogger(__name__)


def _standardise_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Lowercase all column names and replace spaces/hyphens with underscores."""
    gdf.columns = (
        gdf.columns
        .str.lower()
        .str.strip()
        .str.replace(r"[\s\-]+", "_", regex=True)
    )
    return gdf


def _repair_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Attempt to repair invalid geometries using Shapely's make_valid().
    Logs the count of features that required repair.
    """
    invalid_mask = gdf.geometry.notna() & ~gdf.geometry.is_valid
    invalid_count = invalid_mask.sum()

    if invalid_count > 0:
        logger.warning(
            "%d invalid geometries found — attempting repair with make_valid()", invalid_count
        )
        gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].apply(make_valid)

        still_invalid = (~gdf.geometry.is_valid).sum()
        if still_invalid:
            logger.error("%d geometries could not be repaired and will be dropped", still_invalid)
            gdf = gdf[gdf.geometry.is_valid].copy()
        else:
            logger.info("All geometries repaired successfully")
    else:
        logger.info("All geometries valid — no repairs needed")

    return gdf


def _add_derived_fields(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add area and centroid fields. Requires a metric projected CRS.
    Must be called after reprojection to PROJECT_CRS.
    """
    gdf["area_m2"] = gdf.geometry.area.round(2)
    gdf["area_acres"] = (gdf["area_m2"] / 4046.856).round(4)

    # Centroid in project CRS (for spatial joins); stored as separate columns
    centroids = gdf.geometry.centroid
    gdf["centroid_x"] = centroids.x.round(2)
    gdf["centroid_y"] = centroids.y.round(2)

    return gdf


def _add_etl_provenance(gdf: gpd.GeoDataFrame, run_id: str) -> gpd.GeoDataFrame:
    """Stamp each row with the ETL run ID and load timestamp."""
    gdf["etl_run_id"] = run_id
    gdf["etl_loaded_at"] = datetime.now(timezone.utc).isoformat()
    return gdf


def transform(gdf: gpd.GeoDataFrame, run_id: str | None = None) -> gpd.GeoDataFrame:
    """
    Full transform pipeline. Returns a clean GeoDataFrame ready for PostGIS load.

    Args:
        gdf:    Raw GeoDataFrame from extract step (expected CRS: EPSG:4326)
        run_id: ETL run UUID; generated here if not provided

    Returns:
        Transformed GeoDataFrame in EPSG:26986
    """
    if run_id is None:
        run_id = str(uuid.uuid4())

    logger.info("Transform started | run_id=%s | %d input features", run_id, len(gdf))

    # ── 1. Validate source CRS ────────────────────────────────────────────────
    if gdf.crs is None:
        logger.warning("No CRS on input — assigning %s", config.SOURCE_CRS)
        gdf = gdf.set_crs(config.SOURCE_CRS)
    elif str(gdf.crs) != config.SOURCE_CRS:
        logger.warning(
            "Unexpected source CRS %s — expected %s. Reprojecting anyway.", gdf.crs, config.SOURCE_CRS
        )

    # ── 2. Repair invalid geometries ─────────────────────────────────────────
    gdf = _repair_geometries(gdf)

    # ── 3. Reproject to project CRS ──────────────────────────────────────────
    logger.info("Reprojecting: %s → %s", gdf.crs, config.PROJECT_CRS)
    gdf = gdf.to_crs(config.PROJECT_CRS)

    # ── 4. Standardise column names ───────────────────────────────────────────
    gdf = _standardise_columns(gdf)

    # ── 5. Drop empty or null geometries ─────────────────────────────────────
    null_geom_count = gdf.geometry.isna().sum()
    if null_geom_count:
        logger.warning("Dropping %d rows with null geometry", null_geom_count)
        gdf = gdf.dropna(subset=["geometry"])

    # ── 6. Derived fields (requires metric CRS — must be after reproject) ────
    gdf = _add_derived_fields(gdf)

    # ── 7. ETL provenance ─────────────────────────────────────────────────────
    gdf = _add_etl_provenance(gdf, run_id)

    logger.info(
        "Transform complete | %d features | CRS: %s | area range: %.0f–%.0f m²",
        len(gdf),
        gdf.crs,
        gdf["area_m2"].min(),
        gdf["area_m2"].max(),
    )

    return gdf
