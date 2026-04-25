"""
etl/qaqc.py — QA/QC checks for the MAPC Zoning Atlas ETL pipeline.

Checks run against the loaded PostGIS table and the in-memory GeoDataFrame.
Results are written to:
  1. The Python logger (→ logs/etl.log and console)
  2. The qaqc_log table in PostGIS (for audit trail and dashboarding)

A check is 'critical' if a breach should halt the pipeline.
Non-critical checks log warnings but do not raise.
"""

# ── Dependencies ──────────────────────────────────────────────────────────────
import logging
from datetime import datetime, timezone
from typing import Any

import geopandas as gpd
import pandas as pd
from sqlalchemy import text

import config
from etl.load import get_engine

logger = logging.getLogger(__name__)


# ── Check result dataclass ────────────────────────────────────────────────────

class CheckResult:
    def __init__(
        self,
        check_name: str,
        passed: bool,
        value: Any,
        threshold: Any,
        critical: bool,
        note: str = "",
    ):
        self.check_name = check_name
        self.passed = passed
        self.value = value
        self.threshold = threshold
        self.critical = critical
        self.note = note

    def __str__(self) -> str:
        status = "PASS" if self.passed else ("FAIL [CRITICAL]" if self.critical else "WARN")
        return f"{status} | {self.check_name} | value={self.value} threshold={self.threshold} | {self.note}"


# ── In-memory checks (pre-load) ───────────────────────────────────────────────

def check_feature_count(gdf: gpd.GeoDataFrame) -> CheckResult:
    count = len(gdf)
    passed = count >= config.QA_MIN_FEATURE_COUNT
    return CheckResult(
        check_name="feature_count",
        passed=passed,
        value=count,
        threshold=f">= {config.QA_MIN_FEATURE_COUNT}",
        critical=True,
        note=f"{count} features loaded",
    )


def check_null_rates(gdf: gpd.GeoDataFrame) -> list[CheckResult]:
    """Flag columns with null rate above QA_MAX_NULL_RATE."""
    results = []
    for col in gdf.columns:
        if col == "geometry":
            continue
        null_rate = gdf[col].isna().mean()
        passed = null_rate <= config.QA_MAX_NULL_RATE
        results.append(CheckResult(
            check_name=f"null_rate_{col}",
            passed=passed,
            value=round(null_rate, 4),
            threshold=f"<= {config.QA_MAX_NULL_RATE}",
            critical=False,
            note=f"{int(null_rate * len(gdf))} nulls in '{col}'",
        ))
    return results


def check_geometry_validity(gdf: gpd.GeoDataFrame) -> CheckResult:
    invalid_rate = (~gdf.geometry.is_valid).mean()
    passed = invalid_rate <= config.QA_MAX_INVALID_GEOM_RATE
    return CheckResult(
        check_name="geometry_validity",
        passed=passed,
        value=round(invalid_rate, 4),
        threshold=f"<= {config.QA_MAX_INVALID_GEOM_RATE}",
        critical=True,
        note=f"{int(invalid_rate * len(gdf))} invalid geometries",
    )


def check_crs(gdf: gpd.GeoDataFrame) -> CheckResult:
    actual_crs = str(gdf.crs) if gdf.crs else "None"
    passed = actual_crs == config.PROJECT_CRS
    return CheckResult(
        check_name="crs_check",
        passed=passed,
        value=actual_crs,
        threshold=config.PROJECT_CRS,
        critical=True,
        note="CRS must be PROJECT_CRS before load",
    )


def check_small_features(gdf: gpd.GeoDataFrame) -> CheckResult:
    """Flag features suspiciously smaller than QA_MIN_AREA_M2."""
    if "area_m2" not in gdf.columns:
        return CheckResult("small_features", True, "n/a", "n/a", False, "area_m2 column missing")
    small_count = (gdf["area_m2"] < config.QA_MIN_AREA_M2).sum()
    passed = small_count == 0
    return CheckResult(
        check_name="small_features",
        passed=passed,
        value=int(small_count),
        threshold=f"area >= {config.QA_MIN_AREA_M2} m²",
        critical=False,
        note=f"{small_count} features < {config.QA_MIN_AREA_M2} m² — check for slivers",
    )


# ── Post-load DB checks ───────────────────────────────────────────────────────

def check_postgis_row_count(expected_count: int) -> CheckResult:
    """Verify the loaded row count in PostGIS matches the in-memory count."""
    engine = get_engine()
    target = f"{config.DB_SCHEMA}.{config.TABLE_ZONING}"
    with engine.connect() as conn:
        actual = conn.execute(text(f"SELECT COUNT(*) FROM {target}")).scalar()
    passed = actual == expected_count
    return CheckResult(
        check_name="postgis_row_count",
        passed=passed,
        value=actual,
        threshold=f"== {expected_count}",
        critical=True,
        note=f"PostGIS has {actual}, expected {expected_count}",
    )


def check_postgis_geometry_validity() -> CheckResult:
    """Run ST_IsValid across the loaded table in PostGIS."""
    engine = get_engine()
    target = f"{config.DB_SCHEMA}.{config.TABLE_ZONING}"
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT
                COUNT(*) FILTER (WHERE NOT ST_IsValid(geometry)) AS invalid_count,
                COUNT(*) AS total_count
            FROM {target}
        """)).fetchone()
    invalid, total = result
    rate = invalid / total if total else 0
    passed = rate <= config.QA_MAX_INVALID_GEOM_RATE
    return CheckResult(
        check_name="postgis_geometry_validity",
        passed=passed,
        value=round(rate, 4),
        threshold=f"<= {config.QA_MAX_INVALID_GEOM_RATE}",
        critical=True,
        note=f"{invalid}/{total} invalid geometries in PostGIS",
    )


# ── Logging to qaqc_log table ─────────────────────────────────────────────────

def _write_qaqc_results(results: list[CheckResult], run_id: str) -> None:
    """Persist all check results to the qaqc_log table in PostGIS."""
    if config.DRY_RUN:
        logger.info("DRY RUN — skipping qaqc_log write")
        return

    engine = get_engine()
    rows = [
        {
            "run_id": run_id,
            "check_name": r.check_name,
            "passed": r.passed,
            "value": str(r.value),
            "threshold": str(r.threshold),
            "critical": r.critical,
            "note": r.note,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        for r in results
    ]
    df = pd.DataFrame(rows)
    df.to_sql(
        config.TABLE_QAQC_LOG,
        engine,
        schema=config.DB_SCHEMA,
        if_exists="append",
        index=False,
    )
    logger.info("QA/QC results written to %s.%s", config.DB_SCHEMA, config.TABLE_QAQC_LOG)


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_all_checks(
    gdf: gpd.GeoDataFrame,
    run_id: str,
    expected_count: int | None = None,
    skip_db_checks: bool = False,
) -> bool:
    """
    Run the full QA/QC suite.

    Args:
        gdf:            Transformed GeoDataFrame (post-transform, pre- or post-load)
        run_id:         ETL run ID
        expected_count: If provided, verify PostGIS row count matches this
        skip_db_checks: True in dry-run / CI without a live DB

    Returns:
        True if all critical checks passed, False otherwise.
    """
    logger.info("── QA/QC checks starting ──────────────────────────────────────")

    results: list[CheckResult] = []

    # In-memory checks
    results.append(check_feature_count(gdf))
    results.append(check_geometry_validity(gdf))
    results.append(check_crs(gdf))
    results.append(check_small_features(gdf))
    results.extend(check_null_rates(gdf))

    # Post-load DB checks
    if not skip_db_checks and expected_count is not None:
        results.append(check_postgis_row_count(expected_count))
        results.append(check_postgis_geometry_validity())

    # Log all results and determine pass/fail
    critical_failures = []
    for r in results:
        logger.info(str(r))
        if not r.passed and r.critical:
            critical_failures.append(r.check_name)

    if not config.DRY_RUN:
        _write_qaqc_results(results, run_id)

    if critical_failures:
        logger.error(
            "QA/QC FAILED — %d critical check(s) failed: %s",
            len(critical_failures),
            ", ".join(critical_failures),
        )
        return False

    warn_count = sum(1 for r in results if not r.passed and not r.critical)
    logger.info(
        "QA/QC PASSED — %d/%d checks passed (%d warnings)",
        sum(r.passed for r in results),
        len(results),
        warn_count,
    )
    return True
