"""
pipeline.py — MAPC Zoning Atlas spatial ETL — main orchestrator.

Runs the full pipeline:
  1. Extract  — download MAPC Zoning Atlas GeoJSON
  2. Transform — validate, repair, reproject, enrich
  3. QA/QC    — pre-load checks (geometry, CRS, nulls, row count)
  4. Load     — write to PostGIS on AWS RDS
  5. QA/QC    — post-load checks (row count, PostGIS ST_IsValid)

Usage:
    python pipeline.py              # Full run
    DRY_RUN=true python pipeline.py # Validate without writing to DB
    FORCE_DOWNLOAD=true python pipeline.py  # Re-download source data

Exit codes:
    0 — Pipeline completed successfully, all QA/QC checks passed
    1 — Pipeline completed but QA/QC critical check(s) failed
    2 — Pipeline aborted due to an unhandled error
"""

# ── Dependencies ──────────────────────────────────────────────────────────────
import logging
import sys
import uuid
from pathlib import Path
from datetime import datetime

import config
from etl.extract import download_mapc_zoning
from etl.transform import transform
from etl.load import load_zoning, get_engine, ensure_schema
from etl.qaqc import run_all_checks

# ── Logging setup ─────────────────────────────────────────────────────────────
config.LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.LOG_DIR / "etl.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("pipeline")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    run_id = str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat()

    logger.info("═" * 60)
    logger.info("MAPC Zoning Atlas ETL | run_id=%s", run_id)
    logger.info("Started: %s%s", started_at, "  [DRY RUN]" if config.DRY_RUN else "")
    logger.info("═" * 60)

    try:
        # ── 1. Extract ────────────────────────────────────────────────────────
        logger.info("Step 1/4 — Extract")
        gdf_raw = download_mapc_zoning(force=config.FORCE_DOWNLOAD)

        # ── 2. Transform ──────────────────────────────────────────────────────
        logger.info("Step 2/4 — Transform")
        gdf = transform(gdf_raw, run_id=run_id)

        # ── Ensure schema exists before any DB writes ─────────────────────────
        if not config.DRY_RUN:
            ensure_schema(get_engine())

        # ── 3. Pre-load QA/QC ─────────────────────────────────────────────────
        logger.info("Step 3/4 — Pre-load QA/QC")
        qa_passed = run_all_checks(
            gdf,
            run_id=run_id,
            skip_db_checks=True,   # DB checks run after load
        )
        if not qa_passed:
            logger.error("Pre-load QA/QC failed — aborting load")
            return 1

        # ── 4. Load ───────────────────────────────────────────────────────────
        logger.info("Step 4/4 — Load to PostGIS")
        row_count = load_zoning(gdf, run_id=run_id, mode="replace")

        # ── 5. Post-load QA/QC ────────────────────────────────────────────────
        logger.info("Post-load QA/QC")
        if not config.DRY_RUN:
            qa_passed = run_all_checks(
                gdf,
                run_id=run_id,
                expected_count=row_count,
                skip_db_checks=False,
            )
            if not qa_passed:
                logger.error("Post-load QA/QC failed — data is loaded but review qaqc_log")
                return 1

    except Exception as exc:
        logger.exception("Pipeline aborted with unhandled error: %s", exc)
        return 2

    logger.info("═" * 60)
    logger.info("Pipeline complete | run_id=%s | %d features loaded", run_id, row_count)
    logger.info("═" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
