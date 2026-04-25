"""
etl/load.py — Load transformed GeoDataFrame into PostGIS on AWS RDS.

Uses GeoDataFrame.to_postgis() via SQLAlchemy + GeoAlchemy2.
Performs an upsert strategy: truncate-and-reload for a full refresh,
or append for incremental loads. Defaults to truncate-and-reload.

The target schema and tables must already exist (see sql/schema.sql).
"""

# ── Dependencies ──────────────────────────────────────────────────────────────
import logging
from datetime import datetime, timezone

import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

import config

logger = logging.getLogger(__name__)


def _build_connection_string() -> str:
    """Assemble the PostgreSQL connection string from environment-sourced config."""
    if not all([config.DB_HOST, config.DB_USER, config.DB_PASSWORD]):
        raise EnvironmentError(
            "Database credentials missing. Set DB_HOST, DB_USER, DB_PASSWORD in environment."
        )
    return (
        f"postgresql+psycopg2://{config.DB_USER}:{config.DB_PASSWORD}"
        f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )


def get_engine():
    """Create a SQLAlchemy engine with connection pooling and pre-ping."""
    return create_engine(
        _build_connection_string(),
        pool_pre_ping=True,   # Recycle stale connections (important for RDS)
        pool_size=2,
        max_overflow=3,
    )


def ensure_schema(engine) -> None:
    """Create the schema and all audit tables if they don't exist."""
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {config.DB_SCHEMA}"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {config.DB_SCHEMA}.{config.TABLE_ETL_RUNS} (
                run_id       TEXT PRIMARY KEY,
                table_name   TEXT,
                row_count    INTEGER,
                completed_at TIMESTAMPTZ
            )
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {config.DB_SCHEMA}.{config.TABLE_QAQC_LOG} (
                id           SERIAL PRIMARY KEY,
                run_id       TEXT,
                check_name   TEXT,
                status       TEXT,
                value        TEXT,
                threshold    TEXT,
                message      TEXT,
                checked_at   TIMESTAMPTZ DEFAULT now()
            )
        """))
        conn.commit()
    logger.info("Schema '%s' verified", config.DB_SCHEMA)


def load_zoning(
    gdf: gpd.GeoDataFrame,
    run_id: str,
    mode: str = "replace",
) -> int:
    """
    Load the transformed GeoDataFrame to PostGIS.

    Args:
        gdf:    Transformed GeoDataFrame (EPSG:26986)
        run_id: ETL run ID for the etl_runs audit record
        mode:   'replace' (truncate + reload) or 'append'

    Returns:
        Number of rows loaded.
    """
    if config.DRY_RUN:
        logger.info("DRY RUN — skipping database write (%d features would load)", len(gdf))
        return len(gdf)

    engine = get_engine()
    ensure_schema(engine)

    target = f"{config.DB_SCHEMA}.{config.TABLE_ZONING}"
    logger.info("Loading %d features → %s (mode=%s)", len(gdf), target, mode)

    try:
        if mode == "replace":
            # Check if table exists — if yes, truncate to preserve the PostGIS
            # geometry column type; if no, let to_postgis create it fresh.
            with engine.connect() as conn:
                exists = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = :schema AND table_name = :table
                    )
                """), {"schema": config.DB_SCHEMA, "table": config.TABLE_ZONING}).scalar()
                if exists:
                    conn.execute(text(f"TRUNCATE TABLE {target}"))
                    conn.commit()
            gdf.to_postgis(
                name=config.TABLE_ZONING,
                con=engine,
                schema=config.DB_SCHEMA,
                if_exists="append",
                index=False,
            )
        else:
            gdf.to_postgis(
                name=config.TABLE_ZONING,
                con=engine,
                schema=config.DB_SCHEMA,
                if_exists=mode,
                index=False,
            )
    except SQLAlchemyError as exc:
        logger.error("Database load failed: %s", exc)
        raise

    # Create spatial index if it doesn't already exist (idempotent)
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{config.TABLE_ZONING}_geom
            ON {target} USING GIST (geometry)
        """))
        # Update query planner stats after bulk insert
        conn.execute(text(f"ANALYZE {target}"))
        conn.commit()

    _record_etl_run(engine, run_id, len(gdf))

    logger.info("Load complete — %d rows in %s", len(gdf), target)
    return len(gdf)


def _record_etl_run(engine, run_id: str, row_count: int) -> None:
    """Insert a row into the etl_runs audit table."""
    sql = text(f"""
        INSERT INTO {config.DB_SCHEMA}.{config.TABLE_ETL_RUNS}
            (run_id, table_name, row_count, completed_at)
        VALUES
            (:run_id, :table_name, :row_count, :completed_at)
        ON CONFLICT (run_id) DO UPDATE
            SET row_count    = EXCLUDED.row_count,
                completed_at = EXCLUDED.completed_at
    """)
    with engine.connect() as conn:
        conn.execute(sql, {
            "run_id": run_id,
            "table_name": f"{config.DB_SCHEMA}.{config.TABLE_ZONING}",
            "row_count": row_count,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })
        conn.commit()
