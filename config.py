"""
config.py — Central configuration for the MAPC Zoning Atlas ETL pipeline.

All tuneable values live here. Secrets are read from environment variables only —
never hardcoded. Copy .env.example to .env and fill in your RDS credentials.
"""

# ── Dependencies ──────────────────────────────────────────────────────────────
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROC_DIR = PROJECT_ROOT / "data" / "processed"
LOG_DIR = PROJECT_ROOT / "logs"

# ── Source data ───────────────────────────────────────────────────────────────
# MAPC Zoning Atlas — GeoJSON via MAPC Open Data (ArcGIS FeatureServer)
# Find the current URL at: https://data-metroboston.opendata.arcgis.com/
# Search "Zoning Atlas" and copy the GeoJSON download URL.
MAPC_ZONING_URL = os.environ.get(
    "MAPC_ZONING_URL",
    "https://data-metroboston.opendata.arcgis.com/datasets/mapc::zoning-atlas.geojson",
)
MAPC_ZONING_CACHE = DATA_RAW_DIR / "mapc_zoning_atlas.geojson"

# ── CRS ───────────────────────────────────────────────────────────────────────
# MAPC source data arrives in WGS84 (EPSG:4326)
SOURCE_CRS = "EPSG:4326"

# Storage CRS: NAD83 / Massachusetts Mainland (metres) — accurate area/distance
# Used for all geometry stored in PostGIS.
PROJECT_CRS = "EPSG:26986"

# ── Database ──────────────────────────────────────────────────────────────────
# Credentials come exclusively from environment variables.
# Set these in your shell, a .env file (loaded via python-dotenv), or CI secrets.
DB_HOST = os.environ.get("DB_HOST", "")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "gisdb")
DB_USER = os.environ.get("DB_USER", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

# Target schema and table name in PostGIS
DB_SCHEMA = "mapc"
TABLE_ZONING = "zoning_atlas"
TABLE_ETL_RUNS = "etl_runs"
TABLE_QAQC_LOG = "qaqc_log"

# ── QA/QC thresholds ──────────────────────────────────────────────────────────
# Pipeline exits with an error if any critical check breaches these values.
QA_MAX_NULL_RATE = 0.10          # Max allowable null rate per column (10%)
QA_MIN_FEATURE_COUNT = 1000      # Minimum expected zoning features statewide
QA_MAX_INVALID_GEOM_RATE = 0.01  # Max allowable invalid geometry rate (1%)
QA_MIN_AREA_M2 = 100             # Flag features smaller than 100 m² as suspicious

# ── ETL behaviour ─────────────────────────────────────────────────────────────
FORCE_DOWNLOAD = os.environ.get("FORCE_DOWNLOAD", "false").lower() == "true"
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
