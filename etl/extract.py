"""
etl/extract.py — Load the MAPC Zoning Atlas from a local shapefile.

MAPC no longer provides a public GeoJSON API. Download the shapefile zip from:
  https://mapc365.sharepoint.com/:f:/s/DataServicesSP/ErKkXSLH_iBOlDhJrTXldrYBIIZ4ZXe4Bkw7OyVapVpX3Q

Place the zip (or extracted .shp) in data/raw/ before running the pipeline.

Expected input: data/raw/mapc_zoning_atlas.zip  (or .shp / .geojson)
"""

# ── Dependencies ──────────────────────────────────────────────────────────────
import logging
import zipfile
import geopandas as gpd
from pathlib import Path

import config

logger = logging.getLogger(__name__)

# Candidate file paths — checked in order
_CANDIDATES = [
    config.DATA_RAW_DIR / "mapc_zoning_atlas.zip",
    config.DATA_RAW_DIR / "mapc_zoning_atlas.shp",
    config.DATA_RAW_DIR / "mapc_zoning_atlas.geojson",
]


def download_mapc_zoning(force: bool = False) -> gpd.GeoDataFrame:
    """
    Load the MAPC Zoning Atlas from a local file and return as a GeoDataFrame.

    Checks data/raw/ for a zip, shp, or geojson file. Extracts zip if needed.
    The `force` parameter is accepted for API compatibility but has no effect
    (re-reading local files is always fast).
    """
    config.DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

    source_path = _find_source()
    if source_path is None:
        raise FileNotFoundError(
            "No MAPC zoning data found in data/raw/. "
            "Download the shapefile zip from the MAPC SharePoint and place it in data/raw/ "
            "as 'mapc_zoning_atlas.zip'.\n"
            "URL: https://mapc365.sharepoint.com/:f:/s/DataServicesSP/"
            "ErKkXSLH_iBOlDhJrTXldrYBIIZ4ZXe4Bkw7OyVapVpX3Q"
        )

    if source_path.suffix == ".zip":
        source_path = _extract_zip(source_path)

    logger.info("Loading MAPC Zoning Atlas from %s", source_path)
    gdf = gpd.read_file(source_path)

    if gdf.crs is None:
        logger.warning("No CRS in source file — assigning %s (MAPC standard)", config.SOURCE_CRS)
        gdf = gdf.set_crs(config.SOURCE_CRS)

    logger.info("Extracted %d features | CRS: %s", len(gdf), gdf.crs)
    return gdf


def _find_source() -> Path | None:
    for path in _CANDIDATES:
        if path.exists():
            return path
    # Also accept any .shp inside data/raw/
    shps = list(config.DATA_RAW_DIR.glob("*.shp"))
    if shps:
        return shps[0]
    zips = list(config.DATA_RAW_DIR.glob("*.zip"))
    if zips:
        return zips[0]
    return None


def _extract_zip(zip_path: Path) -> Path:
    extract_dir = zip_path.parent / zip_path.stem
    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_dir)
    shps = list(extract_dir.rglob("*.shp"))
    if not shps:
        raise FileNotFoundError(f"No .shp found inside {zip_path}")
    logger.info("Extracted zip → %s", shps[0])
    return shps[0]
