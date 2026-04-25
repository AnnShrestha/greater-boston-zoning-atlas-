"""
process_tiger.py — Reproject, clean, join, and export Massachusetts TIGER data

Reads raw TIGER layers from data/raw/, joins ACS demographics to tracts,
and exports clean GeoPackage + GeoJSON layers ready for analysis or web use.

Input:  data/raw/*.gpkg  +  data/raw/acs_demographics.csv
Output:
  output/gpkg/   — Analysis-ready GeoPackages (EPSG:26986)
  output/geojson/— Web-ready GeoJSON (EPSG:4326, WGS84)
CRS:    Working CRS is EPSG:26986 (NAD83 / Massachusetts Mainland, metres).
        Chosen because it is the official Massachusetts state plane projection
        and gives accurate area/distance calculations in metric units.
        All GeoJSON exports are reprojected to EPSG:4326 for web use.

Usage:
    python process_tiger.py
    # Run download_tiger.py first if data/raw/ is empty
"""

# ── Dependencies ──────────────────────────────────────────────────────────────
import sys
import pandas as pd
import geopandas as gpd
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────
RAW_DIR     = Path("data/raw")
GPKG_DIR    = Path("output/gpkg")
GEOJSON_DIR = Path("output/geojson")

# EPSG:26986 — NAD83 / Massachusetts Mainland (metres)
# Used for all area, distance, and overlay operations
PROJECT_CRS = "EPSG:26986"

# EPSG:4326 — WGS84 geographic; required for GeoJSON and web tile sources
WGS84 = "EPSG:4326"

# Columns to keep per layer (trim TIGER bloat for smaller outputs)
ROADS_KEEP_COLS    = ["LINEARID", "FULLNAME", "RTTYP", "MTFCC", "geometry"]
BOUNDARY_KEEP_COLS = ["GEOID", "NAME", "NAMELSAD", "ALAND", "AWATER", "geometry"]
ADDRESS_KEEP_COLS  = ["TLID", "LFROMHN", "LTOHN", "RFROMHN", "RTOHN",
                      "ZIPL", "ZIPR", "FULLNAME", "geometry"]

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_raw(name: str) -> gpd.GeoDataFrame:
    path = RAW_DIR / f"{name}.gpkg"
    if not path.exists():
        print(f"Error: {path} not found. Run download_tiger.py first.")
        sys.exit(1)
    gdf = gpd.read_file(path)
    print(f"Loaded {name}: {len(gdf):,} features | Source CRS: {gdf.crs}")
    if gdf.crs is None:
        print(f"Error: {name} has no CRS. Cannot reproject safely.")
        sys.exit(1)
    return gdf


def export(gdf: gpd.GeoDataFrame, name: str, export_geojson: bool = True) -> None:
    """Save to GeoPackage (PROJECT_CRS) and optionally GeoJSON (WGS84)."""
    GPKG_DIR.mkdir(parents=True, exist_ok=True)
    gpkg_path = GPKG_DIR / f"{name}.gpkg"
    gdf.to_file(gpkg_path, driver="GPKG")
    print(f"  → {gpkg_path} ({len(gdf):,} features, {gdf.crs.to_epsg()})")

    if export_geojson:
        GEOJSON_DIR.mkdir(parents=True, exist_ok=True)
        geojson_path = GEOJSON_DIR / f"{name}.geojson"
        gdf.to_crs(WGS84).to_file(geojson_path, driver="GeoJSON")
        print(f"  → {geojson_path} (reprojected to WGS84 for web use)")


def reproject(gdf: gpd.GeoDataFrame, label: str) -> gpd.GeoDataFrame:
    print(f"  Reprojecting {label}: {gdf.crs} → {PROJECT_CRS}")
    return gdf.to_crs(PROJECT_CRS)


# ── Processing steps ──────────────────────────────────────────────────────────

def process_boundaries() -> None:
    print("\n── Boundaries ───────────────────────────────────────────────────────")

    for layer in ("counties", "tracts", "block_groups", "places"):
        gdf = load_raw(layer)
        gdf = reproject(gdf, layer)

        # Trim to useful columns; keep all if expected columns are missing
        keep = [c for c in BOUNDARY_KEEP_COLS if c in gdf.columns]
        gdf  = gdf[keep]

        # Add area fields in project CRS units (metres → km²)
        gdf["area_km2"] = (gdf.geometry.area / 1_000_000).round(4)

        export(gdf, layer)


def process_roads() -> None:
    print("\n── Roads ────────────────────────────────────────────────────────────")
    gdf = load_raw("roads")
    gdf = reproject(gdf, "roads")

    keep = [c for c in ROADS_KEEP_COLS if c in gdf.columns]
    gdf  = gdf[keep]

    # MTFCC is the TIGER feature class code — useful for filtering road class
    # S1100=Primary highway, S1200=Secondary road, S1400=Local road, etc.
    if "MTFCC" in gdf.columns:
        print(f"  MTFCC classes: {gdf['MTFCC'].value_counts().to_dict()}")

    export(gdf, "roads")


def process_address_ranges() -> None:
    print("\n── Address Ranges ───────────────────────────────────────────────────")
    gdf = load_raw("address_ranges")
    gdf = reproject(gdf, "address_ranges")

    keep = [c for c in ADDRESS_KEEP_COLS if c in gdf.columns]
    gdf  = gdf[keep]

    export(gdf, "address_ranges")


def process_demographics() -> None:
    """Join ACS tract-level demographics to TIGER tract geometries."""
    print("\n── Demographics (ACS join to tracts) ────────────────────────────────")

    acs_path = RAW_DIR / "acs_demographics.csv"
    if not acs_path.exists():
        print(f"Error: {acs_path} not found. Run download_tiger.py first.")
        sys.exit(1)

    gdf_tracts = load_raw("tracts")
    gdf_tracts = reproject(gdf_tracts, "tracts")

    df_acs = pd.read_csv(acs_path, dtype={"GEOID": str})
    print(f"Loaded ACS data: {len(df_acs):,} tract records")

    # TIGER GEOID and Census API GEOID must match exactly (18-char string)
    if "GEOID" not in gdf_tracts.columns:
        print("Error: TIGER tracts layer has no GEOID column — cannot join.")
        sys.exit(1)

    # Left join keeps all tracts; tracts with no ACS match get NaN demographics
    gdf_demo = gdf_tracts.merge(df_acs, on="GEOID", how="left")

    unmatched = gdf_demo["total_population"].isna().sum()
    if unmatched:
        print(f"  Warning: {unmatched} tracts had no ACS match (check GEOID alignment).")

    # Derived fields
    if {"owner_occupied", "renter_occupied", "housing_units"}.issubset(gdf_demo.columns):
        gdf_demo["pct_owner_occupied"] = (
            gdf_demo["owner_occupied"] / gdf_demo["housing_units"] * 100
        ).round(1)

    if {"population_below_poverty", "total_population"}.issubset(gdf_demo.columns):
        gdf_demo["pct_poverty"] = (
            gdf_demo["population_below_poverty"] / gdf_demo["total_population"] * 100
        ).round(1)

    export(gdf_demo, "tracts_with_demographics")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Processing Massachusetts TIGER data → {PROJECT_CRS}")
    print(f"Output: {GPKG_DIR.resolve()} | {GEOJSON_DIR.resolve()}")

    process_boundaries()
    process_roads()
    process_address_ranges()
    process_demographics()

    print("\nProcessing complete.")
    print("Next steps:")
    print("  - Load output/gpkg/*.gpkg in QGIS for QA")
    print("  - Use output/geojson/*.geojson in a web map or FastAPI backend")
    print("  - Run a spatial API with scripts/fastapi_spatial_server.py")
