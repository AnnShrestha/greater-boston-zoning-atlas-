"""
download_tiger.py — Download Massachusetts TIGER/Line data + ACS demographics

Downloads all four layer groups to data/raw/:
  - Boundaries  : counties, tracts, block groups, places
  - Roads        : all-class road edges (assembled per county)
  - Address ranges: TIGER address range lines (per county)
  - Demographics : ACS 5-year estimates joined to census tracts

Input:  Census TIGER FTP + Census API (internet required)
Output: data/raw/*.gpkg  and  data/raw/acs_demographics.csv
CRS:    TIGER source data is NAD83 (EPSG:4269); exported as-is here.
        Run process_tiger.py to reproject for analysis.

Usage:
    pip install -r requirements.txt
    python download_tiger.py

    Optional: set CENSUS_API_KEY env var for higher ACS rate limits.
    Get a free key at https://api.census.gov/data/key_signup.html
"""

# ── Dependencies ──────────────────────────────────────────────────────────────
import os
import sys
import requests
import pandas as pd
import geopandas as gpd
import pygris
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────
STATE       = "MA"
STATE_FIPS  = "25"
TIGER_YEAR  = 2022

# Massachusetts county FIPS codes (needed for per-county layers)
MA_COUNTIES = {
    "Barnstable": "001", "Berkshire": "003", "Bristol": "005",
    "Dukes":      "007", "Essex":     "009", "Franklin": "011",
    "Hampden":    "013", "Hampshire": "015", "Middlesex": "017",
    "Nantucket":  "019", "Norfolk":   "021", "Plymouth":  "023",
    "Suffolk":    "025", "Worcester": "027",
}

# ACS 5-year variables to download (tract level)
# Format: {variable_id: human_readable_name}
ACS_VARIABLES = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B17001_002E": "population_below_poverty",
    "B15003_022E": "bachelors_degree",
    "B25001_001E": "housing_units",
    "B25003_002E": "owner_occupied",
    "B25003_003E": "renter_occupied",
}

RAW_DIR        = Path("data/raw")
CENSUS_API_KEY = os.environ.get("CENSUS_API_KEY", "")  # Optional but recommended

# ── Helpers ───────────────────────────────────────────────────────────────────

def save_gpkg(gdf: gpd.GeoDataFrame, name: str) -> Path:
    """Save a GeoDataFrame to data/raw/<name>.gpkg and report."""
    path = RAW_DIR / f"{name}.gpkg"
    gdf.to_file(path, driver="GPKG")
    print(f"  Saved {len(gdf):,} features → {path} | CRS: {gdf.crs}")
    return path


def download_boundaries() -> None:
    print("\n── Boundaries ───────────────────────────────────────────────────────")

    print("Downloading counties …")
    gdf_counties = pygris.counties(state=STATE, year=TIGER_YEAR)
    save_gpkg(gdf_counties, "counties")

    print("Downloading census tracts …")
    gdf_tracts = pygris.tracts(state=STATE, year=TIGER_YEAR)
    save_gpkg(gdf_tracts, "tracts")

    print("Downloading block groups …")
    gdf_block_groups = pygris.block_groups(state=STATE, year=TIGER_YEAR)
    save_gpkg(gdf_block_groups, "block_groups")

    print("Downloading places (cities/towns/CDPs) …")
    gdf_places = pygris.places(state=STATE, year=TIGER_YEAR)
    save_gpkg(gdf_places, "places")


def download_roads() -> None:
    """Roads are stored per county in TIGER — assemble into one statewide layer."""
    print("\n── Roads ────────────────────────────────────────────────────────────")
    county_gdfs = []
    for name, fips in MA_COUNTIES.items():
        print(f"  Downloading roads: {name} ({fips}) …")
        try:
            gdf = pygris.roads(state=STATE, county=fips, year=TIGER_YEAR)
            county_gdfs.append(gdf)
        except Exception as exc:
            print(f"  Warning: could not download roads for {name}: {exc}")

    if not county_gdfs:
        print("Error: No road data downloaded.")
        return

    gdf_roads = pd.concat(county_gdfs, ignore_index=True)
    gdf_roads = gpd.GeoDataFrame(gdf_roads, crs=county_gdfs[0].crs)
    save_gpkg(gdf_roads, "roads")


def download_address_ranges() -> None:
    """Address ranges (TIGER/Line) — per county, assembled statewide."""
    print("\n── Address Ranges ───────────────────────────────────────────────────")
    county_gdfs = []
    for name, fips in MA_COUNTIES.items():
        print(f"  Downloading address ranges: {name} ({fips}) …")
        try:
            gdf = pygris.address_ranges(state=STATE, county=fips, year=TIGER_YEAR)
            county_gdfs.append(gdf)
        except Exception as exc:
            print(f"  Warning: could not download address ranges for {name}: {exc}")

    if not county_gdfs:
        print("Error: No address range data downloaded.")
        return

    gdf_addresses = pd.concat(county_gdfs, ignore_index=True)
    gdf_addresses = gpd.GeoDataFrame(gdf_addresses, crs=county_gdfs[0].crs)
    save_gpkg(gdf_addresses, "address_ranges")


def download_acs_demographics() -> None:
    """
    Fetch ACS 5-year tract-level estimates from the Census API.
    The CSV is saved here and joined to tract geometries in process_tiger.py.
    """
    print("\n── ACS Demographics (Census API) ────────────────────────────────────")

    variable_list = ",".join(["NAME"] + list(ACS_VARIABLES.keys()))
    url = f"https://api.census.gov/data/{TIGER_YEAR}/acs/acs5"
    params = {
        "get":  variable_list,
        "for":  "tract:*",
        "in":   f"state:{STATE_FIPS}",
    }
    if CENSUS_API_KEY:
        params["key"] = CENSUS_API_KEY

    print(f"  Requesting ACS5 {TIGER_YEAR} tract data for Massachusetts …")
    response = requests.get(url, params=params, timeout=60)
    if response.status_code != 200:
        print(f"  Error: Census API returned {response.status_code}: {response.text[:200]}")
        return

    json_data  = response.json()
    df_acs     = pd.DataFrame(json_data[1:], columns=json_data[0])

    # Rename variable codes to readable names
    df_acs = df_acs.rename(columns=ACS_VARIABLES)

    # Build GEOID to match TIGER tract GEOID format (state+county+tract, zero-padded)
    df_acs["GEOID"] = df_acs["state"] + df_acs["county"] + df_acs["tract"]

    # Convert numeric columns from string
    numeric_cols = list(ACS_VARIABLES.values())
    df_acs[numeric_cols] = df_acs[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # Sentinel value -666666666 means "not available" in Census API responses
    df_acs[numeric_cols] = df_acs[numeric_cols].replace(-666666666, pd.NA)

    out_path = RAW_DIR / "acs_demographics.csv"
    df_acs.to_csv(out_path, index=False)
    print(f"  Saved {len(df_acs):,} tract records → {out_path}")
    print(f"  Columns: {list(df_acs.columns)}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Downloading Massachusetts TIGER/Line {TIGER_YEAR} data")
    print(f"Output directory: {RAW_DIR.resolve()}")

    download_boundaries()
    download_roads()
    download_address_ranges()
    download_acs_demographics()

    print("\nDownload complete. Run process_tiger.py to reproject and export.")
