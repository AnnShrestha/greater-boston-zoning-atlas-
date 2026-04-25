# MASS TIGER — Massachusetts Zoning Atlas ETL Pipeline

A production-quality **spatial ETL pipeline** that loads the [MAPC Greater Boston Zoning Atlas](https://www.mapc.org/planning101/zoning-atlas/) into a **PostGIS database on AWS RDS**, with full geometry validation, CRS reprojection, QA/QC logging, and an audit trail for every run.

Built as a documented, end-to-end example of a real-world GIS data engineering workflow — from raw shapefile to queryable PostGIS table in the cloud.

---

## What This Project Does

The Metropolitan Area Planning Council (MAPC) publishes a **Zoning Atlas** covering 101 municipalities in Greater Boston — 1,775 zoning district polygons describing land use regulations (residential density, lot sizes, height limits, FAR, etc.). This pipeline:

1. **Extracts** the shapefile from local storage (MAPC distributes this via SharePoint)
2. **Validates and repairs** 62 invalid geometries using Shapely's `make_valid()`
3. **Reprojects** from NAD83 / Massachusetts Mainland (ftUS) to EPSG:26986 (metric)
4. **Runs 88 QA/QC checks** before and after loading — null rates, geometry validity, CRS, row counts
5. **Loads** all 1,775 features into PostGIS on AWS RDS
6. **Logs** every run to an audit table (`mapc.etl_runs`) and QA/QC table (`mapc.qaqc_log`)

---

## Stack

| Layer | Technology |
|---|---|
| Source data | MAPC Zoning Atlas (Shapefile, NAD83 MA Mainland ftUS) |
| Processing | Python 3.11 · GeoPandas · Shapely 2.0 |
| Database | PostgreSQL 16 + PostGIS on AWS RDS (db.t4g.micro) |
| ORM / loader | SQLAlchemy 2.0 + GeoAlchemy2 |
| QA/QC | 88 automated checks → Python logging + `mapc.qaqc_log` table |
| Config | `python-dotenv` — credentials never hardcoded |

**Storage CRS:** EPSG:26986 — NAD83 / Massachusetts Mainland (metres) — chosen for accurate area and distance calculations in Massachusetts.

**Web/export CRS:** Use `ST_Transform(geometry, 4326)` at query time for GeoJSON or web map output.

---

## Project Structure

```
MASS_TIGER/
├── pipeline.py              # Main orchestrator — run this
├── config.py                # All configuration, secrets via env vars
├── requirements.txt
│
├── etl/
│   ├── extract.py           # Load shapefile from local disk (with zip support)
│   ├── transform.py         # Geometry repair, reprojection, derived fields
│   ├── load.py              # Write to PostGIS; creates schema/tables if needed
│   └── qaqc.py              # 88 QA/QC checks, pre- and post-load
│
├── sql/
│   └── schema.sql           # PostGIS DDL reference (pipeline auto-creates tables)
│
├── docs/
│   ├── schema.md                # Column reference + example queries
│   └── METADATA_STANDARDS.md   # FGDC CSDGM-aligned metadata policy
│
├── tests/
│   └── test_pipeline.py     # Unit tests (no DB required)
│
├── data/
│   └── raw/                 # Place source shapefile/zip here (gitignored)
│
└── logs/
    └── etl.log              # Rotating log file (gitignored)
```

---

## Architecture

```
 [MAPC Shapefile]
       │
       ▼
  extract.py          ← auto-detects .zip, .shp, .geojson in data/raw/
       │
       ▼
  transform.py        ← repair geometries · reproject · add area/centroid fields
       │
       ▼
  qaqc.py (pre-load)  ← 88 checks: feature count, nulls, CRS, geometry validity
       │
       ▼
  load.py             ← TRUNCATE + append to PostGIS (preserves geometry type)
       │
       ▼
  qaqc.py (post-load) ← verify row count and ST_IsValid in PostGIS
       │
       ▼
 [mapc.zoning_atlas]  ← AWS RDS PostGIS
 [mapc.etl_runs]      ← one row per pipeline run
 [mapc.qaqc_log]      ← one row per QA/QC check per run
```

### Load Strategy: Full Refresh vs. Incremental

This pipeline uses a **full TRUNCATE + reload** on every run. For the Zoning Atlas (1,775 polygons, ~2 min end-to-end), this is the correct choice: the dataset is small, MAPC publishes it as a complete snapshot rather than a changelog, and a full reload guarantees the PostGIS table exactly mirrors the source with no risk of orphaned or stale districts from boundary changes.

For larger datasets where a full reload is impractical — MAPC's Parcel Database (~1.6M parcels statewide) being the obvious example — the pipeline would be modified for incremental updates:

- Add a **surrogate key** (`muni_id + zo_code`) and use `INSERT ... ON CONFLICT DO UPDATE` (upsert) instead of TRUNCATE
- Track a **`last_modified` watermark** in `etl_runs` and filter the source extract to only changed records
- Run a **delete pass** after upsert to remove districts that are present in PostGIS but absent from the new source snapshot (soft-deleted or rezoned out)
- The QA/QC layer would gain a **row-delta check** — flagging any run where net change exceeds a threshold (e.g. ±5%) as a signal of a bad extract rather than genuine rezoning activity

---

## Setup

### Prerequisites

- Python 3.11+
- A PostgreSQL + PostGIS database (see [AWS RDS setup](#aws-rds-setup) below)
- MAPC Zoning Atlas shapefile downloaded from [MAPC SharePoint](https://www.mapc.org/planning101/zoning-atlas/) and placed in `data/raw/`

### 1. Install dependencies

```bash
git clone https://github.com/your-username/mass-tiger.git
cd mass-tiger
pip install -r requirements.txt
```

### 2. Configure environment

Create a `.env` file in the project root (never committed — see `.gitignore`):

```env
DB_HOST=your-rds-endpoint.rds.amazonaws.com
DB_PORT=5432
DB_NAME=gisdb
DB_USER=postgres
DB_PASSWORD=your-password
```

### 3. Place source data

Download the MAPC Zoning Atlas shapefile and place it in `data/raw/`:

```
data/raw/mapc_zoning_atlas.zip   ← zip of shapefile components, OR
data/raw/mapc_zoning_atlas.shp   ← individual shapefile
```

The extract step auto-detects and handles both `.zip` and `.shp` formats.

### 4. Run the pipeline

```bash
# Full run — extract, transform, QA/QC, load to PostGIS
python pipeline.py

# Dry run — validates everything but skips the DB write
DRY_RUN=true python pipeline.py
```

On first run, the pipeline automatically creates the `mapc` schema and all three tables (`zoning_atlas`, `etl_runs`, `qaqc_log`) in PostGIS — no manual SQL required.

### 5. Run tests

```bash
pytest tests/ -v
```

---

## AWS RDS Setup

The target database is a **PostgreSQL 16 + PostGIS** instance on AWS RDS Free Tier.

**Instance specs used in this project:**
- Engine: PostgreSQL 16
- Instance class: `db.t4g.micro` (Free Tier)
- Storage: 20 GB gp2
- Public access: Enabled (port 5432 locked to specific IP via Security Group)

**One-time setup steps:**
1. Create the RDS instance with PostgreSQL
2. Create a database named `gisdb`
3. Enable the PostGIS extension — connect to `gisdb` and run:
   ```sql
   CREATE EXTENSION IF NOT EXISTS postgis;
   ```
4. Verify:
   ```sql
   SELECT PostGIS_version();
   ```

After that, the pipeline handles all schema and table creation automatically.

---

## QA/QC

The pipeline runs **88 automated checks** at two stages:

| Stage | What's checked |
|---|---|
| Pre-load | Feature count ≥ 1,000 · All geometries valid · CRS = EPSG:26986 · No features < 100 m² · Null rate per column ≤ 10% |
| Post-load | PostGIS row count matches in-memory count · `ST_IsValid` across entire loaded table |

**Typical run result:** 65/88 PASS, 23 WARN (expected — sparse notes/spec fields in source data), 0 FAIL.

The 23 warnings are known data characteristics, not pipeline errors:
- `mnls_oven`, `mxdu_oven`: 100% null — these override fields are unused in the source
- `mf_notes`, `plc_notes`, `mxdu_notes`: 93–98% null — notes are sparse by design
- `dupac_spec`, `far_oven`, `mxht_spec`: sparse specification fields

All QA/QC results are persisted to `mapc.qaqc_log` for trend analysis across runs.

---

## Database Schema

Three tables are created in the `mapc` schema:

### `mapc.zoning_atlas`
The main spatial table. 1,775 rows, one per zoning district.

| Column | Type | Description |
|---|---|---|
| `geometry` | geometry(GEOMETRYZ, 26986) | Polygon geometry in EPSG:26986 |
| `muni` | text | Municipality name |
| `muni_id` | float | Municipality numeric ID |
| `zo_code` | text | Zoning district code |
| `zo_name` | text | Zoning district full name |
| `zo_usety` | bigint | Use type code |
| `zo_abbr` | text | Abbreviation |
| `minlotsize` | float | Minimum lot size (sq ft) |
| `maxheight` | float | Maximum building height (ft) |
| `maxflrs` | float | Maximum number of floors |
| `far` | float | Floor area ratio |
| `dupac` | float | Dwelling units per acre |
| `area_m2` | float | Polygon area in square metres (derived) |
| `area_acres` | float | Polygon area in acres (derived) |
| `centroid_x` | float | Centroid X coordinate, EPSG:26986 (derived) |
| `centroid_y` | float | Centroid Y coordinate, EPSG:26986 (derived) |
| `etl_run_id` | text | UUID of the pipeline run that loaded this row |
| `etl_loaded_at` | text | UTC timestamp of load |

### `mapc.etl_runs`
One row per pipeline execution.

| Column | Type | Description |
|---|---|---|
| `run_id` | text (PK) | UUID |
| `table_name` | text | Target table loaded |
| `row_count` | integer | Features loaded |
| `completed_at` | timestamptz | Run completion time |

### `mapc.qaqc_log`
One row per QA/QC check per run.

| Column | Type | Description |
|---|---|---|
| `run_id` | text | Links to `etl_runs` |
| `check_name` | text | Check identifier |
| `status` | text | PASS / WARN / FAIL |
| `value` | text | Observed value |
| `threshold` | text | Expected threshold |
| `message` | text | Human-readable result |
| `checked_at` | timestamptz | When the check ran |

---

## Example Queries

**All zoning districts in Boston:**
```sql
SELECT zo_code, zo_name, zo_usety, area_acres
FROM mapc.zoning_atlas
WHERE muni = 'Boston'
ORDER BY area_acres DESC;
```

**Municipalities with the most zoning districts:**
```sql
SELECT muni, COUNT(*) AS district_count
FROM mapc.zoning_atlas
GROUP BY muni
ORDER BY district_count DESC
LIMIT 10;
```

**Export to GeoJSON (web-ready WGS84):**
```sql
SELECT
    zo_code,
    zo_name,
    muni,
    area_acres,
    ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
FROM mapc.zoning_atlas
WHERE muni = 'Cambridge';
```

**QA/QC history for a specific run:**
```sql
SELECT check_name, status, value, threshold, message
FROM mapc.qaqc_log
WHERE run_id = 'your-run-id-here'
ORDER BY status, check_name;
```

**ETL run history:**
```sql
SELECT run_id, row_count, completed_at
FROM mapc.etl_runs
ORDER BY completed_at DESC;
```

---

## Challenges & Technical Decisions

### 1. Source data API no longer available
The MAPC originally provided a public GeoJSON API endpoint. By the time this pipeline was built, that endpoint returned HTTP 500 errors. The solution was to pivot to loading from the downloadable shapefile (via MAPC SharePoint), with auto-detection of `.zip`, `.shp`, and `.geojson` formats.

### 2. Unexpected source CRS
The shapefile uses **NAD83 / Massachusetts Mainland (ftUS)** — not WGS84 as documented. The transform step detects any non-EPSG:4326 CRS, logs a warning, and reprojects regardless. Storage is in EPSG:26986 (metric variant) for accurate area calculations.

### 3. 62 invalid geometries in source data
The raw shapefile contains 62 self-intersecting or otherwise invalid polygons. These are automatically repaired using Shapely's `make_valid()` during the transform step. All 62 were successfully repaired with zero data loss.

### 4. PostGIS geometry type registration
Using `GeoDataFrame.to_postgis(if_exists="replace")` drops and recreates the table using generic SQLAlchemy DDL, which doesn't register the PostGIS geometry type correctly, producing `type "geometry" does not exist` errors. The solution: check if the table exists first — if yes, `TRUNCATE` then `if_exists="append"`; if no, let `to_postgis` create it fresh (PostGIS extension provides the geometry type at create time).

### 5. PostGIS extension is per-database
PostGIS must be installed separately in each database, not just globally on the RDS instance. Running `CREATE EXTENSION postgis` on the default `postgres` database does not make the geometry type available in `gisdb`. This is a common AWS RDS gotcha.

### 6. PowerShell environment variable scoping
On Windows, setting `$env:DRY_RUN="true"` in PowerShell persists for the entire session and overrides `.env` file values. Use `Remove-Item Env:DRY_RUN` to clear it before running the pipeline for real.

---

## Viewing Results in QGIS

Connect QGIS directly to the PostGIS database:

1. **Layer → Add Layer → Add PostGIS Layers**
2. Create a new connection with your RDS credentials
3. Connect → expand `mapc` schema → select `zoning_atlas` → Add
4. Style by `zo_usety` to colour-code residential / commercial / industrial zones

---

## Documentation

| Document | Description |
|---|---|
| [docs/schema.md](docs/schema.md) | Full table and column reference, useful queries |
| [docs/METADATA_STANDARDS.md](docs/METADATA_STANDARDS.md) | FGDC CSDGM-aligned metadata policy — required fields, lineage, QA/QC obligations, update frequency, attribution |

---

## Data Source

**MAPC Greater Boston Zoning Atlas**
Metropolitan Area Planning Council (MAPC)
Coverage: 101 municipalities, Greater Boston region
Features: 1,775 zoning district polygons
License: [MAPC Open Data License](https://www.mapc.org/resource-library/mapc-open-data-license/)
