# Database Schema Documentation

**Database:** AWS RDS PostgreSQL + PostGIS  
**Schema:** `mapc`  
**CRS (storage):** EPSG:26986 — NAD83 / Massachusetts Mainland (metres)  
**CRS (web output):** EPSG:4326 — use `ST_Transform(geometry, 4326)` in queries

---

## Tables

### `mapc.zoning_atlas`

Primary data table. One row per zoning district polygon in the MAPC Zoning Atlas (101 Greater Boston municipalities).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | SERIAL | NO | Surrogate primary key (auto-generated) |
| `muni_id` | TEXT | YES | Municipality numeric identifier |
| `muni_name` | TEXT | YES | Municipality name (e.g. `Cambridge`) |
| `zo_id` | TEXT | YES | Unique zoning district ID |
| `zo_abbr` | TEXT | YES | Zoning abbreviation (e.g. `R1`, `B2`, `IND`) |
| `zo_name` | TEXT | YES | Full district name |
| `zo_usety` | TEXT | YES | Use type category: `Residential`, `Commercial`, `Industrial`, `Mixed`, `Other` |
| `multifam` | TEXT | YES | Multifamily housing permitted in this district |
| `aff_req` | TEXT | YES | Affordable housing requirement |
| `by_right` | TEXT | YES | Multifamily allowed by-right (vs. special permit) |
| `area_m2` | NUMERIC(18,2) | YES | District area in square metres (computed in EPSG:26986) |
| `area_acres` | NUMERIC(12,4) | YES | District area in acres |
| `centroid_x` | NUMERIC(12,2) | YES | Centroid easting (EPSG:26986) |
| `centroid_y` | NUMERIC(12,2) | YES | Centroid northing (EPSG:26986) |
| `etl_run_id` | TEXT | NO | UUID of the pipeline run that loaded this row |
| `etl_loaded_at` | TIMESTAMPTZ | YES | UTC timestamp when this row was loaded |
| `geometry` | GEOMETRY(MultiPolygon, 26986) | YES | District boundary in EPSG:26986 |

**Indexes:**
- `idx_zoning_atlas_geom` — GIST on `geometry` (spatial queries)
- `idx_zoning_atlas_muni_id` — B-tree on `muni_id`
- `idx_zoning_atlas_zo_usety` — B-tree on `zo_usety`
- `idx_zoning_atlas_etl_run` — B-tree on `etl_run_id`

---

### `mapc.etl_runs`

Audit table — one row per pipeline execution.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | TEXT PK | UUID generated at pipeline start |
| `table_name` | TEXT | Fully-qualified target table (`mapc.zoning_atlas`) |
| `row_count` | INTEGER | Number of rows loaded in this run |
| `started_at` | TIMESTAMPTZ | When the run started (default: `now()`) |
| `completed_at` | TIMESTAMPTZ | When load finished |

---

### `mapc.qaqc_log`

QA/QC check results — one row per check per run.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment |
| `run_id` | TEXT FK → `etl_runs.run_id` | Pipeline run this check belongs to |
| `check_name` | TEXT | Check identifier (e.g. `feature_count`, `null_rate_muni_name`) |
| `passed` | BOOLEAN | Whether the check passed |
| `value` | TEXT | Observed value (cast to text) |
| `threshold` | TEXT | Acceptable limit evaluated against |
| `critical` | BOOLEAN | If `true`, failure aborts the pipeline |
| `note` | TEXT | Human-readable explanation |
| `checked_at` | TIMESTAMPTZ | When this check ran |

---

## Useful Queries

```sql
-- Most recent ETL run summary
SELECT run_id, row_count, completed_at
FROM mapc.etl_runs
ORDER BY completed_at DESC
LIMIT 1;

-- All failed QA checks for the latest run
SELECT q.check_name, q.value, q.threshold, q.note
FROM mapc.qaqc_log q
JOIN mapc.etl_runs r ON r.run_id = q.run_id
WHERE NOT q.passed
ORDER BY r.completed_at DESC, q.critical DESC;

-- Zoning districts by use type
SELECT zo_usety, COUNT(*), ROUND(SUM(area_acres)::numeric, 0) AS total_acres
FROM mapc.zoning_atlas
GROUP BY zo_usety
ORDER BY total_acres DESC;

-- Multifamily-allowed zones within Cambridge
SELECT zo_id, zo_abbr, zo_name, area_acres
FROM mapc.zoning_atlas
WHERE muni_name = 'Cambridge'
  AND multifam = 'Yes'
ORDER BY area_acres DESC;

-- Export to GeoJSON (WGS84) for web map use
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(ST_AsGeoJSON(t.*)::json)
)
FROM (
    SELECT zo_id, muni_name, zo_abbr, zo_usety, area_acres,
           ST_Transform(geometry, 4326) AS geometry
    FROM mapc.zoning_atlas
    WHERE muni_name = 'Somerville'
) t;
```
