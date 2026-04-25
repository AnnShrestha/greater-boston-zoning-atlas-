# Metadata Standards — MAPC Zoning Atlas ETL Pipeline

**Version:** 1.0  
**Effective date:** 2026-04-25  
**Maintainer:** Ann Shrestha  
**Review cycle:** Annually, or upon any major source data revision

---

## Purpose

This document defines the metadata standards governing the MAPC Zoning Atlas spatial dataset as managed by this ETL pipeline. It establishes required metadata fields, alignment with the Federal Geographic Data Committee (FGDC) Content Standard for Digital Geospatial Metadata (CSDGM), update frequency conventions, and data quality obligations.

These standards exist to ensure that every version of this dataset is **discoverable, interpretable, reproducible, and trustworthy** — by this team, by downstream consumers, and by the public.

---

## Applicable Standards

| Standard | Applicability |
|---|---|
| [FGDC CSDGM](https://www.fgdc.gov/metadata/csdgm-standard) | Primary metadata framework (federal baseline) |
| [ISO 19115-1:2014](https://www.iso.org/standard/53798.html) | Supplementary; used for lineage and data quality sections |
| [MAPC Open Data License](https://www.mapc.org/resource-library/mapc-open-data-license/) | Governs distribution and attribution requirements |
| EPSG:26986 | Mandated storage CRS (NAD83 / Massachusetts Mainland, metres) |

---

## Required Metadata Fields

The following fields are **mandatory** for any published version of this dataset. A dataset version may not be marked `PUBLISHED` until all required fields are populated and verified.

### 1. Identification

| Field | CSDGM Element | Required | Current Value |
|---|---|---|---|
| Dataset title | `idinfo/citation/citeinfo/title` | YES | MAPC Greater Boston Zoning Atlas |
| Abstract | `idinfo/descript/abstract` | YES | See §Abstract below |
| Purpose | `idinfo/descript/purpose` | YES | See §Purpose below |
| Originator | `idinfo/citation/citeinfo/origin` | YES | Metropolitan Area Planning Council (MAPC) |
| Publication date | `idinfo/citation/citeinfo/pubdate` | YES | Per release (see §Update Frequency) |
| Geospatial data presentation form | `idinfo/citation/citeinfo/geoform` | YES | vector digital data |
| Access constraints | `idinfo/accconst` | YES | None (open data) |
| Use constraints | `idinfo/useconst` | YES | Credit MAPC; see MAPC Open Data License |

**Abstract (standard text):**
> This dataset contains 1,775 zoning district polygons covering 101 municipalities in the Greater Boston metropolitan region. It was produced by the Metropolitan Area Planning Council (MAPC) as part of the Greater Boston Zoning Atlas project. The data describes land use regulations including use types, minimum lot sizes, maximum height and floor limits, density allowances, and floor area ratios. This pipeline version has been reprojected from NAD83 / Massachusetts Mainland (ftUS) to EPSG:26986 (metric) and validated for geometry integrity.

**Purpose (standard text):**
> To provide a clean, analysis-ready, cloud-hosted PostGIS version of the MAPC Zoning Atlas suitable for spatial analysis, housing policy research, and regional planning applications.

---

### 2. Spatial Reference

| Field | CSDGM Element | Required | Value |
|---|---|---|---|
| Horizontal coordinate system | `spref/horizsys` | YES | NAD83 / Massachusetts Mainland |
| EPSG code (storage) | — | YES | EPSG:26986 |
| EPSG code (web/export) | — | YES | EPSG:4326 (via `ST_Transform` at query time) |
| Horizontal datum | `spref/horizsys/geodetic/horizdn` | YES | North American Datum of 1983 |
| Ellipsoid | `spref/horizsys/geodetic/ellips` | YES | Geodetic Reference System 80 |
| Units (storage) | `spref/horizsys/planar/planci/plandu` | YES | metres |
| Vertical coordinate system | `spref/vertdef` | NO | Not applicable |

**Rationale for EPSG:26986:** Massachusetts Mainland (metric) is the standard CRS for Massachusetts state agency GIS data. It provides accurate area and distance calculations in metres, consistent with MassGIS conventions. The source data uses the ftUS variant of this projection; this pipeline reprojects to the metric variant for compatibility with SI-unit analysis workflows.

---

### 3. Data Quality

| Field | CSDGM Element | Required | Value |
|---|---|---|---|
| Logical consistency report | `dataqual/logic` | YES | See §QA/QC Policy |
| Completeness report | `dataqual/complete` | YES | See §Known Data Gaps |
| Positional accuracy | `dataqual/posacc` | YES | Inherited from source; not independently assessed |
| Attribute accuracy | `dataqual/attracc` | YES | Validated via automated null-rate checks per column |
| Source citation | `dataqual/lineage/srcinfo` | YES | MAPC Zoning Atlas shapefile (see §Lineage) |

---

### 4. Lineage

Every pipeline run produces a full lineage record traceable through the `mapc.etl_runs` and `mapc.qaqc_log` audit tables.

| Lineage element | CSDGM Element | Value |
|---|---|---|
| Source dataset | `dataqual/lineage/srcinfo/srccite` | MAPC Zoning Atlas shapefile |
| Source scale | `dataqual/lineage/srcinfo/srcscale` | 1:5,000 (estimated, parcel-level) |
| Source CRS | — | NAD83 / Massachusetts Mainland (ftUS), EPSG:2249 |
| Source format | — | ESRI Shapefile (.shp) |
| Process steps | `dataqual/lineage/procstep` | See §Process Steps |

**Process Steps (in order):**

1. **Extract** — Shapefile loaded from local disk; zip extraction if required; CRS assigned if missing
2. **Geometry repair** — 62 invalid geometries repaired using Shapely `make_valid()` (self-intersections resolved; zero features dropped)
3. **Reprojection** — Geometries reprojected from NAD83 MA Mainland (ftUS) to EPSG:26986 using pyproj
4. **Column standardisation** — All column names lowercased; spaces and hyphens replaced with underscores
5. **Derived field computation** — `area_m2`, `area_acres`, `centroid_x`, `centroid_y` calculated in EPSG:26986
6. **ETL provenance stamping** — `etl_run_id` (UUID) and `etl_loaded_at` (UTC timestamp) appended to every row
7. **Pre-load QA/QC** — 88 automated checks executed (see §QA/QC Policy)
8. **Load** — Data written to `mapc.zoning_atlas` via TRUNCATE + append; spatial index rebuilt; ANALYZE run
9. **Post-load QA/QC** — Row count and `ST_IsValid` verified against PostGIS

---

### 5. Distribution

| Field | CSDGM Element | Value |
|---|---|---|
| Distributor | `distinfo/distrib` | Ann Shrestha (pipeline maintainer) |
| Distribution format | `distinfo/stdorder/digform` | PostGIS table; GeoJSON via `ST_AsGeoJSON` |
| Network access | — | AWS RDS endpoint (credentials required) |
| Fees | `distinfo/distrib/stdorder/fees` | None |
| Ordering instructions | — | See repository README |

---

## QA/QC Policy

All data loaded through this pipeline must pass the following automated checks before the load is committed. **Critical failures abort the pipeline.** Warnings are logged but do not block the load.

### Critical Checks (pipeline aborts on failure)

| Check | Threshold | Rationale |
|---|---|---|
| Feature count | ≥ 1,000 | The full dataset has 1,775 features; fewer than 1,000 signals a catastrophic extract failure |
| Geometry validity rate | ≤ 1% invalid | Invalid geometries corrupt spatial queries and area calculations |
| CRS at load time | Must be EPSG:26986 | Prevents loading data in wrong units or projection |

### Warning Checks (logged, load proceeds)

| Check | Threshold | Notes |
|---|---|---|
| Null rate per column | ≤ 10% | 23 fields exceed this threshold in source data — see §Known Data Gaps |
| Small features | Area ≥ 100 m² | Features below this threshold are flagged as potential slivers |

### Post-Load Verification (always run)

| Check | Method |
|---|---|
| Row count match | `SELECT COUNT(*)` from PostGIS matches in-memory count |
| PostGIS geometry validity | `ST_IsValid(geometry)` across entire loaded table |

All results are persisted to `mapc.qaqc_log` with timestamps, enabling trend analysis across pipeline runs.

---

## Known Data Gaps

The following columns have null rates exceeding 10% in the MAPC source data. These are **source data characteristics**, not pipeline defects. They are documented here for downstream consumers.

| Column | Null Rate | Explanation |
|---|---|---|
| `mnls_oven` | 100% | Override notes field — unused in current MAPC dataset version |
| `mxdu_oven` | 100% | Override notes field — unused in current MAPC dataset version |
| `mf_notes` | 98% | Multifamily notes — only populated for districts with complex permit conditions |
| `plc_notes` | 98% | Percent lot coverage notes — sparse by design |
| `dupac_spec` | 99% | Density specification — only for districts with explicit du/ac rules |
| `far_oven` | 100% | FAR override notes — unused in current dataset version |
| `mxht_spec` | 82% | Height specification — only for districts with non-standard height rules |
| `mnls_spec` | 62% | Lot size specification details — partially populated |
| `mxdu_spec` | 66% | Max density specification — partially populated |
| `zo_usede` | 39% | Use type description — not all districts carry a description |
| `createdby` | 33% | Data entry attribution — historical records lack this field |

Consumers performing completeness analysis should use `_eff` (effective value) columns rather than `_spec` or `_oven` columns where available, as effective values represent the calculated regulatory value regardless of how it was specified.

---

## Update Frequency

| Condition | Action | Timeline |
|---|---|---|
| MAPC publishes a new Zoning Atlas version | Full pipeline re-run; new `etl_runs` record created | Within 30 days of MAPC release |
| Any municipality updates its zoning bylaws | Downstream consumers notified via repository release notes | As discovered |
| Pipeline code change affecting geometry or attributes | Re-run with new `run_id`; prior run record retained in `etl_runs` | Before merging to `main` |
| QA/QC thresholds revised | This document updated; changelog entry added | Same commit as threshold change |

**Current dataset currency:** Reflects MAPC Zoning Atlas as downloaded April 2026. MAPC updates this dataset on an irregular schedule as municipalities submit revised zoning bylaws.

---

## Attribution Requirements

Any product, publication, or analysis using this dataset must include the following credit:

> Zoning data sourced from the MAPC Greater Boston Zoning Atlas, Metropolitan Area Planning Council (mapc.org). Processed and hosted by Ann Shrestha.

For academic or policy publications, the full citation is:

> Metropolitan Area Planning Council (MAPC). *Greater Boston Zoning Atlas*. Boston, MA: MAPC, 2024. Accessed April 2026. Processed via ETL pipeline: github.com/AnnShrestha/greater-boston-zoning-atlas-.

---

## Metadata Currency and Review

This metadata document must be reviewed and updated:
- **Annually** on the anniversary of the effective date
- **Immediately** upon any change to source data, CRS decisions, QA thresholds, or distribution method

The metadata version history is maintained via git commit history on this file. Any substantive change must increment the version number in the document header.

| Version | Date | Author | Changes |
|---|---|---|---|
| 1.0 | 2026-04-25 | Ann Shrestha | Initial release |
