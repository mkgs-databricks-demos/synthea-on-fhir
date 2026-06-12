# PROJECT_MEMORY — fhir_declarative_pipeline

Canonical long-term memory for the FHIR Declarative Pipeline bundle.
Read this at the start of every session before making changes.
Last updated: 2026-06-12 (Silver v3 — identity/reference/codes/temporal extraction + full VARIANT; clinical mart ready)

---

## Bundle Identity

- **Bundle name**: `fhir_declarative_pipeline`
- **Bundle root**: `/Workspace/Users/matthew.giglia@databricks.com/fhirETL/fhirETL/fhir_declarative_pipeline`
- **Local path**: `/Users/matthew.giglia@databricks.com/fhirETL/fhirETL/fhir_declarative_pipeline`
- **Targets**:
  - `dev` — fevm-hedis.cloud.databricks.com, mode: development, catalog: `ncqai`, schema: `dev_matthew_giglia_fhir`
  - `hedis` — fevm-hedis.cloud.databricks.com, mode: production, catalog: `ncqai`, schema: `fhir`
  - `hls_fde` — fevm-hls-fde.cloud.databricks.com, mode: production, catalog: `hls_fde`, schema: `fhir`

---

## Deployed Resources (dev target)

### Pipelines

| Resource key | Display name | Pipeline ID |
|---|---|---|
| `fhir_bundle_mover_etl` | [dev matthew_giglia] Streaming FHIR Bundle Mover | `e3e6b853-249a-45e9-ac29-faeec018fdf2` |
| `fhir_bundle_ingestion_etl` | [dev matthew_giglia] FHIR Bundle Resource Parsing ETL | `4782f58f-2b71-4be0-a702-e112eca104c2` |
| `fhir_resource_silver_etl` | [dev matthew_giglia] FHIR Resource Silver ETL | `aace6745-bdbd-4568-964b-5e78b40ac11f` |

### Jobs

| Resource key | Display name | Job ID |
|---|---|---|
| `fhir_bundle_mover_job` | [dev matthew_giglia] FHIR Bundle Mover | `449274568717351` |
| `fhir_etl_orchestration_job` | [dev matthew_giglia] FHIR ETL Orchestration | `447487171209811` |
| `synthetic_fhir_etl_orchestration_job` | [dev matthew_giglia] Synthetic FHIR ETL Orchestration | `868213351234820` |

### Event Logs (all in `ncqai.dev_matthew_giglia_fhir`)

- `fhir_bundle_mover_event_log`
- `fhir_bundle_ingestion_etl_event_log` (renamed from `fhir_declarative_pipeline_etl_event_log` -- old table is an orphan, safe to drop)
- `fhir_resource_silver_etl_event_log`

---

## Pipeline Architecture

### 1. fhir_bundle_mover_etl
- Source: `/Volumes/${var.source_catalog}/synthea_data_gen/synthetic_files_raw/output/fhir/`
- Destination: `/Volumes/${var.catalog}/${var.schema_resolved}/${var.landing_volume}/`
- Auto Loader binaryFile format; UDF distributes writes across cluster via FUSE mount
- Output: `file_tracker` streaming table (source path, dest path, file size, status)
- Skips files already present at destination; deduplication via Auto Loader checkpoint

### 2. fhir_bundle_ingestion_etl
- Source: landing volume (output of mover)
- Six output tables:
  - `fhir_bronze` -- raw text ingestion via Auto Loader
  - `fhir_bronze_variant` -- VARIANT-parsed JSON
  - `bundle_meta` -- bundle-level metadata
  - `fhir_resources_variant` -- **one row per resource with full resource VARIANT** (universal staging)
  - `fhir_resources` -- exploded key-value rows (2.1B rows across 24 resource types, retained for schema discovery)
  - `fhir_resource_schemas` -- one row per resourceType/column with inferred VARIANT and struct schemas (288 rows)
- Config keys: `pipeline.landing_volume_path`, `pipeline.catalog_use`, `pipeline.schema_use`
- `fhir_resources_variant` is the primary source for the silver pipeline AND future FHIR server loading

### 3. fhir_resource_silver_etl (v3 — FULLY STREAMING, clinical mart ready)
- Reads `fhir_resource_schemas` to discover resource types + reference fields at planning time
- Per resource type, TWO objects:
  - `{type}_extract` (temporary view) -- `STREAM(fhir_resources_variant)` filtered by
    resourceType, extracts references/identifiers/codes/temporal via `try_variant_get`.
    Pure append-mode streaming, no aggregation, no shuffle.
  - `{type}` (streaming table) -- Auto CDC Type 1 upserts keyed on `{type}_uuid`,
    sequenced by `ingest_time`
- **Uniform 10-column schema** (all 24 resource types):
  `{type}_uuid PK, bundle_uuid, {type}_url, references, identifiers, codes, status,
  clinical_event_effective_start, clinical_event_effective_end, resource VARIANT`
- All extraction uses `try_variant_get` (NULL-safe, no INVALID_VARIANT_CAST)
- Array iteration: `SEQUENCE(0, GREATEST(COALESCE(size(...), 0) - 1, 0))` (avoids
  negative indices from Spark's SEQUENCE step inference)
- Config keys: `pipeline.catalog_use`, `pipeline.schema_use`
- Must run AFTER ingestion ETL on first deployment (two-pass architecture)
- Incremental runs with no new data: verified clean (2026-06-12)
- **ELIMINATED**: typed columns, PIVOT, MV, CDF bridge, `_raw` tables, schema_as_struct

---

## Job Architecture

### fhir_bundle_mover_job
- Trigger: file arrival on source volume (`synthea_data_gen/synthetic_files_raw/output/fhir/`)
- Parameter: `full_refresh` (default `"false"`)
- Pattern: `condition_task` branches to `incremental_refresh` (false) or `full_refresh_pipeline` (true)

### fhir_etl_orchestration_job
- Trigger: file arrival on landing volume -- CURRENTLY BROKEN due to missing
  `s3:GetBucketNotification` / `s3:PutBucketNotification` on the managed storage
  IAM role `ncqai-ext-role-049629455384-hag0lv` for bucket
  `ncqai-ext-s3-049629455384-hag0lv`. Needs workspace/metastore admin to fix.
  The landing volume is a MANAGED volume; there is no named external location for it.
- Parameter: `full_refresh` (default `"false"`)
- Pattern: same condition_task branch as mover job
- Tasks: `incremental_ingestion_etl` -> `incremental_silver_etl` (or full_refresh equivalents)

### synthetic_fhir_etl_orchestration_job
- No trigger (manual / API-triggered)
- Linear 3-task sequence: `run_synthea` -> `run_fhir_bundle_mover` -> `run_fhir_etl_orchestration`
- `run_synthea` calls `synthea_on_dbx_job` (resolved via `synthea_job_id` lookup) with:
  `catalog_use=${var.catalog}`, `inject_bad_data="false"`, `move_csv_to_landing="false"`
- Always incremental; for full refresh run individual jobs directly

---

## Key Technical Decisions

### Standard table properties (all tables, all pipelines)

    delta.enableChangeDataFeed:          true
    delta.enableDeletionVectors:         true
    delta.enableRowTracking:             true
    delta.autoOptimize.optimizeWrite:    true
    delta.autoOptimize.autoCompact:      true
    delta.enableVariantShredding:        true
    pipelines.channel:                   PREVIEW
    delta.feature.variantType-preview:   supported
    pipelines.reset.allowed:             true

Applied to all bronze tables (including `fhir_resources_variant`) and all 24 silver
CDC target tables. Verified correct as of 2026-06-11.

### Fully streaming silver architecture (PIVOT eliminated)

The silver pipeline was completely rewritten to eliminate all materialized views,
PIVOT operations, CDF bridging, and the associated streaming compatibility issues.

**Previous architecture (DEPRECATED):**

    fhir_resources (key-value EAV, 2.1B rows)
      -> {type}_raw (PIVOT + first() = MV/live table, Complete output mode)
      -> {type}_cdc_source (CDF bridge to get append-only stream)
      -> {type} (Auto CDC Type 1)

Problems solved:
- PIVOT `first()` forced Complete output mode, breaking downstream streaming
- `skipChangeCommits` did not cover streaming Complete-mode writes
- Enzyme incrementalization of PIVOT not guaranteed (risk of 2.1B row recompute)
- EOB PIVOT OOM from large keys (`item` ~90 GB, `contained` ~8 GB) in shuffle
- 24 orphaned `_raw` tables published to schema as side effect

**New architecture (CURRENT):**

    fhir_resources_variant (one row per resource, full VARIANT)
      -> {type}_extract (temporary view: streaming filter + VARIANT path extraction + CAST)
      -> {type} (Auto CDC Type 1, sequenced by ingest_time)

Key properties:
- Fully streaming end-to-end (no batch/MV intermediary)
- No aggregation, no shuffle -- VARIANT path extraction (`resource:fieldName`) is per-row
- No OOM risk -- even large fields (EOB.item, EOB.contained) are just column projections
- `_EXTRACT_SKIP_COLUMNS` is empty by default (all columns now includable)
- No DELTA_SOURCE_TABLE_IGNORE_CHANGES errors (no Complete mode, no file rewrites in path)
- No dependency on Enzyme incrementalization behavior
- Simpler architecture: 2 objects per type (was 3)

### fhir_resources_variant as universal staging layer

`fhir_resources_variant` stores one row per FHIR resource with the complete resource
document preserved as VARIANT. It serves three downstream purposes:

1. **Silver analytics** -- streaming filter by resourceType + VARIANT path extraction
2. **FHIR server loading** -- NDJSON export for HAPI `$import`, or direct VARIANT->JSONB
   for Aidbox on Databricks Lakebase
3. **Ad-hoc queries** -- `SELECT resource:fieldName FROM fhir_resources_variant WHERE ...`

This aligns with how FHIR servers store data (HAPI JPA: resource as CLOB/blob;
Aidbox: resource as JSONB column). The document-per-row model is the natural
staging format for both analytical and transactional FHIR workloads.

### fhir_resources retained for schema discovery only

`fhir_resources` (key-value EAV, 2.1B rows) is retained because
`fhir_resource_schemas` depends on it (`schema_of_variant_agg(value) GROUP BY
resourceType, key`). It is NOT in the streaming path for silver tables.
Future consideration: derive schemas directly from `fhir_resources_variant`
using `schema_of_variant(resource)` to eliminate the EAV table entirely.

Note: `num_output_rows` is NULL for all Auto CDC flows -- expected and documented behavior.
Only `num_upserted_rows` and `num_deleted_rows` are captured for CDC queries.

### synthea_job_id variable lookup

`databricks.yml` resolves the synthea job ID by display name at validate/deploy time:

    synthea_job_id:
      lookup:
        job: "synthea_on_dbx_job"

Requires the synthea bundle deployed in production mode (dev mode prefixes the
name and breaks the lookup). Source repo:
https://github.com/mkgs-databricks-demos/synthea-on-fhir/tree/main/synthea_on_dbx

---

## Silver Table State (as of 2026-06-11, dev target — PRE-V3 REWRITE)

Note: Row counts below are from the v2 typed-column architecture. After deploying
v3 (reference extraction + VARIANT), a full refresh is needed. Row counts should
remain the same (one row per resource per type), but the schema changes to:
`{type}_uuid PK, bundle_uuid, {type}_url, references ARRAY<STRUCT<...>>, resource VARIANT`

Source: 132,313 FHIR bundles, 2,111,798,474 rows in fhir_resources.

| Table | Rows |
|---|---|
| allergyintolerance | 123,375 |
| careplan | 439,628 |
| careteam | 439,628 |
| claim | 14,585,498 |
| condition | 4,831,327 |
| device | 762,977 |
| diagnosticreport | 15,623,128 |
| documentreference | 7,825,053 |
| encounter | 7,825,053 |
| explanationofbenefit | 14,585,498 |
| imagingstudy | 624,700 |
| immunization | 1,905,825 |
| location | 3,590 |
| medication | 2,375,620 |
| medicationadministration | 2,375,620 |
| medicationrequest | 6,760,445 |
| observation | 69,356,393 |
| organization | 3,584 |
| patient | 132,301 |
| practitioner | 3,584 |
| practitionerrole | 3,584 |
| procedure | 21,714,545 |
| provenance | 132,301 |
| supplydelivery | 3,482,717 |
| **TOTAL** | **175,915,974** |

---

## Known Issues / TODOs

- **File arrival trigger broken on fhir_etl_orchestration_job**: missing
  `s3:GetBucketNotification` / `s3:PutBucketNotification` on IAM role
  `ncqai-ext-role-049629455384-hag0lv` for bucket `ncqai-ext-s3-049629455384-hag0lv`.
  Needs metastore admin. Landing volume is UC MANAGED -- no named external location.

- **Orphaned event log table**: `ncqai.dev_matthew_giglia_fhir.fhir_declarative_pipeline_etl_event_log`
  no longer written to. Safe to drop.

- **Orphaned `_raw` tables (24)**: The previous silver architecture published 24
  `{type}_raw` tables to the schema. These are no longer produced by the new pipeline.
  After successful full refresh of the new silver pipeline, drop all `*_raw` tables
  from `ncqai.dev_matthew_giglia_fhir`.

- **synthetic_fhir_etl_orchestration_job missing permissions block**: omitted
  from `synthetic_fhir_etl_orchestration.job.yml` during write. Add manually
  to match the other job files.

- **mkgs-prod service principal**: applicationId `47c0365e-b1af-429c-b56d-07cfb18b5dc7`
  needs `CAN_EDIT` added to `hedis.permissions` block manually.

- ~~**Silver incremental runs fail with DELTA_SOURCE_TABLE_IGNORE_CHANGES**~~:
  RESOLVED. The fully-streaming rewrite eliminates PIVOT, Complete output mode,
  and all DELTA_SOURCE_TABLE_IGNORE_CHANGES errors. Branch `mg-silver-mv-cdf`
  is superseded by the VARIANT path extraction approach (committed to main).

- ~~**Silver incremental runs on unchanged data**~~: VERIFIED 2026-06-12.
  Incremental run with no new data completes cleanly (no errors, no spurious
  upserts). Previously this caused errors in the v1/v2 architecture.

- **Deploy + full refresh required**: After deploying the new silver pipeline:
  1. Full refresh ingestion pipeline (to populate `fhir_resources_variant`)
  2. Full refresh silver pipeline (to rebuild all 24 resource tables from new source)
  3. Drop orphaned `_raw` tables after verifying silver tables are correct

- **Schema evolution test**: not yet performed. Plan: run incremental update after
  adding new synthea population; verify new columns appear in `fhir_resource_schemas`
  and silver tables without manual full refresh.

- **Lakebase/HAPI loading job (FUTURE)**: Design a downstream job that exports
  `fhir_resources_variant` as NDJSON for HAPI `$import`, or writes VARIANT->JSONB
  directly to Aidbox on Databricks Lakebase. Architecture validated via research
  (HAPI stores resources as document blobs; Aidbox uses JSONB per row).

---

## Service Principals

- `hls-fde-prod`: applicationId `acf021b4-87c6-44ff-b3d7-45c59d63fe4d`
- `mkgs-prod`: applicationId `47c0365e-b1af-429c-b56d-07cfb18b5dc7`

---

## Source File Reference

| File | Purpose |
|---|---|
| `src/fhir_bundle_mover/transformations/file_tracker.py` | Auto Loader + UDF file mover; `file_tracker` streaming table |
| `src/fhir_bundle_ingestion_etl/transformations/bronze.py` | `fhir_bronze`, `fhir_bronze_variant` |
| `src/fhir_bundle_ingestion_etl/transformations/resources.py` | `fhir_resources_variant`, `bundle_meta`, `fhir_resources`, `fhir_resource_schemas` |
| `src/fhir_resource_silver_etl/transformations/silver.py` | Dynamic silver table generation; reference/identifier/code/temporal extraction + Auto CDC per resource type |
| `resources/fhir_bundle_mover.pipeline.yml` | Mover pipeline config |
| `resources/fhir_bundle_ingestion_etl.pipeline.yml` | Ingestion pipeline config |
| `resources/fhir_resource_silver_etl.pipeline.yml` | Silver pipeline config |
| `resources/fhir_bundle_mover.job.yml` | FHIR Bundle Mover job |
| `resources/fhir_etl_orchestration.job.yml` | FHIR ETL Orchestration job |
| `resources/synthetic_fhir_etl_orchestration.job.yml` | Synthetic FHIR ETL Orchestration job |
