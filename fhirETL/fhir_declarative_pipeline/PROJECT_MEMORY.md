# PROJECT_MEMORY — fhir_declarative_pipeline

Canonical long-term memory for the FHIR Declarative Pipeline bundle.
Read this at the start of every session before making changes.
Last updated: 2026-06-11 (skipChangeCommits + PO fix)

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
- Five output tables:
  - `fhir_bronze` -- raw text ingestion
  - `fhir_bronze_variant` -- VARIANT-parsed JSON
  - `bundle_meta` -- bundle-level metadata
  - `fhir_resources` -- exploded key-value rows (2.1B rows across 24 resource types)
  - `fhir_resource_schemas` -- one row per resourceType/column with inferred VARIANT and struct schemas (288 rows)
- Config keys: `pipeline.landing_volume_path`, `pipeline.catalog_use`, `pipeline.schema_use`

### 3. fhir_resource_silver_etl
- Reads `fhir_resource_schemas` to discover resource types at pipeline planning time
- Per resource type, three objects:
  - `{type}_raw` (public streaming table) -- PIVOT of `fhir_resources` key-value rows into VARIANT columns.
    Published to catalog (not private) so `_typed_view` can address it via `spark.readStream.table()`.
  - `{type}_typed` (temporary view) -- reads `{type}_raw` via `spark.readStream.option("skipChangeCommits", "true")`
    and CASTs each VARIANT column to its inferred struct type via `selectExpr`
  - `{type}` (streaming table) -- Auto CDC Type 1 upserts keyed on `{type}_uuid`
- Config keys: `pipeline.catalog_use`, `pipeline.schema_use`
- Must run AFTER ingestion ETL on first deployment (two-pass architecture)

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

Applied to all bronze tables, all 24 silver CDC target tables, and all 24 `_raw` intermediate tables.
Verified correct as of 2026-06-11.

Note: `_raw` tables include `autoCompact` and `optimizeWrite` even though they are streaming sources.
This is safe because `_typed_view` uses `skipChangeCommits=true` (see below).

### ExplanationOfBenefit PIVOT fix (silver.py)

The EOB PIVOT was hanging indefinitely (90+ min, OOM on serverless) because
`fhir_resources` contains `item` (~6 KB/row x 14.6M rows = ~90 GB) and
`contained` (~552 bytes/row x 14.6M = ~8 GB) keys that entered the shuffle
even though they were excluded from the PIVOT output columns.

Fix in `_create_resource_tables` in `silver.py`:

    _PIVOT_SKIP_COLUMNS: dict[str, set[str]] = {"ExplanationOfBenefit": {"item", "contained"}}

`skip_filter` is built from the skip set and injected into the WHERE clause of
the PIVOT inner SELECT:

    WHERE resourceType = '{resource_type}'{skip_filter}
    -- skip_filter = "\n                AND key NOT IN ('contained', 'item')"

Result: EOB PIVOT now completes in ~13 seconds.

### skipChangeCommits on _raw streaming sources (silver.py)

Predictive Optimization (PO) automatically runs OPTIMIZE on all SDP UC managed tables,
including `_raw` intermediate tables. OPTIMIZE is a file-rewrite transaction. Structured
Streaming treats file rewrites as non-append operations and fails downstream streams with:

    "failed due to a non-append only streaming source"

This affected all 24 `{type}` CDC flows (each reads `FROM STREAM({type}_typed)` which
reads `_raw`). Root cause confirmed: every `_raw` flow completed; every CDC target flow
failed immediately after.

Important: PO cannot be disabled for SDP UC managed tables. Even with schema-level
`DISABLE PREDICTIVE OPTIMIZATION`, PO still manages SDP pipeline table maintenance
(confirmed by internal account alert, April 2025). `pipelines.autoOptimize.managed = false`
is no longer respected.

Fix applied in `silver.py`:
1. `private=True` removed from `@dp.table` on `_raw` -- private tables are not in the UC
   catalog and cannot be addressed by `spark.readStream.table()`. Public tables are required
   to use the Python readStream API.
2. `_typed_view` rewritten from `spark.sql("... FROM STREAM({rt_lower}_raw)")` to:

       spark.readStream
           .option("skipChangeCommits", "true")
           .table(f"{_catalog}.{_schema}.{rt_lower}_raw")
           .selectExpr(*_cast_exprs)

   SQL `STREAM()` syntax has no options interface; Python readStream API is required.
3. `skipChangeCommits` silently skips OPTIMIZE/compaction commits without re-emitting
   rows. Unlike `ignoreChanges` (which re-reads rewritten files and produces duplicates),
   `skipChangeCommits` is correct here because OPTIMIZE does not change row content.
4. `autoOptimize.autoCompact` and `autoOptimize.optimizeWrite` restored to `_raw`
   table properties since `skipChangeCommits` fully tolerates the resulting transactions.

Side effect: 24 `{type}_raw` tables are now published to the schema (previously private/
pipeline-internal). Naming convention makes them clearly intermediate.

### synthea_job_id variable lookup

`databricks.yml` resolves the synthea job ID by display name at validate/deploy time:

    synthea_job_id:
      lookup:
        job: "synthea_on_dbx_job"

Requires the synthea bundle deployed in production mode (dev mode prefixes the
name and breaks the lookup). Source repo:
https://github.com/mkgs-databricks-demos/synthea-on-fhir/tree/main/synthea_on_dbx

---

## Silver Table State (as of 2026-06-11, dev target)

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

- **synthetic_fhir_etl_orchestration_job missing permissions block**: omitted
  from `synthetic_fhir_etl_orchestration.job.yml` during write. Add manually
  to match the other job files.

- **mkgs-prod service principal**: applicationId `47c0365e-b1af-429c-b56d-07cfb18b5dc7`
  needs `CAN_EDIT` added to `hedis.permissions` block manually.

- **Silver pipeline needs full refresh to recover**: update `23939d4c` failed on all 24
  CDC target flows before the `skipChangeCommits` fix was deployed. The streaming
  checkpoints are poisoned. Run a full refresh of `fhir_resource_silver_etl` (pipeline ID
  `aace6745-bdbd-4568-964b-5e78b40ac11f`) before the next incremental run.

- **Schema evolution test**: not yet performed. Plan: run incremental update after
  adding new synthea population; verify new columns appear in `fhir_resource_schemas`
  and silver tables without manual full refresh.

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
| `src/fhir_bundle_ingestion_etl/transformations/resources.py` | `bundle_meta`, `fhir_resources`, `fhir_resource_schemas` |
| `src/fhir_resource_silver_etl/transformations/silver.py` | Dynamic silver table generation; PIVOT + CDC per resource type |
| `resources/fhir_bundle_mover.pipeline.yml` | Mover pipeline config |
| `resources/fhir_bundle_ingestion_etl.pipeline.yml` | Ingestion pipeline config |
| `resources/fhir_resource_silver_etl.pipeline.yml` | Silver pipeline config |
| `resources/fhir_bundle_mover.job.yml` | FHIR Bundle Mover job |
| `resources/fhir_etl_orchestration.job.yml` | FHIR ETL Orchestration job |
| `resources/synthetic_fhir_etl_orchestration.job.yml` | Synthetic FHIR ETL Orchestration job |
