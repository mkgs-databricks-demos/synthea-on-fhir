# PROJECT_MEMORY — fhir_declarative_pipeline

Canonical long-term memory for the FHIR Declarative Pipeline bundle.
Read this at the start of every session before making changes.
Last updated: 2026-06-13 (PR #24 merged — clinical mart on main; 10 tables live, integrity verified)

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
| `fhir_gold_etl` | [dev matthew_giglia] FHIR Gold Entity Resolution ETL | `74842515-face-4150-a800-c2ea2a3400ac` |
| `fhir_gold_clinical_mart` | [dev matthew_giglia] FHIR Clinical Mart | `ad8a1df0-87ab-4142-bfbb-4ccb467adaec` |

### Jobs

| Resource key | Display name | Job ID |
|---|---|---|
| `fhir_bundle_mover_job` | [dev matthew_giglia] FHIR Bundle Mover | `449274568717351` |
| `fhir_etl_orchestration_job` | [dev matthew_giglia] FHIR ETL Orchestration | `447487171209811` |
| `synthetic_fhir_etl_orchestration_job` | [dev matthew_giglia] Synthetic FHIR ETL Orchestration | `868213351234820` |

### Event Logs

`ncqai.dev_matthew_giglia_fhir`:
- `fhir_bundle_mover_event_log`
- `fhir_bundle_ingestion_etl_event_log` (renamed from `fhir_declarative_pipeline_etl_event_log` -- old table is an orphan, safe to drop)
- `fhir_resource_silver_etl_event_log`
- `fhir_gold_etl_event_log`

`ncqai.dev_matthew_giglia_clinical_mart`:
- `fhir_gold_clinical_mart_event_log`

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
  - `bundle_meta` -- bundle-level metadata (Auto CDC keyed on bundle_uuid)
  - `fhir_resources` -- **one row per resource with full resource VARIANT** (universal staging, Auto CDC keyed on resource_uuid)
  - `fhir_resource_keys` -- EAV-exploded fields, one row per named key within each resource (Auto CDC keyed on resource_key_uuid). Retained for schema inference only.
  - `fhir_resource_schemas` -- one row per resourceType/column with inferred VARIANT and struct schemas (reads from STREAM(fhir_resource_keys))
- Config keys: `pipeline.landing_volume_path`, `pipeline.catalog_use`, `pipeline.schema_use`
- `fhir_resources` is the primary source for the silver pipeline AND future FHIR server loading

### 3. fhir_resource_silver_etl (v3 — FULLY STREAMING, clinical mart ready)
- Reads `fhir_resource_schemas` to discover resource types + reference fields at planning time
- Per resource type, TWO objects:
  - `{type}_extract` (temporary view) -- `STREAM(fhir_resources)` filtered by
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

### 4. fhir_gold_etl (entity resolution + SCD1 — 25 TABLES)
- Reads typed silver tables; produces `_gold` streaming tables in the SAME schema
- **Four transformation files** (3 hand-coded + 1 engine):
  - `entity_resolution.py` — 3 temp views: patient, practitioner, organization
    (identifier normalization, natural key from SSN/NPI cascades)
  - `fhir_gold.py` — 3 streaming tables + Auto CDC (patient, practitioner, organization)
  - `gold_overrides.py` — 2 views + 2 tables + 2 CDC flows:
    - `location_gold` (correlated subquery for managing_organization_nk)
    - `patient_identity_bridge` (LATERAL VIEW EXPLODE of identifiers)
  - `gold_engine.py` — YAML-driven: reads `fixtures/gold_etl/*.gold.yml`, generates
    20 tables (temp view + streaming table + Auto CDC per YAML file)
- **25 gold tables total**: 5 hand-coded + 20 YAML-driven
- Natural key strategy:
  - Patient: SSN > MRN > hospital system fallback (identifier_cascade)
  - Practitioner: NPI (identifier_cascade)
  - Organization: NPI or sha2(name) (identifier_cascade)
  - Location: sha2(name + managing_org_nk) (correlated subquery)
  - Events: sha2(patient_nk + code/class + temporal) (composite_sha2)
  - Reference entities: sha2(code) or sha2(composite fields) (composite_sha2)
- Sequence column: `resource:meta.lastUpdated` (NOT ingest_time)
- Event views: STREAM(event_table) LEFT JOIN patient (static) on bundle_uuid + reference URL
- Entity views: STREAM(entity_table) — no join needed
- Config: `pipeline.catalog_use`, `pipeline.schema_use`, `pipeline.bundle_files_path`
- Pipeline ID: `74842515-face-4150-a800-c2ea2a3400ac`

### 5. fhir_gold_clinical_mart (dimensional model — LIVE, 10 TABLES)
- Reads FROM `_gold` tables in `ncqai.dev_matthew_giglia_fhir`; writes dim/fact tables to `ncqai.dev_matthew_giglia_clinical_mart`
- Schema managed by `${resources.schemas.clinical_mart_schema.name}`; pipeline ID: `ad8a1df0-87ab-4142-bfbb-4ccb467adaec`
- **Two transformation files**:
  - `entity_resolution.py` — 10 `@dp.temporary_view` functions. Reads gold via `_gold()` (STREAM) or `_static()` (snapshot). Computed columns: `full_name`, `age_years`, `age_band` (dim_patient); `length_of_stay_hours`, `is_emergency`, `is_inpatient` (fact_encounter); `is_chronic`, `is_active` (fact_condition); `is_abnormal_low`, `is_abnormal_high` (fact_observation); `duration_minutes` (fact_procedure).
  - `dimensions.py` — all 10 `dp.create_streaming_table()` declarations + co-located `dp.create_auto_cdc_flow()` calls (SCD1). **CDC flows MUST be in the same file as their streaming table declarations — SDP requirement.**
- Config keys: `pipeline.catalog_use` (→ ncqai), `pipeline.silver_schema_use` (→ dev_matthew_giglia_fhir), `pipeline.clinical_mart_schema_use` (→ dev_matthew_giglia_clinical_mart)
- **Critical SDP rule**: `dp.create_auto_cdc_flow()` and its target `dp.create_streaming_table()` must be in the **same Python file**. Splitting across files causes `DLTAnalysisException: No query found for dataset`.
- **Critical streaming rule**: Dimension lookup CTEs in fact temp views must use `_static()` (no `STREAM`). Stream-stream LEFT OUTER joins are not supported without watermarks. Only the primary fact source CTE uses `_gold()` / `STREAM`.
- **Helper pattern** in `entity_resolution.py`:
  - `_gold(table)` → `STREAM({catalog}.{schema}.{table})` — streaming fact source
  - `_static(table)` → `{catalog}.{schema}.{table}` — dimension snapshot lookup
- Metric views: registered by `src/fhir_gold_clinical_mart/register_metric_views.ipynb` (NOT yet run)
- **Tech debt** (see Known Issues): TD-1 fact_encounter FK cols, TD-2 fact_observation value_raw, TD-3 fact_condition encounter FK, fact_claim not yet implemented

### Dual-Gold Architecture

Two distinct gold layers serve different consumers:

| Layer | Schema | Tables | Consumer |
|---|---|---|---|
| FHIR Gold | `{catalog}.{fhir_schema}` (same as silver) | `patient_gold`, `encounter_gold`, ... | FHIR APIs, Smart-on-FHIR, Lakebase |
| Clinical Mart | `{catalog}.{clinical_mart_schema}` (separate) | `dim_patient`, `fact_encounter`, ... | Dashboards, HEDIS, Genie |

Bundle variables:
- `schema` / `schema_resolved` — bronze, silver, FHIR Gold (auto-prefixed in dev)
- `clinical_mart_schema` — separate schema for dimensional model (dev: `dev_{user}_clinical_mart`)

Schema resources:
- `${resources.schemas.fhir_schema.name}` — resolves to full name with dev prefix
- `${resources.schemas.clinical_mart_schema.name}` — resolves to full name with dev prefix


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
- Tasks: `ingestion_etl` -> `silver_etl` -> `fhir_gold_etl` -> `clinical_mart_etl` (incremental or full_refresh)

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

Applied to all bronze tables (including `fhir_resources`) and all 24 silver
CDC target tables. Verified correct as of 2026-06-11.

### Fully streaming silver architecture (PIVOT eliminated)

The silver pipeline was completely rewritten to eliminate all materialized views,
PIVOT operations, CDF bridging, and the associated streaming compatibility issues.

**Previous architecture (DEPRECATED):**

    fhir_resource_keys (key-value EAV, 2.1B rows)
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

## FHIR Gold Table State (as of 2026-06-13, dev target — SECOND FULL REFRESH)

Pipeline: `fhir_gold_etl` (ID: `74842515-face-4150-a800-c2ea2a3400ac`)
Full refresh update: `b42e1031-299d-4f39-ada5-b97f18ccb332` — COMPLETED
(First refresh: `5fd4c657-ad43-4abe-b166-e67053ac02f5` — COMPLETED earlier today)

| Table | Rows |
|---|---|
| patient_gold | 124,565 |
| practitioner_gold | 1,240 |
| organization_gold | 1,126 |
| location_gold | 1,141 |
| encounter_gold | 7,994,774 |
| condition_gold | 4,939,762 |
| observation_gold | 70,599,707 |
| procedure_gold | 22,196,616 |
| medication_request_gold | 6,522,549 |
| immunization_gold | 1,948,242 |

Data quality:
- 100% field population on patient demographics (family_name, given_name, birth_date, gender, address_state)
- Natural keys: SSN (`999-10-1002`) and MRN (`19693241`) formats confirmed — identifier cascade working
- Encounter class distribution: AMB 93.3%, EMER 3.8%, IMP 1.9% (matches Synthea expected distribution)
- Patient dedup: slight increase vs first refresh (124,256 → 124,565) — pipeline running on same data set, minor variance

---

## Clinical Mart Table State (as of 2026-06-13, dev target — FIRST CLEAN FULL REFRESH)

Pipeline: `fhir_gold_clinical_mart` (ID: `ad8a1df0-87ab-4142-bfbb-4ccb467adaec`)
Full refresh run: `f160a6e6-f63f-4d9d-ace9-0c59b15149b0` — COMPLETED (all 10 flows, 0 errors)

| Table | Rows | Gold Source |
|---|---|---|
| dim_patient | 124,565 | patient_gold |
| dim_practitioner | 1,240 | practitioner_gold |
| dim_organization | 1,126 | organization_gold |
| dim_location | 1,141 | location_gold |
| fact_encounter | 7,994,774 | encounter_gold |
| fact_condition | 4,939,762 | condition_gold |
| fact_observation | 70,599,707 | observation_gold |
| fact_procedure | 22,196,616 | procedure_gold |
| fact_medication_request | 6,522,549 | medication_request_gold |
| fact_immunization | 1,948,242 | immunization_gold |

Integrity: 0 null PKs, 0 duplicate PKs, 0 orphan FKs across all tables.
All fact row counts match gold source 1:1.

Observation no-value cohort: 3,332,827 rows (4.7%) with null `value_quantity/string/code`.
Root cause: 100% are multi-component panel headers (`component[]` array present, no top-level `value[x]`).
Dominant codes: `85354-9` blood pressure panel (1.95M), `93025-5` PRAPARE panel (1.35M).
This is expected FHIR semantics — not an extraction gap.

---

## Known Issues / TODOs

- **TD-1: fact_encounter missing practitioner/org/location FK columns**: JOIN logic written in `entity_resolution.py` but columns commented out pending schema update. To activate: add `practitioner_natural_key STRING`, `organization_natural_key STRING`, `location_natural_key STRING` to `fact_encounter` schema in `dimensions.py`, then uncomment the three SELECT columns in `fact_encounter_src`.

- **TD-2: fact_observation missing value_raw VARIANT**: `value_raw` exists on `observation_gold`. To activate: add `value_raw VARIANT` to `fact_observation` schema in `dimensions.py`, then uncomment `value_raw,` in `fact_observation_src`.

- **TD-3: fact_condition missing encounter_natural_key FK**: `_encounter_ref_url` + `_bundle_uuid` on `condition_gold` available for resolution. To activate: add `encounter_natural_key STRING` to `fact_condition` schema in `dimensions.py`, add encounter CTE and LEFT JOIN to `fact_condition_src` in `entity_resolution.py`.

- **fact_claim not implemented**: `claim_gold` exists in FHIR schema with full column set. To implement: add `dp.create_streaming_table()` + `dp.create_auto_cdc_flow()` to `dimensions.py`; add `fact_claim_src` temp view to `entity_resolution.py`.

- **register_metric_views not yet run**: YAML fixtures in `fixtures/metric_views/` define UC metric views for `mv_patient_demographics`, `mv_encounter_utilization`, `mv_clinical_events`. Notebook `src/fhir_gold_clinical_mart/register_metric_views.ipynb` (asset: 2240851736366200) must be executed to create these in `dev_matthew_giglia_clinical_mart`.

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
| `src/fhir_bundle_ingestion_etl/transformations/resources.py` | `fhir_resources` (one-per-resource VARIANT), `bundle_meta`, `fhir_resource_keys` (EAV), `fhir_resource_schemas` |
| `src/fhir_resource_silver_etl/transformations/silver.py` | Dynamic silver table generation; reference/identifier/code/temporal extraction + Auto CDC per resource type |
| `resources/fhir_bundle_mover.pipeline.yml` | Mover pipeline config |
| `resources/fhir_bundle_ingestion_etl.pipeline.yml` | Ingestion pipeline config |
| `resources/fhir_resource_silver_etl.pipeline.yml` | Silver pipeline config |
| `src/fhir_gold_etl/transformations/entity_resolution.py` | 3 temp views: patient_resolved, practitioner_resolved, organization_resolved (identifier cascades) |
| `src/fhir_gold_etl/transformations/fhir_gold.py` | 3 streaming tables + CDC: patient_gold, practitioner_gold, organization_gold |
| `src/fhir_gold_etl/transformations/gold_overrides.py` | 2 edge-case tables: location_gold (correlated subquery) + patient_identity_bridge (LATERAL VIEW EXPLODE) |
| `src/fhir_gold_etl/transformations/gold_engine.py` | YAML engine: reads fixtures/gold_etl/*.gold.yml, generates 20 tables at planning time |
| `src/fhir_gold_etl/schema/gold_table_schema.py` | Pydantic validation models for gold YAML configs |
| `fixtures/gold_etl/*.gold.yml` | 20 YAML table definitions (see Gold YAML Engine section below) |
| `src/fhir_gold_clinical_mart/transformations/dimensions.py` | 10 `dp.create_streaming_table()` + co-located `dp.create_auto_cdc_flow()` for all clinical mart tables (SCD1) |
| `src/fhir_gold_clinical_mart/transformations/entity_resolution.py` | 10 `@dp.temporary_view` functions: `_gold()`/`_static()` helpers + derived column logic per mart table |
| `src/fhir_gold_clinical_mart/register_metric_views.ipynb` | Registers metric views from YAML fixtures into clinical_mart schema |
| `fixtures/clinical_mart_integrity_check.py` | Post-load validation notebook (row counts, null/dupe PKs, orphan FKs, gold alignment, DQ checks, no-value diagnosis) |
| `fixtures/architecture/gold_scd_layer_design.md` | Clinical mart design doc (dual-gold architecture, SCD2 dims, fact tables) |
| `fixtures/architecture/fhir_gold_scd1_design.md` | FHIR Gold table schema design (SCD1, entity resolution, API serving) |
| `fixtures/metric_views/*.metric_view.yml` | UC Metric View YAML definitions (encounter utilization, clinical events, patient demographics) |
| `resources/fhir_gold_etl.pipeline.yml` | FHIR Gold Entity Resolution pipeline config |
| `resources/fhir_gold_clinical_mart.pipeline.yml` | Clinical Mart pipeline config |
| `resources/fhir_bundle_mover.job.yml` | FHIR Bundle Mover job |
| `resources/fhir_etl_orchestration.job.yml` | FHIR ETL Orchestration job |
| `resources/synthetic_fhir_etl_orchestration.job.yml` | Synthetic FHIR ETL Orchestration job |


## Gold YAML Engine — COMPLETE (2026-06-13)

### Architecture
- `gold_engine.py`: reads `fixtures/gold_etl/*.gold.yml` at planning time, generates temp views + streaming tables + Auto CDC flows
- `gold_table_schema.py`: pydantic validation models (GoldTableConfig root model)
- `gold_overrides.py`: hand-coded edge cases (correlated subquery, LATERAL VIEW)
- Design doc: `fixtures/architecture/gold_yaml_engine_design.md`

### Final Table Distribution (25 total)

| Source | Tables | Pattern |
|---|---|---|
| `fhir_gold.py` | patient_gold, practitioner_gold, organization_gold | identifier_cascade (entity) |
| `gold_overrides.py` | location_gold | correlated subquery (entity) |
| `gold_overrides.py` | patient_identity_bridge | LATERAL VIEW EXPLODE (bridge) |
| `gold_engine.py` (YAML) | 20 tables (see below) | event / entity / financial |

### All 20 YAML Fixtures (`fixtures/gold_etl/`)

| YAML File | Table | join_type | patient_ref_field |
|---|---|---|---|
| `encounter_gold.gold.yml` | encounter_gold | event | subject |
| `condition_gold.gold.yml` | condition_gold | event | subject |
| `observation_gold.gold.yml` | observation_gold | event | subject |
| `procedure_gold.gold.yml` | procedure_gold | event | subject |
| `medication_request_gold.gold.yml` | medication_request_gold | event | subject |
| `immunization_gold.gold.yml` | immunization_gold | event | patient |
| `allergyintolerance_gold.gold.yml` | allergyintolerance_gold | event | patient |
| `careplan_gold.gold.yml` | careplan_gold | event | subject |
| `diagnosticreport_gold.gold.yml` | diagnosticreport_gold | event | subject |
| `medicationadministration_gold.gold.yml` | medicationadministration_gold | event | subject |
| `claim_gold.gold.yml` | claim_gold | event | patient |
| `explanationofbenefit_gold.gold.yml` | explanationofbenefit_gold | event | patient |
| `coverage_gold.gold.yml` | coverage_gold | event | beneficiary |
| `careteam_gold.gold.yml` | careteam_gold | event | subject |
| `documentreference_gold.gold.yml` | documentreference_gold | event | subject |
| `device_gold.gold.yml` | device_gold | event | patient |
| `imagingstudy_gold.gold.yml` | imagingstudy_gold | event | subject |
| `supplydelivery_gold.gold.yml` | supplydelivery_gold | event | patient |
| `medication_gold.gold.yml` | medication_gold | entity | null |
| `practitionerrole_gold.gold.yml` | practitionerrole_gold | entity | null |

### Silver Coverage Summary

| Category | Tables | Status |
|---|---|---|
| Gold-covered (all patterns) | 25 | Complete |
| Deferred (no gold table) | provenance, account, messageheader | Low clinical value / infrastructure |
| Total silver resource types | 27 | 25/27 covered (93%) |

### Validated Baselines (full refresh 3dccf159, 2026-06-13)

| Table | Rows | Notes |
|---|---|---|
| encounter_gold | 7,972,040 | Exact match to hand-coded baseline |
| claim_gold | 13,799,381 | Verified |
| coverage_gold | 3 | Silver=6, dedup=3 |

Remaining 22 tables need deploy + full refresh for validation.

### Lessons Learned
- `__file__` is NOT defined in SDP pipeline execution context (code compiled from b64)
- Fix: added `pipeline.bundle_files_path: "${workspace.file_path}"` to pipeline config
- `pyyaml` and `pydantic` are pre-installed on serverless (6.0.2 and 2.10.6 respectively)
- Added `pyyaml` to `environment.dependencies` for explicitness
- Pipeline glob `../src/fhir_gold_etl/transformations/**` auto-discovers new .py files
- `patient_ref_field: null` for entity-type tables (no patient JOIN generated)
- `codes[0].code` syntax works directly in YAML source expressions (engine doesn't transform)

### Future Roadmap: Alert-Driven Governance
- Alert V2 monitors `fhir_resource_schemas` for new types not covered by YAML
- Triggers job to generate draft YAML → Volume staging area
- Gold engine reads both fixtures/ (production) and Volume (provisional)
- Governance app (future Databricks App V2) provides review/approve workflow
