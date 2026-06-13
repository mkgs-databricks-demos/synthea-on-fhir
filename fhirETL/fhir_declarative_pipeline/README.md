# FHIR Declarative Pipeline

A Declarative Automation Bundle (DABs) that ingests synthetic FHIR R4 bundles
through a five-stage Lakeflow Spark Declarative Pipeline stack: file movement,
bronze ingestion, typed silver tables, gold entity resolution, and a clinical
mart dimensional model.

---

## Prerequisites

### 1. synthea_on_dbx (required for synthetic data generation)

The `Synthetic FHIR ETL Orchestration` job calls `synthea_on_dbx_job` as its
first task. This job must be deployed to the same workspace before this bundle.
The `synthea_job_id` variable resolves automatically by job name at validate/deploy
time — no manual ID lookup is needed.

```bash
git clone https://github.com/mkgs-databricks-demos/synthea-on-fhir.git
cd synthea-on-fhir/synthea_on_dbx
databricks bundle deploy --target <target>
```

Repo: https://github.com/mkgs-databricks-demos/synthea-on-fhir/tree/main/synthea_on_dbx

---

## Deployment

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

Supported targets: `dev` (fevm-hedis, development mode), `hedis`
(fevm-hedis, production), `hls_fde` (fevm-hls-fde, production).

---

## Pipeline Architecture

Five Lakeflow Spark Declarative Pipelines run in sequence:

### 1. Streaming FHIR Bundle Mover (`fhir_bundle_mover_etl`)
Reads FHIR bundle files from a source volume using Auto Loader (binaryFile
format). A UDF distributes file copies across the cluster to the landing
volume. Results tracked in the `file_tracker` streaming table.

### 2. FHIR Bundle Resource Parsing ETL (`fhir_bundle_ingestion_etl`)
Auto Loader text ingestion from the landing volume. Produces:
- `fhir_bronze` / `fhir_bronze_variant` — raw text → VARIANT JSON
- `bundle_meta` — bundle-level metadata
- `fhir_resources_variant` — **one row per resource, full VARIANT** (universal staging)
- `fhir_resources` — exploded key-value rows (retained for schema discovery)
- `fhir_resource_schemas` — inferred VARIANT + struct schemas per resource type

### 3. FHIR Resource Silver ETL (`fhir_resource_silver_etl`)
Fully streaming architecture — no PIVOT, no materialized views, no batch intermediaries.
Dynamically generates one streaming table per FHIR resource type (27 total):
```
fhir_resources_variant (STREAM, filter by resourceType)
  → {type}_extract (temp view: VARIANT path extraction + reference/identifier/code parsing)
  → {type} (Auto CDC Type 1 upsert, keyed on {type}_uuid, sequenced by ingest_time)
```
Uniform 10-column schema: `{type}_uuid`, `bundle_uuid`, `{type}_url`,
`references`, `identifiers`, `codes`, `status`, `clinical_event_effective_start`,
`clinical_event_effective_end`, `resource` (VARIANT).

### 4. FHIR Gold Entity Resolution ETL (`fhir_gold_etl`)
Resolves real-world entity identity and deduplicates via Auto CDC Type 1.
Produces 25 gold streaming tables from four transformation files:

| Source | Tables | Pattern |
|---|---|---|
| `entity_resolution.py` + `fhir_gold.py` | patient, practitioner, organization (3) | Identifier cascade (SSN/NPI) |
| `gold_overrides.py` | location, patient_identity_bridge (2) | Correlated subquery, LATERAL VIEW |
| `gold_engine.py` (YAML-driven) | 20 tables | Event join + entity patterns |

The **YAML engine** reads `fixtures/gold_etl/*.gold.yml` at planning time and
generates temp views + streaming tables + Auto CDC flows. Each YAML file defines
source, natural key strategy, columns (with `try_variant_get` expressions), and
data quality expectations. New gold tables can be added by dropping a YAML file —
no Python code required.

Natural key strategies:
- Patient: SSN > MRN > hospital system (identifier cascade)
- Practitioner: NPI
- Organization: NPI or sha2(name)
- Location: sha2(name + managing_org_nk) via correlated subquery
- Events: sha2(patient_nk + code + temporal)

### 5. FHIR Clinical Mart (`fhir_gold_clinical_mart`)
Dimensional model consuming from `_gold` tables into a separate schema.
Planned: `dim_patient`, `dim_provider`, `dim_organization`, `dim_date`,
`fact_encounter`, `fact_condition`, `fact_observation`, `fact_procedure`,
`fact_medication`, `fact_claim`. Not yet implemented.

---

## Directory Structure

```
fhir_declarative_pipeline/
├── databricks.yml                          # Bundle config (targets, variables, schemas)
├── PROJECT_MEMORY.md                       # Canonical long-term AI memory
├── README.md                               # This file
├── resources/
│   ├── fhir_bundle_mover.pipeline.yml
│   ├── fhir_bundle_ingestion_etl.pipeline.yml
│   ├── fhir_resource_silver_etl.pipeline.yml
│   ├── fhir_gold_etl.pipeline.yml
│   ├── fhir_gold_clinical_mart.pipeline.yml
│   ├── fhir_bundle_mover.job.yml
│   ├── fhir_etl_orchestration.job.yml
│   └── synthetic_fhir_etl_orchestration.job.yml
├── src/
│   ├── fhir_bundle_mover/transformations/
│   ├── fhir_bundle_ingestion_etl/transformations/
│   ├── fhir_resource_silver_etl/transformations/
│   ├── fhir_gold_etl/
│   │   ├── schema/gold_table_schema.py     # Pydantic models for YAML validation
│   │   └── transformations/
│   │       ├── entity_resolution.py        # 3 identifier cascade views
│   │       ├── fhir_gold.py                # 3 entity streaming tables + CDC
│   │       ├── gold_overrides.py           # location + bridge (edge cases)
│   │       └── gold_engine.py              # YAML-driven: 20 tables
│   └── fhir_gold_clinical_mart/transformations/
├── fixtures/
│   ├── gold_etl/*.gold.yml                 # 20 gold table YAML definitions
│   ├── metric_views/*.metric_view.yml      # UC Metric View definitions
│   ├── architecture/                       # Design documents
│   └── sessions/                           # Development session logs
└── tests/                                  # (planned)
```

---

## Jobs

**FHIR Bundle Mover** (`fhir_bundle_mover_job`)
File-arrival triggered on source volume. Branches between incremental and
full refresh via `full_refresh` parameter.

**FHIR ETL Orchestration** (`fhir_etl_orchestration_job`)
File-arrival triggered on landing volume. Sequences: ingestion → silver →
gold → clinical mart. Accepts `full_refresh` parameter.

**Synthetic FHIR ETL Orchestration** (`synthetic_fhir_etl_orchestration_job`)
End-to-end: `run_synthea` → `run_fhir_bundle_mover` → `run_fhir_etl_orchestration`.
Always incremental. For full refresh, run individual jobs directly.

---

## Variable Reference

| Variable | Description | Default |
|---|---|---|
| `catalog` | Target UC catalog | per target |
| `schema` | Base schema name | per target |
| `schema_resolved` | Resolved schema (includes dev prefix) | per target |
| `clinical_mart_schema` | Separate schema for dimensional model | `clinical_mart` |
| `source_catalog` | Catalog containing synthea source volume | per target |
| `landing_volume` | Volume name for landed FHIR bundles | `landing` |
| `run_as_user` | User or SP for job run_as | per target |
| `synthea_job_id` | Resolved by lookup: `synthea_on_dbx_job` | lookup |

---

## Documentation

- [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- [DABs configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
- [DABs variable lookups](https://docs.databricks.com/aws/en/dev-tools/bundles/variables/)
- [synthea-on-fhir repo](https://github.com/mkgs-databricks-demos/synthea-on-fhir)
- `fixtures/architecture/` — design docs for gold layer, YAML engine, clinical mart
- `fixtures/sessions/` — chronological development session logs
