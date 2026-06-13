# Gold YAML Engine — Architecture Design

Status: **IMPLEMENTED** | Author: matthew.giglia | Date: 2026-06-13

---

## 1. Problem Statement

The FHIR Gold ETL pipeline currently defines 18 streaming tables across two Python
files (~1,750 lines total). Each table follows near-identical structural patterns
(STREAM → optional JOIN → extract → sha2 natural key → Auto CDC Type 1), yet
changes require editing Python source code — mixing schema definitions with
execution logic and making PR reviews unnecessarily difficult.

Goals:
- Separate table schema/configuration (WHAT) from engine logic (HOW)
- Enable schema evolution via YAML diffs, not code changes
- Support declarative data quality expectations per column/table
- Provide a migration path from hand-coded to metadata-driven
- Enable future governance workflows (Alert → draft YAML → review → deploy)

---

## 2. Architecture Overview

```
fixtures/gold_etl/                    # Version-controlled table definitions
  encounter_gold.gold.yml
  condition_gold.gold.yml
  claim_gold.gold.yml
  ...

/Volumes/{catalog}/{schema}/gold_etl_drafts/   # Staging area (auto-generated)
  new_resource_type.gold.yml                    # Draft from Alert workflow

src/fhir_gold_etl/
  schema/
    gold_table_schema.py              # Pydantic validation model
    __init__.py
  transformations/
    gold_engine.py                    # Generic engine (reads YAML, emits tables) — 20 tables
    gold_overrides.py                 # Hand-coded edge cases (location_gold, patient_identity_bridge)
    entity_resolution.py             # Identifier cascades + event resolution views (ACTIVE)
    fhir_gold.py                     # Entity streaming tables: patient/practitioner/organization (ACTIVE)
```

### Execution Flow

1. Pipeline starts → `gold_engine.py` executes at module level
2. Engine reads all `*.gold.yml` from the fixtures directory
3. Optionally reads draft YAMLs from Volume (for auto-discovered resources)
4. For each config: validates via pydantic → generates SQL → registers view + table
5. Edge cases in `gold_overrides.py` register their own views/tables directly

### Key Design Decisions

- YAML is the source of truth for schema; Python engine is the executor
- Column `source` fields contain raw SQL expressions (not abstracted)
- Natural key SQL lives in YAML (version-controlled alongside the schema)
- `resource_last_updated` and `resource VARIANT` are auto-appended (never in YAML)
- `source_{type}_uuids` is auto-generated from the source table name
- Table properties are global defaults (overridable per table)

---

## 3. YAML Schema Specification

### Full Example: encounter_gold.gold.yml

```yaml
# FHIR Gold Table Definition
# This file is the source of truth for the encounter_gold streaming table.
# Changes here trigger schema evolution on next full refresh.

table:
  name: encounter_gold
  comment: >-
    Entity-resolved canonical encounter (visit). One row per real-world
    healthcare encounter. Dedup key: sha2(patient_nk + class + type_code +
    period_start). Source: silver encounter joined with silver patient.
  cluster_by:
    - patient_natural_key
    - period_start
  table_properties: {}            # empty = use global defaults

source:
  silver_table: encounter         # table in pipeline.schema_use
  join_type: event                # entity | event | correlated | bridge
  patient_ref_field: subject      # which references.field links to patient
  where_clause: null              # optional filter on source stream

natural_key:
  column_name: encounter_natural_key
  strategy: composite_sha2        # composite_sha2 | identifier_cascade | custom
  components:
    - expr: "{{patient_natural_key}}"
      default: "UNKNOWN"
    - expr: "try_variant_get(e.resource, '$.class.code', 'STRING')"
      default: "UNKNOWN"
    - expr: "try_variant_get(e.resource, '$.type[0].coding[0].code', 'STRING')"
      default: "UNTYPED"
    - expr: "e.clinical_event_effective_start"
      default: "NULL"

columns:
  - name: encounter_url
    type: STRING
    comment: "Most recent FHIR fullUrl. Used by downstream views to resolve encounter context references."
    source: "e.encounter_url"

  - name: patient_natural_key
    type: STRING
    comment: "FK to patient_gold. Resolved from encounter.subject reference via intra-bundle join."
    source: "{{patient_natural_key}}"

  - name: encounter_class
    type: STRING
    comment: "Visit classification: AMB (ambulatory), EMER (emergency), IMP (inpatient), HH, VR."
    source: "try_variant_get(e.resource, '$.class.code', 'STRING')"

  - name: encounter_type_code
    type: STRING
    comment: "SNOMED encounter type code from resource.type[0].coding[0].code."
    source: "try_variant_get(e.resource, '$.type[0].coding[0].code', 'STRING')"

  - name: encounter_type_display
    type: STRING
    comment: "Human-readable encounter type."
    source: "try_variant_get(e.resource, '$.type[0].coding[0].display', 'STRING')"

  - name: status
    type: STRING
    comment: "Encounter lifecycle status: planned, arrived, triaged, in-progress, finished, cancelled."
    source: "e.status"

  - name: period_start
    type: TIMESTAMP
    comment: "When the encounter began. Clinical event timestamp — NOT an SCD validity date."
    source: "CAST(e.clinical_event_effective_start AS TIMESTAMP)"

  - name: period_end
    type: TIMESTAMP
    comment: "When the encounter ended. NULL for encounters still in progress."
    source: "CAST(e.clinical_event_effective_end AS TIMESTAMP)"

  - name: reason_code
    type: STRING
    comment: "Primary reason for visit — SNOMED code."
    source: "try_variant_get(e.resource, '$.reasonCode[0].coding[0].code', 'STRING')"

  - name: reason_display
    type: STRING
    comment: "Human-readable reason for visit."
    source: "try_variant_get(e.resource, '$.reasonCode[0].coding[0].display', 'STRING')"

  - name: references
    type: "ARRAY<STRUCT<field: STRING, url: STRING, type: STRING, display: STRING>>"
    comment: "All FHIR references from the encounter (subject, participant, serviceProvider, location)."
    source: "e.references"

expectations:
  - name: encounter_nk_not_null
    expr: "encounter_natural_key IS NOT NULL"
    action: drop

  - name: patient_nk_not_null
    expr: "patient_natural_key IS NOT NULL"
    action: warn

  - name: valid_encounter_class
    expr: "encounter_class IN ('AMB', 'EMER', 'IMP', 'HH', 'VR') OR encounter_class IS NULL"
    action: warn
```

### Template Variables

The engine substitutes these placeholders at SQL generation time:

| Variable | Substitution |
|---|---|
| `{{patient_natural_key}}` | Full COALESCE(FILTER(...)) SQL for patient NK from joined patient row |
| `{{catalog}}` | `pipeline.catalog_use` config value |
| `{{schema}}` | `pipeline.schema_use` config value |

---

## 4. Engine Implementation

### SQL Generation (per table)

The engine produces two SQL artifacts per YAML config:

**Temp View SQL** (replaces entity_resolution.py functions):
```sql
SELECT
    -- Natural key
    sha2(CONCAT(
        COALESCE(<component_1>, '<default_1>'), '|',
        COALESCE(<component_2>, '<default_2>'), '|', ...
    ), 256) AS {natural_key.column_name},

    -- User-defined columns (from YAML)
    <col_1_source> AS <col_1_name>,
    <col_2_source> AS <col_2_name>,
    ...

    -- Auto-appended columns (never in YAML)
    ARRAY(e.{source_table}_uuid) AS source_{source_table}_uuids,
    COALESCE(
        CAST(try_variant_get(e.resource, '$.meta.lastUpdated', 'STRING') AS TIMESTAMP),
        CURRENT_TIMESTAMP()
    ) AS resource_last_updated,
    e.resource

FROM STREAM({catalog}.{schema}.{source_table}) e
[LEFT JOIN {catalog}.{schema}.patient p
  ON p.bundle_uuid = e.bundle_uuid
  AND p.patient_url = FILTER(e.references, x -> x.field = '{patient_ref_field}')[0].url]
[WHERE {where_clause}]
```

**Schema DDL** (for dp.create_streaming_table):
```sql
`{natural_key.column_name}` STRING NOT NULL COMMENT '...',
`col_1` {type_1} COMMENT '{comment_1}',
`col_2` {type_2} COMMENT '{comment_2}',
...
`source_{type}_uuids` ARRAY<STRING> COMMENT '...',
`resource_last_updated` TIMESTAMP NOT NULL COMMENT '...',
`resource` VARIANT NOT NULL COMMENT '...'
```

### Join Pattern Templates

| join_type | FROM clause |
|---|---|
| `entity` | `FROM STREAM({table}) e` (no join) |
| `event` | `FROM STREAM({table}) e LEFT JOIN {schema}.patient p ON bundle_uuid + ref_field` |
| `correlated` | Reserved for location-style subqueries (hand-coded in overrides) |
| `bridge` | Reserved for LATERAL VIEW patterns (hand-coded in overrides) |

---

## 5. Migration Strategy

### Phase 1: Engine + 3 POC tables (current sprint)
- Build `gold_engine.py` and `gold_table_schema.py`
- Convert `claim_gold`, `coverage_gold`, `encounter_gold` to YAML
- Both systems coexist — engine tables + hand-coded tables in same pipeline
- Validate: full refresh produces identical row counts

### Phase 2: Convert all event tables
- Move remaining 10 event tables to YAML
- Remove corresponding functions from entity_resolution.py + fhir_gold.py
- Keep entity tables (patient, practitioner, org) hand-coded for now

### Phase 3: Convert entity tables
- Move patient_gold, practitioner_gold, organization_gold to YAML
- Natural key strategy uses `identifier_cascade` type with SQL fragment

### Phase 4: Edge cases + cleanup
- Decide on location (correlated subquery) and bridge table handling
- Remove deprecated entity_resolution.py and fhir_gold.py
- All tables driven by YAML or overrides

---

## 6. Future Roadmap: Alert-Driven Schema Governance

### Concept

When a new FHIR resource type appears in `fhir_resource_schemas` (from the silver
pipeline auto-discovering new resource types), an automated workflow:

1. **Alert V2** monitors `fhir_resource_schemas` for new resourceType values
   not yet covered by a `*.gold.yml` fixture
2. Alert triggers a **Lakeflow Job** that:
   - Generates a draft YAML from the silver table schema (column inspection)
   - Writes draft to `/Volumes/{catalog}/{schema}/gold_etl_drafts/{type}.gold.yml`
   - Posts notification (Slack/Teams) for data engineering review
3. The **gold_engine.py** reads BOTH `fixtures/gold_etl/` AND the Volume drafts
   - Fixture YAMLs are production (version-controlled, reviewed)
   - Volume YAMLs are provisional (auto-generated, no expectations, basic schema)
4. Engineer reviews draft → refines schema + adds expectations → commits to fixtures
5. Commit triggers CI/CD → bundle deploy → full refresh for schema evolution

### Alert V2 SQL

```sql
-- Fires when a silver resource type exists without a gold YAML fixture
SELECT DISTINCT rs.resourceType
FROM {catalog}.{schema}.fhir_resource_schemas rs
LEFT JOIN {catalog}.{schema}.gold_table_registry gtr
  ON LOWER(rs.resourceType) = gtr.source_table
WHERE gtr.source_table IS NULL
  AND rs.resourceType NOT IN ('MessageHeader', 'Bundle')  -- excluded types
```

The `gold_table_registry` table is populated by the engine at startup (one row
per loaded YAML config). This gives the Alert a single table to query.

### Governance App (future — Databricks App V2)

A lightweight UI that:
- Lists all gold tables (YAML-defined + draft)
- Shows schema diff between current deployed and proposed YAML changes
- Allows approve/reject of draft YAMLs
- On approve: commits YAML to repo via GitHub API, triggers deploy
- Tracks schema evolution history (which columns added/removed/modified, when)
- Shows data quality metrics per table (expectation pass rates from event log)

This is explicitly **out of scope** for the current implementation but the
architecture supports it — the Volume staging area + YAML format + pydantic
validation make it straightforward to build later.

---

## 7. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Generated SQL errors point to engine, not YAML | Log full SQL with table name during planning |
| YAML becomes complex for edge cases | Override mechanism for correlated/bridge patterns |
| Schema drift between YAML and deployed table | Full refresh required for schema changes; engine validates DDL match |
| Two systems coexist during migration | Clear deprecation markers; engine tables checked first |
| Volume draft YAMLs have no expectations | Mark as "provisional" quality; add `provisional: true` flag |

---

## 8. File Reference

| File | Purpose |
|---|---|
| `fixtures/architecture/gold_yaml_engine_design.md` | This design document |
| `src/fhir_gold_etl/schema/__init__.py` | Package marker |
| `src/fhir_gold_etl/schema/gold_table_schema.py` | Pydantic models for YAML validation |
| `src/fhir_gold_etl/transformations/gold_engine.py` | Generic engine (YAML → views + tables) |
| `src/fhir_gold_etl/transformations/gold_overrides.py` | Edge case tables (location, bridge) |
| `fixtures/gold_etl/*.gold.yml` | Production table definitions |
