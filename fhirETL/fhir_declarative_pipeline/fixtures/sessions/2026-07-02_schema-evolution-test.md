# 2026-07-02 Schema Evolution Test — New Resource Type Auto-Discovery

## Goals
1. Verify the pipeline auto-discovers a previously unseen FHIR resource type without code changes
2. Confirm `fhir_resource_schemas` infers the new type's schema from VARIANT data
3. Confirm the silver pipeline creates a new streaming table for the new resource type
4. Verify existing tables are unaffected (no regression)
5. Validate the full resource VARIANT is queryable on the new table

---

## Test Design

**Approach**: Inject a minimal valid FHIR R4 transaction bundle containing a `Goal` resource (not present in the existing 27 resource types) into the landing volume, then run ingestion and silver pipelines incrementally.

**Test bundle contents**:
- `Patient` — reference subject for the Goal
- `Encounter` — referenced via `addresses` field on the Goal
- `Goal` — NEW resource type with 13 fields including `lifecycleStatus`, `achievementStatus`, `category`, `description`, `subject`, `startDate`, `target` (with measure + detailQuantity + dueDate), `expressedBy`, `addresses`, and `note`

**File**: `/Volumes/ncqai/dev_matthew_giglia_fhir/landing/schema_evolution_test_9773d8af-729d-4c7c-b404-ef32523bd2a9.json`

---

## Baseline (Before Test)

- `fhir_resource_schemas`: 27 resource types
- No `goal` table exists in `ncqai.dev_matthew_giglia_fhir`
- Existing silver tables: patient (138,403), encounter (8,175,981), observation (72,421,008), condition (5,053,240)

---

## Pipeline Runs

| Pipeline | Update ID | Duration | Result |
|----------|-----------|----------|--------|
| `fhir_bundle_ingestion_etl` (4782f58f) | `e43b7ec8-9e3d-4787-9681-21577f1d6957` | ~6 min | COMPLETED |
| `fhir_resource_silver_etl` (aace6745) | `8d247367-08a5-4292-bab8-838b14cf89cf` | ~9 min | COMPLETED |

Both ran as incremental updates (no full refresh).

---

## Results

### Ingestion Pipeline

- `fhir_resource_schemas`: 27 → **28 resource types** (Goal added)
- Goal schema: 13 fields auto-inferred from VARIANT data:

| Field | Inferred Schema |
|-------|-----------------|
| achievementStatus | `OBJECT<coding: ARRAY<OBJECT<code: STRING, display: STRING, system: STRING>>>` |
| addresses | `ARRAY<OBJECT<display: STRING, reference: STRING>>` |
| category | `ARRAY<OBJECT<coding: ARRAY<OBJECT<code: STRING, display: STRING, system: STRING>>>>` |
| description | `OBJECT<coding: ARRAY<OBJECT<code: STRING, display: STRING, system: STRING>>, text: STRING>` |
| expressedBy | `OBJECT<reference: STRING>` |
| id | `STRING` |
| lifecycleStatus | `STRING` |
| meta | `OBJECT<lastUpdated: STRING>` |
| note | `ARRAY<OBJECT<text: STRING>>` |
| resourceType | `STRING` |
| startDate | `STRING` |
| subject | `OBJECT<reference: STRING>` |
| target | `ARRAY<OBJECT<detailQuantity: OBJECT<code: STRING, system: STRING, unit: STRING, value: BIGINT>, dueDate: STRING, measure: OBJECT<coding: ARRAY<OBJECT<code: STRING, display: STRING, system: STRING>>>>>` |

- `fhir_resources`: 1 Goal row (uuid: `31a1955a...`)

### Silver Pipeline

- New `ncqai.dev_matthew_giglia_fhir.goal` streaming table created automatically
- Uniform 10-column schema applied (same as all other silver tables):
  - `goal_uuid` (PK, SHA-256)
  - `bundle_uuid`
  - `goal_url`
  - `references` (ARRAY<STRUCT>)
  - `identifiers` (ARRAY<STRUCT>)
  - `codes` (ARRAY<STRUCT>)
  - `status`
  - `clinical_event_effective_start`
  - `clinical_event_effective_end`
  - `resource` (VARIANT — full Goal resource preserved)

### Reference Extraction

3 references extracted from the Goal resource:

| Field | URL | Type | Display |
|-------|-----|------|---------|
| subject | `urn:uuid:a6001052-6fc0-47e7-8c00-4124772b22d6` | — | — |
| expressedBy | `urn:uuid:a6001052-6fc0-47e7-8c00-4124772b22d6` | — | — |
| addresses | `urn:uuid:7be4c3eb-dfd0-4e58-9486-e2f4a9114d7d` | — | Encounter for check up |

### VARIANT Queryability

Confirmed arbitrary field access via `resource:fieldName` syntax:
- `resource:lifecycleStatus::STRING` → `"active"`
- `resource:description.text::STRING` → `"Exercise 30 minutes per day"`
- `resource:target[0].detailQuantity.value::INT` → `10000`

### Regression Check

Existing silver tables unchanged:

| Table | Rows (after test) | Status |
|-------|-------------------|--------|
| patient | 138,403 | Unchanged |
| encounter | 8,175,981 | Unchanged |
| observation | 72,421,008 | Unchanged |
| condition | 5,053,240 | Unchanged |

---

## Key Findings

1. **Zero-code schema evolution works**: A new resource type is fully auto-discovered, schema-inferred, and materialized as a new streaming table without any code changes, YAML additions, or full refreshes.
2. **VARIANT schema inference is robust**: All 13 Goal fields were correctly typed including deeply nested structures (arrays of objects with nested arrays).
3. **Reference extraction generalizes**: The silver pipeline's reference extraction logic correctly identified `subject`, `expressedBy`, and `addresses` as reference fields — even though `addresses` uses a non-standard FHIR reference pattern (array of references rather than single reference).
4. **Incremental processing preserved**: Both pipelines processed only the new bundle; no reprocessing of existing data.
5. **No gold/clinical mart table created**: This is expected — gold tables require explicit YAML fixtures or hand-coded Python. The Goal resource is available at silver for ad-hoc VARIANT queries but won't appear in the clinical mart until a gold fixture is authored.

---

## Decisions

- **Test data left in place**: The schema evolution test bundle remains in the landing volume and the Goal resource persists in bronze/silver. This is intentional — it demonstrates a real incremental state change and can serve as a regression marker for future pipeline runs.
- **No gold YAML for Goal**: Goal is a valid clinical resource but low priority for the dimensional model. Can be added later via a YAML fixture if needed for quality measures.
- **SESSION_MEMORY not needed**: This is a one-time validation test, not an ongoing development session. Results documented here for reference.

---

## Conclusion

**Schema evolution test: PASSED.** The FHIR ETL pipeline correctly auto-discovers new resource types end-to-end (ingestion → schema inference → silver table creation) without manual intervention, code changes, or full refreshes.
