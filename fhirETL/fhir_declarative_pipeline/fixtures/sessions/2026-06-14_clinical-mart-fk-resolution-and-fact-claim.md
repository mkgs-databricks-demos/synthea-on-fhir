# 2026-06-14 Clinical Mart FK Resolution, fact_claim, and Metric Views

## Goals
1. Resolve TD-1: Add practitioner/organization/location FK columns to `fact_encounter`
2. Resolve TD-2: Add `value_raw VARIANT` to `fact_observation`
3. Resolve TD-3: Add `encounter_natural_key` FK to `fact_condition`
4. Implement `fact_claim` streaming table (11th clinical mart table)
5. Add `mv_claims` metric view for cost and utilization analysis
6. Add rely hints to `mv_encounter_utilization` for aggregation pushdown
7. Build `fhir_relationship_registry` MV for downstream Genie/metric-view consumers
8. Update integrity check fixture for new tables and FK paths

---

## Problems Discovered

1. **fact_encounter practitioner FK extraction**: FHIR search-format reference URLs (`Practitioner?identifier=http://hl7.org/fhir/sid/us-npi|9999999539`) required parsing — not a simple `urn:uuid:` strip
2. **Organization/location FK resolution**: Synthea uses UUID-based references (`urn:uuid:<synthea-uuid>`) for org and location; gold tables key on NPI/sha2(name) — needed intermediate identifier extraction
3. **Pharmacy claims have no facility**: `location_natural_key` NULL for 5.9M pharmacy claims — not a bug, FHIR spec does not include facility reference for pharmacy claim type

---

## Root Causes & Fixes

| # | Problem | Root Cause | Fix |
|---|---------|-----------|-----|
| 1 | Practitioner FK extraction | FHIR `participant.individual` reference uses search-format URL with NPI embedded after pipe delimiter | Regex extract: `regexp_extract(_practitioner_ref_url, '\\|([^|]+), 1)` to pull NPI directly |
| 2 | Org/location FK resolution | Gold natural keys use `sha2(name)` or NPI, but encounter references use Synthea UUIDs | Extract UUID from `urn:uuid:` URL, match against `identifiers[0].value` on org_gold / `REPLACE(location_url, 'urn:uuid:', '')` on location_gold |
| 3 | Pharmacy claims NULL location | FHIR ClaimType `pharmacy` has no `facility` reference in the spec | No fix needed — added documentation. 50.7% location coverage is correct (institutional + professional only) |

---

## Work Completed

### Modified Files

| File | Change |
|------|--------|
| `src/fhir_gold_clinical_mart/transformations/dimensions.py` | Added `fact_claim` streaming table declaration + Auto CDC flow (SCD1). Added `practitioner_natural_key`, `organization_natural_key`, `location_natural_key` to `fact_encounter` schema. Added `encounter_natural_key` to `fact_condition` schema. Added `value_raw VARIANT` to `fact_observation` schema. |
| `src/fhir_gold_clinical_mart/transformations/entity_resolution.py` | Rewrote `_fact_encounter_src`: 3 new dimension lookup CTEs (practitioner, org, location) with identifier extraction from FHIR search-format URLs. Added `_fact_condition_src` encounter CTE + LEFT JOIN on `encounter_url = _encounter_ref_url`. Uncommented `value_raw` in `_fact_observation_src`. Added `_fact_claim_src` temp view (org/location FK resolution, claim type/use extraction, billable period). |
| `fixtures/metric_views/mv_encounter_utilization.metric_view.yml` | Added organization + location joins (4 new dimensions: org/location name and state). Added 2 new measures (unique_organizations, unique_locations). Now 11 dims / 9 measures. Added rely hints for aggregation pushdown. |
| `fixtures/metric_views/mv_claims.metric_view.yml` | **NEW**. 9 dimensions (claim_month/year, type, use, patient demographics, org, location, state). 11 measures (volume, spend by type, per-patient cost, org count). Joins to dim_patient, dim_organization, dim_location with rely hints. |
| `src/fhir_gold_clinical_mart/register_metric_views.ipynb` | Registered mv_claims; revalidated all 35 measures across 4 metric views. |
| `src/fhir_resource_silver_etl/transformations/silver.py` | Added `fhir_relationship_registry` materialized view — dynamically discovers all resource types, EXPLODE references/identifiers/codes, aggregates into registry grain (~500 rows). |
| `fixtures/genie_context_instructions.md` | **NEW**. System instructions for Genie spaces: data model, join paths, URL formats, identifier systems, code vocabularies, query patterns. |
| `fixtures/clinical_mart_integrity_check.py` | Added fact_claim to all 5 existing checks. Added section 5: dimension FK orphan detection (6 FK paths). Added cells for encounter/condition/claim FK coverage percentages and claim type breakdown. |
| `PROJECT_MEMORY.md` | Marked TD-1/2/3 and fact_claim as resolved. Updated clinical mart table state to 11 tables / ~130M rows. Documented metric view additions. |

---

## Key Architectural Decisions

- **Identifier extraction pattern for encounter FKs**: Practitioner uses NPI from search-format URL (`regexp_extract`). Organization and location use Synthea UUID extracted from `urn:uuid:` references, matched against gold table identifier values. Same pattern reused in `fact_claim`.
- **Static dimension reads for FK resolution**: All dimension lookup CTEs use `_static()` (no STREAM). Only the primary fact source CTE uses `_gold()` (STREAM). Consistent with pattern established in the clinical-mart-first-run session.
- **fact_claim cluster keys**: `patient_natural_key, claim_type_code, billable_period_start` — optimized for patient-level cost queries filtered by claim type and date range.
- **mv_claims rely hints**: Joins to dim_patient, dim_organization, dim_location all verified many-to-one via FK integrity checks — safe for aggregation pushdown.
- **fhir_relationship_registry as MV in silver pipeline**: Recomputes on each pipeline run. Bounded output (~500 rows = unique relationship patterns). Provides dynamic context for Genie spaces and future metric view construction without hard-coding FHIR schema knowledge.

---

## Commits (mg-td1-encounter-fk-columns branch)

| SHA | Date | Message |
|-----|------|---------|
| `40943f2` | Jun 14 02:10 | feat: activate clinical mart FK columns and value_raw (TD-1, TD-2, TD-3) |
| `b4742e3` | Jun 14 02:20 | feat: implement fact_claim in clinical mart |
| `9f2a402` | Jun 14 02:32 | feat: add mv_claims metric view for cost and utilization analysis |
| `d6d1931` | Jun 14 03:05 | perf: add rely hints to mv_encounter_utilization joins |
| `f4b5d86` | Jun 14 20:17 | docs: update PROJECT_MEMORY with TD-1/2/3 resolution and fact_claim |
| `e75d95e` | Jun 15 20:33 | feat: add fhir_relationship_registry MV and Genie context instructions |
| `3cb149b` | Jun 15 21:33 | test: update integrity check for fact_claim and dimension FKs |

---

## Pipeline Run Summary

All runs against pipeline `fhir_gold_clinical_mart` (ID: `ad8a1df0-87ab-4142-bfbb-4ccb467adaec`).

Latest incremental run (2026-06-14): all 11 flows COMPLETED, 0 errors.

---

## Verification

### Row Counts (11 tables, ~130M total)

| Table | Rows | Notes |
|-------|------|-------|
| dim_patient | 124,565 | — |
| dim_practitioner | 1,240 | — |
| dim_organization | 1,126 | — |
| dim_location | 1,141 | — |
| fact_encounter | 8,111,035 | +116K vs first run (incremental) |
| fact_condition | 5,013,809 | — |
| fact_observation | 71,555,435 | — |
| fact_procedure | 22,520,875 | — |
| fact_medication_request | 6,606,711 | — |
| fact_immunization | 1,978,187 | — |
| fact_claim | 14,031,564 | NEW — professional 7.6M, pharmacy 5.9M, institutional 546K |

### FK Coverage

| Fact Table | FK Column | Coverage | Integrity |
|------------|-----------|----------|-----------|
| fact_encounter | practitioner_natural_key | 100% | 100% (0 orphans) |
| fact_encounter | organization_natural_key | 89.2% | 100% (0 orphans) |
| fact_encounter | location_natural_key | 87.6% | 100% (0 orphans) |
| fact_condition | encounter_natural_key | 100% | 100% (0 orphans) |
| fact_claim | organization_natural_key | 89.9% | 100% (0 orphans) |
| fact_claim | location_natural_key | 50.7% | 100% (0 orphans) |

### Metric Views

- 4 metric views registered in `dev_matthew_giglia_clinical_mart`
- 35/35 measures validated (all return non-null via `MEASURE()` queries)
- mv_encounter_utilization: updated to 11 dims / 9 measures (org/location joins + rely hints)
- mv_claims: 9 dims / 11 measures (new)

### Observation No-Value Note

- `value_raw` column added to `fact_observation` schema — currently all-NULL
- Root cause: `observation_gold` does not yet populate `value_raw` upstream
- Schema is ready for when gold ETL activates value extraction
- ~3.3M rows (4.7%) with null value fields are panel headers (`component[]`) — expected FHIR semantics (unchanged from prior session)

---

## Known Remaining Issues

1. **File arrival trigger broken**: `fhir_etl_orchestration_job` missing `s3:GetBucketNotification` / `s3:PutBucketNotification` on IAM role `ncqai-ext-role-049629455384-hag0lv`. Needs metastore admin.
2. **observation_gold value_raw not populated**: Gold ETL does not extract `value[x]` into a dedicated column yet. `fact_observation.value_raw` is schema-ready but all-NULL.
3. **Schema evolution test not performed**: Need to run incremental update after adding new synthea population to verify auto-discovery.
4. **PR not yet opened**: Branch `mg-td1-encounter-fk-columns` (7 commits) pushed to origin but no PR created against main.
5. **Lakebase/HAPI loading job**: Future — NDJSON export or VARIANT->JSONB for Aidbox on Databricks Lakebase.
6. **Alert-driven governance**: Future — monitor `fhir_resource_schemas` for new types, auto-generate draft YAML.
