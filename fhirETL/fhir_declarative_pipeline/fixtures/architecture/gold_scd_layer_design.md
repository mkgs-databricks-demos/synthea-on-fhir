# Gold SCD Layer — Design Document

> Status: DRAFT — for discussion
> Created: 2026-06-12
> Context: fhir_declarative_pipeline silver v3 is complete (177M rows, 24 tables, uniform 10-column schema)

---

## 1. Problem Statement

The silver layer provides **bundle-scoped** resource identity (`{type}_uuid = sha2(bundle_uuid + fullUrl)`). This means:

- The same real-world patient admitted at two hospitals produces TWO `patient_uuid` values
- The same condition recorded in two bundles produces TWO `condition_uuid` values
- Cross-source analytics (population health, HEDIS measures) require entity resolution

The gold layer must:
1. Resolve real-world entity identity across sources using `identifiers` (SSN, MRN, NPI)
2. Provide temporal history via SCD Type 2 (slowly changing dimensions)
3. Deduplicate clinical events using `codes` + `identifiers` + `clinical_event_effective_start`
4. Present a queryable, performant analytical surface for clinical marts

---

## 2. Architecture Overview

```
SILVER (per-resource, bundle-scoped)          GOLD (entity-resolved, temporal)
┌─────────────────────────────────┐           ┌────────────────────────────────┐
│ patient (133K rows)             │──┐        │ dim_patient (SCD2)             │
│   patient_uuid, identifiers,    │  │        │   patient_key (surrogate)      │
│   resource VARIANT              │  ├──MK──▶ │   patient_nk (SSN/MRN)         │
├─────────────────────────────────┤  │        │   valid_from, valid_to         │
│ encounter (7.9M rows)           │──┤        │   is_current                   │
│   references -> patient_url     │  │        │   demographics (flattened)     │
├─────────────────────────────────┤  │        ├────────────────────────────────┤
│ condition (4.9M rows)           │──┤        │ dim_practitioner (SCD2)        │
│   codes, temporal, references   │  │        │ dim_organization (SCD2)        │
├─────────────────────────────────┤  │        │ dim_location (SCD2)            │
│ observation (69.8M rows)        │──┘        ├────────────────────────────────┤
│   codes, temporal, references   │           │ fact_encounter                 │
└─────────────────────────────────┘           │ fact_condition                 │
                                              │ fact_observation               │
         MK = Master Key resolution           │ fact_procedure                 │
         (identifier matching)                │ fact_claim                     │
                                              └────────────────────────────────┘
```

---

## 3. Entity Resolution Strategy

### 3.1 Natural Key Selection (per resource type)

| Resource | Natural Key | Source | Dedup Logic |
|---|---|---|---|
| Patient | SSN or MRN | `identifiers` where system = `http://hl7.org/fhir/sid/us-ssn` or `http://hospital.smarthealthit.org` | Prefer SSN; fall back to MRN if SSN absent |
| Practitioner | NPI | `identifiers` where system = `http://hl7.org/fhir/sid/us-npi` | Exact match on NPI |
| Organization | NPI or name hash | `identifiers` or `resource:name` | NPI preferred; fuzzy match on name as fallback |
| Location | name + managing_org | `resource:name` + `resource:managingOrganization` | Composite key |

### 3.2 Identifier Extraction SQL Pattern

```sql
-- Extract natural key for Patient
SELECT
  patient_uuid,
  COALESCE(
    FILTER(identifiers, x -> x.system = 'http://hl7.org/fhir/sid/us-ssn')[0].value,
    FILTER(identifiers, x -> x.type_code = 'MR')[0].value,
    FILTER(identifiers, x -> x.system LIKE '%hospital%')[0].value
  ) AS patient_natural_key,
  ...
FROM silver.patient
```

### 3.3 Multi-Source Resolution

When the same patient appears from multiple EMRs:
- Same SSN → same `patient_natural_key` → same SCD2 dimension row
- Different bundle_uuid values → multiple silver rows collapse to one gold entity
- Resource VARIANT merged via `ingest_time` ordering (latest wins for SCD1 attributes)

---

## 4. Dimension Tables (SCD Type 2)

### 4.1 dim_patient

```sql
CREATE TABLE gold.dim_patient (
  -- Surrogate key
  patient_key         BIGINT GENERATED ALWAYS AS IDENTITY,
  
  -- Natural key (stable across sources)
  patient_natural_key STRING NOT NULL,  -- SSN or MRN
  
  -- SCD2 validity
  valid_from          TIMESTAMP NOT NULL,
  valid_to            TIMESTAMP,         -- NULL = current
  is_current          BOOLEAN NOT NULL,
  
  -- Flattened demographics (SCD1: overwritten on change)
  family_name         STRING,
  given_name          STRING,
  birth_date          DATE,
  gender              STRING,
  deceased            BOOLEAN,
  address_city        STRING,
  address_state       STRING,
  address_postal_code STRING,
  
  -- SCD2 tracked attributes (new row on change)
  marital_status      STRING,
  
  -- Provenance
  source_patient_uuids  ARRAY<STRING>,  -- all silver patient_uuid values for this entity
  last_updated        TIMESTAMP
)
```

### 4.2 SCD2 Implementation Pattern

Using Auto CDC with Type 2:

```python
dp.create_auto_cdc_flow(
    target="dim_patient",
    source="patient_resolved",
    keys=["patient_natural_key"],
    sequence_by="ingest_time",
    scd_type=2,
    track_history_column_list=["marital_status"],  # columns that trigger new SCD2 row
    stored_as_scd_type_1=["family_name", "given_name", ..., "source_patient_uuids"]
)
```

### 4.3 dim_practitioner

```sql
CREATE TABLE gold.dim_practitioner (
  practitioner_key         BIGINT GENERATED ALWAYS AS IDENTITY,
  practitioner_natural_key STRING NOT NULL,  -- NPI
  valid_from               TIMESTAMP NOT NULL,
  valid_to                 TIMESTAMP,
  is_current               BOOLEAN NOT NULL,
  family_name              STRING,
  given_name               STRING,
  specialty                STRING,           -- from PractitionerRole.code
  organization_natural_key STRING,           -- FK to dim_organization
  last_updated             TIMESTAMP
)
```

### 4.4 dim_organization / dim_location

Similar pattern — NPI-based natural key for organization, composite key for location.

---

## 5. Fact Tables

### 5.1 Design Principles

- **Grain**: one row per deduplicated clinical event
- **Deduplication**: `codes` + `patient_natural_key` + `clinical_event_effective_start` define uniqueness
- **Foreign keys**: reference dimension natural keys (joinable on `is_current = true` for current state)
- **Temporal**: `clinical_event_effective_start/end` from silver (NOT SCD dates)
- **Measures**: extracted from resource VARIANT as needed per fact type

### 5.2 fact_encounter

```sql
CREATE TABLE gold.fact_encounter (
  -- Identity
  encounter_key             BIGINT GENERATED ALWAYS AS IDENTITY,
  encounter_dedup_key       STRING NOT NULL,  -- sha2(patient_nk + class + period_start)
  
  -- Dimension FKs
  patient_natural_key       STRING NOT NULL,
  practitioner_natural_key  STRING,
  organization_natural_key  STRING,
  location_natural_key      STRING,
  
  -- Event attributes
  encounter_class           STRING,     -- ambulatory, emergency, inpatient...
  encounter_type_code       STRING,     -- SNOMED code
  encounter_type_display    STRING,
  status                    STRING,
  
  -- Temporal (clinical event, NOT SCD)
  period_start              TIMESTAMP,
  period_end                TIMESTAMP,
  
  -- Measures
  length_of_stay_hours      DOUBLE,
  
  -- Provenance
  source_encounter_uuids    ARRAY<STRING>,
  last_updated              TIMESTAMP
)
```

### 5.3 fact_condition

```sql
CREATE TABLE gold.fact_condition (
  condition_key             BIGINT GENERATED ALWAYS AS IDENTITY,
  condition_dedup_key       STRING NOT NULL,  -- sha2(patient_nk + code + onset)
  
  patient_natural_key       STRING NOT NULL,
  encounter_dedup_key       STRING,           -- FK to fact_encounter
  
  -- Clinical coding
  condition_code            STRING,     -- SNOMED/ICD-10
  condition_system          STRING,
  condition_display         STRING,
  clinical_status           STRING,     -- active, resolved, etc.
  verification_status       STRING,
  category                  STRING,     -- encounter-diagnosis, problem-list-item
  
  -- Temporal
  onset_datetime            TIMESTAMP,
  abatement_datetime        TIMESTAMP,
  
  source_condition_uuids    ARRAY<STRING>,
  last_updated              TIMESTAMP
)
```

### 5.4 fact_observation

```sql
CREATE TABLE gold.fact_observation (
  observation_key           BIGINT GENERATED ALWAYS AS IDENTITY,
  observation_dedup_key     STRING NOT NULL,  -- sha2(patient_nk + code + effective_dt)
  
  patient_natural_key       STRING NOT NULL,
  encounter_dedup_key       STRING,
  
  -- Clinical coding
  observation_code          STRING,     -- LOINC
  observation_system        STRING,
  observation_display       STRING,
  category                  STRING,     -- vital-signs, laboratory, etc.
  
  -- Value (polymorphic in FHIR)
  value_quantity            DOUBLE,
  value_unit                STRING,
  value_string              STRING,
  value_code                STRING,     -- for coded observations
  
  -- Temporal
  effective_datetime        TIMESTAMP,
  
  source_observation_uuids  ARRAY<STRING>,
  last_updated              TIMESTAMP
)
```

### 5.5 Additional Fact Tables (same pattern)

| Fact Table | Dedup Key | Key Measures |
|---|---|---|
| fact_procedure | patient_nk + code + performed_start | procedure_code, body_site, outcome |
| fact_medication_request | patient_nk + medication_code + authored_on | dosage, refills, days_supply |
| fact_immunization | patient_nk + vaccine_code + occurrence_dt | dose_number, site, route |
| fact_claim | patient_nk + type + billable_start + provider_nk | total_amount, currency |
| fact_diagnostic_report | patient_nk + code + effective_dt | conclusion, result_count |

---

## 6. Implementation Strategy

### 6.1 Pipeline Structure

New pipeline: `fhir_gold_clinical_mart` (SDP, same bundle)

```
fhir_gold_clinical_mart/
├── transformations/
│   ├── entity_resolution.py    # identifier matching + natural key assignment
│   ├── dimensions.py           # dim_patient, dim_practitioner, dim_organization, dim_location
│   ├── fact_encounters.py      # fact_encounter + deduplication
│   ├── fact_clinical.py        # fact_condition, fact_observation, fact_procedure
│   └── fact_financial.py       # fact_claim, fact_eob
```

### 6.2 Entity Resolution Pipeline (first pass)

```python
# entity_resolution.py

@dp.temporary_view()
def patient_resolved():
    return spark.sql("""
        SELECT
            COALESCE(
                FILTER(identifiers, x -> x.system = 'http://hl7.org/fhir/sid/us-ssn')[0].value,
                FILTER(identifiers, x -> x.type_code = 'MR')[0].value
            ) AS patient_natural_key,
            patient_uuid,
            bundle_uuid,
            resource,
            ingest_time
        FROM STREAM(catalog.schema.patient)
        WHERE identifiers IS NOT NULL AND size(identifiers) > 0
    """)
```

### 6.3 Reference Resolution Pattern

Silver `references` array contains URLs like `urn:uuid:abc-123`. Cross-table joins:

```sql
-- Resolve encounter -> patient relationship
SELECT
    e.encounter_uuid,
    p.patient_natural_key
FROM silver.encounter e
JOIN silver.patient p
  ON p.bundle_uuid = e.bundle_uuid  -- same bundle (intra-bundle reference)
  AND p.patient_url = FILTER(e.references, x -> x.field = 'subject')[0].url
```

This leverages:
- `bundle_uuid` for scoping (references are intra-bundle in Synthea)
- `{type}_url` for matching the reference target
- For cross-bundle references (future): resolve via `patient_natural_key` instead

### 6.4 Deduplication Strategy

Clinical event deduplication (same event from multiple bundles):

```sql
-- Dedup key for conditions
sha2(CONCAT(
    patient_natural_key, '|',
    condition_code, '|',
    COALESCE(CAST(onset_datetime AS STRING), 'NULL')
), 256) AS condition_dedup_key
```

When duplicates exist (same dedup_key from multiple sources):
- Latest `ingest_time` wins (most recent source data)
- All source UUIDs preserved in `source_{type}_uuids` array for audit

### 6.5 Streaming vs. Batch

| Component | Mode | Rationale |
|---|---|---|
| Entity resolution views | Streaming (temp view) | Pure filter + transform on silver stream |
| Dimensions (SCD2) | Auto CDC Type 2 | Native SCD2 support in SDP |
| Fact tables | Auto CDC Type 1 | Dedup key as primary key; latest wins |

---

## 7. Schema and Catalog

### 7.1 Target Schema

```yaml
# Per target:
#   dev:   ncqai.dev_matthew_giglia_fhir_gold
#   hedis: ncqai.fhir_gold
#   hls_fde: hls_fde.fhir_gold

variables:
  gold_schema:
    default: "${var.schema}_gold"
```

Alternative: same schema as silver, with `gold_` prefix on table names. TBD.

### 7.2 Table Naming Convention

- Dimensions: `dim_{entity}` (dim_patient, dim_practitioner, dim_organization, dim_location)
- Facts: `fact_{event}` (fact_encounter, fact_condition, fact_observation, ...)
- Bridges: `bridge_{relationship}` (if needed for many-to-many)

---

## 8. Open Questions

1. **Separate schema or same schema?** Gold tables in `fhir_gold` schema (clean separation)
   vs. same schema with `gold_` prefix (simpler job config). Recommend: separate schema.

2. **SCD2 granularity for Patient**: Which attributes trigger a new historical row?
   - Candidates: marital_status, address (moves), deceased flag
   - Demographics that rarely change (birth_date, gender) should be SCD1

3. **Cross-source reference resolution**: Synthea data is single-source (all bundles
   from same synthetic EMR). Multi-EMR scenario needs:
   - Master Patient Index (MPI) logic
   - Probabilistic matching when SSN not available
   - Should this be a separate "matching" pipeline or inline?

4. **Encounter-based fact grain vs. event grain**: Some measures (HEDIS) need
   encounter-level aggregation. Others (lab trending) need observation-level.
   Current design: one fact per event type. Add encounter-level aggregation in marts?

5. **Performance at scale**: 69.8M observations → fact_observation will be large.
   Partition strategy: `effective_datetime` (monthly? yearly?).
   Liquid clustering candidate: `(patient_natural_key, observation_code, effective_datetime)`

6. **HEDIS measure compatibility**: The gold layer must support HEDIS value set
   membership queries (`codes.code IN (SELECT code FROM hedis_value_set WHERE ...)`)
   — does the array-of-structs `codes` column support this efficiently, or should
   we flatten codes into the fact table?

7. **Incremental entity resolution**: When a new bundle arrives with a known patient
   (same SSN), Auto CDC handles the upsert. But if identifiers CHANGE (patient gets
   a new MRN), how do we handle natural key evolution?

---

## 9. Dependencies and Prerequisites

- Silver v3 pipeline: COMPLETE (177M rows, 24 tables, all extractions verified)
- Identifier quality: verified (Patient SSN/MRN populated, Practitioner NPI populated)
- Reference resolution: verified (intra-bundle joins via bundle_uuid + url work)
- Auto CDC Type 2: available in SDP PREVIEW channel
- Separate gold schema: needs creation (`CREATE SCHEMA ncqai.dev_matthew_giglia_fhir_gold`)

---

## 10. Phased Rollout

| Phase | Scope | Deliverable |
|---|---|---|
| Phase 1 | Entity resolution + dim_patient | Prove MPI pattern works at scale |
| Phase 2 | All dimensions + fact_encounter | Star schema with encounter grain |
| Phase 3 | Clinical facts (condition, observation, procedure) | Full clinical mart |
| Phase 4 | Financial facts (claim, EOB) | Revenue cycle analytics |
| Phase 5 | HEDIS measure views | Quality measure reporting |

---

## 11. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| SSN not available (real-world data) | Can't resolve patients | Probabilistic matching; MPI service integration |
| Auto CDC Type 2 bugs in PREVIEW channel | SCD2 rows incorrect | Monitor `num_upserted_rows` metrics; validate with point-in-time queries |
| Observation table too large (69.8M -> gold) | Slow queries | Liquid clustering on (patient_nk, code, effective_dt); Z-order fallback |
| Identifier format variation across sources | False non-matches | Normalize identifiers (strip dashes from SSN, standardize NPI format) |
| Circular references in FHIR | Infinite loops in resolution | Cap resolution depth; entity resources only (no clinical event self-refs) |
