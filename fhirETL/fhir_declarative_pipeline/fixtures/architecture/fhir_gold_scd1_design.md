# FHIR Gold (SCD1) — Table Schema Design

> Status: IMPLEMENTED
> Created: 2026-07-16
> Context: Dual-gold architecture; these tables live in the FHIR schema alongside
>          bronze and silver, providing entity-resolved current-state views suitable
>          for FHIR API serving (HAPI, Smart-on-FHIR, Lakebase).

---

## 1. Purpose

The FHIR Gold layer answers: **"What is the current, canonical state of each real-world
entity?"** — shaped so a FHIR server can reconstruct valid JSON for `$read`, `$search`,
and `$export` operations without additional transformation.

Key properties:
- **SCD Type 1** — latest state wins, no history tracking (that's the Clinical Mart's job)
- **Entity-resolved** — one row per real-world patient/encounter/condition regardless of
  how many bundles/sources contributed to it
- **Resource VARIANT preserved** — full FHIR JSON stored for lossless reconstitution
- **Same schema** as bronze/silver — no additional schema resource needed
- **Serves as input** to the Clinical Mart pipeline (which reshapes into dim_/fact_)

---

## 2. Pipeline Location

These tables are produced by the existing `fhir_resource_silver_etl` pipeline (extended),
NOT by a separate pipeline. They are the natural "resolved" output of silver processing.

```
fhir_resource_silver_etl/
├── transformations/
│   ├── silver.py                    # Existing: per-type extract + Auto CDC Type 1
│   ├── entity_resolution.py         # NEW: identifier normalization + natural key assignment
│   └── fhir_gold.py                 # NEW: resolved _gold tables via Auto CDC Type 1
```

Alternative: a lightweight separate pipeline. Decision depends on whether the entity
resolution logic needs different compute/refresh cadence than silver. For now, same
pipeline keeps the dependency chain simple.

---

## 3. Table Schemas

### Conventions

- Table suffix: `_gold` (e.g., `patient_gold`, `encounter_gold`)
- Primary key: `{type}_natural_key` (deterministic, stable across sources)
- Sequence column: `resource_last_updated` (from `resource:meta.lastUpdated`)
- Full resource preserved as `resource VARIANT` for FHIR JSON reconstruction
- Scalar columns extracted for indexing, filtering, and reference resolution
- Liquid Clustering key noted per table

---

### 3.1 patient_gold

The canonical patient record. One row per real-world patient.

```sql
CREATE OR REFRESH STREAMING TABLE patient_gold (
  -- Identity
  patient_natural_key       STRING NOT NULL
    COMMENT 'Resolved patient identity (SSN preferred, MRN fallback). Stable across sources.',
  
  -- FHIR resource identity
  patient_url               STRING
    COMMENT 'Most recent FHIR fullUrl for this patient (for reference resolution)',
  
  -- Demographics (scalar extraction for filtering)
  family_name               STRING,
  given_name                STRING,
  birth_date                DATE,
  gender                    STRING,
  deceased                  BOOLEAN,
  address_city              STRING,
  address_state             STRING,
  address_postal_code       STRING,
  marital_status            STRING,
  
  -- Identifiers (all known identifiers for this entity)
  identifiers               ARRAY<STRUCT<system STRING, value STRING, type_code STRING>>
    COMMENT 'All identifiers from all contributing silver records',
  
  -- Provenance
  source_patient_uuids      ARRAY<STRING>
    COMMENT 'All silver patient_uuid values that resolved to this entity',
  source_bundle_uuids       ARRAY<STRING>
    COMMENT 'All bundle_uuid values that contributed data',
  resource_last_updated     TIMESTAMP NOT NULL
    COMMENT 'meta.lastUpdated from the most recent contributing resource',
  
  -- Full FHIR resource (for API reconstitution)
  resource                  VARIANT NOT NULL
    COMMENT 'Complete FHIR Patient resource as VARIANT. Source of truth for $read.',

  CONSTRAINT pk_patient_gold PRIMARY KEY (patient_natural_key)
)
CLUSTER BY (patient_natural_key, address_state)
COMMENT 'Entity-resolved canonical patient. SCD1 — latest state wins. One row per real-world patient.';
```

**Auto CDC configuration:**
```python
dp.create_auto_cdc_flow(
    target="patient_gold",
    source="patient_resolved",
    keys=["patient_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)
```

---

### 3.2 practitioner_gold

```sql
CREATE OR REFRESH STREAMING TABLE practitioner_gold (
  practitioner_natural_key  STRING NOT NULL
    COMMENT 'NPI (National Provider Identifier). Unique per practitioner.',
  
  practitioner_url          STRING,
  family_name               STRING,
  given_name                STRING,
  specialty_code            STRING
    COMMENT 'Primary specialty from PractitionerRole or qualification',
  specialty_display         STRING,
  
  identifiers               ARRAY<STRUCT<system STRING, value STRING, type_code STRING>>,
  source_practitioner_uuids ARRAY<STRING>,
  resource_last_updated     TIMESTAMP NOT NULL,
  resource                  VARIANT NOT NULL,

  CONSTRAINT pk_practitioner_gold PRIMARY KEY (practitioner_natural_key)
)
CLUSTER BY (practitioner_natural_key)
COMMENT 'Entity-resolved canonical practitioner. Keyed on NPI.';
```

---

### 3.3 organization_gold

```sql
CREATE OR REFRESH STREAMING TABLE organization_gold (
  organization_natural_key  STRING NOT NULL
    COMMENT 'NPI or sha2(name) for orgs without NPI',
  
  organization_url          STRING,
  name                      STRING,
  type_code                 STRING
    COMMENT 'Organization type (prov, dept, ins, etc.)',
  type_display              STRING,
  address_city              STRING,
  address_state             STRING,
  
  identifiers               ARRAY<STRUCT<system STRING, value STRING, type_code STRING>>,
  source_organization_uuids ARRAY<STRING>,
  resource_last_updated     TIMESTAMP NOT NULL,
  resource                  VARIANT NOT NULL,

  CONSTRAINT pk_organization_gold PRIMARY KEY (organization_natural_key)
)
CLUSTER BY (organization_natural_key, address_state)
COMMENT 'Entity-resolved canonical organization. Keyed on NPI or name hash.';
```

---

### 3.4 location_gold

```sql
CREATE OR REFRESH STREAMING TABLE location_gold (
  location_natural_key      STRING NOT NULL
    COMMENT 'sha2(name + managing_org_nk). Composite key.',
  
  location_url              STRING,
  name                      STRING,
  managing_organization_nk  STRING
    COMMENT 'FK to organization_gold.organization_natural_key',
  address_city              STRING,
  address_state             STRING,
  address_postal_code       STRING,
  
  source_location_uuids     ARRAY<STRING>,
  resource_last_updated     TIMESTAMP NOT NULL,
  resource                  VARIANT NOT NULL,

  CONSTRAINT pk_location_gold PRIMARY KEY (location_natural_key)
)
CLUSTER BY (location_natural_key)
COMMENT 'Entity-resolved canonical location. Keyed on name + managing org.';
```

---

### 3.5 encounter_gold

```sql
CREATE OR REFRESH STREAMING TABLE encounter_gold (
  encounter_natural_key     STRING NOT NULL
    COMMENT 'sha2(patient_nk + class + type_code + period_start). Dedup key.',
  
  encounter_url             STRING,
  patient_natural_key       STRING NOT NULL
    COMMENT 'FK to patient_gold',
  practitioner_natural_key  STRING
    COMMENT 'FK to practitioner_gold (primary participant)',
  organization_natural_key  STRING
    COMMENT 'FK to organization_gold (serviceProvider)',
  location_natural_key      STRING
    COMMENT 'FK to location_gold',
  
  -- Encounter attributes
  encounter_class           STRING     COMMENT 'AMB, EMER, IMP, etc.',
  encounter_type_code       STRING     COMMENT 'SNOMED encounter type',
  encounter_type_display    STRING,
  status                    STRING     COMMENT 'finished, in-progress, etc.',
  period_start              TIMESTAMP,
  period_end                TIMESTAMP,
  reason_code               STRING     COMMENT 'Primary reason for visit (SNOMED)',
  reason_display            STRING,
  
  -- References (for FHIR $include support)
  references                ARRAY<STRUCT<field STRING, url STRING, type STRING, display STRING>>,
  
  source_encounter_uuids    ARRAY<STRING>,
  resource_last_updated     TIMESTAMP NOT NULL,
  resource                  VARIANT NOT NULL,

  CONSTRAINT pk_encounter_gold PRIMARY KEY (encounter_natural_key)
)
CLUSTER BY (patient_natural_key, period_start)
COMMENT 'Entity-resolved canonical encounter. One row per real-world visit.';
```

---

### 3.6 condition_gold

```sql
CREATE OR REFRESH STREAMING TABLE condition_gold (
  condition_natural_key     STRING NOT NULL
    COMMENT 'sha2(patient_nk + code + onset_datetime). Dedup key.',
  
  condition_url             STRING,
  patient_natural_key       STRING NOT NULL,
  encounter_natural_key     STRING
    COMMENT 'FK to encounter_gold (context of diagnosis)',
  
  -- Clinical coding
  code                      STRING     COMMENT 'Primary code (SNOMED or ICD-10)',
  code_system               STRING     COMMENT 'http://snomed.info/sct or ICD-10',
  code_display              STRING,
  category                  STRING     COMMENT 'encounter-diagnosis, problem-list-item',
  clinical_status           STRING     COMMENT 'active, resolved, remission, etc.',
  verification_status       STRING,
  
  -- Temporal
  onset_datetime            TIMESTAMP,
  abatement_datetime        TIMESTAMP,
  
  -- All codings (for multi-system scenarios)
  codes                     ARRAY<STRUCT<system STRING, code STRING, display STRING>>,
  
  source_condition_uuids    ARRAY<STRING>,
  resource_last_updated     TIMESTAMP NOT NULL,
  resource                  VARIANT NOT NULL,

  CONSTRAINT pk_condition_gold PRIMARY KEY (condition_natural_key)
)
CLUSTER BY (patient_natural_key, code)
COMMENT 'Entity-resolved canonical condition. One row per unique diagnosis.';
```

---

### 3.7 observation_gold

```sql
CREATE OR REFRESH STREAMING TABLE observation_gold (
  observation_natural_key   STRING NOT NULL
    COMMENT 'sha2(patient_nk + code + effective_datetime). Dedup key.',
  
  observation_url           STRING,
  patient_natural_key       STRING NOT NULL,
  encounter_natural_key     STRING,
  
  -- Clinical coding
  code                      STRING     COMMENT 'LOINC code',
  code_system               STRING,
  code_display              STRING,
  category                  STRING     COMMENT 'vital-signs, laboratory, survey, etc.',
  
  -- Value (polymorphic)
  value_quantity            DOUBLE,
  value_unit                STRING,
  value_string              STRING,
  value_code                STRING     COMMENT 'For coded observations (e.g., blood type)',
  value_raw                 VARIANT    COMMENT 'Full value[x] for types beyond quantity/string/code',
  
  -- Temporal
  effective_datetime        TIMESTAMP,
  
  -- Reference ranges (for lab results)
  reference_range_low       DOUBLE,
  reference_range_high      DOUBLE,
  
  codes                     ARRAY<STRUCT<system STRING, code STRING, display STRING>>,
  source_observation_uuids  ARRAY<STRING>,
  resource_last_updated     TIMESTAMP NOT NULL,
  resource                  VARIANT NOT NULL,

  CONSTRAINT pk_observation_gold PRIMARY KEY (observation_natural_key)
)
CLUSTER BY (patient_natural_key, code, effective_datetime)
COMMENT 'Entity-resolved canonical observation. One row per unique lab/vital measurement.';
```

---

### 3.8 procedure_gold

```sql
CREATE OR REFRESH STREAMING TABLE procedure_gold (
  procedure_natural_key     STRING NOT NULL
    COMMENT 'sha2(patient_nk + code + performed_start). Dedup key.',
  
  procedure_url             STRING,
  patient_natural_key       STRING NOT NULL,
  encounter_natural_key     STRING,
  practitioner_natural_key  STRING
    COMMENT 'Performer',
  
  code                      STRING     COMMENT 'CPT or SNOMED',
  code_system               STRING,
  code_display              STRING,
  status                    STRING     COMMENT 'completed, in-progress, etc.',
  
  performed_start           TIMESTAMP,
  performed_end             TIMESTAMP,
  body_site_code            STRING,
  body_site_display         STRING,
  
  codes                     ARRAY<STRUCT<system STRING, code STRING, display STRING>>,
  source_procedure_uuids    ARRAY<STRING>,
  resource_last_updated     TIMESTAMP NOT NULL,
  resource                  VARIANT NOT NULL,

  CONSTRAINT pk_procedure_gold PRIMARY KEY (procedure_natural_key)
)
CLUSTER BY (patient_natural_key, code)
COMMENT 'Entity-resolved canonical procedure. One row per unique procedure event.';
```

---

### 3.9 medication_request_gold

```sql
CREATE OR REFRESH STREAMING TABLE medication_request_gold (
  medication_request_natural_key  STRING NOT NULL
    COMMENT 'sha2(patient_nk + medication_code + authored_on). Dedup key.',
  
  medication_request_url    STRING,
  patient_natural_key       STRING NOT NULL,
  encounter_natural_key     STRING,
  practitioner_natural_key  STRING
    COMMENT 'Prescriber',
  
  medication_code           STRING     COMMENT 'RxNorm or NDC',
  medication_system         STRING,
  medication_display        STRING,
  status                    STRING     COMMENT 'active, completed, stopped, etc.',
  intent                    STRING     COMMENT 'order, plan, proposal',
  
  authored_on               TIMESTAMP,
  dosage_text               STRING,
  
  codes                     ARRAY<STRUCT<system STRING, code STRING, display STRING>>,
  source_medication_request_uuids ARRAY<STRING>,
  resource_last_updated     TIMESTAMP NOT NULL,
  resource                  VARIANT NOT NULL,

  CONSTRAINT pk_medication_request_gold PRIMARY KEY (medication_request_natural_key)
)
CLUSTER BY (patient_natural_key, medication_code)
COMMENT 'Entity-resolved canonical medication request. One row per unique prescription.';
```

---

### 3.10 immunization_gold

```sql
CREATE OR REFRESH STREAMING TABLE immunization_gold (
  immunization_natural_key  STRING NOT NULL
    COMMENT 'sha2(patient_nk + vaccine_code + occurrence_datetime). Dedup key.',
  
  immunization_url          STRING,
  patient_natural_key       STRING NOT NULL,
  encounter_natural_key     STRING,
  
  vaccine_code              STRING     COMMENT 'CVX code',
  vaccine_system            STRING,
  vaccine_display           STRING,
  status                    STRING     COMMENT 'completed, entered-in-error, etc.',
  
  occurrence_datetime       TIMESTAMP,
  
  codes                     ARRAY<STRUCT<system STRING, code STRING, display STRING>>,
  source_immunization_uuids ARRAY<STRING>,
  resource_last_updated     TIMESTAMP NOT NULL,
  resource                  VARIANT NOT NULL,

  CONSTRAINT pk_immunization_gold PRIMARY KEY (immunization_natural_key)
)
CLUSTER BY (patient_natural_key, vaccine_code)
COMMENT 'Entity-resolved canonical immunization. One row per unique vaccination event.';
```

---

### 3.11 claim_gold / explanation_of_benefit_gold (Phase 4)

Deferred to Phase 4. Same pattern — natural key based on patient + type + service period.
`Account` and `Coverage` from the new resource types also land here.

---

## 4. Entity Resolution Views (Shared)

The entity resolution logic produces temporary views consumed by BOTH the `_gold` tables
above AND (downstream) the Clinical Mart pipeline. This avoids duplicating matching logic.

```python
# entity_resolution.py — produces these temp views:

@dp.temporary_view()
def patient_resolved():
    """
    Resolves patient identity from silver.patient.
    Output: one row per (patient_natural_key, patient_uuid) with the resolved NK
    and extracted scalar columns. Fed into patient_gold via Auto CDC Type 1.
    """
    ...

@dp.temporary_view()
def practitioner_resolved():
    """Resolves on NPI from silver.practitioner."""
    ...

@dp.temporary_view()
def encounter_resolved():
    """
    Joins silver.encounter with patient_resolved to get patient_natural_key,
    then generates encounter_natural_key = sha2(patient_nk + class + type + period_start).
    """
    ...
```

### Identifier Normalization (from design doc section 8a.3)

Applied inside entity resolution before natural key assignment:

```sql
CASE
  WHEN x.system IN ('http://hl7.org/fhir/sid/us-ssn', 'urn:oid:2.16.840.1.113883.4.1')
    THEN STRUCT('SSN' AS system_canonical, x.value AS value)
  WHEN x.system LIKE '%us-npi%' OR x.system = 'urn:oid:2.16.840.1.113883.4.6'
    THEN STRUCT('NPI' AS system_canonical, x.value AS value)
  WHEN x.type_code = 'MR'
    THEN STRUCT('MRN' AS system_canonical, x.value AS value)
END
```

---

## 5. Reference Resolution Pattern

FHIR Gold tables maintain FK relationships via `_natural_key` columns. Resolution handles
both reference styles:

| Style | Example | Resolution |
|---|---|---|
| Intra-bundle (urn:uuid) | `urn:uuid:abc-123` | Join on `bundle_uuid + {type}_url` |
| Absolute | `Patient/12345` | Join on `resource:id` field |

```sql
-- In encounter_resolved: resolve patient reference
COALESCE(
  -- Intra-bundle reference
  p_intra.patient_natural_key,
  -- Absolute reference (Patient/id)
  p_abs.patient_natural_key
) AS patient_natural_key
```

---

## 6. Auto CDC Configuration Summary

All FHIR Gold tables use **Auto CDC Type 1** (SCD1 — latest wins):

| Table | keys | sequence_by |
|---|---|---|
| patient_gold | `[patient_natural_key]` | `resource_last_updated` |
| practitioner_gold | `[practitioner_natural_key]` | `resource_last_updated` |
| organization_gold | `[organization_natural_key]` | `resource_last_updated` |
| location_gold | `[location_natural_key]` | `resource_last_updated` |
| encounter_gold | `[encounter_natural_key]` | `resource_last_updated` |
| condition_gold | `[condition_natural_key]` | `resource_last_updated` |
| observation_gold | `[observation_natural_key]` | `resource_last_updated` |
| procedure_gold | `[procedure_natural_key]` | `resource_last_updated` |
| medication_request_gold | `[medication_request_natural_key]` | `resource_last_updated` |
| immunization_gold | `[immunization_natural_key]` | `resource_last_updated` |

All use `resource:meta.lastUpdated` as the sequence column (NOT `ingest_time`).

---

## 7. patient_identity_bridge

Shared between FHIR Gold and Clinical Mart. Maps all known identifiers to a canonical NK:

```sql
CREATE OR REFRESH STREAMING TABLE patient_identity_bridge (
  patient_natural_key       STRING NOT NULL
    COMMENT 'Canonical NK (links to patient_gold and dim_patient)',
  identifier_system         STRING NOT NULL
    COMMENT 'Normalized system: SSN, MRN, NPI, or original URI',
  identifier_value          STRING NOT NULL
    COMMENT 'The identifier value in that system',
  source_patient_uuid       STRING NOT NULL
    COMMENT 'Silver patient_uuid that contributed this identifier',
  first_seen                TIMESTAMP,
  last_seen                 TIMESTAMP,

  CONSTRAINT pk_identity_bridge PRIMARY KEY (patient_natural_key, identifier_system, identifier_value)
)
CLUSTER BY (patient_natural_key)
COMMENT 'Maps all known identifiers to a canonical patient_natural_key. Grows monotonically.';
```

This table grows (never shrinks) as new identifiers are discovered for existing patients.
It enables:
- MRN → patient lookup for FHIR API `$search`
- SSN → patient lookup for cross-system matching
- Audit trail of which silver records contributed each identifier

---

## 8. Downstream Consumers

### 8.1 Clinical Mart Pipeline

The `fhir_gold_clinical_mart` pipeline reads FROM these `_gold` tables to produce
`dim_patient`, `fact_encounter`, etc. The Clinical Mart adds:
- SCD Type 2 temporal tracking
- Surrogate keys (sha2-based)
- Denormalized dimension attributes on facts
- Bridge tables for many-to-many relationships

### 8.2 FHIR Server Loading (Future)

A scheduled job exports `_gold` table content as NDJSON (using the `resource` VARIANT column)
and loads it into HAPI FHIR or Lakebase via `$import`:

```python
# Future: export patient_gold → NDJSON → HAPI $import
df = spark.table("catalog.schema.patient_gold")
ndjson = df.select(to_json(col("resource")).alias("json"))
ndjson.write.mode("overwrite").text("/Volumes/.../fhir_export/Patient.ndjson")
```

### 8.3 Smart-on-FHIR / Lakebase (Future)

When Databricks Lakebase supports FHIR natively, these tables become the direct backing
store — no ETL export needed. The `resource VARIANT` column already contains conformant
FHIR JSON.

---

## 9. Open Design Decisions

1. **Same pipeline or separate?** — Currently proposed as extension of `fhir_resource_silver_etl`.
   If entity resolution needs different refresh cadence or compute, split to a separate
   lightweight pipeline that reads silver via STREAM.

2. **source_{type}_uuids accumulation** — Auto CDC Type 1 overwrites arrays. Options:
   - Pre-aggregate via `collect_set` in entity resolution view (requires stateful streaming)
   - Separate `patient_identity_bridge` handles the many-to-one mapping (preferred)
   - Accept that `source_patient_uuids` only reflects the LATEST contributing record

3. **Observation volume** — 69.8M silver observations → still large after dedup. May need
   partition-level optimization or a `recent_observations_gold` view (last 2 years) for
   FHIR server use cases.

4. **DiagnosticReport, CarePlan, AllergyIntolerance** — Not yet in silver (27 types currently).
   Add `_gold` tables when silver discovers them. Same pattern applies.
