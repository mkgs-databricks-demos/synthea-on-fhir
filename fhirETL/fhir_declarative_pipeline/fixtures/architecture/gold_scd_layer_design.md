# Gold Layer — Design Document (Dual-Gold Architecture)

> Status: IMPLEMENTED (FHIR Gold layer complete; Clinical Mart LIVE — 10 tables, integrity verified)
> Created: 2026-06-12
> Updated: 2026-06-13
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

## 1a. Dual-Gold Architecture

This project produces TWO gold layers with distinct purposes:

### FHIR Gold (same schema as bronze/silver)

Entity-resolved, document-oriented tables optimized for **FHIR API serving** (HAPI FHIR,
Smart-on-FHIR, Lakebase). One canonical row per real-world entity, SCD Type 1 (latest
state wins). Preserves full `resource VARIANT` for lossless FHIR JSON reconstitution.

- Schema: `{catalog}.{fhir_schema}` (co-located with bronze/silver)
- Table naming: `{resource_type}_gold` (e.g., `patient_gold`, `encounter_gold`)
- Consumer: FHIR servers, $export, Smart-on-FHIR apps, data exchange partners
- Pipeline: `fhir_resource_silver_etl` (extends existing silver pipeline)
- Grain: one row per resolved real-world entity (latest version)

### Clinical Mart (separate schema)

Dimensional model optimized for **analytical queries**, population health, quality
measures, and dashboards. Star schema with surrogate keys, dedup keys, temporal grain.

- Schema: `{catalog}.{clinical_mart_schema}` (separate, bundle-managed)
- Table naming: `dim_{entity}`, `fact_{event}`, `bridge_{relationship}`
- Consumer: Dashboards, Genie spaces, HEDIS (via ncq-ai), population health analytics
- Pipeline: `fhir_gold_clinical_mart` (separate pipeline, reads from FHIR Gold)
- Grain: one row per deduplicated clinical event (facts), temporal history (dims)

### Dependency Chain

```
ingestion (bronze) → silver → FHIR Gold (entity resolution + SCD1) → Clinical Mart (dimensional reshape)
                                  ↓                                           ↓
                           FHIR API servers                         Dashboards, HEDIS, Genie
```

### Schema Resolution

| Layer | dev | hedis | hls_fde |
|---|---|---|---|
| Bronze/Silver/FHIR Gold | `dev_{user}_fhir` | `fhir` | `fhir` |
| Clinical Mart | `dev_{user}_clinical_mart` | `clinical_mart` | `clinical_mart` |

### Bundle Variables

```yaml
variables:
  schema:                 # Base FHIR schema (bronze, silver, FHIR Gold)
  clinical_mart_schema:   # Separate schema for dimensional model
```

FHIR Gold tables publish into `${resources.schemas.fhir_schema.name}` — no additional
schema needed. Clinical Mart tables publish into `${resources.schemas.clinical_mart_schema.name}`.

---

## 2. Architecture Overview (Clinical Mart)

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

## 4. Dimension Tables (SCD Type 1 — IMPLEMENTED)

> **Implementation note**: The original design proposed SCD2 for dimensions.
> The actual implementation uses **SCD1** (latest state wins) for all tables,
> including dimensions. This simplifies the architecture and aligns with the
> FHIR Gold layer pattern. SCD2 may be added in future if temporal history
> is needed for HEDIS measurement years.

### 4.1 dim_patient

```sql
-- Actual implementation: SCD1, keyed on patient_natural_key
CREATE STREAMING TABLE dim_patient (
  patient_natural_key STRING NOT NULL,  -- SSN or MRN
  full_name           STRING,
  family_name         STRING,
  given_name          STRING,
  birth_date          DATE,
  gender              STRING,
  deceased            BOOLEAN,
  age_years           INT,              -- computed: datediff(years, birth_date, current_date)
  age_band            STRING,           -- computed: 0-17, 18-34, 35-49, 50-64, 65+
  address_city        STRING,
  address_state       STRING,
  address_postal_code STRING,
  marital_status      STRING,
  resource_last_updated TIMESTAMP NOT NULL
)
```

### 4.2 SCD1 Implementation Pattern (ACTUAL)

Using Auto CDC with Type 1:

```python
dp.create_auto_cdc_flow(
    target="dim_patient",
    source="dim_patient_src",
    keys=["patient_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
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

### 7.1 Target Schemas (Dual-Gold)

**FHIR Gold** tables live in the same schema as silver (no additional schema):

| Target | FHIR Gold Schema | Example Table |
|---|---|---|
| dev | `ncqai.dev_matthew_giglia_fhir` | `patient_gold`, `encounter_gold` |
| hedis | `ncqai.fhir` | `patient_gold`, `encounter_gold` |
| hls_fde | `hls_fde.fhir` | `patient_gold`, `encounter_gold` |

**Clinical Mart** tables live in a separate schema:

| Target | Clinical Mart Schema | Example Table |
|---|---|---|
| dev | `ncqai.dev_matthew_giglia_clinical_mart` | `dim_patient`, `fact_encounter` |
| hedis | `ncqai.clinical_mart` | `dim_patient`, `fact_encounter` |
| hls_fde | `hls_fde.clinical_mart` | `dim_patient`, `fact_encounter` |

```yaml
variables:
  clinical_mart_schema:
    description: Schema for the clinical mart (dimensional model).
# Resource reference for resolved name: ${resources.schemas.clinical_mart_schema.name}
```

### 7.2 Table Naming Convention

- Dimensions: `dim_{entity}` (dim_patient, dim_practitioner, dim_organization, dim_location)
- Facts: `fact_{event}` (fact_encounter, fact_condition, fact_observation, ...)
- Bridges: `bridge_{relationship}` (if needed for many-to-many)

---

## 8. Open Questions — RESOLVED (2026-06-12)

1. **Separate schema or same schema?**
   RESOLVED: Separate schema (`fhir_gold`). Clean lineage, independent permissions,
   no naming conflicts with silver tables.

2. **SCD2 granularity for Patient**:
   RESOLVED: Track `marital_status` and `address_state` as SCD2 (HEDIS measures
   stratify by geography). Keep `birth_date`, `gender`, `deceased` as SCD1.

3. **Cross-source reference resolution**:
   RESOLVED: Separate `entity_resolution.py` file producing `patient_resolved` as
   a temp view. Isolates matching logic and makes it swappable (deterministic now,
   probabilistic later via MPI service). NOT inline in dimension logic.

4. **Encounter-based fact grain vs. event grain**:
   RESOLVED: Keep both. Facts at event grain (one row per clinical event).
   Add `bridge_encounter_condition`, `bridge_encounter_observation` for
   encounter-level aggregation in HEDIS measure views.

5. **Performance at scale**:
   RESOLVED: Liquid Clustering on `(patient_natural_key, observation_code)` for
   fact_observation. The `effective_datetime` range scan is served by Delta's
   file-level min/max stats without needing it in the cluster key.

6. **HEDIS measure compatibility**:
   RESOLVED: Gold fact tables should explode the primary code (first in codes array)
   into scalar columns (`condition_code`, `condition_system`) for direct predicate
   pushdown. Retain `codes` as secondary array only for multi-coding scenarios
   (dual SNOMED + ICD-10 coded conditions).

7. **Incremental entity resolution / natural key evolution**:
   RESOLVED: Use a `patient_identity_bridge` table mapping all known identifiers
   to a canonical `patient_natural_key`. When a new MRN appears for an existing SSN,
   the bridge grows but the natural key stays stable.

---

## 8a. Implementation Corrections (from architectural review 2026-06-12)

Critical changes required before implementation:

### 8a.1 Replace identity columns with deterministic surrogates

`BIGINT GENERATED ALWAYS AS IDENTITY` is NOT supported in SDP streaming tables.
Use deterministic surrogates instead:

```sql
-- Dimension surrogate key
sha2(CONCAT(patient_natural_key, '|', CAST(valid_from AS STRING)), 256) AS patient_key

-- Fact surrogate key (same as dedup key)
sha2(CONCAT(patient_nk, '|', condition_code, '|', COALESCE(CAST(onset AS STRING), 'NULL')), 256)
```

### 8a.2 Use resource:meta.lastUpdated as SCD2 sequence column

`ingest_time` reflects when data arrived, not when the resource was authored.
For SCD2 dimensions, the sequence column must reflect business time:

```python
dp.create_auto_cdc_flow(
    target="dim_patient",
    source="patient_resolved",
    keys=["patient_natural_key"],
    sequence_by=col("resource_last_updated"),  # NOT ingest_time
    stored_as_scd_type=2,
    track_history_column_list=["marital_status", "address_state"],
    except_column_list=["resource_last_updated"]
)
```

Extract in the entity resolution view:
```sql
try_variant_get(resource, '$.meta.lastUpdated', 'STRING') AS resource_last_updated
```

### 8a.3 Normalize identifier system URIs

Real-world data uses variant URI forms for the same system. Normalize before matching:

```sql
-- In entity_resolution.py patient_resolved view
CASE
  WHEN x.system IN ('http://hl7.org/fhir/sid/us-ssn', 'urn:oid:2.16.840.1.113883.4.1')
    THEN 'SSN'
  WHEN x.system LIKE '%us-npi%' OR x.system = 'urn:oid:2.16.840.1.113883.4.6'
    THEN 'NPI'
  WHEN x.type_code = 'MR'
    THEN 'MRN'
END AS identifier_type
```

### 8a.4 source_{type}_uuids accumulation

`source_patient_uuids` is an accumulating array. SCD1 overwrites, it does not append.
Pre-aggregate in the resolution view using `collect_set`:

```sql
collect_set(patient_uuid) OVER (
    PARTITION BY patient_natural_key
) AS source_patient_uuids
```

Note: window functions require materialized view or batch step. Consider making
`source_patient_uuids` a separate lookup table rather than embedding in the dimension.

### 8a.5 Handle absolute and relative FHIR references

Synthea uses `urn:uuid:...` (intra-bundle). Real EMR exports use absolute URLs
(`Patient/12345`). The reference resolution must handle both:

```sql
CASE
  WHEN ref_url LIKE 'urn:uuid:%' THEN
    -- Intra-bundle: join on bundle_uuid + url
    p.patient_url = ref_url AND p.bundle_uuid = e.bundle_uuid
  WHEN ref_url LIKE 'Patient/%' THEN
    -- Absolute: join on resource id
    try_variant_get(p.resource, '$.id', 'STRING') = SUBSTRING(ref_url, 9)
END
```

### 8a.6 Observation.valueString and polymorphic value[x]

The latest data introduced `Observation.valueString`. FHIR defines additional
value types: `valueBoolean`, `valueDateTime`, `valuePeriod`, `valueRatio`.

Add a `value_raw VARIANT` escape hatch to fact_observation:

```sql
`value_raw` VARIANT
    COMMENT 'Full value[x] element as VARIANT for types beyond quantity/string/code.'
```

### 8a.7 Drop is_current column

`is_current BOOLEAN` is redundant with `valid_to IS NULL`. Drop it unless query
performance testing shows measurable benefit from Liquid Clustering on
`(patient_natural_key, is_current)`. If retained, it must be maintained as a
computed column or post-CDC update — Auto CDC Type 2 does not natively manage it.

### 8a.8 Encounter dedup key refinement

Current: `sha2(patient_nk + class + period_start)`. Risk: two ambulatory visits
on the same day would collide. Add encounter type code:

```sql
sha2(CONCAT(
    patient_natural_key, '|',
    encounter_class, '|',
    COALESCE(encounter_type_code, 'UNTYPED'), '|',
    CAST(period_start AS STRING)
), 256) AS encounter_dedup_key
```

### 8a.9 New resource types discovered (2026-06-12)

The latest ingestion introduced 3 new FHIR resource types:
- **Account** (6 columns) — billing accounts, references Patient + Coverage
- **Coverage** (10 columns) — insurance policy details, references Patient
- **MessageHeader** (9 columns) — bundle routing metadata, non-clinical

And 5 new columns in existing types:
- `Encounter.account` — reference to Account (new reference extraction opportunity)
- `ExplanationOfBenefit.referral` — direct practitioner reference
- `Observation.valueString` — polymorphic value expansion (handled in 8a.6)
- `Patient.active` — boolean, no impact on gold design
- `Patient.text` — narrative XHTML, cosmetic

Gold layer impact: `Account` and `Coverage` feed into Phase 4 (financial facts).
`MessageHeader` is routing metadata — excluded from clinical mart.

---

## 9. Dependencies and Prerequisites

- Silver v3 pipeline: COMPLETE (177M rows, 24 tables → now 27 types with Account/Coverage/MessageHeader)
- Identifier quality: verified (Patient SSN/MRN populated, Practitioner NPI populated)
- Reference resolution: verified (intra-bundle joins via bundle_uuid + url work)
- Auto CDC Type 2: available in SDP PREVIEW channel
- Clinical mart schema: bundle-managed (`resources.schemas.clinical_mart_schema`); FHIR Gold uses existing fhir schema
- Identifier normalization: implement URI aliasing before Phase 1
- Surrogate key pattern: deterministic sha2-based (identity columns not supported in SDP)

---

## 10. Phased Rollout

| Phase | Scope | Deliverable |
|---|---|---|
| Phase 1 | Entity resolution + dim_patient + patient_identity_bridge | Prove MPI pattern works at scale |
| Phase 2 | All dimensions + fact_encounter + bridge tables | Star schema with encounter grain |
| Phase 3 | Clinical facts (condition, observation, procedure) | Full clinical mart |
| Phase 4 | Financial facts (claim, EOB, Account, Coverage) | Revenue cycle analytics |
| Phase 5 | Clinical metric views (UC Metric Views) | Operational analytics (non-HEDIS) |

---

## 10a. Metric View CI/CD Strategy

### Ownership Delineation

- **This bundle (`fhir_declarative_pipeline`)**: Owns clinical mart metric
  views — encounter utilization, clinical event aggregation, patient demographics.
- **ncq-ai bundle (`mkgs-databricks-demos/ncq-ai`)**: Owns HEDIS-specific measure metric views.
  That project ingests HEDIS specifications and defines numerator/denominator logic against
  the clinical mart tables produced here. Cross-bundle dependency: ncq-ai reads from
  `{catalog}.{clinical_mart_schema}.fact_*` and `{catalog}.{clinical_mart_schema}.dim_*`.

### File Layout

```
fhir_declarative_pipeline/
├── fixtures/
│   └── metric_views/
│       ├── mv_encounter_utilization.metric_view.yml
│       ├── mv_clinical_events.metric_view.yml
│       └── mv_patient_demographics.metric_view.yml
└── src/
    └── fhir_gold_clinical_mart/
        └── register_metric_views.ipynb
```

### Pattern

1. YAML definitions live in `fixtures/metric_views/` with naming convention
   `{name}.metric_view.yml`. These are the source of truth — version-controlled,
   diffable, and reviewed in PRs.

2. Table/schema references use Python format placeholders: `{catalog}.{clinical_mart_schema}.table_name`.
   NO Jinja, NO SQL variables inside the YAML — substitution happens at registration time.

3. Registration notebook (`register_metric_views.ipynb`) accepts `catalog_use` and
   `clinical_mart_schema_use` widget parameters, globs all YAML files, substitutes placeholders
   via `.format()`, and executes `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML AS $...$`.

4. The notebook runs as a task in the gold orchestration job AFTER all gold tables are
   populated. It is idempotent — safe to re-run on every deployment.

### Bundle Variable

```yaml
variables:
  clinical_mart_schema:
    description: Schema for the clinical mart (dimensional model — dim_/fact_ tables and metric views).
# Per target (resolved via ${resources.schemas.clinical_mart_schema.name}):
#   dev:     dev_matthew_giglia_clinical_mart
#   hedis:   clinical_mart
#   hls_fde: clinical_mart
```

### Materialization (future)

Once query volume justifies it, add `materialization:` stanzas to the YAML files:

```yaml
materialization:
  schedule: EVERY 1 DAY
  mode: relaxed
  materialized_views:
    - name: daily_encounters
      type: aggregated
      dimensions: [encounter_month, encounter_class, organization_name]
      measures: [total_encounters, unique_patients, avg_length_of_stay_hours]
```

This pre-computes common groupings without changing the query interface.

---

## 11. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| SSN not available (real-world data) | Can't resolve patients | Probabilistic matching; MPI service integration; patient_identity_bridge |
| Auto CDC Type 2 bugs in PREVIEW channel | SCD2 rows incorrect | Monitor `num_upserted_rows` metrics; validate with point-in-time queries |
| Observation table too large (69.8M -> gold) | Slow queries | Liquid clustering on (patient_nk, observation_code); Delta min/max stats for datetime |
| Identifier format variation across sources | False non-matches | Normalize via URI aliasing in entity_resolution.py (SSN/NPI/MRN canonical forms) |
| Circular references in FHIR | Infinite loops in resolution | Cap resolution depth; entity resources only (no clinical event self-refs) |
| Identity columns unsupported in SDP | Schema errors at deploy | Use deterministic sha2 surrogates throughout |
| ingest_time vs business time for SCD2 | Wrong historical ordering | Use resource:meta.lastUpdated as sequence_by column |
| New resource types (Account, Coverage) | Missing from gold model | Addressed in Phase 4; silver tables already created dynamically |
