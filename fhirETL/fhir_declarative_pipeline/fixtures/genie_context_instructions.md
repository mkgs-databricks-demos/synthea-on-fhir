# FHIR Clinical Mart — Genie Space & Metric View Context

Use this document as system instructions for Genie spaces, metric view comments,
or AI-assisted query generation against the FHIR Clinical Mart.

---

## Data Model Overview

The clinical mart is a dimensional model (star schema) with 4 dimension tables
and 7 fact tables, all in `{catalog}.{clinical_mart_schema}`.

### Dimensions (slowly changing, SCD1)

| Table | Grain | Natural Key | Description |
|---|---|---|---|
| dim_patient | One row per patient | SHA2(SSN) or SHA2(MRN) | Demographics, address, insurance |
| dim_practitioner | One row per provider | NPI | Provider name, specialty |
| dim_organization | One row per org | NPI or SHA2(name) | Healthcare organizations |
| dim_location | One row per site | SHA2(name + managing_org_nk) | Care delivery locations |

### Facts (append/upsert via streaming CDC)

| Table | Grain | Volume | Key FKs |
|---|---|---|---|
| fact_encounter | One visit | \~8M | patient, practitioner, organization, location |
| fact_condition | One diagnosis event | \~5M | patient, encounter |
| fact_observation | One lab/vital result | \~72M | patient |
| fact_procedure | One procedure | \~23M | patient |
| fact_medication_request | One prescription | \~7M | patient |
| fact_immunization | One vaccination | \~2M | patient |
| fact_claim | One insurance claim | \~14M | patient, organization, location |

---

## Join Paths (FK Resolution)

All fact tables join to `dim_patient` via `patient_natural_key`.

### fact_encounter → Dimensions

```sql
-- Practitioner (provider who saw the patient)
fact_encounter.practitioner_natural_key = dim_practitioner.practitioner_natural_key
-- Organization (service provider)
fact_encounter.organization_natural_key = dim_organization.organization_natural_key
-- Location (care site)
fact_encounter.location_natural_key = dim_location.location_natural_key
```

Resolution strategy: Encounter references use FHIR search-format URLs
(`Resource?identifier=system|value`). The identifier value is extracted and
matched to the target dimension's natural key or identifier array.

### fact_condition → fact_encounter

```sql
fact_condition.encounter_natural_key = fact_encounter.encounter_natural_key
```

Resolution: Direct `urn:uuid` URL match between condition's encounter reference
and encounter's URL. \~100% coverage.

### fact_claim → Dimensions

```sql
-- Billing organization (from claim.provider reference)
fact_claim.organization_natural_key = dim_organization.organization_natural_key
-- Facility (from claim.facility reference)
fact_claim.location_natural_key = dim_location.location_natural_key
```

Note: Pharmacy claims (42% of volume) have no facility — `location_natural_key`
will be NULL for those rows. This is expected FHIR semantics.

---

## Reference URL Formats

FHIR resources reference each other using several URL patterns:

| Format | Pattern | Join Strategy |
|---|---|---|
| search_identifier | `Type?identifier=system\|value` | Extract value after `\|`, match to target natural key or identifiers array |
| urn_uuid | `urn:uuid:xxxxxxxx-...` | Direct URL string match to target `_url` column |
| relative | `Type/id` | Rare in bundles; match to target `_url` or by ID |
| contained | `#resource-id` | Bundle-local only; resolve within same bundle_uuid |

The `fhir_relationship_registry` materialized view catalogs all observed
patterns with counts, coverage, and sample values.

---

## Identifier Systems

| System URI | What it identifies | Used by |
|---|---|---|
| `http://hl7.org/fhir/sid/us-npi` | National Provider Identifier | Practitioners, Organizations |
| `http://hl7.org/fhir/sid/us-ssn` | Social Security Number | Patients |
| `http://hospital.smarthealthit.org` | MRN (Medical Record Number) | Patients |
| `https://github.com/synthetichealth/synthea` | Synthea UUID | Organizations, Locations, Devices |

In production, expect additional systems (DEA numbers, state licenses,
facility IDs, payer member IDs). The relationship registry discovers these
automatically from incoming data.

---

## Code Systems (Vocabularies)

| System URI | Abbreviation | Used for |
|---|---|---|
| `http://snomed.info/sct` | SNOMED CT | Conditions, procedures, observations |
| `http://loinc.org` | LOINC | Lab observations, vital signs |
| `http://www.nlm.nih.gov/research/umls/rxnorm` | RxNorm | Medications |
| `urn:oid:2.16.840.1.113883.6.88` | NDC | Drug products (pharmacy claims) |
| `http://hl7.org/fhir/sid/cvx` | CVX | Immunizations |
| `http://www.ama-assn.org/go/cpt` | CPT | Procedures (claims) |
| `http://hl7.org/fhir/sid/icd-10-cm` | ICD-10 | Diagnoses (claims) |

---

## Query Patterns for Common Questions

### "How many patients were seen by Organization X?"

```sql
SELECT COUNT(DISTINCT e.patient_natural_key)
FROM fact_encounter e
JOIN dim_organization o ON o.organization_natural_key = e.organization_natural_key
WHERE o.name = 'Organization X'
```

### "What is the total spend per claim type?"

```sql
SELECT claim_type_code, SUM(total_value) AS total_spend
FROM fact_claim
GROUP BY claim_type_code
```

### "Which conditions are most common for patients over 65?"

```sql
SELECT c.code_display, COUNT(*) AS prevalence
FROM fact_condition c
JOIN dim_patient p ON p.patient_natural_key = c.patient_natural_key
WHERE p.age_years >= 65
GROUP BY c.code_display
ORDER BY prevalence DESC
```

### "Show encounters with their provider and facility"

```sql
SELECT
    e.encounter_natural_key,
    p.full_name AS provider,
    o.name AS organization,
    l.name AS facility,
    e.encounter_class,
    e.period_start
FROM fact_encounter e
LEFT JOIN dim_practitioner p ON p.practitioner_natural_key = e.practitioner_natural_key
LEFT JOIN dim_organization o ON o.organization_natural_key = e.organization_natural_key
LEFT JOIN dim_location l ON l.location_natural_key = e.location_natural_key
```

---

## Metric Views Available

| Metric View | Source Table | Measures |
|---|---|---|
| mv_patient_demographics | dim_patient | total_patients, gender breakdown, age bands, mortality |
| mv_encounter_utilization | fact_encounter | visit volume, LOS, ED rate, inpatient rate, per org/location |
| mv_clinical_events | fact_condition + observation + procedure | event counts by domain, abnormal rates |
| mv_claims | fact_claim | spend, claim counts by type, per-patient cost, org/location |

Query metric views using `MEASURE()` syntax:
```sql
SELECT claim_type, MEASURE(total_spend) AS spend
FROM {catalog}.{clinical_mart_schema}.mv_claims
GROUP BY claim_type
```

---

## Important Caveats

1. **NULL FKs are expected** — Not all encounters have a recorded practitioner;
   not all claims have a facility. Use LEFT JOIN, not INNER JOIN, for dimension lookups.

2. **Observation value types vary** — `value_quantity` (numeric), `value_string` (text),
   `value_code` (coded). Multi-component panels (blood pressure, PRAPARE) have NULL
   for all value fields — their data is in sub-observations.

3. **Natural keys are NOT UUIDs** — They are derived (SHA2 hashes, NPIs, SSNs).
   Do not confuse with `_url` columns which are `urn:uuid:` format.

4. **Time dimensions** — Use `period_start` for encounters, `onset_datetime` for
   conditions, `effective_datetime` for observations, `billable_period_start` for claims.

5. **Encounter class values** — `ambulatory`, `emergency`, `inpatient`, `wellness`,
   `urgentcare`. Note: these are lowercase strings, not FHIR V3 codes.

---

## Using the Relationship Registry

The `fhir_relationship_registry` MV (in the silver schema) provides machine-readable
context for building new metric views or Genie spaces:

```sql
-- Discover all joinable dimensions for a resource type
SELECT field_path, target_resource_type, system, url_format, observation_count
FROM {catalog}.{schema}.fhir_relationship_registry
WHERE resource_type = 'encounter'
  AND relationship_type = 'reference'
ORDER BY observation_count DESC
```

```sql
-- Find all code systems used by a resource type
SELECT system, observation_count, distinct_values
FROM {catalog}.{schema}.fhir_relationship_registry
WHERE resource_type = 'observation'
  AND relationship_type = 'code'
```

```sql
-- Detect new/unknown reference patterns (schema evolution)
SELECT *
FROM {catalog}.{schema}.fhir_relationship_registry
WHERE first_observed > CURRENT_DATE() - INTERVAL 7 DAYS
  AND relationship_type = 'reference'
```
