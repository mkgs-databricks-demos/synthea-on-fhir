# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # Clinical Mart — Integrity & Data Quality Check
# MAGIC
# MAGIC Post-load validation for `ncqai.dev_matthew_giglia_clinical_mart`.
# MAGIC Run after every full-refresh to confirm:
# MAGIC - All 11 flows completed with correct row counts
# MAGIC - Zero null PKs, duplicate PKs, and orphan FKs
# MAGIC - Fact-to-patient FK integrity (all facts)
# MAGIC - Dimension FK integrity (encounter→practitioner/org/location, condition→encounter, claim→org/location)
# MAGIC - Fact counts align 1:1 with FHIR Gold source tables
# MAGIC - Computed column distributions are clinically plausible
# MAGIC - Observation no-value cohort is understood and expected

# COMMAND ----------

# DBTITLE 1,Row counts, null PKs, dupe PKs, orphan FKs, dimension FK integrity, gold alignment
display(spark.sql("""
WITH

-- 1. Row counts
counts AS (
  SELECT 'dim_patient'            AS tbl, COUNT(*) AS n FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient
  UNION ALL SELECT 'dim_practitioner',      COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.dim_practitioner
  UNION ALL SELECT 'dim_organization',      COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.dim_organization
  UNION ALL SELECT 'dim_location',          COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.dim_location
  UNION ALL SELECT 'fact_encounter',        COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_encounter
  UNION ALL SELECT 'fact_condition',        COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_condition
  UNION ALL SELECT 'fact_observation',      COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_observation
  UNION ALL SELECT 'fact_procedure',        COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_procedure
  UNION ALL SELECT 'fact_medication_request', COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_medication_request
  UNION ALL SELECT 'fact_immunization',     COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_immunization
  UNION ALL SELECT 'fact_claim',            COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_claim
),

-- 2. Null PKs (includes null patient_natural_key for fact tables)
null_pks AS (
  SELECT 'dim_patient'            AS tbl, COUNT(*) AS null_pk FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient            WHERE patient_natural_key IS NULL
  UNION ALL SELECT 'dim_practitioner',      COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.dim_practitioner                   WHERE practitioner_natural_key IS NULL
  UNION ALL SELECT 'dim_organization',      COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.dim_organization                   WHERE organization_natural_key IS NULL
  UNION ALL SELECT 'dim_location',          COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.dim_location                       WHERE location_natural_key IS NULL
  UNION ALL SELECT 'fact_encounter',        COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_encounter                     WHERE encounter_natural_key IS NULL OR patient_natural_key IS NULL
  UNION ALL SELECT 'fact_condition',        COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_condition                     WHERE condition_natural_key IS NULL OR patient_natural_key IS NULL
  UNION ALL SELECT 'fact_observation',      COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_observation                   WHERE observation_natural_key IS NULL OR patient_natural_key IS NULL
  UNION ALL SELECT 'fact_procedure',        COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_procedure                     WHERE procedure_natural_key IS NULL OR patient_natural_key IS NULL
  UNION ALL SELECT 'fact_medication_request', COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_medication_request          WHERE medication_request_natural_key IS NULL OR patient_natural_key IS NULL
  UNION ALL SELECT 'fact_immunization',     COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_immunization                  WHERE immunization_natural_key IS NULL OR patient_natural_key IS NULL
  UNION ALL SELECT 'fact_claim',            COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_claim                         WHERE claim_natural_key IS NULL OR patient_natural_key IS NULL
),

-- 3. Duplicate PKs
dupes AS (
  SELECT 'dim_patient'            AS tbl, COUNT(*)-COUNT(DISTINCT patient_natural_key)            AS dupe_pks FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient
  UNION ALL SELECT 'fact_encounter',        COUNT(*)-COUNT(DISTINCT encounter_natural_key)         FROM ncqai.dev_matthew_giglia_clinical_mart.fact_encounter
  UNION ALL SELECT 'fact_condition',        COUNT(*)-COUNT(DISTINCT condition_natural_key)         FROM ncqai.dev_matthew_giglia_clinical_mart.fact_condition
  UNION ALL SELECT 'fact_observation',      COUNT(*)-COUNT(DISTINCT observation_natural_key)       FROM ncqai.dev_matthew_giglia_clinical_mart.fact_observation
  UNION ALL SELECT 'fact_procedure',        COUNT(*)-COUNT(DISTINCT procedure_natural_key)         FROM ncqai.dev_matthew_giglia_clinical_mart.fact_procedure
  UNION ALL SELECT 'fact_medication_request', COUNT(*)-COUNT(DISTINCT medication_request_natural_key) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_medication_request
  UNION ALL SELECT 'fact_immunization',     COUNT(*)-COUNT(DISTINCT immunization_natural_key)      FROM ncqai.dev_matthew_giglia_clinical_mart.fact_immunization
  UNION ALL SELECT 'fact_claim',            COUNT(*)-COUNT(DISTINCT claim_natural_key)             FROM ncqai.dev_matthew_giglia_clinical_mart.fact_claim
),

-- 4. Orphan FKs (fact patient_natural_key not found in dim_patient)
orphans AS (
  SELECT 'fact_encounter' AS tbl, COUNT(*) AS orphan_fks
  FROM ncqai.dev_matthew_giglia_clinical_mart.fact_encounter f
  WHERE NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient d WHERE d.patient_natural_key = f.patient_natural_key)
  UNION ALL
  SELECT 'fact_condition', COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_condition f
  WHERE NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient d WHERE d.patient_natural_key = f.patient_natural_key)
  UNION ALL
  SELECT 'fact_observation', COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_observation f
  WHERE NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient d WHERE d.patient_natural_key = f.patient_natural_key)
  UNION ALL
  SELECT 'fact_procedure', COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_procedure f
  WHERE NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient d WHERE d.patient_natural_key = f.patient_natural_key)
  UNION ALL
  SELECT 'fact_medication_request', COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_medication_request f
  WHERE NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient d WHERE d.patient_natural_key = f.patient_natural_key)
  UNION ALL
  SELECT 'fact_immunization', COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_immunization f
  WHERE NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient d WHERE d.patient_natural_key = f.patient_natural_key)
  UNION ALL
  SELECT 'fact_claim', COUNT(*) FROM ncqai.dev_matthew_giglia_clinical_mart.fact_claim f
  WHERE NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient d WHERE d.patient_natural_key = f.patient_natural_key)
),

-- 5. Dimension FK integrity (non-patient FKs — NULL-tolerant, checks populated values only)
dim_fk_orphans AS (
  -- fact_encounter → dim_practitioner
  SELECT 'encounter→practitioner' AS fk_path, COUNT(*) AS orphan_fks
  FROM ncqai.dev_matthew_giglia_clinical_mart.fact_encounter f
  WHERE f.practitioner_natural_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_practitioner d WHERE d.practitioner_natural_key = f.practitioner_natural_key)
  UNION ALL
  -- fact_encounter → dim_organization
  SELECT 'encounter→organization', COUNT(*)
  FROM ncqai.dev_matthew_giglia_clinical_mart.fact_encounter f
  WHERE f.organization_natural_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_organization d WHERE d.organization_natural_key = f.organization_natural_key)
  UNION ALL
  -- fact_encounter → dim_location
  SELECT 'encounter→location', COUNT(*)
  FROM ncqai.dev_matthew_giglia_clinical_mart.fact_encounter f
  WHERE f.location_natural_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_location d WHERE d.location_natural_key = f.location_natural_key)
  UNION ALL
  -- fact_condition → fact_encounter
  SELECT 'condition→encounter', COUNT(*)
  FROM ncqai.dev_matthew_giglia_clinical_mart.fact_condition f
  WHERE f.encounter_natural_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.fact_encounter e WHERE e.encounter_natural_key = f.encounter_natural_key)
  UNION ALL
  -- fact_claim → dim_organization
  SELECT 'claim→organization', COUNT(*)
  FROM ncqai.dev_matthew_giglia_clinical_mart.fact_claim f
  WHERE f.organization_natural_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_organization d WHERE d.organization_natural_key = f.organization_natural_key)
  UNION ALL
  -- fact_claim → dim_location
  SELECT 'claim→location', COUNT(*)
  FROM ncqai.dev_matthew_giglia_clinical_mart.fact_claim f
  WHERE f.location_natural_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM ncqai.dev_matthew_giglia_clinical_mart.dim_location d WHERE d.location_natural_key = f.location_natural_key)
),

-- 6. Gold source row counts (mart must match 1:1)
gold_counts AS (
  SELECT 'patient_gold'           AS tbl, COUNT(*) AS n FROM ncqai.dev_matthew_giglia_fhir.patient_gold
  UNION ALL SELECT 'encounter_gold',        COUNT(*) FROM ncqai.dev_matthew_giglia_fhir.encounter_gold
  UNION ALL SELECT 'condition_gold',        COUNT(*) FROM ncqai.dev_matthew_giglia_fhir.condition_gold
  UNION ALL SELECT 'observation_gold',      COUNT(*) FROM ncqai.dev_matthew_giglia_fhir.observation_gold
  UNION ALL SELECT 'procedure_gold',        COUNT(*) FROM ncqai.dev_matthew_giglia_fhir.procedure_gold
  UNION ALL SELECT 'medication_request_gold', COUNT(*) FROM ncqai.dev_matthew_giglia_fhir.medication_request_gold
  UNION ALL SELECT 'immunization_gold',     COUNT(*) FROM ncqai.dev_matthew_giglia_fhir.immunization_gold
  UNION ALL SELECT 'claim_gold',            COUNT(*) FROM ncqai.dev_matthew_giglia_fhir.claim_gold
)

SELECT 'ROW COUNTS'  AS check_type, tbl, n    AS value, NULL AS expected FROM counts
UNION ALL
SELECT 'NULL PKs',   tbl, null_pk,              0            FROM null_pks
UNION ALL
SELECT 'DUPE PKs',   tbl, dupe_pks,             0            FROM dupes
UNION ALL
SELECT 'ORPHAN FKs (patient)', tbl, orphan_fks, 0            FROM orphans
UNION ALL
SELECT 'ORPHAN FKs (dimension)', fk_path, orphan_fks, 0     FROM dim_fk_orphans
UNION ALL
SELECT 'GOLD SOURCE', tbl, n,                   NULL         FROM gold_counts
ORDER BY check_type, tbl
"""))

# COMMAND ----------

# DBTITLE 1,dim_patient: age bands and identifier completeness
# Age band distribution + identifier completeness for dim_patient
# Expect: smooth population pyramid, 0 missing primary IDs

display(spark.sql("""
  SELECT age_band, COUNT(*) AS n,
         ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
  FROM   ncqai.dev_matthew_giglia_clinical_mart.dim_patient
  GROUP BY age_band ORDER BY age_band
"""))

display(spark.sql("""
  SELECT
    ROUND(AVG(age_years), 1)                                        AS avg_age,
    MIN(age_years)                                                  AS min_age,
    MAX(age_years)                                                  AS max_age,
    COUNT(CASE WHEN age_years IS NULL THEN 1 END)                   AS null_age,
    COUNT(CASE WHEN primary_identifier_value IS NULL THEN 1 END)    AS missing_primary_id,
    COUNT(CASE WHEN identifier_count = 0 THEN 1 END)                AS zero_identifiers,
    ROUND(AVG(identifier_count), 2)                                 AS avg_identifier_count
  FROM ncqai.dev_matthew_giglia_clinical_mart.dim_patient
"""))

# COMMAND ----------

# DBTITLE 1,fact_encounter: class breakdown, LOS, flags
# MAGIC %sql
# MAGIC -- Encounter class distribution with avg LOS and flag alignment check.
# MAGIC -- is_emergency must equal EMER count; is_inpatient must equal IMP count.
# MAGIC SELECT
# MAGIC   encounter_class,
# MAGIC   COUNT(*)                                             AS n,
# MAGIC   ROUND(AVG(length_of_stay_hours), 2)                  AS avg_los_hours,
# MAGIC   ROUND(AVG(length_of_stay_hours) / 24.0, 2)           AS avg_los_days,
# MAGIC   SUM(CASE WHEN is_emergency THEN 1 ELSE 0 END)        AS flagged_emergency,
# MAGIC   SUM(CASE WHEN is_inpatient THEN 1 ELSE 0 END)        AS flagged_inpatient,
# MAGIC   COUNT(CASE WHEN length_of_stay_hours IS NULL THEN 1 END) AS null_los
# MAGIC FROM ncqai.dev_matthew_giglia_clinical_mart.fact_encounter
# MAGIC GROUP BY encounter_class
# MAGIC ORDER BY n DESC

# COMMAND ----------

# DBTITLE 1,fact_encounter: dimension FK coverage
# MAGIC %sql
# MAGIC -- Dimension FK population rates for fact_encounter.
# MAGIC -- Expected: practitioner ~100%, organization ~89%, location ~88%.
# MAGIC -- NULLs indicate references that could not be resolved to a dimension row.
# MAGIC SELECT
# MAGIC   COUNT(*)                                                                     AS total_encounters,
# MAGIC   ROUND(COUNT(practitioner_natural_key) * 100.0 / COUNT(*), 1)                AS practitioner_fk_pct,
# MAGIC   ROUND(COUNT(organization_natural_key) * 100.0 / COUNT(*), 1)                AS organization_fk_pct,
# MAGIC   ROUND(COUNT(location_natural_key) * 100.0 / COUNT(*), 1)                    AS location_fk_pct,
# MAGIC   COUNT(*) - COUNT(practitioner_natural_key)                                   AS null_practitioner,
# MAGIC   COUNT(*) - COUNT(organization_natural_key)                                   AS null_organization,
# MAGIC   COUNT(*) - COUNT(location_natural_key)                                       AS null_location
# MAGIC FROM ncqai.dev_matthew_giglia_clinical_mart.fact_encounter

# COMMAND ----------

# DBTITLE 1,fact_condition: chronic/active flags, patient coverage
# MAGIC %sql
# MAGIC -- 25-30% chronic expected for a synthetic adult population.
# MAGIC -- distinct_patients should be nearly equal to dim_patient count.
# MAGIC SELECT
# MAGIC   ROUND(COUNT(CASE WHEN is_chronic THEN 1 END) * 100.0 / COUNT(*), 1)  AS pct_chronic,
# MAGIC   ROUND(COUNT(CASE WHEN is_active  THEN 1 END) * 100.0 / COUNT(*), 1)  AS pct_active,
# MAGIC   COUNT(DISTINCT patient_natural_key)                                    AS distinct_patients,
# MAGIC   COUNT(DISTINCT code)                                                   AS distinct_condition_codes,
# MAGIC   COUNT(DISTINCT code_system)                                            AS distinct_code_systems
# MAGIC FROM ncqai.dev_matthew_giglia_clinical_mart.fact_condition

# COMMAND ----------

# DBTITLE 1,fact_condition: encounter FK coverage
# MAGIC %sql
# MAGIC -- fact_condition -> fact_encounter FK coverage.
# MAGIC -- Expected: ~100% encounter_natural_key populated (direct urn:uuid match).
# MAGIC SELECT
# MAGIC   COUNT(*)                                                                    AS total_conditions,
# MAGIC   ROUND(COUNT(encounter_natural_key) * 100.0 / COUNT(*), 1)                  AS encounter_fk_pct,
# MAGIC   COUNT(*) - COUNT(encounter_natural_key)                                     AS null_encounter_fk
# MAGIC FROM ncqai.dev_matthew_giglia_clinical_mart.fact_condition

# COMMAND ----------

# DBTITLE 1,fact_claim: type breakdown, FK coverage, spend sanity
# MAGIC %sql
# MAGIC -- Claim type distribution, FK coverage percentages, and spend sanity.
# MAGIC -- Expected: professional > pharmacy > institutional by volume.
# MAGIC -- Organization FK ~89%, location FK ~51% (pharmacy claims have no facility).
# MAGIC SELECT
# MAGIC   claim_type_code,
# MAGIC   COUNT(*)                                                             AS n,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)                  AS pct_of_total,
# MAGIC   ROUND(COUNT(organization_natural_key) * 100.0 / COUNT(*), 1)        AS org_fk_coverage_pct,
# MAGIC   ROUND(COUNT(location_natural_key) * 100.0 / COUNT(*), 1)            AS loc_fk_coverage_pct,
# MAGIC   ROUND(SUM(total_value), 2)                                          AS total_spend,
# MAGIC   ROUND(AVG(total_value), 2)                                          AS avg_claim_value,
# MAGIC   COUNT(DISTINCT patient_natural_key)                                  AS distinct_patients
# MAGIC FROM ncqai.dev_matthew_giglia_clinical_mart.fact_claim
# MAGIC GROUP BY claim_type_code
# MAGIC ORDER BY n DESC

# COMMAND ----------

# DBTITLE 1,fact_observation: value type coverage and abnormal flag rate
# MAGIC %sql
# MAGIC -- Breakdown of which value column is populated per observation.
# MAGIC -- NULL across all three value columns is the no-value cohort (see cell below).
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN value_quantity IS NOT NULL THEN 'quantity'
# MAGIC     WHEN value_string   IS NOT NULL THEN 'string'
# MAGIC     WHEN value_code     IS NOT NULL THEN 'code'
# MAGIC     ELSE                                 'no_value'
# MAGIC   END                                                            AS value_type,
# MAGIC   COUNT(*)                                                       AS n,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)            AS pct,
# MAGIC   ROUND(COUNT(CASE WHEN is_abnormal_low  THEN 1 END) * 100.0
# MAGIC         / NULLIF(COUNT(CASE WHEN value_quantity IS NOT NULL THEN 1 END), 0), 2)  AS pct_low_of_qty,
# MAGIC   ROUND(COUNT(CASE WHEN is_abnormal_high THEN 1 END) * 100.0
# MAGIC         / NULLIF(COUNT(CASE WHEN value_quantity IS NOT NULL THEN 1 END), 0), 2)  AS pct_high_of_qty
# MAGIC FROM ncqai.dev_matthew_giglia_clinical_mart.fact_observation
# MAGIC GROUP BY 1
# MAGIC ORDER BY n DESC

# COMMAND ----------

# DBTITLE 1,No-value observation diagnosis: top LOINC codes by category
# MAGIC %sql
# MAGIC -- Diagnose the ~3.3M observations where value_quantity, value_string,
# MAGIC -- and value_code are all NULL. Source is observation_gold which retains
# MAGIC -- the raw value_raw VARIANT for inspection.
# MAGIC --
# MAGIC -- Root cause hypothesis: Synthea emits panel-header observations
# MAGIC -- (e.g., CBC, BMP, metabolic panel) that have component[] children but
# MAGIC -- no top-level value[x]. Those components are separate observation resources
# MAGIC -- and carry the actual values. Panel headers intentionally have no value.
# MAGIC
# MAGIC SELECT
# MAGIC   category,
# MAGIC   code,
# MAGIC   code_display,
# MAGIC   COUNT(*)                                                             AS no_value_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY category), 1) AS pct_of_category,
# MAGIC   -- Check whether value_raw is also null (no value anywhere in FHIR resource)
# MAGIC   COUNT(CASE WHEN value_raw IS NOT NULL THEN 1 END)                    AS has_value_raw,
# MAGIC   -- Check for component array (panel header indicator)
# MAGIC   COUNT(CASE WHEN try_variant_get(resource, '$.component', 'STRING') IS NOT NULL THEN 1 END) AS has_component
# MAGIC FROM ncqai.dev_matthew_giglia_fhir.observation_gold
# MAGIC WHERE value_quantity IS NULL
# MAGIC   AND value_string   IS NULL
# MAGIC   AND value_code     IS NULL
# MAGIC GROUP BY category, code, code_display
# MAGIC ORDER BY no_value_count DESC
# MAGIC LIMIT 30

# COMMAND ----------

# DBTITLE 1,No-value observations: value_raw type distribution
# MAGIC %sql
# MAGIC -- For the no-value cohort that DOES have value_raw populated,
# MAGIC -- inspect what FHIR value type is present but not extracted.
# MAGIC -- Anything here represents a gap in the gold ETL's extraction logic.
# MAGIC SELECT
# MAGIC   -- FHIR value[x] discriminator keys present in the resource
# MAGIC   CASE
# MAGIC     WHEN try_variant_get(resource, '$.valueQuantity',        'STRING') IS NOT NULL THEN 'valueQuantity (missed)'
# MAGIC     WHEN try_variant_get(resource, '$.valueCodeableConcept', 'STRING') IS NOT NULL THEN 'valueCodeableConcept (missed)'
# MAGIC     WHEN try_variant_get(resource, '$.valueString',          'STRING') IS NOT NULL THEN 'valueString (missed)'
# MAGIC     WHEN try_variant_get(resource, '$.valueBoolean',         'STRING') IS NOT NULL THEN 'valueBoolean (not extracted)'
# MAGIC     WHEN try_variant_get(resource, '$.valueInteger',         'STRING') IS NOT NULL THEN 'valueInteger (not extracted)'
# MAGIC     WHEN try_variant_get(resource, '$.component',            'STRING') IS NOT NULL THEN 'panel header (component[])'
# MAGIC     ELSE                                                                              'truly no value'
# MAGIC   END                                AS root_cause,
# MAGIC   COUNT(*)                           AS n,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM ncqai.dev_matthew_giglia_fhir.observation_gold
# MAGIC WHERE value_quantity IS NULL
# MAGIC   AND value_string   IS NULL
# MAGIC   AND value_code     IS NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY n DESC