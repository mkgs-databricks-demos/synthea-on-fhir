"""Clinical Mart — Dimensional tables (dim_* and fact_*).

Reads FROM _gold entity-resolved tables in the FHIR schema
and produces a star-schema dimensional model in the clinical_mart schema
for dashboards, HEDIS, Genie spaces, and population health analytics.

Config keys:
  pipeline.catalog_use           — UC catalog
  pipeline.silver_schema_use     — source schema (where _gold tables live)
  pipeline.clinical_mart_schema_use — destination schema for dim_/fact_ tables
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col

# CDC flows (dp.create_auto_cdc_flow) are co-located here with their
# dp.create_streaming_table() declarations, following the fhir_gold.py convention.
# Source temp views are defined in entity_resolution.py.


# ---------------------------------------------------------------------------
# Standard table properties
# ---------------------------------------------------------------------------
_MART_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "pipelines.channel": "PREVIEW",
    "pipelines.reset.allowed": "true",
    "quality": "gold",
}


# ===========================================================================
# DIMENSION TABLES (SCD Type 1 — latest state wins)
# ===========================================================================

# --- dim_patient ----------------------------------------------------------

dp.create_streaming_table(
    name="dim_patient",
    comment=(
        "Patient dimension — one row per real-world patient. "
        "Derived from patient_gold with analytics-friendly flattening. "
        "Adds age_band, is_active, and census region for stratification. "
        "SCD1 (latest state wins). Primary consumer: HEDIS denominators, population health dashboards."
    ),
    schema="""
    `patient_natural_key` STRING NOT NULL
        COMMENT 'Business key — same as patient_gold.patient_natural_key (SSN/MRN). Stable FK for all fact tables.',
    `family_name` STRING
        COMMENT 'Patient family (last) name.',
    `given_name` STRING
        COMMENT 'Patient given (first) name.',
    `full_name` STRING
        COMMENT 'Formatted display name (given + family). Convenience for dashboards.',
    `birth_date` DATE
        COMMENT 'Date of birth. Source of truth for age calculations.',
    `age_years` INT
        COMMENT 'Current age in years (computed at pipeline refresh). For real-time age, compute from birth_date.',
    `age_band` STRING
        COMMENT 'Age stratification band (0-17, 18-34, 35-49, 50-64, 65+). Aligns with HEDIS age brackets.',
    `gender` STRING
        COMMENT 'Administrative gender: male, female, other, unknown.',
    `deceased` BOOLEAN
        COMMENT 'Whether patient is deceased. Used to filter active populations.',
    `address_city` STRING
        COMMENT 'City of residence. May change over time (SCD1 overwrites).',
    `address_state` STRING
        COMMENT 'State of residence. Key stratification dimension for regulatory reporting.',
    `address_postal_code` STRING
        COMMENT 'ZIP code. Used for social determinants, geographic analysis, and network adequacy.',
    `marital_status` STRING
        COMMENT 'Marital status code (S/M/D/W/NULL).',
    `primary_identifier_system` STRING
        COMMENT 'System URI of the primary identifier used as natural key (e.g., us-ssn, MR).',
    `primary_identifier_value` STRING
        COMMENT 'Value of the primary identifier (the actual SSN or MRN string).',
    `identifier_count` INT
        COMMENT 'Number of distinct identifiers for this patient. Higher count = better identity confidence.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of the most recent source record. Used for incremental refresh tracking.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["patient_natural_key", "address_state"],
)

dp.create_auto_cdc_flow(
    target="dim_patient",
    source="dim_patient_src",
    keys=["patient_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- dim_practitioner -----------------------------------------------------

dp.create_streaming_table(
    name="dim_practitioner",
    comment=(
        "Practitioner dimension — one row per healthcare provider. "
        "Keyed on NPI. Used for attribution, referral patterns, and network adequacy."
    ),
    schema="""
    `practitioner_natural_key` STRING NOT NULL
        COMMENT 'National Provider Identifier (NPI). FK for fact tables involving provider attribution.',
    `family_name` STRING
        COMMENT 'Provider family (last) name.',
    `given_name` STRING
        COMMENT 'Provider given (first) name.',
    `full_name` STRING
        COMMENT 'Formatted display name (given + family).',
    `specialty_code` STRING
        COMMENT 'Primary specialty code.',
    `specialty_display` STRING
        COMMENT 'Human-readable specialty name (e.g., "Internal Medicine", "Pediatrics").',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of the most recent source record.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["practitioner_natural_key"],
)

dp.create_auto_cdc_flow(
    target="dim_practitioner",
    source="dim_practitioner_src",
    keys=["practitioner_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- dim_organization -----------------------------------------------------

dp.create_streaming_table(
    name="dim_organization",
    comment=(
        "Organization dimension — one row per healthcare organization. "
        "Used for network analysis, cost attribution, and facility performance."
    ),
    schema="""
    `organization_natural_key` STRING NOT NULL
        COMMENT 'Organization identity (NPI or sha2 name hash). FK for fact tables.',
    `name` STRING
        COMMENT 'Organization display name.',
    `type_code` STRING
        COMMENT 'Organization type code (prov, dept, ins, pay, etc.).',
    `type_display` STRING
        COMMENT 'Human-readable organization type.',
    `address_city` STRING
        COMMENT 'Organization city.',
    `address_state` STRING
        COMMENT 'Organization state. Used for geographic analysis and network adequacy.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of the most recent source record.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["organization_natural_key"],
)

dp.create_auto_cdc_flow(
    target="dim_organization",
    source="dim_organization_src",
    keys=["organization_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- dim_location ---------------------------------------------------------

dp.create_streaming_table(
    name="dim_location",
    comment=(
        "Location dimension — one row per care delivery site. "
        "Used for site-level utilization, capacity planning, and geographic access."
    ),
    schema="""
    `location_natural_key` STRING NOT NULL
        COMMENT 'Location identity sha2(name + managing_org_nk). FK for encounter location references.',
    `name` STRING
        COMMENT 'Location display name (e.g., "Main Campus", "Urgent Care - North").',
    `managing_organization_nk` STRING
        COMMENT 'FK to dim_organization. Which organization manages this location.',
    `address_city` STRING
        COMMENT 'Location city.',
    `address_state` STRING
        COMMENT 'Location state.',
    `address_postal_code` STRING
        COMMENT 'Location ZIP code.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of the most recent source record.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["location_natural_key"],
)

dp.create_auto_cdc_flow(
    target="dim_location",
    source="dim_location_src",
    keys=["location_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# ===========================================================================
# FACT TABLES (immutable clinical events — append-only with dedup)
# ===========================================================================

# --- fact_encounter -------------------------------------------------------

dp.create_streaming_table(
    name="fact_encounter",
    comment=(
        "Encounter fact — one row per healthcare visit. "
        "Grain: one encounter per patient per visit. "
        "Central fact table linking patient, practitioner, organization, and location. "
        "Primary consumer: utilization dashboards, HEDIS visit-based measures, readmission analysis."
    ),
    schema="""
    `encounter_natural_key` STRING NOT NULL
        COMMENT 'Dedup key sha2(patient_nk + class + type_code + period_start). Grain = one real-world visit.',
    `patient_natural_key` STRING NOT NULL
        COMMENT 'FK to dim_patient. The patient who had the encounter.',
    `practitioner_natural_key` STRING
        COMMENT 'FK to dim_practitioner. The primary participant/provider for this encounter. Resolved via encounter references.participant URL.',
    `organization_natural_key` STRING
        COMMENT 'FK to dim_organization. The service provider organization. Resolved via encounter references.serviceProvider URL.',
    `location_natural_key` STRING
        COMMENT 'FK to dim_location. The care delivery site. Resolved via encounter references.location URL.',
    `encounter_class` STRING
        COMMENT 'Visit class: AMB (ambulatory), EMER (emergency), IMP (inpatient), HH (home health), VR (virtual).',
    `encounter_type_code` STRING
        COMMENT 'SNOMED code for encounter type. Defines the clinical reason/category of the visit.',
    `encounter_type_display` STRING
        COMMENT 'Human-readable encounter type.',
    `status` STRING
        COMMENT 'Final encounter status (typically "finished" for completed visits).',
    `period_start` TIMESTAMP
        COMMENT 'Visit start timestamp. Primary time dimension for utilization analysis.',
    `period_end` TIMESTAMP
        COMMENT 'Visit end timestamp. NULL for encounters still in progress. period_end - period_start = LOS.',
    `length_of_stay_hours` DOUBLE
        COMMENT 'Computed: (period_end - period_start) in hours. NULL if encounter still open. Key metric for inpatient utilization.',
    `reason_code` STRING
        COMMENT 'Primary visit reason SNOMED code.',
    `reason_display` STRING
        COMMENT 'Human-readable reason for visit.',
    `is_emergency` BOOLEAN
        COMMENT 'Convenience flag: encounter_class = EMER. Used in ED utilization dashboards.',
    `is_inpatient` BOOLEAN
        COMMENT 'Convenience flag: encounter_class = IMP. Used in admission/readmission analysis.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of most recent source update. Used for incremental refresh.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["patient_natural_key", "period_start"],
)

dp.create_auto_cdc_flow(
    target="fact_encounter",
    source="fact_encounter_src",
    keys=["encounter_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- fact_condition -------------------------------------------------------

dp.create_streaming_table(
    name="fact_condition",
    comment=(
        "Condition fact — one row per unique patient+diagnosis+onset. "
        "Grain: one diagnosis event per patient. "
        "Consumer: disease prevalence, chronic condition management, HEDIS condition-based measures."
    ),
    schema="""
    `condition_natural_key` STRING NOT NULL
        COMMENT 'Dedup key sha2(patient_nk + code + onset_datetime). Grain = one diagnosis event.',
    `patient_natural_key` STRING NOT NULL
        COMMENT 'FK to dim_patient. The patient with this condition.',
    `encounter_natural_key` STRING
        COMMENT 'FK to fact_encounter. The encounter during which this condition was recorded. Resolved via _encounter_ref_url on condition_gold.',
    `code` STRING
        COMMENT 'Condition code (SNOMED or ICD-10). Used for cohort definitions and measure denominators.',
    `code_system` STRING
        COMMENT 'Code system URI identifying whether this is SNOMED or ICD-10.',
    `code_display` STRING
        COMMENT 'Human-readable condition name.',
    `category` STRING
        COMMENT 'encounter-diagnosis (acute/resolved) or problem-list-item (chronic/ongoing). Key for chronic disease registries.',
    `clinical_status` STRING
        COMMENT 'Current clinical status: active, inactive, resolved, remission.',
    `verification_status` STRING
        COMMENT 'Diagnosis certainty: confirmed, provisional, differential, refuted.',
    `onset_datetime` TIMESTAMP
        COMMENT 'When the condition first appeared. Time dimension for incidence analysis.',
    `abatement_datetime` TIMESTAMP
        COMMENT 'When the condition resolved. NULL if still active. Used for duration analysis.',
    `is_chronic` BOOLEAN
        COMMENT 'Convenience flag: category = problem-list-item OR clinical_status IN (active, recurrence). Used in chronic care dashboards.',
    `is_active` BOOLEAN
        COMMENT 'Convenience flag: clinical_status NOT IN (inactive, resolved, remission). Used for current problem lists.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of most recent source update.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["patient_natural_key", "code"],
)

dp.create_auto_cdc_flow(
    target="fact_condition",
    source="fact_condition_src",
    keys=["condition_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- fact_observation -----------------------------------------------------

dp.create_streaming_table(
    name="fact_observation",
    comment=(
        "Observation fact — one row per clinical measurement (labs, vitals, surveys). "
        "Largest fact table (~70M rows). "
        "Consumer: lab trending, vital signs monitoring, quality measures (A1c control, BP control)."
    ),
    schema="""
    `observation_natural_key` STRING NOT NULL
        COMMENT 'Dedup key sha2(patient_nk + code + effective_datetime). Grain = one measurement event.',
    `patient_natural_key` STRING NOT NULL
        COMMENT 'FK to dim_patient. The patient measured.',
    `code` STRING
        COMMENT 'LOINC code identifying what was observed (e.g., 2093-3 = Cholesterol, 4548-4 = A1c).',
    `code_display` STRING
        COMMENT 'Human-readable observation name.',
    `category` STRING
        COMMENT 'Observation category: vital-signs, laboratory, survey, social-history.',
    `value_quantity` DOUBLE
        COMMENT 'Numeric result value. Primary analytic column for trending and thresholds (e.g., A1c < 9.0).',
    `value_unit` STRING
        COMMENT 'Unit of measurement (e.g., "mg/dL", "mmHg", "%"). Required context for value_quantity.',
    `value_string` STRING
        COMMENT 'String result (e.g., survey free-text, qualitative results like "positive").',
    `value_code` STRING
        COMMENT 'Coded result value (e.g., blood type A/B/AB/O, pos/neg).',
    `value_raw` VARIANT
        COMMENT 'Complete FHIR value[x] as VARIANT. Preserves full structure for complex value types (CodeableConcept, Quantity with comparator, etc.).',
    `effective_datetime` TIMESTAMP
        COMMENT 'When the observation was taken. Primary time dimension for lab/vital trending.',
    `is_abnormal_low` BOOLEAN
        COMMENT 'value_quantity < reference_range_low. Flag for out-of-range alerts.',
    `is_abnormal_high` BOOLEAN
        COMMENT 'value_quantity > reference_range_high. Flag for out-of-range alerts.',
    `reference_range_low` DOUBLE
        COMMENT 'Lower normal bound. NULL if no reference range defined.',
    `reference_range_high` DOUBLE
        COMMENT 'Upper normal bound. NULL if no reference range defined.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of most recent source update.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["patient_natural_key", "code", "effective_datetime"],
)

dp.create_auto_cdc_flow(
    target="fact_observation",
    source="fact_observation_src",
    keys=["observation_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- fact_procedure -------------------------------------------------------

dp.create_streaming_table(
    name="fact_procedure",
    comment=(
        "Procedure fact — one row per unique surgical/therapeutic procedure. "
        "Consumer: surgical volume analysis, procedure-based quality measures, cost attribution."
    ),
    schema="""
    `procedure_natural_key` STRING NOT NULL
        COMMENT 'Dedup key sha2(patient_nk + code + performed_start). Grain = one procedure event.',
    `patient_natural_key` STRING NOT NULL
        COMMENT 'FK to dim_patient. The patient who underwent the procedure.',
    `code` STRING
        COMMENT 'CPT or SNOMED procedure code. Used for surgical volume and quality measures.',
    `code_system` STRING
        COMMENT 'Code system URI identifying CPT vs SNOMED.',
    `code_display` STRING
        COMMENT 'Human-readable procedure name.',
    `status` STRING
        COMMENT 'Final procedure status (typically "completed").',
    `performed_start` TIMESTAMP
        COMMENT 'When the procedure started. Time dimension for volume trending.',
    `performed_end` TIMESTAMP
        COMMENT 'When the procedure ended. NULL for point-in-time procedures.',
    `duration_minutes` DOUBLE
        COMMENT 'Computed: (performed_end - performed_start) in minutes. NULL if no end time. Used for OR utilization.',
    `body_site_code` STRING
        COMMENT 'Anatomical site code.',
    `body_site_display` STRING
        COMMENT 'Human-readable body site name.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of most recent source update.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["patient_natural_key", "code"],
)

dp.create_auto_cdc_flow(
    target="fact_procedure",
    source="fact_procedure_src",
    keys=["procedure_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- fact_medication_request ----------------------------------------------

dp.create_streaming_table(
    name="fact_medication_request",
    comment=(
        "Medication request fact — one row per prescription. "
        "Consumer: formulary adherence, polypharmacy analysis, medication-based HEDIS measures."
    ),
    schema="""
    `medication_request_natural_key` STRING NOT NULL
        COMMENT 'Dedup key sha2(patient_nk + medication_code + authored_on). Grain = one prescription event.',
    `patient_natural_key` STRING NOT NULL
        COMMENT 'FK to dim_patient. The patient prescribed the medication.',
    `medication_code` STRING
        COMMENT 'RxNorm or NDC code for the prescribed medication.',
    `medication_system` STRING
        COMMENT 'Code system URI (RxNorm or NDC).',
    `medication_display` STRING
        COMMENT 'Human-readable medication name with strength (e.g., "Metformin 500 MG Oral Tablet").',
    `status` STRING
        COMMENT 'Prescription status: active, completed, cancelled, stopped.',
    `intent` STRING
        COMMENT 'Request intent: order (most common), proposal, plan.',
    `authored_on` TIMESTAMP
        COMMENT 'When the prescription was written. Time dimension for prescribing patterns.',
    `dosage_text` STRING
        COMMENT 'Free-text dosage instructions as written by prescriber.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of most recent source update.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["patient_natural_key", "medication_code"],
)

dp.create_auto_cdc_flow(
    target="fact_medication_request",
    source="fact_medication_request_src",
    keys=["medication_request_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- fact_immunization ----------------------------------------------------

dp.create_streaming_table(
    name="fact_immunization",
    comment=(
        "Immunization fact — one row per vaccination event. "
        "Consumer: immunization coverage rates, childhood schedule compliance, COVID tracking."
    ),
    schema="""
    `immunization_natural_key` STRING NOT NULL
        COMMENT 'Dedup key sha2(patient_nk + vaccine_code + occurrence). Grain = one vaccination event.',
    `patient_natural_key` STRING NOT NULL
        COMMENT 'FK to dim_patient. The patient who received the vaccine.',
    `vaccine_code` STRING
        COMMENT 'CVX vaccine administered code (e.g., 140 = Flu, 208 = COVID mRNA, 21 = Varicella).',
    `vaccine_display` STRING
        COMMENT 'Human-readable vaccine name.',
    `status` STRING
        COMMENT 'Immunization status: completed or not-done.',
    `occurrence_datetime` TIMESTAMP
        COMMENT 'When the vaccine was administered. Time dimension for coverage analysis.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of most recent source update.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["patient_natural_key", "vaccine_code"],
)

dp.create_auto_cdc_flow(
    target="fact_immunization",
    source="fact_immunization_src",
    keys=["immunization_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- fact_claim ---------------------------------------------------------------

dp.create_streaming_table(
    name="fact_claim",
    comment=(
        "Claim fact — one row per unique insurance claim submission. "
        "Grain: one claim per patient per billable period. "
        "Consumer: cost analysis, utilization-based quality measures, payer mix, claims adjudication."
    ),
    schema="""
    `claim_natural_key` STRING NOT NULL
        COMMENT 'Dedup key (Auto CDC primary key). Grain = one claim submission.',
    `patient_natural_key` STRING NOT NULL
        COMMENT 'FK to dim_patient. The patient this claim is for.',
    `organization_natural_key` STRING
        COMMENT 'FK to dim_organization. The billing provider organization. Resolved via _provider_ref_url identifier extraction.',
    `location_natural_key` STRING
        COMMENT 'FK to dim_location. The facility where services were rendered. Resolved via references.facility URL.',
    `claim_type_code` STRING
        COMMENT 'Claim type: institutional, pharmacy, professional.',
    `claim_type_display` STRING
        COMMENT 'Human-readable claim type.',
    `status` STRING
        COMMENT 'Claim status: active, cancelled, draft, entered-in-error.',
    `claim_use` STRING
        COMMENT 'Claim use: claim (payment request), preauthorization, predetermination.',
    `billable_period_start` TIMESTAMP
        COMMENT 'Start of the service/billing period. Primary time dimension for claims trending.',
    `billable_period_end` TIMESTAMP
        COMMENT 'End of the service/billing period.',
    `total_value` DOUBLE
        COMMENT 'Total claimed amount in currency units (typically USD).',
    `total_currency` STRING
        COMMENT 'Currency code (typically USD).',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of most recent source update.'
    """,
    table_properties=_MART_TABLE_PROPERTIES,
    cluster_by=["patient_natural_key", "claim_type_code", "billable_period_start"],
)

dp.create_auto_cdc_flow(
    target="fact_claim",
    source="fact_claim_src",
    keys=["claim_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)
