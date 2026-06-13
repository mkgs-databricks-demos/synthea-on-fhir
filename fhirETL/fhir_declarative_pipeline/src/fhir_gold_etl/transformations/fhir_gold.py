"""FHIR Gold streaming tables — Auto CDC Type 1 flows.

Consumes the _resolved temporary views from entity_resolution.py and materializes
them as _gold streaming tables via Auto CDC Type 1 (latest state wins, keyed on
natural key, sequenced by resource_last_updated).

Table naming: {resource_type}_gold (e.g., patient_gold, encounter_gold)
Schema: same as silver (pipeline.schema_use)
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col


# ---------------------------------------------------------------------------
# Standard table properties (all gold tables)
# ---------------------------------------------------------------------------
_GOLD_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.enableVariantShredding": "true",
    "pipelines.channel": "PREVIEW",
    "pipelines.reset.allowed": "true",
    "quality": "gold",
}


# ===========================================================================
# ENTITY TABLES (dimensions — resolve from own identifiers)
# ===========================================================================

# --- patient_gold ---------------------------------------------------------

dp.create_streaming_table(
    name="patient_gold",
    comment=(
        "Entity-resolved canonical patient. SCD1 — latest state wins. "
        "One row per real-world patient (keyed on SSN/MRN). "
        "Preserves full FHIR resource VARIANT for API reconstitution. "
        "Source: silver patient table via entity_resolution.py identifier matching."
    ),
    schema="""
    `patient_natural_key` STRING NOT NULL
        COMMENT 'Resolved patient identity (SSN preferred, MRN fallback). Stable across bundles and sources. Used as FK in all event _gold tables.',
    `patient_url` STRING
        COMMENT 'Most recent FHIR fullUrl for this patient. Used by downstream event views to resolve subject references via intra-bundle joins.',
    `bundle_uuid` STRING
        COMMENT 'Bundle UUID of the most recent contributing record. Retained for reference resolution traceability.',
    `family_name` STRING
        COMMENT 'Patient family (last) name extracted from resource.name[0].family.',
    `given_name` STRING
        COMMENT 'Patient given (first) name extracted from resource.name[0].given[0].',
    `birth_date` DATE
        COMMENT 'Patient date of birth (resource.birthDate). Used for age-band stratification in analytics.',
    `gender` STRING
        COMMENT 'Administrative gender (male, female, other, unknown). Source: resource.gender.',
    `deceased` BOOLEAN
        COMMENT 'Whether the patient is deceased. Derived from deceasedBoolean or presence of deceasedDateTime.',
    `address_city` STRING
        COMMENT 'Most recent city from resource.address[0].city.',
    `address_state` STRING
        COMMENT 'Most recent state from resource.address[0].state. Key stratification dimension for HEDIS and population health.',
    `address_postal_code` STRING
        COMMENT 'Most recent postal/ZIP code from resource.address[0].postalCode.',
    `marital_status` STRING
        COMMENT 'Marital status code from resource.maritalStatus.coding[0].code (S=single, M=married, D=divorced, W=widowed).',
    `identifiers` ARRAY<STRUCT<system: STRING, value: STRING, type_code: STRING>>
        COMMENT 'All identifiers from the most recent contributing silver record. Includes SSN, MRN, DL, PPN. System = naming authority URI, value = identifier, type_code = category.',
    `source_patient_uuids` ARRAY<STRING>
        COMMENT 'Silver patient_uuid values that resolved to this entity. Note: SCD1 overwrites — only latest contributing UUID retained. See patient_identity_bridge for full history.',
    `source_bundle_uuids` ARRAY<STRING>
        COMMENT 'Bundle UUIDs that contributed data to this entity. Note: SCD1 overwrites — only latest retained.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'resource.meta.lastUpdated from the most recent contributing FHIR resource. Used as Auto CDC sequence column for SCD1 ordering.',
    `resource` VARIANT NOT NULL
        COMMENT 'Complete FHIR Patient resource as VARIANT. Source of truth for FHIR API $read/$search/$export. Query any field via resource:fieldName syntax.'
    """,
    table_properties=_GOLD_TABLE_PROPERTIES,
    cluster_by=["patient_natural_key", "address_state"],
)

dp.create_auto_cdc_flow(
    target="patient_gold",
    source="patient_resolved",
    keys=["patient_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- practitioner_gold ----------------------------------------------------

dp.create_streaming_table(
    name="practitioner_gold",
    comment=(
        "Entity-resolved canonical practitioner. One row per real-world provider, keyed on NPI. "
        "Source: silver practitioner table via NPI identifier extraction."
    ),
    schema="""
    `practitioner_natural_key` STRING NOT NULL
        COMMENT 'National Provider Identifier (NPI). Unique per practitioner across all US healthcare systems.',
    `practitioner_url` STRING
        COMMENT 'Most recent FHIR fullUrl. Used by encounter/procedure views to resolve participant references.',
    `bundle_uuid` STRING
        COMMENT 'Bundle UUID of the most recent contributing record.',
    `family_name` STRING
        COMMENT 'Practitioner family (last) name from resource.name[0].family.',
    `given_name` STRING
        COMMENT 'Practitioner given (first) name from resource.name[0].given[0].',
    `specialty_code` STRING
        COMMENT 'Primary specialty code from resource.qualification[0].code.coding[0].code.',
    `specialty_display` STRING
        COMMENT 'Human-readable specialty display from resource.qualification[0].code.coding[0].display.',
    `identifiers` ARRAY<STRUCT<system: STRING, value: STRING, type_code: STRING>>
        COMMENT 'All practitioner identifiers (NPI, state license, DEA). Same structure as patient identifiers.',
    `source_practitioner_uuids` ARRAY<STRING>
        COMMENT 'Silver practitioner_uuid values that resolved to this entity.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'resource.meta.lastUpdated — Auto CDC sequence column.',
    `resource` VARIANT NOT NULL
        COMMENT 'Complete FHIR Practitioner resource as VARIANT for API reconstitution.'
    """,
    table_properties=_GOLD_TABLE_PROPERTIES,
    cluster_by=["practitioner_natural_key"],
)

dp.create_auto_cdc_flow(
    target="practitioner_gold",
    source="practitioner_resolved",
    keys=["practitioner_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- organization_gold ----------------------------------------------------

dp.create_streaming_table(
    name="organization_gold",
    comment=(
        "Entity-resolved canonical organization. One row per real-world healthcare organization. "
        "Keyed on NPI (preferred) or sha2(name) for orgs without NPI."
    ),
    schema="""
    `organization_natural_key` STRING NOT NULL
        COMMENT 'Organization identity — NPI if available, otherwise sha2(name). Stable across bundles.',
    `organization_url` STRING
        COMMENT 'Most recent FHIR fullUrl. Used by encounter views to resolve serviceProvider references.',
    `bundle_uuid` STRING
        COMMENT 'Bundle UUID of the most recent contributing record.',
    `name` STRING
        COMMENT 'Organization display name from resource.name.',
    `type_code` STRING
        COMMENT 'Organization type code (prov=provider, dept=department, ins=insurer) from resource.type[0].coding[0].code.',
    `type_display` STRING
        COMMENT 'Human-readable organization type from resource.type[0].coding[0].display.',
    `address_city` STRING
        COMMENT 'Organization city from resource.address[0].city.',
    `address_state` STRING
        COMMENT 'Organization state from resource.address[0].state.',
    `identifiers` ARRAY<STRUCT<system: STRING, value: STRING, type_code: STRING>>
        COMMENT 'Organization identifiers (NPI, tax ID). Same structure as patient identifiers.',
    `source_organization_uuids` ARRAY<STRING>
        COMMENT 'Silver organization_uuid values that resolved to this entity.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'resource.meta.lastUpdated — Auto CDC sequence column.',
    `resource` VARIANT NOT NULL
        COMMENT 'Complete FHIR Organization resource as VARIANT for API reconstitution.'
    """,
    table_properties=_GOLD_TABLE_PROPERTIES,
    cluster_by=["organization_natural_key", "address_state"],
)

dp.create_auto_cdc_flow(
    target="organization_gold",
    source="organization_resolved",
    keys=["organization_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# --- location_gold (override) --------------------------------------------
# MOVED to gold_overrides.py (correlated subquery pattern).
# View + table + CDC flow all in gold_overrides.py.


# ===========================================================================
# EVENT TABLES (facts — resolve patient FK via reference join)
# ===========================================================================

# --- encounter_gold (YAML-driven) ----------------------------------------
# MOVED to YAML: fixtures/gold_etl/encounter_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# --- condition_gold (YAML-driven) ------------------------------------
# MOVED to YAML: fixtures/gold_etl/condition_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# --- observation_gold (YAML-driven) ----------------------------------
# MOVED to YAML: fixtures/gold_etl/observation_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# --- procedure_gold (YAML-driven) ------------------------------------
# MOVED to YAML: fixtures/gold_etl/procedure_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# --- medication_request_gold (YAML-driven) ---------------------------
# MOVED to YAML: fixtures/gold_etl/medication_request_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# --- immunization_gold (YAML-driven) ---------------------------------
# MOVED to YAML: fixtures/gold_etl/immunization_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# ===========================================================================
# ADDITIONAL CLINICAL EVENT TABLES
# ===========================================================================

# --- allergyintolerance_gold (YAML-driven) ---------------------------
# MOVED to YAML: fixtures/gold_etl/allergyintolerance_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# --- careplan_gold (YAML-driven) -------------------------------------
# MOVED to YAML: fixtures/gold_etl/careplan_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# --- diagnosticreport_gold (YAML-driven) -----------------------------
# MOVED to YAML: fixtures/gold_etl/diagnosticreport_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# --- medicationadministration_gold (YAML-driven) ---------------------
# MOVED to YAML: fixtures/gold_etl/medicationadministration_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# ===========================================================================
# FINANCIAL TABLES
# ===========================================================================

# --- claim_gold (YAML-driven) ---------------------------------------------
# MOVED to YAML: fixtures/gold_etl/claim_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# --- explanationofbenefit_gold (YAML-driven) -------------------------
# MOVED to YAML: fixtures/gold_etl/explanationofbenefit_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# --- coverage_gold (YAML-driven) ------------------------------------------
# MOVED to YAML: fixtures/gold_etl/coverage_gold.gold.yml
# Generated by gold_engine.py at pipeline planning time.


# ===========================================================================
# IDENTITY BRIDGE TABLE (override)
# ===========================================================================

# --- patient_identity_bridge (override) -----------------------------------
# MOVED to gold_overrides.py (LATERAL VIEW EXPLODE pattern).
# View + table + CDC flow all in gold_overrides.py.
