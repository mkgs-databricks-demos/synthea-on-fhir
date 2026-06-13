"""Entity Resolution — identifier normalization and natural key assignment.

Produces temporary views consumed by the _gold Auto CDC Type 1 flows (fhir_gold.py):

  Entity views (resolve from own identifiers):
    - patient_resolved
    - practitioner_resolved
    - organization_resolved
    - location_resolved

  Event views (resolve patient FK via intra-bundle reference join):
    - encounter_resolved
    - condition_resolved
    - observation_resolved
    - procedure_resolved
    - medication_request_resolved
    - immunization_resolved

Each view extracts the natural key from silver table identifiers, normalizes
URI systems (SSN, NPI, MRN aliasing), resolves inter-resource references,
and prepares the columns needed by the downstream _gold streaming tables.

Design principles:
  - Identifier normalization applied BEFORE natural key selection (URI aliasing)
  - Natural key priority: SSN > MRN for patients, NPI for practitioners
  - Intra-bundle reference resolution: join on bundle_uuid + {type}_url
  - Absolute reference resolution: deferred (Synthea uses urn:uuid exclusively)
  - resource:meta.lastUpdated used as sequence column for SCD1 ordering
  - Full resource VARIANT preserved for FHIR server reconstitution
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col


# ---------------------------------------------------------------------------
# Pipeline configuration
# ---------------------------------------------------------------------------
try:
    _catalog = spark.conf.get("pipeline.catalog_use")
    _schema = spark.conf.get("pipeline.schema_use")
except Exception:
    _catalog = ""
    _schema = ""


# ---------------------------------------------------------------------------
# Identifier normalization SQL fragments
# ---------------------------------------------------------------------------
# URI aliasing: multiple system URIs map to the same canonical identifier type.
# Applied inline via CASE expressions before natural key extraction.

_PATIENT_NATURAL_KEY_SQL = """
    COALESCE(
        -- SSN (highest priority — universal patient identifier)
        FILTER(identifiers, x ->
            x.system IN (
                'http://hl7.org/fhir/sid/us-ssn',
                'urn:oid:2.16.840.1.113883.4.1'
            ) OR x.type_code = 'SS'
        )[0].value,
        -- MRN (facility-scoped, fallback)
        FILTER(identifiers, x -> x.type_code = 'MR')[0].value,
        -- Hospital system identifier (last resort)
        FILTER(identifiers, x -> x.system LIKE '%hospital%')[0].value
    )
""".strip()

_PRACTITIONER_NATURAL_KEY_SQL = """
    COALESCE(
        FILTER(identifiers, x ->
            x.system IN (
                'http://hl7.org/fhir/sid/us-npi',
                'urn:oid:2.16.840.1.113883.4.6'
            ) OR x.type_code = 'NPI'
        )[0].value,
        -- Fallback: first available identifier value
        identifiers[0].value
    )
""".strip()

_ORGANIZATION_NATURAL_KEY_SQL = """
    COALESCE(
        -- NPI (preferred for organizations)
        FILTER(identifiers, x ->
            x.system IN (
                'http://hl7.org/fhir/sid/us-npi',
                'urn:oid:2.16.840.1.113883.4.6'
            ) OR x.type_code = 'NPI'
        )[0].value,
        -- Name-based hash fallback (for orgs without NPI)
        sha2(COALESCE(try_variant_get(resource, '$.name', 'STRING'), 'UNKNOWN'), 256)
    )
""".strip()


# ---------------------------------------------------------------------------
# Patient natural key extraction (reusable for event type joins)
# ---------------------------------------------------------------------------
def _patient_nk_from_joined_identifiers(alias: str = "p") -> str:
    """SQL fragment to extract patient_natural_key from a joined patient row.

    Used by event-type resolved views that JOIN against the silver patient table
    to resolve the subject/patient reference.
    """
    return f"""
    COALESCE(
        FILTER({alias}.identifiers, x ->
            x.system IN (
                'http://hl7.org/fhir/sid/us-ssn',
                'urn:oid:2.16.840.1.113883.4.1'
            ) OR x.type_code = 'SS'
        )[0].value,
        FILTER({alias}.identifiers, x -> x.type_code = 'MR')[0].value,
        FILTER({alias}.identifiers, x -> x.system LIKE '%hospital%')[0].value
    )
    """.strip()


# ===========================================================================
# ENTITY RESOLUTION VIEWS (resolve from own identifiers — no joins needed)
# ===========================================================================

@dp.temporary_view(name="patient_resolved")
def _patient_resolved():
    """Resolve patient identity from silver.patient.

    Extracts natural key (SSN/MRN), scalar demographics, and full resource.
    One row per silver patient_uuid; multiple rows may share the same natural key
    (which is how Auto CDC Type 1 deduplicates them downstream in patient_gold).
    """
    return spark.sql(f"""
        SELECT
            -- Natural key (entity identity)
            {_PATIENT_NATURAL_KEY_SQL} AS patient_natural_key,

            -- FHIR resource identity (for reference resolution by downstream views)
            patient_url,
            bundle_uuid,

            -- Scalar demographics (for indexing and filtering)
            try_variant_get(resource, '$.name[0].family', 'STRING') AS family_name,
            try_variant_get(resource, '$.name[0].given[0]', 'STRING') AS given_name,
            CAST(try_variant_get(resource, '$.birthDate', 'STRING') AS DATE) AS birth_date,
            try_variant_get(resource, '$.gender', 'STRING') AS gender,
            COALESCE(
                CAST(try_variant_get(resource, '$.deceasedBoolean', 'STRING') AS BOOLEAN),
                try_variant_get(resource, '$.deceasedDateTime', 'STRING') IS NOT NULL
            ) AS deceased,
            try_variant_get(resource, '$.address[0].city', 'STRING') AS address_city,
            try_variant_get(resource, '$.address[0].state', 'STRING') AS address_state,
            try_variant_get(resource, '$.address[0].postalCode', 'STRING') AS address_postal_code,
            try_variant_get(resource, '$.maritalStatus.coding[0].code', 'STRING') AS marital_status,

            -- All identifiers (preserved for patient_identity_bridge)
            identifiers,

            -- Provenance
            ARRAY(patient_uuid) AS source_patient_uuids,
            ARRAY(bundle_uuid) AS source_bundle_uuids,
            COALESCE(
                CAST(try_variant_get(resource, '$.meta.lastUpdated', 'STRING') AS TIMESTAMP),
                CURRENT_TIMESTAMP()
            ) AS resource_last_updated,

            -- Full resource for FHIR API reconstitution
            resource

        FROM STREAM({_catalog}.{_schema}.patient)
        WHERE identifiers IS NOT NULL
          AND size(identifiers) > 0
    """)


@dp.temporary_view(name="practitioner_resolved")
def _practitioner_resolved():
    """Resolve practitioner identity from silver.practitioner. Keyed on NPI."""
    return spark.sql(f"""
        SELECT
            {_PRACTITIONER_NATURAL_KEY_SQL} AS practitioner_natural_key,
            practitioner_url,
            bundle_uuid,
            try_variant_get(resource, '$.name[0].family', 'STRING') AS family_name,
            try_variant_get(resource, '$.name[0].given[0]', 'STRING') AS given_name,
            -- Specialty from qualification or PractitionerRole (if embedded)
            try_variant_get(resource, '$.qualification[0].code.coding[0].code', 'STRING') AS specialty_code,
            try_variant_get(resource, '$.qualification[0].code.coding[0].display', 'STRING') AS specialty_display,
            identifiers,
            ARRAY(practitioner_uuid) AS source_practitioner_uuids,
            COALESCE(
                CAST(try_variant_get(resource, '$.meta.lastUpdated', 'STRING') AS TIMESTAMP),
                CURRENT_TIMESTAMP()
            ) AS resource_last_updated,
            resource
        FROM STREAM({_catalog}.{_schema}.practitioner)
        WHERE identifiers IS NOT NULL
          AND size(identifiers) > 0
    """)


@dp.temporary_view(name="organization_resolved")
def _organization_resolved():
    """Resolve organization identity. Keyed on NPI or sha2(name)."""
    return spark.sql(f"""
        SELECT
            {_ORGANIZATION_NATURAL_KEY_SQL} AS organization_natural_key,
            organization_url,
            bundle_uuid,
            try_variant_get(resource, '$.name', 'STRING') AS name,
            try_variant_get(resource, '$.type[0].coding[0].code', 'STRING') AS type_code,
            try_variant_get(resource, '$.type[0].coding[0].display', 'STRING') AS type_display,
            try_variant_get(resource, '$.address[0].city', 'STRING') AS address_city,
            try_variant_get(resource, '$.address[0].state', 'STRING') AS address_state,
            identifiers,
            ARRAY(organization_uuid) AS source_organization_uuids,
            COALESCE(
                CAST(try_variant_get(resource, '$.meta.lastUpdated', 'STRING') AS TIMESTAMP),
                CURRENT_TIMESTAMP()
            ) AS resource_last_updated,
            resource
        FROM STREAM({_catalog}.{_schema}.organization)
    """)


# location_resolved — MOVED to gold_overrides.py (correlated subquery pattern)
# View definition now in gold_overrides.py alongside the table + CDC flow.


# ===========================================================================
# EVENT RESOLUTION VIEWS (join with silver patient to resolve subject FK)
# ===========================================================================
# Pattern: STREAM(event_table) LEFT JOIN patient (static) on intra-bundle reference.
# The patient's natural key is extracted inline from the joined identifiers.

# encounter_resolved — MOVED to YAML (fixtures/gold_etl/encounter_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.


# condition_resolved — MOVED to YAML (fixtures/gold_etl/condition_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.

# observation_resolved — MOVED to YAML (fixtures/gold_etl/observation_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.

# procedure_resolved — MOVED to YAML (fixtures/gold_etl/procedure_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.

# medication_request_resolved — MOVED to YAML (fixtures/gold_etl/medication_request_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.

# immunization_resolved — MOVED to YAML (fixtures/gold_etl/immunization_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.

# allergyintolerance_resolved — MOVED to YAML (fixtures/gold_etl/allergyintolerance_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.

# careplan_resolved — MOVED to YAML (fixtures/gold_etl/careplan_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.

# diagnosticreport_resolved — MOVED to YAML (fixtures/gold_etl/diagnosticreport_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.

# medicationadministration_resolved — MOVED to YAML (fixtures/gold_etl/medicationadministration_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.

# explanationofbenefit_resolved — MOVED to YAML (fixtures/gold_etl/explanationofbenefit_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.

# coverage_resolved — MOVED to YAML (fixtures/gold_etl/coverage_gold.gold.yml)
# Generated by gold_engine.py at pipeline planning time.


# ===========================================================================
# IDENTITY BRIDGE VIEW (override)
# ===========================================================================

# patient_identity_bridge_resolved — MOVED to gold_overrides.py (LATERAL VIEW EXPLODE pattern)
# View definition now in gold_overrides.py alongside the table + CDC flow.
