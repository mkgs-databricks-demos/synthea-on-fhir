"""Gold Overrides — hand-coded tables requiring non-standard patterns.

The YAML engine (gold_engine.py) handles all tables with standard entity or
event join patterns. This file handles the two edge cases:

  1. location_gold — requires a CORRELATED SUBQUERY to resolve the managing
     organization's natural key (can't be expressed as a simple LEFT JOIN)

  2. patient_identity_bridge — uses LATERAL VIEW EXPLODE to fan out the
     patient identifiers array into a cross-reference lookup table

Both tables use temporary views + streaming tables + Auto CDC Type 1, same as
the engine-generated tables. They remain hand-coded because their SQL patterns
cannot be generalized into the YAML schema.
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
# Standard table properties
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


# ---------------------------------------------------------------------------
# Natural key SQL fragments (shared with entity_resolution.py)
# ---------------------------------------------------------------------------
_ORGANIZATION_NATURAL_KEY_SQL = """
    COALESCE(
        FILTER(identifiers, x ->
            x.system IN (
                'http://hl7.org/fhir/sid/us-npi',
                'urn:oid:2.16.840.1.113883.4.6'
            ) OR x.type_code = 'NPI'
        )[0].value,
        sha2(COALESCE(try_variant_get(resource, '$.name', 'STRING'), 'UNKNOWN'), 256)
    )
""".strip()

_PATIENT_NATURAL_KEY_SQL = """
    COALESCE(
        FILTER(identifiers, x ->
            x.system IN (
                'http://hl7.org/fhir/sid/us-ssn',
                'urn:oid:2.16.840.1.113883.4.1'
            ) OR x.type_code = 'SS'
        )[0].value,
        FILTER(identifiers, x -> x.type_code = 'MR')[0].value,
        FILTER(identifiers, x -> x.system LIKE '%hospital%')[0].value
    )
""".strip()


# ===========================================================================
# LOCATION — correlated subquery pattern
# ===========================================================================
# The location_resolved view uses a correlated subquery to resolve the
# managing organization's natural key by joining back to the organization
# table within the same bundle. This cannot be expressed as a simple
# LEFT JOIN because it requires extracting the org's natural key inline.

@dp.temporary_view(name="location_resolved")
def _location_resolved():
    """Resolve location identity. Keyed on sha2(name + managing_org_nk)."""
    return spark.sql(f"""
        SELECT
            -- Composite natural key: name + managing organization
            sha2(CONCAT(
                COALESCE(try_variant_get(resource, '$.name', 'STRING'), 'UNKNOWN'), '|',
                COALESCE(
                    (SELECT {_ORGANIZATION_NATURAL_KEY_SQL}
                     FROM {_catalog}.{_schema}.organization o
                     WHERE o.bundle_uuid = l.bundle_uuid
                       AND o.organization_url = FILTER(l.references, x -> x.field = 'managingOrganization')[0].url
                     LIMIT 1),
                    'NO_ORG'
                )
            ), 256) AS location_natural_key,

            location_url,
            bundle_uuid,
            try_variant_get(resource, '$.name', 'STRING') AS name,
            -- FK to organization_gold (resolved managing org)
            (SELECT {_ORGANIZATION_NATURAL_KEY_SQL}
             FROM {_catalog}.{_schema}.organization o
             WHERE o.bundle_uuid = l.bundle_uuid
               AND o.organization_url = FILTER(l.references, x -> x.field = 'managingOrganization')[0].url
             LIMIT 1) AS managing_organization_nk,
            try_variant_get(resource, '$.address.city', 'STRING') AS address_city,
            try_variant_get(resource, '$.address.state', 'STRING') AS address_state,
            try_variant_get(resource, '$.address.postalCode', 'STRING') AS address_postal_code,
            ARRAY(location_uuid) AS source_location_uuids,
            COALESCE(
                CAST(try_variant_get(resource, '$.meta.lastUpdated', 'STRING') AS TIMESTAMP),
                CURRENT_TIMESTAMP()
            ) AS resource_last_updated,
            resource
        FROM STREAM({_catalog}.{_schema}.location) l
    """)


dp.create_streaming_table(
    name="location_gold",
    comment=(
        "Entity-resolved canonical care delivery location. One row per physical location. "
        "Keyed on sha2(name + managing_organization_nk) composite key."
    ),
    schema="""
    `location_natural_key` STRING NOT NULL
        COMMENT 'sha2(name + managing_org_nk). Composite key since locations lack universal identifiers.',
    `location_url` STRING
        COMMENT 'Most recent FHIR fullUrl. Used by encounter views to resolve location references.',
    `bundle_uuid` STRING
        COMMENT 'Bundle UUID of the most recent contributing record.',
    `name` STRING
        COMMENT 'Location display name from resource.name (e.g., "ICU Ward 3", "Main Campus ED").',
    `managing_organization_nk` STRING
        COMMENT 'FK to organization_gold.organization_natural_key. Resolved from resource.managingOrganization reference.',
    `address_city` STRING
        COMMENT 'Location city from resource.address.city.',
    `address_state` STRING
        COMMENT 'Location state from resource.address.state.',
    `address_postal_code` STRING
        COMMENT 'Location postal code from resource.address.postalCode.',
    `source_location_uuids` ARRAY<STRING>
        COMMENT 'Silver location_uuid values that resolved to this entity.',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'resource.meta.lastUpdated — Auto CDC sequence column.',
    `resource` VARIANT NOT NULL
        COMMENT 'Complete FHIR Location resource as VARIANT for API reconstitution.'
    """,
    table_properties=_GOLD_TABLE_PROPERTIES,
    cluster_by=["location_natural_key"],
)

dp.create_auto_cdc_flow(
    target="location_gold",
    source="location_resolved",
    keys=["location_natural_key"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)


# ===========================================================================
# PATIENT IDENTITY BRIDGE — LATERAL VIEW EXPLODE pattern
# ===========================================================================
# Fans out the identifiers array from STREAM(patient) into a cross-reference
# lookup table. One row per identifier per patient. Composite key prevents
# duplicates while preserving all known identifiers.

@dp.temporary_view(name="patient_identity_bridge_resolved")
def _patient_identity_bridge_resolved():
    """Explode patient identifiers into a lookup table.

    Reads from STREAM(patient) and produces one row per identifier per patient.
    Keyed on (patient_natural_key, identifier_system, identifier_value) —
    the composite key prevents duplicates while preserving all known identifiers.
    """
    return spark.sql(f"""
        SELECT
            {_PATIENT_NATURAL_KEY_SQL} AS patient_natural_key,
            id.system AS identifier_system,
            id.value AS identifier_value,
            id.type_code AS identifier_type_code,
            COALESCE(
                CAST(try_variant_get(resource, '$.meta.lastUpdated', 'STRING') AS TIMESTAMP),
                CURRENT_TIMESTAMP()
            ) AS resource_last_updated
        FROM STREAM({_catalog}.{_schema}.patient)
        LATERAL VIEW EXPLODE(identifiers) AS id
        WHERE identifiers IS NOT NULL
          AND size(identifiers) > 0
          AND id.value IS NOT NULL
    """)


dp.create_streaming_table(
    name="patient_identity_bridge",
    comment=(
        "Cross-reference bridge: maps every known patient identifier "
        "(SSN, MRN, DL, PPN, etc.) back to the canonical patient_natural_key. "
        "One row per identifier per patient. "
        "Consumer: patient matching, identifier lookup, cross-system integration."
    ),
    schema="""
    `patient_natural_key` STRING NOT NULL
        COMMENT 'FK to patient_gold. The resolved canonical patient identity.',
    `identifier_system` STRING NOT NULL
        COMMENT 'Identifier naming authority URI (e.g., http://hl7.org/fhir/sid/us-ssn, urn:oid:2.16.840.1.113883.4.1). Normalized during ingestion.',
    `identifier_value` STRING NOT NULL
        COMMENT 'The identifier value itself (e.g., SSN digits, MRN number, DL number).',
    `identifier_type_code` STRING
        COMMENT 'Identifier type classification (SS=SSN, MR=MRN, DL=Drivers License, PPN=Passport, etc.).',
    `resource_last_updated` TIMESTAMP NOT NULL
        COMMENT 'Timestamp of the patient record that contributed this identifier. Tracks freshness.'
    """,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.enableRowTracking": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.channel": "PREVIEW",
        "pipelines.reset.allowed": "true",
        "quality": "gold",
    },
    cluster_by=["identifier_system", "identifier_value"],
)

dp.create_auto_cdc_flow(
    target="patient_identity_bridge",
    source="patient_identity_bridge_resolved",
    keys=["patient_natural_key", "identifier_system", "identifier_value"],
    sequence_by=col("resource_last_updated"),
    stored_as_scd_type=1,
)
