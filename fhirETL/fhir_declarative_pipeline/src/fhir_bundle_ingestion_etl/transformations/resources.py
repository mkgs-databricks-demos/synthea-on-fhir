from pyspark import pipelines as dp
from pyspark.sql.functions import col


# ---------------------------------------------------------------------------
# fhir_resources_variant — CDC upsert target keyed on resource_uuid
# ---------------------------------------------------------------------------

@dp.temporary_view(name="fhir_resources_variant_src")
def _fhir_resources_variant_src():
    """Streaming source: one row per bundle entry with the full resource VARIANT.

    Feeds the CDC flow for fhir_resources_variant. bundle_uuid is now a
    deterministic hash of (file_path + unix_millis(file_modification_time)),
    so resource_uuid is stable across replays of the same file.
    """
    return spark.sql("""
        SELECT
            sha2(concat(bundle_uuid, entry.value:fullUrl::string), 256) AS resource_uuid,
            bundle_uuid,
            CAST(entry.value:fullUrl AS STRING) AS fullUrl,
            CAST(entry.value:resource.resourceType AS STRING) AS resourceType,
            ingest_time,
            entry.value:resource AS resource
        FROM
            STREAM(fhir_bronze_variant),
            LATERAL variant_explode(fhir:entry) AS entry
    """)


dp.create_streaming_table(
    name="fhir_resources_variant",
    comment=(
        "One row per FHIR resource with the full resource preserved as VARIANT. "
        "CDC upsert target keyed on resource_uuid (SCD Type 1): the most recently "
        "ingested version of each resource wins. Stable across file replays — "
        "replaying a bundle produces identical resource_uuid values and updates "
        "existing rows rather than appending duplicates. "
        "Universal staging layer for: (1) fully-streaming silver analytics tables "
        "(VARIANT path extraction, no PIVOT), (2) FHIR server loading via NDJSON "
        "export or direct VARIANT->JSONB for Aidbox on Databricks Lakebase, and "
        "(3) ad-hoc VARIANT path queries against the complete resource document."
    ),
    schema="""
        resource_uuid STRING NOT NULL PRIMARY KEY
            COMMENT 'Deterministic identifier for the FHIR resource (SHA-256 of bundle_uuid + fullUrl). Stable across replays because bundle_uuid is derived from file path and modification time, not uuid().',
        bundle_uuid STRING NOT NULL
            COMMENT 'Stable identifier for the FHIR bundle, derived from file path and modification time.',
        fullUrl STRING NOT NULL
            COMMENT 'The full URL of the resource in the bundle entry array.',
        resourceType STRING NOT NULL
            COMMENT 'The FHIR resource type (e.g., Patient, Encounter, Condition).',
        ingest_time TIMESTAMP NOT NULL
            COMMENT 'The timestamp the source bundle file was most recently ingested. Used as CDC sequence; the most recent ingestion wins (SCD Type 1).',
        resource VARIANT
            COMMENT 'The complete FHIR resource as a VARIANT document. Queryable via path expressions (resource:fieldName).'
    """,
    cluster_by_auto=True,
    table_properties={
        "delta.enableChangeDataFeed":          "true",
        "delta.enableDeletionVectors":         "true",
        "delta.enableRowTracking":             "true",
        "delta.autoOptimize.optimizeWrite":    "true",
        "delta.autoOptimize.autoCompact":      "true",
        "delta.enableVariantShredding":        "true",
        "pipelines.channel":                   "PREVIEW",
        "quality": "bronze",
    },
)

dp.create_auto_cdc_flow(
    target="fhir_resources_variant",
    source="fhir_resources_variant_src",
    keys=["resource_uuid"],
    sequence_by=col("ingest_time"),
    stored_as_scd_type=1,
)


@dp.table(
    comment="Original FHIR Bundle metadata extracted from parsed FHIR bundles.",
    schema="""
        bundle_uuid STRING NOT NULL PRIMARY KEY
            COMMENT 'Unique identifier for the FHIR bundle. Required for all joins among resource tables as resource primary keys are only guaranteed to be unique inside a bundle.',
        file_metadata STRUCT<
            file_path: STRING,
            file_name: STRING,
            file_size: BIGINT,
            file_block_start: BIGINT,
            file_block_length: BIGINT,
            file_modification_time: TIMESTAMP
        > COMMENT 'Original metadata of the file ingested from the volume.',
        ingest_time TIMESTAMP COMMENT 'The timestamp the file was ingested.',
        bundle_resourceType STRING COMMENT 'The FHIR Bundle overall resource type.',
        bundle_type STRING COMMENT 'The FHIR Bundle overall type.',
        meta VARIANT COMMENT 'Metadata about the FHIR Bundle overall. Always NULL for Synthea-generated data.'
    """,
    cluster_by_auto=True,
    table_properties={
        "delta.enableChangeDataFeed":          "true",
        "delta.enableDeletionVectors":         "true",
        "delta.enableRowTracking":             "true",
        "delta.autoOptimize.optimizeWrite":    "true",
        "delta.autoOptimize.autoCompact":      "true",
        "delta.enableVariantShredding":        "true",
        "pipelines.channel":                   "PREVIEW",
        "delta.feature.variantType-preview":   "supported",
        "quality": "bronze",
    },
)
def bundle_meta():
    return spark.sql("""
        SELECT
            bundle_uuid,
            file_metadata,
            ingest_time,
            fhir:resourceType::string AS bundle_resourceType,
            fhir:type::string AS bundle_type,
            fhir:Meta AS meta
        FROM STREAM(fhir_bronze_variant)
    """)


@dp.table(
    comment="Exploded FHIR resources from bundle entries, one row per resource field.",
    schema="""
        resource_uuid STRING NOT NULL PRIMARY KEY
            COMMENT 'Unique identifier for the FHIR resource in a bundle (SHA-256 of bundle_uuid + fullUrl).',
        bundle_uuid STRING NOT NULL
            COMMENT 'Unique identifier for the FHIR bundle.',
        fullUrl STRING NOT NULL
            COMMENT 'The full URL of the resource in the entry array. Used to join related resources within a bundle.',
        resourceType STRING NOT NULL
            COMMENT 'The type of resource from the bundle entry array.',
        pos INT
            COMMENT 'The position of the resource element within the resource itself.',
        key STRING NOT NULL
            COMMENT 'The name of the resource element extracted. Serves as column names in resource target tables.',
        value VARIANT
            COMMENT 'The value of the resource element extracted. May contain nested variants.'
    """,
    cluster_by_auto=True,
    table_properties={
        "delta.enableChangeDataFeed":          "true",
        "delta.enableDeletionVectors":         "true",
        "delta.enableRowTracking":             "true",
        "delta.autoOptimize.optimizeWrite":    "true",
        "delta.autoOptimize.autoCompact":      "true",
        "delta.enableVariantShredding":        "true",
        "pipelines.channel":                   "PREVIEW",
        "delta.feature.variantType-preview":   "supported",
        "quality": "bronze",
    },
)
def fhir_resources():
    return spark.sql("""
        SELECT
            sha2(concat(bundle_uuid, entry.value:fullUrl::string), 256) AS resource_uuid,
            bundle_uuid,
            CAST(entry.value:fullUrl AS STRING) AS fullUrl,
            CAST(entry.value:resource.resourceType AS STRING) AS resourceType,
            resource.*
        FROM
            STREAM(fhir_bronze_variant),
            LATERAL variant_explode(fhir:entry) AS entry,
            LATERAL variant_explode(entry.value:resource) AS resource
    """)


@dp.table(
    comment="Schemas inferred from FHIR resource VARIANT data types, aggregated per resource type and column.",
    schema="""
        resourceType STRING
            COMMENT 'The type of resource from the bundle entry array.',
        column_name STRING
            COMMENT 'The name of the resource element. Serves as column names in resource target tables.',
        schema_of_variant STRING
            COMMENT 'The inferred schema of the resource element as VARIANT. More robust than FHIR specification schemas as it reflects actual data received.',
        schema_as_struct STRING
            COMMENT 'The same schema with OBJECT replaced by STRUCT for Spark compatibility.'
    """,
    cluster_by_auto=True,
    table_properties={
        "delta.enableChangeDataFeed":          "true",
        "delta.enableDeletionVectors":         "true",
        "delta.enableRowTracking":             "true",
        "delta.autoOptimize.optimizeWrite":    "true",
        "delta.autoOptimize.autoCompact":      "true",
        "delta.enableVariantShredding":        "true",
        "pipelines.channel":                   "PREVIEW",
        "delta.feature.variantType-preview":   "supported",
        "quality": "bronze",
    },
)
def fhir_resource_schemas():
    return spark.sql("""
        SELECT
            resourceType,
            key AS column_name,
            schema_of_variant_agg(value) AS schema_of_variant,
            REPLACE(schema_of_variant_agg(value), 'OBJECT', 'STRUCT') AS schema_as_struct
        FROM STREAM(fhir_resources)
        GROUP BY resourceType, key
    """)
