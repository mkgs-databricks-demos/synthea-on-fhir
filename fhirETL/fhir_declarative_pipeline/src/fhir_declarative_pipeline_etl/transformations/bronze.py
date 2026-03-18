from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr


@dp.table(
    comment="Ingest FHIR JSON records as full text STRING from the landing volume via Auto Loader.",
    schema="""
        file_metadata STRUCT<
            file_path: STRING,
            file_name: STRING,
            file_size: BIGINT,
            file_block_start: BIGINT,
            file_block_length: BIGINT,
            file_modification_time: TIMESTAMP
        > NOT NULL COMMENT 'Original metadata of the file ingested from the volume.',
        ingest_time TIMESTAMP NOT NULL COMMENT 'The timestamp the file was ingested.',
        bundle_uuid STRING NOT NULL COMMENT 'Unique identifier for the FHIR bundle.',
        value STRING COMMENT 'Original JSON record ingested from the volume as a full text string value.'
    """,
    cluster_by="auto",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.enableRowTracking": "true",
        "pipelines.channel": "PREVIEW",
        "delta.feature.variantType-preview": "supported",
        "quality": "bronze",
    },
)
def fhir_bronze():
    volume_path = spark.conf.get("pipeline.landing_volume_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .load(volume_path)
        .select(
            col("_metadata").alias("file_metadata"),
            current_timestamp().alias("ingest_time"),
            expr("uuid()").alias("bundle_uuid"),
            col("value"),
        )
    )


@dp.table(
    comment="Evaluate FHIR JSON records as VARIANT data type for structured querying.",
    schema="""
        bundle_uuid STRING NOT NULL COMMENT 'Unique identifier for the FHIR bundle.',
        ingest_time TIMESTAMP NOT NULL COMMENT 'The timestamp the file was ingested.',
        file_metadata STRUCT<
            file_path: STRING,
            file_name: STRING,
            file_size: BIGINT,
            file_block_start: BIGINT,
            file_block_length: BIGINT,
            file_modification_time: TIMESTAMP
        > NOT NULL COMMENT 'Original metadata of the file ingested from the volume.',
        fhir VARIANT COMMENT 'Original JSON record fully parsed as a VARIANT data type.'
    """,
    cluster_by="auto",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.enableRowTracking": "true",
        "pipelines.channel": "PREVIEW",
        "delta.feature.variantType-preview": "supported",
        "quality": "bronze",
    },
)
def fhir_bronze_variant():
    return (
        spark.readStream.table("fhir_bronze")
        .select(
            col("bundle_uuid"),
            col("ingest_time"),
            col("file_metadata"),
            expr("try_parse_json(value)").alias("fhir"),
        )
    )
