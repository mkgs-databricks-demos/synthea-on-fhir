import os
from pyspark import pipelines as dp
from pyspark.sql.functions import udf, col, lit, current_timestamp, sha2
from pyspark.sql.types import StructType, StructField, BooleanType, StringType


# Result struct: {file_moved: bool, new_path: str|null, message: str|null}
move_result_schema = StructType([
    StructField("file_moved", BooleanType(), False),
    StructField("new_path", StringType(), True),
    StructField("message", StringType(), True)
])


@udf(returnType=move_result_schema)
def move_file_udf(file_path: str, content: bytes, dest_base: str) -> dict:
    """Write file content to destination volume via FUSE mount, return move result."""
    try:
        filename = os.path.basename(file_path)
        new_path = os.path.join(dest_base, filename)
        if os.path.exists(new_path):
            return {"file_moved": False, "new_path": new_path, "message": "SKIPPED (exists)"}
        os.makedirs(dest_base, exist_ok=True)
        with open(new_path, "wb") as f:
            f.write(content)
        return {"file_moved": True, "new_path": new_path, "message": None}
    except Exception as e:
        return {"file_moved": False, "new_path": None, "message": str(e)}


@dp.table(
    comment=(
        "Tracks FHIR bundle files moved from source to destination volume via Auto Loader. "
        "Each row represents a file move attempt with success/failure status."
    ),
    schema="""
        file_hash STRING NOT NULL PRIMARY KEY COMMENT 'SHA-256 hash of source_path \u2014 deterministic primary key',
        source_path STRING NOT NULL COMMENT 'Original file path in the source volume',
        file_size_bytes LONG COMMENT 'Size of the source file in bytes',
        binary_content BINARY COMMENT 'File content as binary data',
        file_moved BOOLEAN NOT NULL COMMENT 'True if file was successfully written to destination volume',
        destination_path STRING COMMENT 'Full destination path when file_moved is true, null otherwise',
        message STRING COMMENT 'Error details or SKIPPED reason when file_moved is false, null on success',
        source_modified_at TIMESTAMP COMMENT 'Last modification timestamp of the source file',
        moved_at TIMESTAMP NOT NULL COMMENT 'Timestamp when this move operation was executed'
    """,
    cluster_by_auto=True,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.reset.allowed": "true",
    },
)
def file_tracker():
    source_path = spark.conf.get("pipeline.source_volume_path")
    dest_path = spark.conf.get("pipeline.dest_volume_path")

    df_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.cleanSource", "DELETE")
        .option("cloudFiles.cleanSource.retentionDuration", "14 days")
        .load(source_path)
    )

    return (
        df_stream
        .withColumn(
            "move_result",
            move_file_udf(col("path"), col("content"), lit(dest_path)),
        )
        .select(
            sha2(col("path"), 256).alias("file_hash"),
            col("path").alias("source_path"),
            col("length").alias("file_size_bytes"),
            col("content").alias("binary_content"),
            col("move_result.file_moved").alias("file_moved"),
            col("move_result.new_path").alias("destination_path"),
            col("move_result.message").alias("message"),
            col("modificationTime").alias("source_modified_at"),
            current_timestamp().alias("moved_at"),
        )
    )
