"""Dynamic silver table generation for FHIR resource types.

Architecture (three-step pattern per resource type):

    fhir_resources
        -> {resource_type}_raw        (Live table: PIVOT + CAST combined, typed columns,
                                       CDF enabled. SDP/Enzyme handles incremental refresh.)
        -> {resource_type}_cdc_source (Temporary view: CDF stream from _raw filtered to
                                       insert + update_postimage events only)
        -> {resource_type}            (Target streaming table: Auto CDC Type 1 upserts)

The _raw live table is backed by a batch PIVOT + CAST query over fhir_resources. SDP
treats it as a live table (not streaming) because the source is a batch read (no
STREAM()). Enzyme handles incremental processing where possible. CDF is enabled on _raw
so the downstream Auto CDC flow reads only row-level change events -- CDF is always
append-only regardless of how the underlying table was written (OPTIMIZE, MERGE, batch
overwrite, streaming Complete mode), which eliminates the
DELTA_SOURCE_TABLE_IGNORE_CHANGES error seen with direct streaming reads.

The _cdc_source temporary view reads _raw via Change Data Feed, filtering to insert and
update_postimage events. _commit_timestamp from CDF drives Auto CDC sequencing.

The final silver table is a target streaming table that receives SCD Type 1
(upsert/overwrite) changes via create_auto_cdc_flow. This means:
  - New resources are inserted.
  - Updated resources overwrite existing rows (matched by {resource_type}_uuid).
  - Ordering is determined by _commit_timestamp from the CDF stream.

Schema evolution is handled automatically: when new columns or changed struct
types appear in fhir_resource_schemas, the table definitions change and DLT
triggers a full refresh (pipelines.reset.allowed = true).

Why SQL CAST instead of a UDF:
  Spark UDFs have fixed return types, so a single UDF cannot dynamically cast
  to different types per column. SQL CAST(variant_col AS complex_type) natively
  handles VARIANT-to-typed conversions including nested ARRAY<STRUCT<...>> types.

Two-pass behavior:
  - First run of ingestion pipeline: Bronze and resource tables are populated.
  - First run of this silver pipeline: Silver tables are dynamically generated
    for each discovered resource type (e.g., Patient, Encounter, Condition).
  - Schema changes: If fhir_resource_schemas has new columns or changed types,
    the table definitions change and DLT triggers a full refresh automatically.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col


# ---------------------------------------------------------------------------
# Columns excluded from the PIVOT per resource type.
#
# Very large VARIANT values (e.g., ExplanationOfBenefit.item at ~6 KB/row ×
# 14.6M rows = ~90 GB total, ExplanationOfBenefit.contained at ~552 bytes/row
# × 14.6M rows = ~8 GB) cause the PIVOT shuffle to OOM on serverless compute
# and hang indefinitely.
#
# Excluded columns are omitted from the silver table. Raw values remain
# queryable via:
#   SELECT value FROM {catalog}.{schema}.fhir_resources
#   WHERE resourceType = '<Type>' AND key = '<column>'
# ---------------------------------------------------------------------------
_PIVOT_SKIP_COLUMNS: dict[str, set[str]] = {
    "ExplanationOfBenefit": {"item", "contained"},
}


# ---------------------------------------------------------------------------
# Discover resource types and their schemas from the ingestion pipeline
# ---------------------------------------------------------------------------
try:
    _catalog = spark.conf.get("pipeline.catalog_use")
    _schema = spark.conf.get("pipeline.schema_use")
    _fq_schemas_table = f"{_catalog}.{_schema}.fhir_resource_schemas"

    _resource_meta = (
        spark.table(_fq_schemas_table)
        .select("resourceType", "column_name", "schema_of_variant", "schema_as_struct")
        .collect()
    )

    # Build {resource_type: [{column_name, schema_of_variant, schema_as_struct}, ...]}
    _resource_map: dict[str, list[dict]] = {}
    for row in _resource_meta:
        _resource_map.setdefault(row.resourceType, []).append(
            {
                "column_name": row.column_name,
                "schema_of_variant": row.schema_of_variant,
                "schema_as_struct": row.schema_as_struct,
            }
        )

    # Sort columns alphabetically for consistent ordering across runs
    for rt in _resource_map:
        _resource_map[rt].sort(key=lambda x: x["column_name"])

except Exception:
    _resource_map = {}


# ---------------------------------------------------------------------------
# Schema evolution detection (observability)
# ---------------------------------------------------------------------------
def _detect_schema_evolution(resource_type: str, columns: list[dict]) -> bool:
    """Check if the silver table schema has changed, requiring a full refresh.

    Returns True if new columns or changed types are detected. DLT handles the
    actual full refresh via pipelines.reset.allowed = true.
    """
    rt_lower = resource_type.lower()
    try:
        existing_cols = {
            row.column_name
            for row in spark.sql(
                f"SELECT column_name FROM {_catalog}.information_schema.columns "
                f"WHERE table_catalog = '{_catalog}' "
                f"AND table_schema = '{_schema}' "
                f"AND table_name = '{rt_lower}'"
            ).collect()
        }
        expected_cols = (
            {f"{rt_lower}_uuid", "bundle_uuid", f"{rt_lower}_url"}
            | {c["column_name"] for c in columns}
        )
        new_cols = expected_cols - existing_cols
        if new_cols:
            print(
                f"[Schema Evolution] {resource_type}: "
                f"new columns detected: {new_cols}. "
                f"Full refresh will be triggered by DLT."
            )
            return True
    except Exception:
        pass  # Table does not exist yet (first run)
    return False


# ---------------------------------------------------------------------------
# SQL generation helpers
# ---------------------------------------------------------------------------
def _build_cast_sql(columns: list[dict], rt_lower: str) -> str:
    """Build SELECT expressions that CAST each VARIANT column to its inferred type."""
    exprs = [f"`{rt_lower}_uuid`", "`bundle_uuid`", f"`{rt_lower}_url`"]
    for col in columns:
        name = col["column_name"]
        dtype = col["schema_as_struct"]
        exprs.append(f"CAST(`{name}` AS {dtype}) AS `{name}`")
    return ",\n                ".join(exprs)


def _build_schema_ddl(columns: list[dict], resource_type: str) -> str:
    """Build the schema DDL string for the typed silver table."""
    rt_lower = resource_type.lower()
    parts = [
        (
            f"`{rt_lower}_uuid` STRING NOT NULL PRIMARY KEY "
            f"COMMENT 'Unique identifier for the FHIR {resource_type} resource.'"
        ),
        (
            f"`bundle_uuid` STRING NOT NULL "
            f"COMMENT 'Unique identifier for the FHIR bundle.'"
        ),
        (
            f"`{rt_lower}_url` STRING "
            f"COMMENT 'Full URL of the {resource_type} resource in the entry array.'"
        ),
    ]
    for col in columns:
        name = col["column_name"]
        dtype = col["schema_as_struct"]
        parts.append(
            f"`{name}` {dtype} "
            f"COMMENT 'FHIR {resource_type}.{name} element.'"
        )
    return ",\n        ".join(parts)


# ---------------------------------------------------------------------------
# Dynamic table generation
# ---------------------------------------------------------------------------
def _create_resource_tables(resource_type: str, columns: list[dict]) -> None:
    """Create a private raw, typed view, and CDC target table for a FHIR resource type."""
    rt_lower = resource_type.lower()

    # Exclude known oversized columns from the PIVOT to prevent OOM shuffles.
    # Skipped columns are omitted from the silver table; raw values remain in fhir_resources.
    skip = _PIVOT_SKIP_COLUMNS.get(resource_type, set())
    if skip:
        skipped = [c["column_name"] for c in columns if c["column_name"] in skip]
        columns = [c for c in columns if c["column_name"] not in skip]
        print(
            f"[{resource_type}] Skipping oversized PIVOT columns (excluded from silver): "
            f"{skipped}"
        )

    keys = [c["column_name"] for c in columns]
    keys_sql = ", ".join([f"'{k}'" for k in keys])

    # Build a key predicate to exclude oversized columns from the shuffle input.
    # The PIVOT reads all rows matching resourceType regardless of whether a key
    # appears in keys_sql. For ExplanationOfBenefit, 'item' averages ~6 KB/row
    # across 14.6M rows (~90 GB) and 'contained' adds ~8 GB more. Without this
    # filter those rows enter the shuffle even though they produce no output column,
    # causing serverless OOM. Filtering them before the GROUP BY reduces the EOB
    # shuffle from ~200 GB to ~25 GB.
    skip_filter = ""
    if skip:
        skipped_keys_sql = ", ".join([f"'{k}'" for k in sorted(skip)])
        skip_filter = f"\n                AND key NOT IN ({skipped_keys_sql})"

    # Log schema evolution if applicable
    _detect_schema_evolution(resource_type, columns)

    # --- Live table: PIVOT + CAST with typed columns, Enzyme-managed ----------
    # SDP treats this as a live table (not streaming) because the source is a
    # batch read of fhir_resources (no STREAM()). Enzyme handles incremental
    # processing -- only new fhir_resources rows are processed per update where
    # possible. PIVOT and CAST are combined in one query, eliminating the
    # separate _typed_view step.
    #
    # CDF is enabled so _cdc_source reads only row-level change events from this
    # table. CDF is always append-only regardless of how the underlying table is
    # written (OPTIMIZE, MERGE, batch overwrite), which eliminates the
    # DELTA_SOURCE_TABLE_IGNORE_CHANGES error seen with direct streaming reads.
    cast_sql = _build_cast_sql(columns, rt_lower)
    @dp.table(
        name=f"{rt_lower}_raw",
        comment=(
            f"Live table: typed PIVOT of fhir_resources for {resource_type}. "
            f"PIVOT + CAST combined; source for Auto CDC via Change Data Feed."
        ),
        table_properties={
            "delta.enableChangeDataFeed":          "true",
            "delta.enableDeletionVectors":         "true",
            "delta.enableRowTracking":             "true",
            "delta.autoOptimize.optimizeWrite":    "true",
            "delta.autoOptimize.autoCompact":      "true",
            # autoCompact and PO run OPTIMIZE on this table. This is safe because
            # _cdc_source reads via CDF, which is append-only and unaffected by
            # OPTIMIZE (OPTIMIZE does not produce CDF change events).
            "delta.enableVariantShredding":        "true",
            "pipelines.channel":                   "PREVIEW",
            "delta.feature.variantType-preview":   "supported",
            "pipelines.reset.allowed":             "true",
            "quality": "silver",
        },
    )
    def _raw():
        return spark.sql(f"""
            SELECT
                {cast_sql}
            FROM (
                SELECT
                    resource_uuid AS {rt_lower}_uuid,
                    bundle_uuid,
                    fullUrl AS {rt_lower}_url,
                    key,
                    value
                FROM {_catalog}.{_schema}.fhir_resources
                WHERE resourceType = '{resource_type}'{skip_filter}
            ) PIVOT (
                first(value) FOR key IN ({keys_sql})
            )
        """)

    # --- CDF source view: reads _raw via Change Data Feed -------------------
    # CDF records are always append-only new rows regardless of how the source
    # table was written. Filtering to insert + update_postimage excludes pre-image
    # rows that would otherwise produce spurious upserts in the Auto CDC target.
    # _commit_timestamp is used as the Auto CDC sequence column.
    @dp.temporary_view(name=f"{rt_lower}_cdc_source")
    def _cdc_source():
        return (
            spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .table(f"{_catalog}.{_schema}.{rt_lower}_raw")
            .filter(col("_change_type").isin("insert", "update_postimage"))
        )

    # --- Target silver table: Auto CDC Type 1 upserts ----------------------
    schema_ddl = _build_schema_ddl(columns, resource_type)

    dp.create_streaming_table(
        name=rt_lower,
        comment=(
            f"Typed FHIR {resource_type} records with columns cast "
            f"from VARIANT to their inferred schemas. "
            f"Auto CDC Type 1 upserts keyed on {rt_lower}_uuid."
        ),
        schema=f"\n        {schema_ddl}\n        ",
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
            "pipelines.reset.allowed":             "true",
            "quality": "silver",
        },
    )

    dp.create_auto_cdc_flow(
        target=rt_lower,
        source=f"{rt_lower}_cdc_source",
        keys=[f"{rt_lower}_uuid"],
        sequence_by=col("_commit_timestamp"),
        except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
        stored_as_scd_type=1,
    )


# ---------------------------------------------------------------------------
# Generate tables for each discovered resource type
# ---------------------------------------------------------------------------
for _rt, _cols in _resource_map.items():
    _create_resource_tables(_rt, _cols)
