"""Dynamic silver table generation for FHIR resource types.

Architecture (two-table pattern per resource type):

    fhir_resources
        -> {resource_type}_raw   (PIVOT, all columns VARIANT)
        -> {resource_type}       (CAST each column to its inferred type)

The intermediate _raw table isolates the PIVOT from the CAST so that:
  - Column ordering is consistent (alphabetically sorted keys).
  - Schema evolution is cleanly handled: when fhir_resource_schemas is updated
    with new columns or changed struct types, the table definitions change on
    the next pipeline run. DLT detects this and triggers a full refresh
    (pipelines.reset.allowed = true).

Why SQL CAST instead of a UDF:
  Spark UDFs have fixed return types, so a single UDF cannot dynamically cast
  to different types per column. SQL CAST(variant_col AS complex_type) natively
  handles VARIANT-to-typed conversions including nested ARRAY<STRUCT<...>> types.

Two-pass behavior:
  - First run: Bronze and resource tables are populated. No silver tables are
    created since fhir_resource_schemas does not yet exist in the catalog.
  - Subsequent runs: Silver tables are dynamically generated for each discovered
    resource type (e.g., Patient, Encounter, Condition, Observation, etc.).
  - Schema changes: If fhir_resource_schemas has new columns or changed types,
    the table definitions change and DLT triggers a full refresh automatically.
"""

from pyspark import pipelines as dp


# ---------------------------------------------------------------------------
# Discover resource types and their schemas from previous pipeline runs
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
    """Create a raw (VARIANT) and typed (silver) table pair for a FHIR resource type."""
    rt_lower = resource_type.lower()
    keys = [c["column_name"] for c in columns]
    keys_sql = ", ".join([f"'{k}'" for k in keys])

    # Log schema evolution if applicable
    _detect_schema_evolution(resource_type, columns)

    # --- Intermediate raw table: PIVOT with VARIANT columns -----------------
    @dp.table(
        name=f"{rt_lower}_raw",
        comment=(
            f"Intermediate FHIR {resource_type} records. "
            f"PIVOT of fhir_resources with all columns as VARIANT."
        ),
        cluster_by="auto",
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true",
            "delta.enableRowTracking": "true",
            "pipelines.channel": "PREVIEW",
            "delta.feature.variantType-preview": "supported",
            "pipelines.reset.allowed": "true",
            "quality": "bronze",
        },
    )
    def _raw():
        return spark.sql(f"""
            SELECT * FROM (
                SELECT
                    resource_uuid AS {rt_lower}_uuid,
                    bundle_uuid,
                    fullUrl AS {rt_lower}_url,
                    key,
                    value
                FROM STREAM(fhir_resources)
                WHERE resourceType = '{resource_type}'
            ) PIVOT (
                first(value) FOR key IN ({keys_sql})
            )
        """)

    # --- Typed silver table: CAST from VARIANT to inferred types ------------
    cast_sql = _build_cast_sql(columns, rt_lower)
    schema_ddl = _build_schema_ddl(columns, resource_type)

    @dp.table(
        name=rt_lower,
        comment=(
            f"Typed FHIR {resource_type} records with columns cast "
            f"from VARIANT to their inferred schemas."
        ),
        schema=f"\n        {schema_ddl}\n        ",
        cluster_by="auto",
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true",
            "delta.enableRowTracking": "true",
            "pipelines.channel": "PREVIEW",
            "delta.feature.variantType-preview": "supported",
            "pipelines.reset.allowed": "true",
            "quality": "silver",
        },
    )
    def _typed():
        return spark.sql(f"""
            SELECT
                {cast_sql}
            FROM STREAM({rt_lower}_raw)
        """)


# ---------------------------------------------------------------------------
# Generate table pairs for each discovered resource type
# ---------------------------------------------------------------------------
for _rt, _cols in _resource_map.items():
    _create_resource_tables(_rt, _cols)
