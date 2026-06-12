"""Dynamic silver table generation for FHIR resource types — Fully Streaming.

Architecture (two-step pattern per resource type):

    fhir_resources_variant
        -> {resource_type}_extract  (Temporary view: streaming filter + reference
                                     extraction + full resource VARIANT preservation)
        -> {resource_type}          (Target streaming table: Auto CDC Type 1 upserts)

Silver table schema (same structure for all 24 resource types):
    {type}_uuid     STRING NOT NULL PK   -- resource identity
    bundle_uuid     STRING NOT NULL      -- bundle provenance
    {type}_url      STRING               -- full URL for FK joins
    references      ARRAY<STRUCT<...>>   -- extracted reference fields for cross-resource joins
    resource        VARIANT              -- complete resource document (flexible querying)

The references column extracts all FHIR Reference fields into a uniform array:
    ARRAY<STRUCT<field: STRING, url: STRING, type: STRING, display: STRING>>

Three reference patterns are handled:
1. Direct references (42 fields): resource:{field}.reference
2. Array of direct references (17 fields): resource:{field}[i].reference
3. Array with nested references (14 fields): resource:{field}[i].{sub}.reference

This design:
- Preserves the full resource as VARIANT (no INVALID_VARIANT_CAST errors)
- Extracts only typed identity + reference fields (safe STRING extraction)
- Enables cross-resource JOINs via references.url = other_table.{type}_url
- Is directly compatible with FHIR server loading (resource VARIANT -> NDJSON/JSONB)
- Supports future gold-layer FK flattening (references -> proper PK/FK columns)

Two-pass behavior:
  - First run of ingestion pipeline: Bronze, fhir_resources_variant, and schema
    tables are populated.
  - First run of this silver pipeline: Silver tables are dynamically generated
    for each discovered resource type.
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
# Reference field classification
# ---------------------------------------------------------------------------
def _has_top_level_reference(schema_str: str, prefix: str) -> bool:
    """Check if 'reference: STRING' appears at the top level of an OBJECT.

    Verifies that 'reference: STRING' is a direct field, not nested inside
    another OBJECT (which would indicate a sub-field reference pattern).
    """
    inner = schema_str[len(prefix):-len(">")]
    if "reference: STRING" not in inner:
        return False
    before_ref = inner.split("reference: STRING")[0]
    return "OBJECT<" not in before_ref


def _find_reference_sub_fields(schema_str: str) -> list[str]:
    """Find sub-field names within ARRAY<OBJECT<...>> that are reference objects.

    Parses the top-level fields of the array element OBJECT and identifies any
    whose type is OBJECT<...reference: STRING...> (i.e., a FHIR Reference).
    Returns the list of sub-field names (e.g., ['individual', 'who']).
    """
    if not schema_str.startswith("ARRAY<OBJECT<"):
        return []

    # Strip ARRAY<OBJECT< and closing >>
    inner = schema_str[len("ARRAY<OBJECT<"):-len(">>")]

    # Parse top-level fields handling nested angle brackets
    fields = []
    depth = 0
    current = ""
    for ch in inner:
        if ch == "<":
            depth += 1
            current += ch
        elif ch == ">":
            depth -= 1
            current += ch
        elif ch == "," and depth == 0:
            fields.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        fields.append(current.strip())

    # Identify fields whose type is OBJECT<...reference: STRING...>
    ref_fields = []
    for f in fields:
        colon_idx = f.find(":")
        if colon_idx == -1:
            continue
        name = f[:colon_idx].strip()
        ftype = f[colon_idx + 1:].strip()
        if ftype.startswith("OBJECT<") and "reference: STRING" in ftype:
            # Confirm reference is at the top level of this sub-OBJECT
            sub_inner = ftype[len("OBJECT<"):-len(">")]
            before = sub_inner.split("reference: STRING")[0]
            if "OBJECT<" not in before:
                ref_fields.append(name)
    return ref_fields


def _classify_references(columns: list[dict]) -> dict:
    """Classify all reference fields for a resource type.

    Returns dict with keys:
        'direct': list of field names (direct FHIR References)
        'array_direct': list of field names (arrays where element IS a Reference)
        'array_nested': list of (field_name, [sub_field_names]) tuples
    """
    result = {"direct": [], "array_direct": [], "array_nested": []}

    for col_info in columns:
        schema = col_info["schema_of_variant"]
        name = col_info["column_name"]

        if "reference: STRING" not in schema:
            continue

        # Direct reference: OBJECT<...reference: STRING...> (not array)
        if schema.startswith("OBJECT<") and not schema.startswith("ARRAY"):
            if _has_top_level_reference(schema, "OBJECT<"):
                result["direct"].append(name)

        # Array patterns
        elif schema.startswith("ARRAY<OBJECT<"):
            # Check if reference is at top level of array element
            inner_obj = "OBJECT<" + schema[len("ARRAY<OBJECT<"):-len(">")]
            if _has_top_level_reference(inner_obj, "OBJECT<"):
                result["array_direct"].append(name)
            else:
                # Find nested sub-fields that are references
                sub_fields = _find_reference_sub_fields(schema)
                if sub_fields:
                    result["array_nested"].append((name, sub_fields))

    return result


# ---------------------------------------------------------------------------
# SQL generation for reference extraction
# ---------------------------------------------------------------------------
def _build_direct_ref_sql(field: str) -> str:
    """SQL for a single direct reference field."""
    return (
        f"named_struct("
        f"'field', '{field}', "
        f"'url', try_variant_get(resource, '$.{field}.reference', 'STRING'), "
        f"'type', try_variant_get(resource, '$.{field}.type', 'STRING'), "
        f"'display', try_variant_get(resource, '$.{field}.display', 'STRING'))"
    )


def _build_array_direct_ref_sql(field: str) -> str:
    """SQL for an array-of-references field (each element IS a reference)."""
    return (
        f"TRANSFORM("
        f"SEQUENCE(0, COALESCE(size(try_variant_get(resource, '$.{field}', 'ARRAY<VARIANT>')) - 1, -1)), "
        f"i -> named_struct("
        f"'field', '{field}', "
        f"'url', try_variant_get(resource, CONCAT('$.{field}[', i, '].reference'), 'STRING'), "
        f"'type', try_variant_get(resource, CONCAT('$.{field}[', i, '].type'), 'STRING'), "
        f"'display', try_variant_get(resource, CONCAT('$.{field}[', i, '].display'), 'STRING')))"
    )


def _build_array_nested_ref_sql(field: str, sub_field: str) -> str:
    """SQL for an array with nested reference sub-field."""
    return (
        f"TRANSFORM("
        f"SEQUENCE(0, COALESCE(CAST(variant_array_length(resource:{field}) AS INT) - 1, -1)), "
        f"i -> named_struct("
        f"'field', '{field}.{sub_field}', "
        f"'url', try_variant_get(resource, CONCAT('$.{field}[', i, '].{sub_field}.reference'), 'STRING'), "
        f"'type', try_variant_get(resource, CONCAT('$.{field}[', i, '].{sub_field}.type'), 'STRING'), "
        f"'display', try_variant_get(resource, CONCAT('$.{field}[', i, '].{sub_field}.display'), 'STRING')))"
    )


def _build_references_sql(ref_info: dict) -> str:
    """Build the full FILTER(FLATTEN(ARRAY(...))) SQL for the references column."""
    array_parts = []

    # Direct references as a single ARRAY of structs
    if ref_info["direct"]:
        direct_items = ",\n                    ".join(
            _build_direct_ref_sql(f) for f in sorted(ref_info["direct"])
        )
        array_parts.append(f"ARRAY({direct_items})")

    # Array-of-direct-references: each TRANSFORM produces an array
    for field in sorted(ref_info["array_direct"]):
        array_parts.append(_build_array_direct_ref_sql(field))

    # Array-with-nested-references: each (field, sub_field) pair produces an array
    for field, sub_fields in sorted(ref_info["array_nested"]):
        for sub in sorted(sub_fields):
            array_parts.append(_build_array_nested_ref_sql(field, sub))

    if not array_parts:
        return "CAST(NULL AS ARRAY<STRUCT<field: STRING, url: STRING, type: STRING, display: STRING>>)"

    inner = ",\n                ".join(array_parts)
    return (
        f"FILTER(\n"
        f"                FLATTEN(ARRAY(\n"
        f"                    {inner}\n"
        f"                )),\n"
        f"                x -> x.url IS NOT NULL\n"
        f"            )"
    )


# ---------------------------------------------------------------------------
# Discover resource types from the ingestion pipeline
# ---------------------------------------------------------------------------
try:
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
except Exception:
    _resource_map = {}


# ---------------------------------------------------------------------------
# Dynamic table generation — fully streaming
# ---------------------------------------------------------------------------
def _create_resource_tables(resource_type: str, columns: list[dict]) -> None:
    """Create a streaming extract view and CDC target for a FHIR resource type.

    Silver table schema:
        {type}_uuid  STRING PK    -- resource identity
        bundle_uuid  STRING       -- bundle provenance
        {type}_url   STRING       -- full URL for FK joins
        references   ARRAY<STRUCT<field, url, type, display>>  -- extracted refs
        resource     VARIANT      -- complete resource document
    """
    rt_lower = resource_type.lower()

    # Classify reference fields for this resource type
    ref_info = _classify_references(columns)
    ref_count = (
        len(ref_info["direct"])
        + len(ref_info["array_direct"])
        + sum(len(subs) for _, subs in ref_info["array_nested"])
    )
    print(f"[{resource_type}] {ref_count} reference paths detected: "
          f"{len(ref_info['direct'])} direct, "
          f"{len(ref_info['array_direct'])} array, "
          f"{sum(len(s) for _, s in ref_info['array_nested'])} nested")

    # Build references extraction SQL
    references_sql = _build_references_sql(ref_info)

    # --- Streaming extract view -----------------------------------------------
    @dp.temporary_view(name=f"{rt_lower}_extract")
    def _extract():
        return spark.sql(f"""
            SELECT
                resource_uuid AS `{rt_lower}_uuid`,
                bundle_uuid,
                fullUrl AS `{rt_lower}_url`,
                ingest_time,
                {references_sql} AS references,
                resource
            FROM STREAM({_catalog}.{_schema}.fhir_resources_variant)
            WHERE resourceType = '{resource_type}'
        """)

    # --- Target silver streaming table ----------------------------------------
    dp.create_streaming_table(
        name=rt_lower,
        comment=(
            f"FHIR {resource_type} resources with extracted reference fields "
            f"for cross-resource joins. Full resource preserved as VARIANT. "
            f"Auto CDC Type 1 upserts keyed on {rt_lower}_uuid."
        ),
        schema=f"""
        `{rt_lower}_uuid` STRING NOT NULL PRIMARY KEY
            COMMENT 'Unique identifier for the FHIR {resource_type} resource.',
        `bundle_uuid` STRING NOT NULL
            COMMENT 'Unique identifier for the FHIR bundle.',
        `{rt_lower}_url` STRING
            COMMENT 'Full URL of the {resource_type} resource. Use for FK joins via references.url.',
        `references` ARRAY<STRUCT<field: STRING, url: STRING, type: STRING, display: STRING>>
            COMMENT 'Extracted FHIR Reference fields. JOIN via references.url = other.{{type}}_url.',
        `resource` VARIANT
            COMMENT 'Complete FHIR {resource_type} resource as VARIANT. Query via resource:fieldName syntax.'
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
            "pipelines.reset.allowed":             "true",
            "quality": "silver",
        },
    )

    dp.create_auto_cdc_flow(
        target=rt_lower,
        source=f"{rt_lower}_extract",
        keys=[f"{rt_lower}_uuid"],
        sequence_by=col("ingest_time"),
        except_column_list=["ingest_time"],
        stored_as_scd_type=1,
    )


# ---------------------------------------------------------------------------
# Generate tables for each discovered resource type
# ---------------------------------------------------------------------------
for _rt, _cols in _resource_map.items():
    _create_resource_tables(_rt, _cols)
