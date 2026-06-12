"""Dynamic silver table generation for FHIR resource types — Fully Streaming.

Architecture (two-step pattern per resource type):

    fhir_resources_variant
        -> {resource_type}_extract  (Temporary view: streaming filter + reference
                                     extraction + full resource VARIANT preservation)
        -> {resource_type}          (Target streaming table: Auto CDC Type 1 upserts)

Silver table schema (same structure for all 24 resource types):
    {type}_uuid                       STRING NOT NULL PK   -- resource identity
    bundle_uuid                       STRING NOT NULL      -- bundle provenance
    {type}_url                        STRING               -- full URL for intra-bundle joins
    references                        ARRAY<STRUCT<...>>   -- cross-resource reference fields
    identifiers                       ARRAY<STRUCT<...>>   -- business identifiers (MRN, NPI, SSN)
    codes                             ARRAY<STRUCT<...>>   -- primary clinical classification codes
    status                            STRING               -- resource lifecycle status
    clinical_event_effective_start    STRING               -- when the clinical event began (ISO 8601)
    clinical_event_effective_end      STRING               -- when the clinical event ended (ISO 8601)
    resource                          VARIANT              -- complete resource document

Extracted arrays:
    references:  ARRAY<STRUCT<field: STRING, url: STRING, type: STRING, display: STRING>>
    identifiers: ARRAY<STRUCT<system: STRING, value: STRING, type_code: STRING>>
    codes:       ARRAY<STRUCT<system: STRING, code: STRING, display: STRING>>

Design principles:
- All extraction uses try_variant_get (returns NULL on cast failure, no exceptions)
- Preserves the full resource as VARIANT (no INVALID_VARIANT_CAST errors)
- Enables cross-resource JOINs via references.url = other_table.{type}_url
- Enables cross-source entity matching via identifiers (MRN, NPI, SSN)
- Enables clinical event dedup via codes + identifiers + temporal fields
- Is directly compatible with FHIR server loading (resource VARIANT -> NDJSON/JSONB)
- Supports future gold-layer SCD1/SCD2 clinical mart construction

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
        f"SEQUENCE(0, GREATEST(COALESCE(size(try_variant_get(resource, '$.{field}', 'ARRAY<VARIANT>')), 0) - 1, 0)), "
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
        f"SEQUENCE(0, GREATEST(COALESCE(size(try_variant_get(resource, '$.{field}', 'ARRAY<VARIANT>')), 0) - 1, 0)), "
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
# Identifiers extraction
# ---------------------------------------------------------------------------
def _build_identifiers_sql(columns: list[dict]) -> str:
    """Build SQL to extract FHIR identifiers into a uniform array.

    FHIR identifiers are business-level keys (MRN, NPI, SSN, etc.) used for
    cross-source entity matching in the gold layer. Each identifier has:
      - system: the naming authority URI (e.g., http://hl7.org/fhir/sid/us-npi)
      - value: the actual identifier value (e.g., "9999992693")
      - type_code: the identifier type code (e.g., "MR", "SS", "NPI")
    """
    # Check if this resource type has an identifier field
    has_identifier = any(c["column_name"] == "identifier" for c in columns)
    if not has_identifier:
        return "CAST(NULL AS ARRAY<STRUCT<system: STRING, value: STRING, type_code: STRING>>)"

    return (
        "FILTER(\n"
        "                TRANSFORM(\n"
        "                    SEQUENCE(0, GREATEST(COALESCE(size(try_variant_get(resource, '$.identifier', 'ARRAY<VARIANT>')), 0) - 1, 0)),\n"
        "                    i -> named_struct(\n"
        "                        'system', try_variant_get(resource, CONCAT('$.identifier[', i, '].system'), 'STRING'),\n"
        "                        'value', try_variant_get(resource, CONCAT('$.identifier[', i, '].value'), 'STRING'),\n"
        "                        'type_code', try_variant_get(resource, CONCAT('$.identifier[', i, '].type.coding[0].code'), 'STRING')\n"
        "                    )\n"
        "                ),\n"
        "                x -> x.value IS NOT NULL\n"
        "            )"
    )


# ---------------------------------------------------------------------------
# Codes extraction
# ---------------------------------------------------------------------------
# Maps resource types to their primary clinical code field.
# Resources not listed here have no primary code (codes column will be NULL).
_CODE_FIELD_MAP: dict[str, str] = {
    "AllergyIntolerance": "code",
    "Condition": "code",
    "DiagnosticReport": "code",
    "Medication": "code",
    "Observation": "code",
    "Procedure": "code",
    "PractitionerRole": "code",
    "Immunization": "vaccineCode",
}


def _build_codes_sql(resource_type: str) -> str:
    """Build SQL to extract primary clinical classification codes.

    Extracts the coding array from the resource's primary code field into a
    uniform array of (system, code, display) structs. These are the standard
    vocabulary codes (SNOMED, LOINC, ICD-10, CPT, RxNorm, CVX) that define
    what the clinical event IS — essential for event-level deduplication and
    quality measure evaluation in the gold layer.
    """
    code_field = _CODE_FIELD_MAP.get(resource_type)
    if not code_field:
        return "CAST(NULL AS ARRAY<STRUCT<system: STRING, code: STRING, display: STRING>>)"

    return (
        f"FILTER(\n"
        f"                TRANSFORM(\n"
        f"                    SEQUENCE(0, GREATEST(COALESCE(size(try_variant_get(resource, '$.{code_field}.coding', 'ARRAY<VARIANT>')), 0) - 1, 0)),\n"
        f"                    i -> named_struct(\n"
        f"                        'system', try_variant_get(resource, CONCAT('$.{code_field}.coding[', i, '].system'), 'STRING'),\n"
        f"                        'code', try_variant_get(resource, CONCAT('$.{code_field}.coding[', i, '].code'), 'STRING'),\n"
        f"                        'display', try_variant_get(resource, CONCAT('$.{code_field}.coding[', i, '].display'), 'STRING')\n"
        f"                    )\n"
        f"                ),\n"
        f"                x -> x.code IS NOT NULL\n"
        f"            )"
    )


# ---------------------------------------------------------------------------
# Temporal extraction
# ---------------------------------------------------------------------------
# Maps resource types to their temporal field paths.
# Tuple of (start_path, end_path). end_path is None for point-in-time events.
_TEMPORAL_FIELD_MAP: dict[str, tuple[str, str | None]] = {
    "AllergyIntolerance": ("recordedDate", None),
    "CarePlan": ("period.start", "period.end"),
    "CareTeam": ("period.start", "period.end"),
    "Claim": ("billablePeriod.start", "billablePeriod.end"),
    "Condition": ("onsetDateTime", None),
    "DiagnosticReport": ("effectiveDateTime", None),
    "DocumentReference": ("date", None),
    "Encounter": ("period.start", "period.end"),
    "ExplanationOfBenefit": ("billablePeriod.start", "billablePeriod.end"),
    "ImagingStudy": ("started", None),
    "Immunization": ("occurrenceDateTime", None),
    "MedicationAdministration": ("effectiveDateTime", None),
    "MedicationRequest": ("authoredOn", None),
    "Observation": ("effectiveDateTime", None),
    "Procedure": ("performedPeriod.start", "performedPeriod.end"),
    "SupplyDelivery": ("occurrenceDateTime", None),
}


def _build_temporal_sql(resource_type: str) -> tuple[str, str]:
    """Build SQL for clinical event temporal fields.

    Returns (start_sql, end_sql) expressions. Resources without temporal
    context return NULL for both.
    """
    mapping = _TEMPORAL_FIELD_MAP.get(resource_type)
    if not mapping:
        return "CAST(NULL AS STRING)", "CAST(NULL AS STRING)"

    start_path, end_path = mapping
    start_sql = f"try_variant_get(resource, '$.{start_path}', 'STRING')"
    end_sql = (
        f"try_variant_get(resource, '$.{end_path}', 'STRING')"
        if end_path
        else "CAST(NULL AS STRING)"
    )
    return start_sql, end_sql


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

    Silver table schema (universal across all resource types):
        {type}_uuid                       STRING PK     -- bundle-scoped identity
        bundle_uuid                       STRING        -- bundle provenance
        {type}_url                        STRING        -- full URL for intra-bundle joins
        references                        ARRAY<STRUCT> -- cross-resource reference fields
        identifiers                       ARRAY<STRUCT> -- business identifiers (MRN, NPI, SSN)
        codes                             ARRAY<STRUCT> -- primary clinical classification codes
        status                            STRING        -- resource lifecycle status
        clinical_event_effective_start    STRING        -- when the clinical event began
        clinical_event_effective_end      STRING        -- when the clinical event ended
        resource                          VARIANT       -- complete resource document
    """
    rt_lower = resource_type.lower()

    # --- Classify reference fields ---
    ref_info = _classify_references(columns)
    ref_count = (
        len(ref_info["direct"])
        + len(ref_info["array_direct"])
        + sum(len(subs) for _, subs in ref_info["array_nested"])
    )

    # --- Build extraction SQL fragments ---
    references_sql = _build_references_sql(ref_info)
    identifiers_sql = _build_identifiers_sql(columns)
    codes_sql = _build_codes_sql(resource_type)
    status_sql = "try_variant_get(resource, '$.status', 'STRING')"
    temporal_start_sql, temporal_end_sql = _build_temporal_sql(resource_type)

    # Log extraction summary
    has_ids = any(c["column_name"] == "identifier" for c in columns)
    has_codes = resource_type in _CODE_FIELD_MAP
    has_temporal = resource_type in _TEMPORAL_FIELD_MAP
    print(
        f"[{resource_type}] refs={ref_count}, "
        f"identifiers={'Y' if has_ids else 'N'}, "
        f"codes={'Y' if has_codes else 'N'}, "
        f"temporal={'Y' if has_temporal else 'N'}"
    )

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
                {identifiers_sql} AS identifiers,
                {codes_sql} AS codes,
                {status_sql} AS status,
                {temporal_start_sql} AS clinical_event_effective_start,
                {temporal_end_sql} AS clinical_event_effective_end,
                resource
            FROM STREAM({_catalog}.{_schema}.fhir_resources_variant)
            WHERE resourceType = '{resource_type}'
        """)

    # --- Target silver streaming table ----------------------------------------
    dp.create_streaming_table(
        name=rt_lower,
        comment=(
            f"FHIR {resource_type} resources with extracted references, identifiers, "
            f"codes, and temporal fields for clinical mart construction. "
            f"Full resource preserved as VARIANT. "
            f"Auto CDC Type 1 upserts keyed on {rt_lower}_uuid."
        ),
        schema=f"""
        `{rt_lower}_uuid` STRING NOT NULL PRIMARY KEY
            COMMENT 'Unique identifier for the FHIR {resource_type} resource (SHA-256 of bundle_uuid + fullUrl). Bundle-scoped; use identifiers column for cross-source matching.',
        `bundle_uuid` STRING NOT NULL
            COMMENT 'Unique identifier for the FHIR bundle that delivered this resource.',
        `{rt_lower}_url` STRING
            COMMENT 'Full URL of the {resource_type} resource within its bundle. Used for intra-bundle joins via references.url.',
        `references` ARRAY<STRUCT<field: STRING, url: STRING, type: STRING, display: STRING>>
            COMMENT 'Extracted FHIR Reference fields linking this resource to others. JOIN pattern: references.url = other_table.{{type}}_url.',
        `identifiers` ARRAY<STRUCT<system: STRING, value: STRING, type_code: STRING>>
            COMMENT 'Business identifiers (MRN, NPI, SSN, etc.) for cross-source entity matching. system = naming authority URI, value = the identifier, type_code = identifier category (MR, SS, NPI, DL, PPN).',
        `codes` ARRAY<STRUCT<system: STRING, code: STRING, display: STRING>>
            COMMENT 'Primary clinical classification codes (SNOMED, LOINC, ICD-10, CPT, RxNorm, CVX). Defines what this clinical event IS for deduplication and quality measures.',
        `status` STRING
            COMMENT 'Resource lifecycle status (e.g., active, finished, completed, final, entered-in-error). Use to filter out cancelled or erroneous records.',
        `clinical_event_effective_start` STRING
            COMMENT 'When the clinical event began (ISO 8601). This is the clinical event timestamp, NOT an SCD record validity date. Source field varies by resource type (e.g., Encounter.period.start, Observation.effectiveDateTime, Condition.onsetDateTime).',
        `clinical_event_effective_end` STRING
            COMMENT 'When the clinical event ended (ISO 8601), or NULL for point-in-time events. This is the clinical event duration endpoint, NOT an SCD record validity date. Source field varies by resource type (e.g., Encounter.period.end, Procedure.performedPeriod.end).',
        `resource` VARIANT
            COMMENT 'Complete FHIR {resource_type} resource as VARIANT. Query any field via resource:fieldName syntax at read time. Preserved for ad-hoc analysis and FHIR server loading (NDJSON export or VARIANT-to-JSONB).'
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
