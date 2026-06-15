"""FHIR Relationship Registry — Materialized View.

Aggregates all observed references, identifiers, and codes across all silver
resource tables into a single registry. Provides:

  1. Join recipes for the gold layer (field path → target type → join strategy)
  2. Vocabulary coverage for Genie/metric view grounding
  3. Identifier system discovery for entity resolution
  4. Schema evolution signals (new patterns appearing over time)

Architecture:
    All silver {resource_type} tables
        -> EXPLODE references / identifiers / codes
        -> UNION ALL
        -> GROUP BY (resource_type, relationship_type, field_path,
                     target_resource_type, system, url_format)
        -> fhir_relationship_registry (Materialized View)

The MV recomputes on each pipeline run. Result set is bounded (~200-1000 rows)
regardless of data volume — it's a metadata catalog, not a fact table.
"""

from pyspark import pipelines as dp

# ---------------------------------------------------------------------------
# Pipeline configuration (same as silver.py)
# ---------------------------------------------------------------------------
try:
    _catalog = spark.conf.get("pipeline.catalog_use")
    _schema = spark.conf.get("pipeline.schema_use")
except Exception:
    _catalog = ""
    _schema = ""


# ---------------------------------------------------------------------------
# Discover resource types from fhir_resource_schemas (same source as silver.py)
# ---------------------------------------------------------------------------
try:
    _resource_types = [
        row.resourceType.lower()
        for row in (
            spark.table(f"{_catalog}.{_schema}.fhir_resource_schemas")
            .select("resourceType")
            .distinct()
            .collect()
        )
    ]
except Exception:
    _resource_types = []


# ---------------------------------------------------------------------------
# Build the registry SQL dynamically
# ---------------------------------------------------------------------------
def _build_registry_sql() -> str:
    """Generate the full UNION ALL aggregation query across all silver tables."""

    if not _resource_types or not _catalog or not _schema:
        # Fallback: empty registry with correct schema
        return """
            SELECT
                CAST(NULL AS STRING) AS resource_type,
                CAST(NULL AS STRING) AS relationship_type,
                CAST(NULL AS STRING) AS field_path,
                CAST(NULL AS STRING) AS target_resource_type,
                CAST(NULL AS STRING) AS system,
                CAST(NULL AS STRING) AS url_format,
                CAST(NULL AS BIGINT) AS observation_count,
                CAST(NULL AS BIGINT) AS distinct_values,
                CAST(NULL AS DOUBLE) AS coverage_pct,
                CAST(NULL AS ARRAY<STRING>) AS sample_values,
                CAST(NULL AS STRING) AS first_observed,
                CAST(NULL AS STRING) AS last_observed
            WHERE 1 = 0
        """

    fq = f"{_catalog}.{_schema}"
    union_parts = []

    for rtype in sorted(_resource_types):
        fq_table = f"{fq}.{rtype}"

        # --- References ---
        union_parts.append(f"""
            SELECT
                '{rtype}' AS resource_type,
                'reference' AS relationship_type,
                ref.field AS field_path,
                -- Derive target resource type from URL prefix or type field
                COALESCE(
                    ref.type,
                    CASE
                        WHEN ref.url LIKE '%?identifier=%'
                            THEN SUBSTRING_INDEX(ref.url, '?', 1)
                        WHEN ref.url LIKE '%/%' AND ref.url NOT LIKE 'urn:%' AND ref.url NOT LIKE '#%'
                            THEN REGEXP_EXTRACT(ref.url, '^([A-Z][a-zA-Z]+)/', 1)
                        ELSE NULL
                    END
                ) AS target_resource_type,
                -- Extract identifier system from search-format URLs
                CASE
                    WHEN ref.url LIKE '%?identifier=%'
                        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(ref.url, '|', 1), '=', -1)
                    ELSE NULL
                END AS system,
                -- Classify URL format
                CASE
                    WHEN ref.url LIKE '%?identifier=%' THEN 'search_identifier'
                    WHEN ref.url LIKE 'urn:uuid:%' THEN 'urn_uuid'
                    WHEN ref.url LIKE '#%' THEN 'contained'
                    WHEN ref.url LIKE '%/%' AND ref.url NOT LIKE 'urn:%' THEN 'relative'
                    ELSE 'unknown'
                END AS url_format,
                COUNT(*) AS observation_count,
                COUNT(DISTINCT ref.url) AS distinct_values,
                ref.url AS _sample_url,
                clinical_event_effective_start AS _effective
            FROM {fq_table}
            LATERAL VIEW EXPLODE(references) AS ref
            WHERE ref.url IS NOT NULL
        """)

        # --- Identifiers ---
        union_parts.append(f"""
            SELECT
                '{rtype}' AS resource_type,
                'identifier' AS relationship_type,
                'identifier' AS field_path,
                NULL AS target_resource_type,
                ident.system AS system,
                NULL AS url_format,
                COUNT(*) AS observation_count,
                COUNT(DISTINCT ident.value) AS distinct_values,
                ident.value AS _sample_url,
                clinical_event_effective_start AS _effective
            FROM {fq_table}
            LATERAL VIEW EXPLODE(identifiers) AS ident
            WHERE ident.value IS NOT NULL
        """)

        # --- Codes ---
        union_parts.append(f"""
            SELECT
                '{rtype}' AS resource_type,
                'code' AS relationship_type,
                'codes' AS field_path,
                NULL AS target_resource_type,
                cd.system AS system,
                NULL AS url_format,
                COUNT(*) AS observation_count,
                COUNT(DISTINCT cd.code) AS distinct_values,
                CONCAT(cd.code, '|', COALESCE(cd.display, '')) AS _sample_url,
                clinical_event_effective_start AS _effective
            FROM {fq_table}
            LATERAL VIEW EXPLODE(codes) AS cd
            WHERE cd.code IS NOT NULL
        """)

    # Each sub-query pre-aggregates to avoid massive intermediate datasets.
    # We need to restructure: the LATERAL VIEW + COUNT pattern above won't work
    # directly in a UNION ALL because COUNT is an aggregate. Let me use a CTE approach.

    # Actually, let's use a two-phase approach:
    # Phase 1: UNION ALL of raw exploded rows (no aggregation)
    # Phase 2: GROUP BY aggregation over the union

    raw_parts = []
    for rtype in sorted(_resource_types):
        fq_table = f"{fq}.{rtype}"

        # References (raw exploded)
        raw_parts.append(f"""
            SELECT
                '{rtype}' AS resource_type,
                'reference' AS relationship_type,
                ref.field AS field_path,
                COALESCE(
                    ref.type,
                    CASE
                        WHEN ref.url LIKE '%?identifier=%'
                            THEN SUBSTRING_INDEX(ref.url, '?', 1)
                        WHEN ref.url LIKE '%/%' AND ref.url NOT LIKE 'urn:%' AND ref.url NOT LIKE '#%'
                            THEN REGEXP_EXTRACT(ref.url, '^([A-Z][a-zA-Z]+)/', 1)
                        ELSE NULL
                    END
                ) AS target_resource_type,
                CASE
                    WHEN ref.url LIKE '%?identifier=%'
                        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(ref.url, '|', 1), '=', -1)
                    ELSE NULL
                END AS system,
                CASE
                    WHEN ref.url LIKE '%?identifier=%' THEN 'search_identifier'
                    WHEN ref.url LIKE 'urn:uuid:%' THEN 'urn_uuid'
                    WHEN ref.url LIKE '#%' THEN 'contained'
                    WHEN ref.url LIKE '%/%' AND ref.url NOT LIKE 'urn:%' THEN 'relative'
                    ELSE 'unknown'
                END AS url_format,
                ref.url AS _value,
                clinical_event_effective_start AS _effective
            FROM {fq_table}
            LATERAL VIEW EXPLODE(references) AS ref
            WHERE ref.url IS NOT NULL
              AND SIZE(references) > 0
        """)

        # Identifiers (raw exploded)
        raw_parts.append(f"""
            SELECT
                '{rtype}' AS resource_type,
                'identifier' AS relationship_type,
                'identifier' AS field_path,
                CAST(NULL AS STRING) AS target_resource_type,
                ident.system AS system,
                CAST(NULL AS STRING) AS url_format,
                ident.value AS _value,
                clinical_event_effective_start AS _effective
            FROM {fq_table}
            LATERAL VIEW EXPLODE(identifiers) AS ident
            WHERE ident.value IS NOT NULL
              AND SIZE(identifiers) > 0
        """)

        # Codes (raw exploded)
        raw_parts.append(f"""
            SELECT
                '{rtype}' AS resource_type,
                'code' AS relationship_type,
                'codes' AS field_path,
                CAST(NULL AS STRING) AS target_resource_type,
                cd.system AS system,
                CAST(NULL AS STRING) AS url_format,
                CONCAT(cd.code, '|', COALESCE(cd.display, '')) AS _value,
                clinical_event_effective_start AS _effective
            FROM {fq_table}
            LATERAL VIEW EXPLODE(codes) AS cd
            WHERE cd.code IS NOT NULL
              AND SIZE(codes) > 0
        """)

    raw_union = "\n            UNION ALL\n".join(raw_parts)

    # Phase 2: Aggregation over the raw union
    return f"""
        WITH raw_relationships AS (
            {raw_union}
        )
        SELECT
            resource_type,
            relationship_type,
            field_path,
            target_resource_type,
            system,
            url_format,
            COUNT(*) AS observation_count,
            COUNT(DISTINCT _value) AS distinct_values,
            SLICE(COLLECT_SET(_value), 1, 5) AS sample_values,
            MIN(_effective) AS first_observed,
            MAX(_effective) AS last_observed
        FROM raw_relationships
        GROUP BY
            resource_type,
            relationship_type,
            field_path,
            target_resource_type,
            system,
            url_format
    """


# ---------------------------------------------------------------------------
# Materialized View definition
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="fhir_relationship_registry",
    comment=(
        "Cross-resource relationship registry. Aggregates all observed references, "
        "identifiers, and code systems across silver resource tables. "
        "Use for: gold layer FK resolution planning, Smart-on-FHIR $include/$revinclude, "
        "Genie space context, metric view join discovery, schema evolution alerting."
    ),
)
def _fhir_relationship_registry():
    return spark.sql(_build_registry_sql())
