"""Gold Engine — YAML-driven streaming table generator.

Reads *.gold.yml configs from the fixtures directory and generates:
  1. Temporary views (entity resolution SQL)
  2. Streaming tables (schema DDL with column comments)
  3. Auto CDC Type 1 flows (keys + sequence_by)
  4. Data quality expectations

This replaces the hand-coded entity_resolution.py + fhir_gold.py pattern
with a metadata-driven approach where YAML is the source of truth.

Usage (in SDP pipeline context):
    # This module executes at planning time — import triggers table generation
    import gold_engine  # noqa: F401

Coexistence:
    During migration, both gold_engine.py and the hand-coded files can coexist.
    The engine only creates tables defined in YAML — hand-coded tables remain
    unaffected. Remove hand-coded definitions only AFTER validating YAML equivalents
    produce identical results.
"""

import os
import sys
from pathlib import Path
from typing import Any

import yaml
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
# Standard table properties (applied to all engine-generated tables)
# ---------------------------------------------------------------------------
_DEFAULT_TABLE_PROPERTIES = {
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
# Patient natural key SQL (reused across all event-type joins)
# ---------------------------------------------------------------------------
_PATIENT_NK_SQL = """
    COALESCE(
        FILTER(p.identifiers, x ->
            x.system IN (
                'http://hl7.org/fhir/sid/us-ssn',
                'urn:oid:2.16.840.1.113883.4.1'
            ) OR x.type_code = 'SS'
        )[0].value,
        FILTER(p.identifiers, x -> x.type_code = 'MR')[0].value,
        FILTER(p.identifiers, x -> x.system LIKE '%hospital%')[0].value
    )
""".strip()


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------

def _find_yaml_configs() -> list[dict[str, Any]]:
    """Discover and load all *.gold.yml files from fixtures/gold_etl/.

    Uses pipeline.bundle_files_path config (set in pipeline YAML) to locate
    the bundle root. Falls back to __file__-relative resolution for local testing.

    Returns list of parsed YAML dicts (validation happens in _validate_config).
    """
    # In SDP pipeline context, __file__ is not available (code is exec'd from b64).
    # Use the pipeline config to find the deployed bundle files path.
    try:
        bundle_root = Path(spark.conf.get("pipeline.bundle_files_path"))
    except Exception:
        # Fallback for local testing (non-pipeline context)
        try:
            engine_dir = Path(__file__).resolve().parent
            bundle_root = engine_dir.parent.parent.parent
        except NameError:
            print("[gold_engine] Cannot determine bundle root — neither pipeline config nor __file__ available")
            return []
    fixtures_dir = bundle_root / "fixtures" / "gold_etl"

    configs = []
    if not fixtures_dir.exists():
        print(f"[gold_engine] No fixtures directory at {fixtures_dir}")
        return configs

    for yml_path in sorted(fixtures_dir.glob("*.gold.yml")):
        try:
            with open(yml_path) as f:
                raw = yaml.safe_load(f)
            if raw:
                raw["_source_path"] = str(yml_path)
                configs.append(raw)
                print(f"[gold_engine] Loaded: {yml_path.name}")
        except Exception as e:
            print(f"[gold_engine] ERROR loading {yml_path.name}: {e}")

    return configs


def _validate_config(raw: dict) -> dict:
    """Validate a raw YAML dict against the pydantic schema.

    Returns the validated dict (with defaults applied) or raises.
    For POC, we do lightweight validation without importing pydantic
    (which may not be available in the pipeline runtime).
    """
    # Required top-level keys
    for key in ("table", "source", "natural_key", "columns"):
        if key not in raw:
            raise ValueError(f"Missing required key: {key}")

    # Required sub-keys
    if "name" not in raw["table"]:
        raise ValueError("table.name is required")
    if "silver_table" not in raw["source"]:
        raise ValueError("source.silver_table is required")
    if "strategy" not in raw["natural_key"]:
        raise ValueError("natural_key.strategy is required")

    return raw


# ---------------------------------------------------------------------------
# SQL Generation
# ---------------------------------------------------------------------------

def _build_natural_key_sql(nk_config: dict) -> str:
    """Generate the natural key SQL expression."""
    strategy = nk_config["strategy"]

    if strategy == "composite_sha2":
        components = nk_config.get("components", [])
        if not components:
            raise ValueError("composite_sha2 requires at least one component")

        concat_parts = []
        for i, comp in enumerate(components):
            expr = comp["expr"].replace("{{patient_natural_key}}", _PATIENT_NK_SQL)
            default = comp.get("default", "NULL")
            part = f"COALESCE({expr}, '{default}')"
            concat_parts.append(part)

        concat_inner = ", '|', ".join(concat_parts)
        return f"sha2(CONCAT({concat_inner}), 256)"

    elif strategy in ("identifier_cascade", "custom"):
        sql = nk_config.get("sql", "")
        if not sql:
            raise ValueError(f"{strategy} strategy requires 'sql' field")
        return sql

    else:
        raise ValueError(f"Unknown natural key strategy: {strategy}")


def _build_join_clause(source_config: dict) -> str:
    """Generate the FROM + JOIN clause."""
    table = source_config["silver_table"]
    join_type = source_config.get("join_type", "entity")
    fq_table = f"{_catalog}.{_schema}.{table}"

    # Handle reserved word tables
    if table == "procedure":
        fq_table = f"{_catalog}.{_schema}.`procedure`"

    if join_type == "entity":
        return f"FROM STREAM({fq_table}) e"

    elif join_type == "event":
        ref_field = source_config.get("patient_ref_field", "subject")
        return (
            f"FROM STREAM({fq_table}) e\n"
            f"        LEFT JOIN {_catalog}.{_schema}.patient p\n"
            f"          ON p.bundle_uuid = e.bundle_uuid\n"
            f"          AND p.patient_url = FILTER(e.references, x -> x.field = '{ref_field}')[0].url"
        )

    else:
        raise ValueError(f"join_type '{join_type}' must be handled in gold_overrides.py")


def _build_select_columns(columns: list[dict]) -> str:
    """Generate the SELECT column list from YAML column definitions."""
    parts = []
    for col_def in columns:
        source = col_def["source"].replace("{{patient_natural_key}}", _PATIENT_NK_SQL)
        name = col_def["name"]
        parts.append(f"            {source} AS {name}")
    return ",\n".join(parts)


def _build_schema_ddl(nk_config: dict, columns: list[dict], source_table: str) -> str:
    """Generate the schema DDL string for dp.create_streaming_table()."""
    lines = []

    # Natural key column
    nk_name = nk_config.get("column_name", f"{source_table}_natural_key")
    lines.append(
        f"    `{nk_name}` STRING NOT NULL\n"
        f"        COMMENT 'Entity dedup key (Auto CDC primary key). "
        f"See natural_key section in YAML config for derivation logic.'"
    )

    # User-defined columns
    for col_def in columns:
        col_type = col_def.get("type", "STRING")
        comment = col_def.get("comment", "").replace("'", "\\'")
        lines.append(f"    `{col_def['name']}` {col_type}\n        COMMENT '{comment}'")

    # Auto-appended columns
    lines.append(
        f"    `source_{source_table}_uuids` ARRAY<STRING>\n"
        f"        COMMENT 'Silver {source_table}_uuid values that resolved to this entity.'"
    )
    lines.append(
        "    `resource_last_updated` TIMESTAMP NOT NULL\n"
        "        COMMENT 'resource.meta.lastUpdated — Auto CDC sequence column.'"
    )
    lines.append(
        "    `resource` VARIANT NOT NULL\n"
        "        COMMENT 'Complete FHIR resource as VARIANT for API reconstitution.'"
    )

    return ",\n".join(lines)


# ---------------------------------------------------------------------------
# Table Generation (core loop)
# ---------------------------------------------------------------------------

def _create_gold_table(config: dict) -> None:
    """Generate a complete gold table from a validated YAML config.

    Creates:
      1. @dp.temporary_view — the resolved view (SQL extraction + joins)
      2. dp.create_streaming_table — schema DDL with comments
      3. dp.create_auto_cdc_flow — SCD1 keyed on natural_key
    """
    table_name = config["table"]["name"]
    source_table = config["source"]["silver_table"]
    nk_config = config["natural_key"]
    columns = config["columns"]
    nk_column = nk_config.get("column_name", f"{source_table}_natural_key")
    view_name = f"{table_name.replace('_gold', '')}_resolved"

    # --- Build SQL components ---
    nk_sql = _build_natural_key_sql(nk_config)
    join_clause = _build_join_clause(config["source"])
    select_cols = _build_select_columns(columns)
    where_clause = config["source"].get("where_clause")
    where_sql = f"\n        WHERE {where_clause}" if where_clause else ""

    # --- Full SELECT SQL ---
    view_sql = f"""
        SELECT
            {nk_sql} AS {nk_column},
{select_cols},
            ARRAY(e.{source_table}_uuid) AS source_{source_table}_uuids,
            COALESCE(
                CAST(try_variant_get(e.resource, '$.meta.lastUpdated', 'STRING') AS TIMESTAMP),
                CURRENT_TIMESTAMP()
            ) AS resource_last_updated,
            e.resource
        {join_clause}{where_sql}
    """

    # Log generated SQL for debugging
    source_path = config.get("_source_path", "unknown")
    print(f"[gold_engine] Generating: {table_name} (view={view_name}, source={source_table})")

    # --- Register temporary view ---
    @dp.temporary_view(name=view_name)
    def _resolved_view():
        return spark.sql(view_sql)

    # --- Schema DDL ---
    schema_ddl = _build_schema_ddl(nk_config, columns, source_table)

    # --- Table properties (merge defaults with overrides) ---
    props = {**_DEFAULT_TABLE_PROPERTIES}
    props.update(config["table"].get("table_properties", {}))

    # --- Create streaming table ---
    cluster_by = config["table"].get("cluster_by", [])
    dp.create_streaming_table(
        name=table_name,
        comment=config["table"].get("comment", f"FHIR Gold {source_table} (YAML-driven)"),
        schema=schema_ddl,
        table_properties=props,
        cluster_by=cluster_by if cluster_by else None,
    )

    # --- Auto CDC flow ---
    dp.create_auto_cdc_flow(
        target=table_name,
        source=view_name,
        keys=[nk_column],
        sequence_by=col("resource_last_updated"),
        stored_as_scd_type=1,
    )

    # --- Expectations ---
    expectations = config.get("expectations", [])
    for exp in expectations:
        # Note: dp.expect() and dp.expect_or_drop() are table-level decorators
        # in SDP Python API. For now, log them; full integration TBD.
        action = exp.get("action", "warn")
        print(f"[gold_engine]   expectation: {exp['name']} ({action}): {exp['expr']}")


# ---------------------------------------------------------------------------
# Module-level execution — runs at pipeline planning time
# ---------------------------------------------------------------------------

_configs = _find_yaml_configs()
_generated_count = 0

for _cfg in _configs:
    try:
        _validated = _validate_config(_cfg)
        _create_gold_table(_validated)
        _generated_count += 1
    except Exception as _e:
        _source = _cfg.get("_source_path", "unknown")
        print(f"[gold_engine] FAILED to generate from {_source}: {_e}")

if _generated_count > 0:
    print(f"[gold_engine] Successfully generated {_generated_count} gold table(s) from YAML")
else:
    print("[gold_engine] No YAML configs found — no tables generated (hand-coded tables still active)")
