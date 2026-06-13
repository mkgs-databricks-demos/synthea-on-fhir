"""Pydantic validation models for Gold Table YAML configurations.

Validates YAML structure before the engine generates SQL. Run at planning time
(module import) to fail fast on invalid configs.

Usage:
    from fhir_gold_etl.schema.gold_table_schema import GoldTableConfig
    import yaml

    with open("fixtures/gold_etl/encounter_gold.gold.yml") as f:
        config = GoldTableConfig(**yaml.safe_load(f))
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class JoinType(str, Enum):
    """How the resolved view connects to the silver source."""
    entity = "entity"           # No join (patient, practitioner, org)
    event = "event"             # LEFT JOIN patient on bundle_uuid + ref URL
    correlated = "correlated"   # Scalar subquery (location)
    bridge = "bridge"           # LATERAL VIEW EXPLODE pattern


class NaturalKeyStrategy(str, Enum):
    """How the dedup key is computed."""
    composite_sha2 = "composite_sha2"         # sha2(CONCAT(field1|field2|...))
    identifier_cascade = "identifier_cascade"  # COALESCE(FILTER(identifiers, ...))
    custom = "custom"                          # Raw SQL expression


class ExpectationAction(str, Enum):
    """What to do when an expectation fails."""
    warn = "warn"     # Log warning, keep row
    drop = "drop"     # Drop the failing row
    fail = "fail"     # Fail the pipeline update


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------

class ColumnDef(BaseModel):
    """A single column in the gold table schema."""
    name: str = Field(..., description="Column name in the output table")
    type: str = Field(..., description="Spark SQL type string (e.g., STRING, TIMESTAMP, DOUBLE)")
    comment: str = Field(..., description="UC column comment — explains meaning and source")
    source: str = Field(..., description="SQL expression to compute this column from the source view. Supports {{patient_natural_key}} template variable.")

    @field_validator("name")
    @classmethod
    def name_is_valid_identifier(cls, v: str) -> str:
        if not v.replace("_", "").isalnum():
            raise ValueError(f"Column name must be alphanumeric + underscores: {v}")
        return v


class NaturalKeyComponent(BaseModel):
    """One component of a composite_sha2 natural key."""
    expr: str = Field(..., description="SQL expression for this key component")
    default: str = Field("NULL", description="Default value if expr is NULL (used in COALESCE)")


class NaturalKeyDef(BaseModel):
    """Natural key definition — how to deduplicate rows."""
    column_name: str = Field(..., description="Output column name for the natural key")
    strategy: NaturalKeyStrategy
    components: Optional[list[NaturalKeyComponent]] = Field(
        None, description="Components for composite_sha2 strategy"
    )
    sql: Optional[str] = Field(
        None, description="Raw SQL for identifier_cascade or custom strategy"
    )

    @field_validator("components")
    @classmethod
    def components_required_for_sha2(cls, v, info):
        if info.data.get("strategy") == NaturalKeyStrategy.composite_sha2 and not v:
            raise ValueError("composite_sha2 strategy requires at least one component")
        return v

    @field_validator("sql")
    @classmethod
    def sql_required_for_cascade(cls, v, info):
        strategy = info.data.get("strategy")
        if strategy in (NaturalKeyStrategy.identifier_cascade, NaturalKeyStrategy.custom) and not v:
            raise ValueError(f"{strategy} strategy requires 'sql' field")
        return v


class SourceDef(BaseModel):
    """Where the data comes from."""
    silver_table: str = Field(..., description="Silver table name (without catalog/schema prefix)")
    join_type: JoinType = Field(..., description="How to join with patient for FK resolution")
    patient_ref_field: Optional[str] = Field(
        None,
        description="Which references.field links to patient (required for event join_type)"
    )
    where_clause: Optional[str] = Field(
        None,
        description="Optional SQL WHERE filter on the source stream"
    )

    @field_validator("patient_ref_field")
    @classmethod
    def ref_field_required_for_event(cls, v, info):
        if info.data.get("join_type") == JoinType.event and not v:
            raise ValueError("event join_type requires patient_ref_field")
        return v


class ExpectationDef(BaseModel):
    """A data quality expectation on the output table."""
    name: str = Field(..., description="Expectation name (snake_case, descriptive)")
    expr: str = Field(..., description="SQL boolean expression that should be TRUE for valid rows")
    action: ExpectationAction = Field(
        ExpectationAction.warn,
        description="What to do when the expectation fails"
    )


class TableDef(BaseModel):
    """Top-level table metadata."""
    name: str = Field(..., description="Output table name (e.g., encounter_gold)")
    comment: str = Field(..., description="UC table comment — describes purpose, grain, consumers")
    cluster_by: list[str] = Field(
        default_factory=list,
        description="Liquid clustering columns"
    )
    table_properties: dict[str, str] = Field(
        default_factory=dict,
        description="Table property overrides (merged with global defaults)"
    )


# ---------------------------------------------------------------------------
# Root model
# ---------------------------------------------------------------------------

class GoldTableConfig(BaseModel):
    """Complete configuration for a single FHIR Gold streaming table.

    Validated from a *.gold.yml file. The engine uses this to generate:
      1. A temporary view (entity resolution SQL)
      2. A streaming table (schema DDL with comments)
      3. An Auto CDC Type 1 flow (keys + sequence_by)
      4. Data quality expectations (dp.expect / dp.expect_or_drop)
    """
    table: TableDef
    source: SourceDef
    natural_key: NaturalKeyDef
    columns: list[ColumnDef] = Field(
        ..., min_length=1, description="At least one user-defined column required"
    )
    expectations: list[ExpectationDef] = Field(
        default_factory=list,
        description="Data quality expectations (optional)"
    )

    @field_validator("columns")
    @classmethod
    def no_reserved_columns(cls, v: list[ColumnDef]) -> list[ColumnDef]:
        """Ensure users don't define auto-appended columns."""
        reserved = {"resource_last_updated", "resource"}
        for col in v:
            if col.name in reserved:
                raise ValueError(
                    f"Column '{col.name}' is auto-appended by the engine — "
                    f"do not include it in the YAML columns list"
                )
        return v

    def get_source_uuids_column_name(self) -> str:
        """Auto-generated provenance column name."""
        return f"source_{self.source.silver_table}_uuids"
