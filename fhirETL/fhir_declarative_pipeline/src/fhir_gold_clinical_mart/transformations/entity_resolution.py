"""Clinical Mart — Entity Resolution Source Views.

Reads from the FHIR Gold tables (entity-resolved SCD1, in the FHIR schema)
and produces streaming temp views (*_src) that compute analytics-friendly
columns required by the clinical mart star schema but absent from the gold
tables.

CDC flows (dp.create_auto_cdc_flow) are declared in dimensions.py,
co-located with their dp.create_streaming_table() declarations — matching
the convention in fhir_gold.py.

Gold tables are already fully entity-resolved (natural keys assigned by the
FHIR Gold ETL). This layer adds:
  - dim_patient   : age_years, age_band, full_name, primary_identifier_* (SSN/MRN)
  - dim_practitioner: full_name
  - fact_encounter: length_of_stay_hours, is_emergency, is_inpatient;
                    practitioner/org/location FK join logic present but
                    commented out (TD-1 — columns not yet in dimensions.py schema)
  - fact_condition: is_chronic, is_active flags
  - fact_observation: is_abnormal_low/high (NULL-safe CASE)
  - fact_procedure: duration_minutes

Tech-debt notes
  TD-1  fact_encounter missing practitioner_natural_key, organization_natural_key,
        location_natural_key — columns omitted from dimensions.py schema; FK
        resolution is implemented in fact_encounter_src but output columns
        are commented out pending schema update. URL-only join (no bundle_uuid
        scoping) is valid for Synthea exports; real-EMR data requires
        _bundle_uuid on encounter_gold.
  TD-2  fact_observation missing value_raw VARIANT (design doc §8a.6). Add
        `value_raw VARIANT` to dimensions.py fact_observation schema, then
        uncomment the corresponding SELECT column below.
  TD-3  fact_condition missing encounter_natural_key FK. Add column to
        dimensions.py and uncomment the encounter join in fact_condition_src.

Config keys (set in fhir_gold_clinical_mart.pipeline.yml):
  pipeline.catalog_use          — Unity Catalog catalog
  pipeline.silver_schema_use    — FHIR schema (where *_gold tables live)
  pipeline.clinical_mart_schema_use — destination schema (dim_/fact_ tables)
"""

from pyspark import pipelines as dp


# ---------------------------------------------------------------------------
# Pipeline configuration
# ---------------------------------------------------------------------------
try:
    _catalog = spark.conf.get("pipeline.catalog_use")
    _schema  = spark.conf.get("pipeline.silver_schema_use")
except Exception:
    _catalog = ""
    _schema  = ""


def _gold(table: str) -> str:
    """Fully-qualified STREAM reference to a gold table in the FHIR schema."""
    return f"STREAM({_catalog}.{_schema}.{table})"


def _static(table: str) -> str:
    """Fully-qualified static (snapshot) reference to a gold table.

    Use for dimension lookup tables in stream-stream join CTEs.
    Structured Streaming does not support stream-stream LEFT OUTER joins
    without watermarks; the dimension side must be a static read.
    """
    return f"{_catalog}.{_schema}.{table}"


# ===========================================================================
# DIMENSION SOURCE VIEWS + CDC FLOWS
# ===========================================================================

# ---------------------------------------------------------------------------
# dim_patient
# ---------------------------------------------------------------------------

@dp.temporary_view(name="dim_patient_src")
def _dim_patient_src():
    """Patient dimension source.

    Adds age_years, age_band, full_name, and primary_identifier_* columns
    that are not available on patient_gold but are required by dim_patient.
    Identifier priority: SSN (http://hl7.org/fhir/sid/us-ssn or OID alias)
    preferred; MRN (type_code = MR) as fallback — matching the natural key
    selection logic in the FHIR Gold ETL entity_resolution.py.
    """
    return spark.sql(f"""
        SELECT
            patient_natural_key,
            family_name,
            given_name,
            CONCAT_WS(' ', given_name, family_name)                         AS full_name,
            birth_date,
            -- Age computed at pipeline refresh time. For real-time age use birth_date.
            CAST(DATEDIFF(current_date(), birth_date) / 365.25 AS INT)      AS age_years,
            CASE
                WHEN birth_date IS NULL
                                                              THEN NULL
                WHEN DATEDIFF(current_date(), birth_date) / 365.25 <  18   THEN '0-17'
                WHEN DATEDIFF(current_date(), birth_date) / 365.25 <  35   THEN '18-34'
                WHEN DATEDIFF(current_date(), birth_date) / 365.25 <  50   THEN '35-49'
                WHEN DATEDIFF(current_date(), birth_date) / 365.25 <  65   THEN '50-64'
                ELSE '65+'
            END                                                              AS age_band,
            gender,
            deceased,
            address_city,
            address_state,
            address_postal_code,
            marital_status,
            -- Primary identifier system (SSN URI preferred, MRN type_code fallback)
            COALESCE(
                FILTER(identifiers, x -> x.system IN (
                    'http://hl7.org/fhir/sid/us-ssn',
                    'urn:oid:2.16.840.1.113883.4.1'
                ))[0].system,
                FILTER(identifiers, x -> x.type_code = 'MR')[0].system
            )                                                                AS primary_identifier_system,
            -- Primary identifier value (the actual SSN or MRN string)
            COALESCE(
                FILTER(identifiers, x -> x.system IN (
                    'http://hl7.org/fhir/sid/us-ssn',
                    'urn:oid:2.16.840.1.113883.4.1'
                ))[0].value,
                FILTER(identifiers, x -> x.type_code = 'MR')[0].value
            )                                                                AS primary_identifier_value,
            COALESCE(SIZE(identifiers), 0)                                   AS identifier_count,
            resource_last_updated
        FROM {_gold('patient_gold')}
        WHERE patient_natural_key IS NOT NULL
    """)




# ---------------------------------------------------------------------------
# dim_practitioner
# ---------------------------------------------------------------------------

@dp.temporary_view(name="dim_practitioner_src")
def _dim_practitioner_src():
    """Practitioner dimension source. Adds full_name display column."""
    return spark.sql(f"""
        SELECT
            practitioner_natural_key,
            family_name,
            given_name,
            CONCAT_WS(' ', given_name, family_name)  AS full_name,
            specialty_code,
            specialty_display,
            resource_last_updated
        FROM {_gold('practitioner_gold')}
        WHERE practitioner_natural_key IS NOT NULL
    """)




# ---------------------------------------------------------------------------
# dim_organization
# ---------------------------------------------------------------------------

@dp.temporary_view(name="dim_organization_src")
def _dim_organization_src():
    """Organization dimension source. Pass-through from organization_gold."""
    return spark.sql(f"""
        SELECT
            organization_natural_key,
            name,
            type_code,
            type_display,
            address_city,
            address_state,
            resource_last_updated
        FROM {_gold('organization_gold')}
        WHERE organization_natural_key IS NOT NULL
    """)




# ---------------------------------------------------------------------------
# dim_location
# ---------------------------------------------------------------------------

@dp.temporary_view(name="dim_location_src")
def _dim_location_src():
    """Location dimension source. Pass-through from location_gold."""
    return spark.sql(f"""
        SELECT
            location_natural_key,
            name,
            managing_organization_nk,
            address_city,
            address_state,
            address_postal_code,
            resource_last_updated
        FROM {_gold('location_gold')}
        WHERE location_natural_key IS NOT NULL
    """)




# ===========================================================================
# FACT SOURCE VIEWS + CDC FLOWS
# ===========================================================================

# ---------------------------------------------------------------------------
# fact_encounter
# ---------------------------------------------------------------------------

@dp.temporary_view(name="fact_encounter_src")
def _fact_encounter_src():
    """Encounter fact source.

    Computes length_of_stay_hours, is_emergency, is_inpatient from scalars
    already on encounter_gold.

    TD-1: practitioner_natural_key, organization_natural_key, location_natural_key
    are resolved below (via references array URL joins to the respective gold
    tables) but excluded from the SELECT until those columns are added to the
    fact_encounter schema in dimensions.py. The join logic is preserved and
    commented out to make the schema addition a 3-line change.

    Reference join strategy: URL-only (no bundle_uuid scope). Valid for
    Synthea exports where practitioner/org UUIDs are deterministic across
    patient bundles. See module docstring for multi-source caveat.
    """
    return spark.sql(f"""
        WITH enc AS (
            SELECT
                encounter_natural_key,
                patient_natural_key,
                encounter_class,
                encounter_type_code,
                encounter_type_display,
                status,
                period_start,
                period_end,
                reason_code,
                reason_display,
                resource_last_updated,
                -- Extract reference URLs for dimension FK resolution (TD-1)
                FILTER(references, r -> r.field = 'participant')[0].url      AS _participant_url,
                FILTER(references, r -> r.field = 'serviceProvider')[0].url  AS _service_provider_url,
                FILTER(references, r -> r.field = 'location')[0].url         AS _location_url
            FROM {_gold('encounter_gold')}
            WHERE encounter_natural_key IS NOT NULL
              AND patient_natural_key   IS NOT NULL
        ),
        -- Dimension lookups are static reads (no STREAM) to avoid
        -- unsupported stream-stream LEFT OUTER join (no watermark available).
        prac AS (
            SELECT practitioner_natural_key, practitioner_url
            FROM   {_static('practitioner_gold')}
        ),
        org  AS (
            SELECT organization_natural_key, organization_url
            FROM   {_static('organization_gold')}
        ),
        loc  AS (
            SELECT location_natural_key, location_url
            FROM   {_static('location_gold')}
        )
        SELECT
            enc.encounter_natural_key,
            enc.patient_natural_key,
            -- TD-1: uncomment the three lines below after adding FK columns to
            -- fact_encounter in dimensions.py:
            -- prac.practitioner_natural_key,
            -- org.organization_natural_key,
            -- loc.location_natural_key,
            enc.encounter_class,
            enc.encounter_type_code,
            enc.encounter_type_display,
            enc.status,
            enc.period_start,
            enc.period_end,
            -- LOS in hours; NULL if encounter is still open (period_end IS NULL)
            CAST(
                (UNIX_TIMESTAMP(enc.period_end) - UNIX_TIMESTAMP(enc.period_start)) / 3600.0
            AS DOUBLE)                                                        AS length_of_stay_hours,
            enc.reason_code,
            enc.reason_display,
            enc.encounter_class = 'EMER'                                      AS is_emergency,
            enc.encounter_class = 'IMP'                                       AS is_inpatient,
            enc.resource_last_updated
        FROM enc
        -- TD-1: activate joins after adding FK columns to fact_encounter schema:
        LEFT JOIN prac ON prac.practitioner_url = enc._participant_url
        LEFT JOIN org  ON org.organization_url  = enc._service_provider_url
        LEFT JOIN loc  ON loc.location_url      = enc._location_url
    """)




# ---------------------------------------------------------------------------
# fact_condition
# ---------------------------------------------------------------------------

@dp.temporary_view(name="fact_condition_src")
def _fact_condition_src():
    """Condition fact source.

    Derives is_chronic and is_active flags from category and clinical_status.

    TD-3: encounter_natural_key FK resolution is available via the
    _encounter_ref_url column on condition_gold. To activate:
      1. Add `encounter_natural_key STRING` to fact_condition in dimensions.py.
      2. Add the encounter CTE and LEFT JOIN below, uncomment the column.
    """
    return spark.sql(f"""
        SELECT
            condition_natural_key,
            patient_natural_key,
            -- TD-3: add encounter_natural_key here after schema update
            code,
            code_system,
            code_display,
            category,
            clinical_status,
            verification_status,
            onset_datetime,
            abatement_datetime,
            -- Chronic: problem list item OR clinically active/recurring
            (
                category = 'problem-list-item'
                OR clinical_status IN ('active', 'recurrence', 'relapse')
            )                                                                AS is_chronic,
            -- Active: any status that is not conclusively resolved
            clinical_status NOT IN (
                'inactive', 'resolved', 'remission',
                'entered-in-error', 'refuted'
            )                                                                AS is_active,
            resource_last_updated
        FROM {_gold('condition_gold')}
        WHERE condition_natural_key IS NOT NULL
          AND patient_natural_key   IS NOT NULL
    """)




# ---------------------------------------------------------------------------
# fact_observation
# ---------------------------------------------------------------------------

@dp.temporary_view(name="fact_observation_src")
def _fact_observation_src():
    """Observation fact source.

    Derives is_abnormal_low / is_abnormal_high from numeric value and
    reference ranges. NULL-safe: flags are NULL when value_quantity or the
    relevant range bound is NULL.

    TD-2: value_raw VARIANT (design doc §8a.6) is present on observation_gold
    but absent from fact_observation in dimensions.py. Add `value_raw VARIANT`
    to the schema in dimensions.py, then uncomment the SELECT line below.
    """
    return spark.sql(f"""
        SELECT
            observation_natural_key,
            patient_natural_key,
            code,
            code_display,
            category,
            value_quantity,
            value_unit,
            value_string,
            value_code,
            -- TD-2: uncomment after adding value_raw VARIANT to dimensions.py:
            -- value_raw,
            effective_datetime,
            -- Abnormal flags; NULL when value_quantity or bounds are unavailable
            CASE
                WHEN value_quantity IS NULL OR reference_range_low  IS NULL THEN NULL
                ELSE value_quantity < reference_range_low
            END                                                              AS is_abnormal_low,
            CASE
                WHEN value_quantity IS NULL OR reference_range_high IS NULL THEN NULL
                ELSE value_quantity > reference_range_high
            END                                                              AS is_abnormal_high,
            reference_range_low,
            reference_range_high,
            resource_last_updated
        FROM {_gold('observation_gold')}
        WHERE observation_natural_key IS NOT NULL
          AND patient_natural_key     IS NOT NULL
    """)




# ---------------------------------------------------------------------------
# fact_procedure
# ---------------------------------------------------------------------------

@dp.temporary_view(name="fact_procedure_src")
def _fact_procedure_src():
    """Procedure fact source. Derives duration_minutes from performed timestamps."""
    return spark.sql(f"""
        SELECT
            procedure_natural_key,
            patient_natural_key,
            code,
            code_system,
            code_display,
            status,
            performed_start,
            performed_end,
            -- Duration in minutes; NULL for instantaneous procedures (no end time)
            CAST(
                (UNIX_TIMESTAMP(performed_end) - UNIX_TIMESTAMP(performed_start)) / 60.0
            AS DOUBLE)                                                        AS duration_minutes,
            body_site_code,
            body_site_display,
            resource_last_updated
        FROM {_gold('procedure_gold')}
        WHERE procedure_natural_key IS NOT NULL
          AND patient_natural_key   IS NOT NULL
    """)




# ---------------------------------------------------------------------------
# fact_medication_request
# ---------------------------------------------------------------------------

@dp.temporary_view(name="fact_medication_request_src")
def _fact_medication_request_src():
    """Medication request (prescription) fact source. Pass-through from gold."""
    return spark.sql(f"""
        SELECT
            medication_request_natural_key,
            patient_natural_key,
            medication_code,
            medication_system,
            medication_display,
            status,
            intent,
            authored_on,
            dosage_text,
            resource_last_updated
        FROM {_gold('medication_request_gold')}
        WHERE medication_request_natural_key IS NOT NULL
          AND patient_natural_key            IS NOT NULL
    """)




# ---------------------------------------------------------------------------
# fact_immunization
# ---------------------------------------------------------------------------

@dp.temporary_view(name="fact_immunization_src")
def _fact_immunization_src():
    """Immunization fact source. Pass-through from gold."""
    return spark.sql(f"""
        SELECT
            immunization_natural_key,
            patient_natural_key,
            vaccine_code,
            vaccine_display,
            status,
            occurrence_datetime,
            resource_last_updated
        FROM {_gold('immunization_gold')}
        WHERE immunization_natural_key IS NOT NULL
          AND patient_natural_key      IS NOT NULL
    """)


