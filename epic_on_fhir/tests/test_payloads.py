"""Tests for notebook payload generation and NaN handling.

Verifies the generate_new_payloads() function produces valid FHIR input DataFrames
and that the NaN→None conversion pattern (used in notebook cells 30/36) works correctly
for MLflow schema enforcement.
"""

import json
from datetime import date

import pandas as pd
import numpy as np
import pytest


# ---------------------------------------------------------------------------
# Replicate generate_new_payloads from the notebook (cell 14)
# ---------------------------------------------------------------------------
import random
from datetime import timedelta


def generate_new_payloads():
    """Replicated from notebook cell 14 for isolated testing."""
    _obs_date = "2019-09-05"
    _obs_time = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
    _effective_dt = f"{_obs_date}T{_obs_time}Z"
    _recorded_date = (date(2024, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat()

    observation_payload = {
        "resourceType": "Observation",
        "status": "final",
        "category": [{"coding": [{"system": "http://hl7.org/fhir/observation-category", "code": "vital-signs", "display": "Vital Signs"}], "text": "Vital Signs"}],
        "code": {"coding": [{"system": "urn:oid:1.2.840.114350.1.13.0.1.7.2.707679", "code": "8", "display": "Heart Rate"}], "text": "Heart Rate"},
        "subject": {"reference": "Patient/T1wI5bk8n1YVgvWk9D05BmRV0Pi3ECImNSK8DKyKltsMB"},
        "encounter": {"reference": "Encounter/e0u1fd.jUCNqz8ZQuTaMtsQ3"},
        "effectiveDateTime": _effective_dt,
        "valueQuantity": {"value": 75},
    }

    allergy_payload = {
        "resourceType": "AllergyIntolerance",
        "clinicalStatus": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical", "code": "active", "display": "Active"}], "text": "Active"},
        "verificationStatus": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification", "code": "unconfirmed", "display": "Unconfirmed"}], "text": "Unconfirmed"},
        "type": "allergy",
        "category": ["medication"],
        "criticality": "low",
        "code": {"coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "7980", "display": "Penicillin G"}], "text": "Penicillin"},
        "patient": {"reference": "Patient/T1wI5bk8n1YVgvWk9D05BmRV0Pi3ECImNSK8DKyKltsMB"},
        "recorder": {"reference": "Practitioner/eM5CWtq15N0WJeuCet5bJlQ3", "display": "Physician Family Medicine, MD"},
        "recordedDate": _recorded_date,
        "reaction": [{"manifestation": [{"coding": [{"system": "http://snomed.info/sct", "code": "247472004", "display": "Hives"}], "text": "Hives"}]}],
    }

    return pd.DataFrame([
        {"http_method": "get", "resource": "Patient", "action": "T1wI5bk8n1YVgvWk9D05BmRV0Pi3ECImNSK8DKyKltsMB"},
        {"http_method": "post", "resource": "Observation", "action": "", "data": json.dumps(observation_payload)},
        {"http_method": "post", "resource": "AllergyIntolerance", "action": "", "data": json.dumps(allergy_payload)},
    ])


# ---------------------------------------------------------------------------
# Tests: DataFrame structure
# ---------------------------------------------------------------------------

class TestPayloadStructure:
    """Verify generate_new_payloads() produces valid DataFrames."""

    def test_returns_dataframe(self):
        result = generate_new_payloads()
        assert isinstance(result, pd.DataFrame)

    def test_has_three_rows(self):
        result = generate_new_payloads()
        assert len(result) == 3

    def test_required_columns_present(self):
        """Must have http_method, resource, action, data."""
        result = generate_new_payloads()
        for col in ["http_method", "resource", "action", "data"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_first_row_is_get(self):
        """Row 0 should be a GET Patient request."""
        result = generate_new_payloads()
        row = result.iloc[0]
        assert row["http_method"] == "get"
        assert row["resource"] == "Patient"

    def test_second_row_is_post_observation(self):
        result = generate_new_payloads()
        row = result.iloc[1]
        assert row["http_method"] == "post"
        assert row["resource"] == "Observation"

    def test_third_row_is_post_allergy(self):
        result = generate_new_payloads()
        row = result.iloc[2]
        assert row["http_method"] == "post"
        assert row["resource"] == "AllergyIntolerance"

    def test_get_row_has_nan_data(self):
        """GET row should have NaN for data (no request body)."""
        result = generate_new_payloads()
        assert pd.isna(result.iloc[0]["data"])

    def test_post_rows_have_valid_json_data(self):
        """POST rows should have valid JSON strings in data."""
        result = generate_new_payloads()
        for idx in [1, 2]:
            data = result.iloc[idx]["data"]
            assert isinstance(data, str)
            parsed = json.loads(data)
            assert "resourceType" in parsed


class TestPayloadContent:
    """Verify FHIR payload content is valid."""

    def test_observation_has_required_fields(self):
        result = generate_new_payloads()
        obs = json.loads(result.iloc[1]["data"])
        assert obs["resourceType"] == "Observation"
        assert obs["status"] == "final"
        assert "subject" in obs
        assert "effectiveDateTime" in obs

    def test_observation_effective_datetime_format(self):
        """effectiveDateTime should be ISO 8601 with Z suffix."""
        result = generate_new_payloads()
        obs = json.loads(result.iloc[1]["data"])
        dt = obs["effectiveDateTime"]
        assert dt.startswith("2019-09-05T")
        assert dt.endswith("Z")

    def test_allergy_has_required_fields(self):
        result = generate_new_payloads()
        allergy = json.loads(result.iloc[2]["data"])
        assert allergy["resourceType"] == "AllergyIntolerance"
        assert "patient" in allergy
        assert "recordedDate" in allergy

    def test_allergy_recorded_date_is_2024(self):
        """recordedDate should be in 2024."""
        result = generate_new_payloads()
        allergy = json.loads(result.iloc[2]["data"])
        assert allergy["recordedDate"].startswith("2024-")

    def test_payloads_are_randomized(self):
        """Two calls should produce different effectiveDateTime values (probabilistic)."""
        results = [generate_new_payloads() for _ in range(5)]
        datetimes = set()
        for r in results:
            obs = json.loads(r.iloc[1]["data"])
            datetimes.add(obs["effectiveDateTime"])
        # With 5 attempts and random time, extremely unlikely to get all same
        assert len(datetimes) > 1, "Payloads should be randomized across calls"


# ---------------------------------------------------------------------------
# Tests: NaN → None conversion (notebook cells 30 and 36)
# ---------------------------------------------------------------------------

class TestNanToNoneConversion:
    """Verify the .where(notna, None) pattern used in notebook cells 30/36.

    This is critical for MLflow schema enforcement — NaN (float64) cannot be
    safely converted to string, but None is accepted for optional string columns.
    """

    def test_row_where_converts_nan_to_none(self):
        """row.where(row.notna(), None) should replace NaN with None."""
        payloads = generate_new_payloads()
        row = payloads.iloc[0]  # GET row — has NaN in data

        assert pd.isna(row["data"]), "GET row should have NaN in data column"

        cleaned = row.where(row.notna(), None)
        assert cleaned["data"] is None, "NaN should be converted to None"

    def test_dataframe_where_converts_nan_to_none(self):
        """DataFrame .where(lambda df: df.notna(), None) should replace NaN."""
        payloads = generate_new_payloads()
        single_row = payloads.iloc[[0]]  # GET row as DataFrame

        cleaned = single_row.where(lambda df: df.notna(), None)
        assert cleaned.iloc[0]["data"] is None

    def test_none_preserves_non_nan_values(self):
        """Non-NaN values should be preserved after where()."""
        payloads = generate_new_payloads()
        row = payloads.iloc[0]

        cleaned = row.where(row.notna(), None)
        assert cleaned["http_method"] == "get"
        assert cleaned["resource"] == "Patient"
        assert len(cleaned["action"]) > 0

    def test_post_row_data_unchanged(self):
        """POST rows have real data — should not be affected by where()."""
        payloads = generate_new_payloads()
        row = payloads.iloc[1]  # POST Observation

        cleaned = row.where(row.notna(), None)
        assert cleaned["data"] is not None
        parsed = json.loads(cleaned["data"])
        assert parsed["resourceType"] == "Observation"

    def test_cleaned_dataframe_dtype_is_object(self):
        """After NaN→None, the data column should be object dtype (not float64)."""
        payloads = generate_new_payloads()
        row = payloads.iloc[0]
        cleaned_df = pd.DataFrame([row.where(row.notna(), None)])

        # Object dtype accepts both strings and None
        assert cleaned_df["data"].dtype == object

    def test_all_rows_survive_cleaning(self):
        """Cleaning all rows in a loop should preserve all data."""
        payloads = generate_new_payloads()
        cleaned_rows = []
        for _, row in payloads.iterrows():
            cleaned_rows.append(row.where(row.notna(), None))

        assert len(cleaned_rows) == 3
        assert cleaned_rows[0]["data"] is None  # GET → None
        assert cleaned_rows[1]["data"] is not None  # POST → JSON string
        assert cleaned_rows[2]["data"] is not None  # POST → JSON string
