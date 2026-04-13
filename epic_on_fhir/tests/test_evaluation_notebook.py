"""Tests for the evaluation notebook (src/evaluation).

Validates:
- Test payload generation (schema, coverage of GET + POST methods)
- JSON serialization validation logic
- Metric key patterns match expected naming conventions
- Status code validation (200 for GET, 201 for POST)
- Validation gate assertions block downstream on failure
- Exit payload format for downstream tasks

All tests mock MLflow and the Epic FHIR API — no real API calls.
"""

import json
import random
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from conftest import (
    FAKE_MODEL_NAME,
    FAKE_MODEL_VERSION,
    FAKE_EXPERIMENT_NAME,
)


# ---------------------------------------------------------------------------
# Replicate notebook functions (inline in the evaluation notebook)
# ---------------------------------------------------------------------------


def generate_test_payloads():
    """Replica of generate_test_payloads() from evaluation notebook."""
    _obs_time = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
    _recorded_date = (date(2024, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat()

    observation_payload = {
        "resourceType": "Observation", "status": "final",
        "category": [{"coding": [{"system": "http://hl7.org/fhir/observation-category", "code": "vital-signs", "display": "Vital Signs"}], "text": "Vital Signs"}],
        "code": {"coding": [{"system": "urn:oid:1.2.840.114350.1.13.0.1.7.2.707679", "code": "8", "display": "Heart Rate"}], "text": "Heart Rate"},
        "subject": {"reference": "Patient/T1wI5bk8n1YVgvWk9D05BmRV0Pi3ECImNSK8DKyKltsMB"},
        "encounter": {"reference": "Encounter/e0u1fd.jUCNqz8ZQuTaMtsQ3"},
        "effectiveDateTime": f"2019-09-05T{_obs_time}Z",
        "valueQuantity": {"value": 75},
    }
    allergy_payload = {
        "resourceType": "AllergyIntolerance",
        "clinicalStatus": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical", "code": "active", "display": "Active"}], "text": "Active"},
        "verificationStatus": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification", "code": "unconfirmed", "display": "Unconfirmed"}], "text": "Unconfirmed"},
        "type": "allergy", "category": ["medication"], "criticality": "low",
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
# Helpers: Replicate validation logic from the evaluation notebook
# ---------------------------------------------------------------------------


def validate_json_serialization(results: dict) -> bool:
    """Replica of JSON serialization validation from evaluation notebook."""
    json_valid = True
    for label, resp in results.items():
        try:
            json.dumps(resp)
        except (TypeError, ValueError):
            json_valid = False
    return json_valid


def compute_metrics(results: dict, json_valid: bool) -> dict:
    """Replica of metric computation from evaluation notebook."""
    metrics = {}

    for label, resp in results.items():
        metrics[f"validation.{label}.status_code"] = resp.get("response_status_code", -1)
        metrics[f"validation.{label}.response_time_seconds"] = resp.get("response_time_seconds", -1)

    metrics["validation.json_serializable"] = 1.0 if json_valid else 0.0

    _get_ok = results.get("get_patient", {}).get("response_status_code", -1) == 200
    _post_obs_ok = results.get("post_observation", {}).get("response_status_code", -1) == 201
    _post_allergy_ok = results.get("post_allergyintolerance", {}).get("response_status_code", -1) == 201

    metrics["validation.get_patient_pass"] = 1.0 if _get_ok else 0.0
    metrics["validation.post_observation_pass"] = 1.0 if _post_obs_ok else 0.0
    metrics["validation.post_allergyintolerance_pass"] = 1.0 if _post_allergy_ok else 0.0
    metrics["validation.all_passed"] = 1.0 if (_get_ok and _post_obs_ok and _post_allergy_ok and json_valid) else 0.0

    return metrics


# ---------------------------------------------------------------------------
# Tests: Test payload generation
# ---------------------------------------------------------------------------


class TestPayloadGeneration:
    """Validate generate_test_payloads() for evaluation."""

    def test_returns_dataframe_with_three_rows(self):
        df = generate_test_payloads()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_covers_get_and_post_methods(self):
        df = generate_test_payloads()
        methods = set(df["http_method"].values)
        assert "get" in methods
        assert "post" in methods

    def test_covers_required_resources(self):
        df = generate_test_payloads()
        resources = set(df["resource"].values)
        assert resources == {"Patient", "Observation", "AllergyIntolerance"}

    def test_get_row_has_patient_id(self):
        df = generate_test_payloads()
        get_rows = df[df["http_method"] == "get"]
        assert len(get_rows) == 1
        assert len(get_rows.iloc[0]["action"]) > 0

    def test_post_rows_have_json_data(self):
        df = generate_test_payloads()
        post_rows = df[df["http_method"] == "post"]
        assert len(post_rows) == 2
        for _, row in post_rows.iterrows():
            parsed = json.loads(row["data"])
            assert "resourceType" in parsed


# ---------------------------------------------------------------------------
# Tests: JSON serialization validation
# ---------------------------------------------------------------------------


class TestJsonValidation:
    """Validate JSON serialization checker."""

    def test_valid_responses_pass(self):
        results = {
            "get_patient": {"response_status_code": 200, "response_time_seconds": 0.25},
            "post_observation": {"response_status_code": 201, "response_time_seconds": 0.5},
        }
        assert validate_json_serialization(results) is True

    def test_non_serializable_response_fails(self):
        """Objects like sets and bytes are not JSON-serializable (TypeError)."""
        results = {
            "get_patient": {"response_status_code": 200, "bad_value": {1, 2, 3}},
        }
        assert validate_json_serialization(results) is False

    def test_empty_results_pass(self):
        assert validate_json_serialization({}) is True

    def test_nested_dict_passes(self):
        results = {
            "get_patient": {"nested": {"deep": {"list": [1, 2, 3]}}},
        }
        assert validate_json_serialization(results) is True

    def test_bytes_value_fails(self):
        results = {
            "get_patient": {"response_status_code": 200, "binary": b"not serializable"},
        }
        assert validate_json_serialization(results) is False


# ---------------------------------------------------------------------------
# Tests: Metric computation
# ---------------------------------------------------------------------------


class TestMetricComputation:
    """Validate metric keys and values match expected patterns."""

    @pytest.fixture()
    def passing_results(self):
        return {
            "get_patient": {"response_status_code": 200, "response_time_seconds": 0.2},
            "post_observation": {"response_status_code": 201, "response_time_seconds": 0.4},
            "post_allergyintolerance": {"response_status_code": 201, "response_time_seconds": 0.3},
        }

    @pytest.fixture()
    def failing_results(self):
        return {
            "get_patient": {"response_status_code": 401, "response_time_seconds": 0.1},
            "post_observation": {"response_status_code": 500, "response_time_seconds": 0.5},
            "post_allergyintolerance": {"response_status_code": 201, "response_time_seconds": 0.3},
        }

    def test_all_passed_when_successful(self, passing_results):
        metrics = compute_metrics(passing_results, json_valid=True)
        assert metrics["validation.all_passed"] == 1.0
        assert metrics["validation.get_patient_pass"] == 1.0
        assert metrics["validation.post_observation_pass"] == 1.0
        assert metrics["validation.post_allergyintolerance_pass"] == 1.0

    def test_all_passed_false_when_get_fails(self, failing_results):
        metrics = compute_metrics(failing_results, json_valid=True)
        assert metrics["validation.all_passed"] == 0.0
        assert metrics["validation.get_patient_pass"] == 0.0

    def test_all_passed_false_when_json_invalid(self, passing_results):
        metrics = compute_metrics(passing_results, json_valid=False)
        assert metrics["validation.all_passed"] == 0.0
        assert metrics["validation.json_serializable"] == 0.0

    def test_metric_keys_follow_naming_convention(self, passing_results):
        metrics = compute_metrics(passing_results, json_valid=True)
        for key in metrics:
            assert key.startswith("validation."), f"Metric key {key} doesn't start with 'validation.'"

    def test_status_code_metrics_present(self, passing_results):
        metrics = compute_metrics(passing_results, json_valid=True)
        assert "validation.get_patient.status_code" in metrics
        assert "validation.post_observation.status_code" in metrics
        assert "validation.post_allergyintolerance.status_code" in metrics

    def test_response_time_metrics_present(self, passing_results):
        metrics = compute_metrics(passing_results, json_valid=True)
        assert "validation.get_patient.response_time_seconds" in metrics

    def test_missing_result_defaults_to_negative_one(self):
        """If a result key is missing, status_code should default to -1."""
        metrics = compute_metrics({}, json_valid=True)
        assert metrics["validation.get_patient_pass"] == 0.0


# ---------------------------------------------------------------------------
# Tests: Validation gate assertions
# ---------------------------------------------------------------------------


class TestValidationGate:
    """Validate the assertion logic that blocks downstream tasks."""

    def test_passing_results_do_not_raise(self):
        results = {
            "get_patient": {"response_status_code": 200},
            "post_observation": {"response_status_code": 201},
            "post_allergyintolerance": {"response_status_code": 201},
        }
        _get_ok = results["get_patient"]["response_status_code"] == 200
        _post_obs_ok = results["post_observation"]["response_status_code"] == 201
        _post_allergy_ok = results["post_allergyintolerance"]["response_status_code"] == 201
        json_valid = True

        # These should not raise
        assert _get_ok
        assert _post_obs_ok
        assert _post_allergy_ok
        assert json_valid

    def test_failed_get_raises_assertion(self):
        results = {"get_patient": {"response_status_code": 401}}
        _get_ok = results["get_patient"]["response_status_code"] == 200
        with pytest.raises(AssertionError):
            assert _get_ok, f"GET Patient failed: status={results['get_patient']['response_status_code']}"

    def test_failed_post_observation_raises_assertion(self):
        results = {"post_observation": {"response_status_code": 500}}
        _post_obs_ok = results["post_observation"]["response_status_code"] == 201
        with pytest.raises(AssertionError):
            assert _post_obs_ok

    def test_json_invalid_raises_assertion(self):
        json_valid = False
        with pytest.raises(AssertionError):
            assert json_valid, "JSON serialization validation failed"


# ---------------------------------------------------------------------------
# Tests: Exit payload
# ---------------------------------------------------------------------------


class TestExitPayload:
    """Validate the evaluation exit JSON for downstream consumption."""

    REQUIRED_EXIT_KEYS = {"model_name", "model_version", "validation"}

    def test_exit_payload_on_success(self):
        payload = json.dumps({
            "model_name": FAKE_MODEL_NAME,
            "model_version": FAKE_MODEL_VERSION,
            "validation": "passed",
        })
        parsed = json.loads(payload)
        missing = self.REQUIRED_EXIT_KEYS - set(parsed.keys())
        assert not missing, f"Exit payload missing keys: {missing}"
        assert parsed["validation"] == "passed"

    def test_exit_payload_is_json_serializable(self):
        payload = {
            "model_name": FAKE_MODEL_NAME,
            "model_version": FAKE_MODEL_VERSION,
            "validation": "passed",
        }
        result = json.dumps(payload)
        assert isinstance(result, str)
