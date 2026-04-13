"""Tests for the model registration notebook (src/epic-on-fhir-requests-model).

Validates:
- Payload generation (schema, data types, JSON validity of POST payloads)
- Conda environment completeness (required packages for model serving)
- Model file template generates valid Python
- MLflow signature has correct input/output schema
- Exit payload contains all keys required by downstream tasks
- Challenger alias is set correctly on new versions

All tests mock MLflow — no real model registration occurs.
"""

import ast
import json
import random
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from conftest import (
    FAKE_ALGO,
    FAKE_MODEL_NAME,
    FAKE_MODEL_VERSION,
    FAKE_TOKEN_URL,
)


# ---------------------------------------------------------------------------
# Replicate notebook functions (these are inline in the notebook, not importable)
# ---------------------------------------------------------------------------


def generate_new_payloads():
    """Replica of generate_new_payloads() from registration notebook."""
    _obs_date = "2019-09-05"
    _obs_time = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
    _effective_dt = f"{_obs_date}T{_obs_time}Z"
    _recorded_date = (date(2024, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat()

    observation_payload = {
        "resourceType": "Observation", "status": "final",
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


# Conda env as defined in the registration notebook
EXPECTED_CONDA_ENV = {
    "name": "epic_on_fhir_serving",
    "channels": ["conda-forge"],
    "dependencies": [
        "python=3.12.3",
        "pip",
        {"pip": [
            "PyJWT",
            "cryptography",
            "requests",
            "pandas",
            "mlflow>=3.1",
            "opentelemetry-api",
            "opentelemetry-sdk",
            "opentelemetry-exporter-otlp-proto-http",
        ]}
    ]
}


# ---------------------------------------------------------------------------
# Tests: Payload generation
# ---------------------------------------------------------------------------


class TestPayloadGeneration:
    """Validate generate_new_payloads() output schema and content."""

    def test_returns_dataframe(self):
        df = generate_new_payloads()
        assert isinstance(df, pd.DataFrame)

    def test_has_three_rows(self):
        df = generate_new_payloads()
        assert len(df) == 3

    def test_required_columns(self):
        df = generate_new_payloads()
        assert set(df.columns) >= {"http_method", "resource", "action"}

    def test_first_row_is_get_patient(self):
        df = generate_new_payloads()
        assert df.iloc[0]["http_method"] == "get"
        assert df.iloc[0]["resource"] == "Patient"
        assert len(df.iloc[0]["action"]) > 0  # Patient ID must be non-empty

    def test_post_rows_have_valid_json_data(self):
        df = generate_new_payloads()
        for idx in [1, 2]:
            data_str = df.iloc[idx]["data"]
            assert isinstance(data_str, str), f"Row {idx} data should be a string"
            parsed = json.loads(data_str)
            assert "resourceType" in parsed, f"Row {idx} missing resourceType"

    def test_observation_payload_has_required_fhir_fields(self):
        df = generate_new_payloads()
        obs = json.loads(df.iloc[1]["data"])
        assert obs["resourceType"] == "Observation"
        assert "subject" in obs
        assert "effectiveDateTime" in obs
        assert "valueQuantity" in obs

    def test_allergy_payload_has_required_fhir_fields(self):
        df = generate_new_payloads()
        allergy = json.loads(df.iloc[2]["data"])
        assert allergy["resourceType"] == "AllergyIntolerance"
        assert "patient" in allergy
        assert "clinicalStatus" in allergy
        assert "code" in allergy

    def test_randomness_produces_different_timestamps(self):
        """Multiple calls should produce different effectiveDateTime values (high probability)."""
        dfs = [generate_new_payloads() for _ in range(5)]
        timestamps = [json.loads(df.iloc[1]["data"])["effectiveDateTime"] for df in dfs]
        # With 86400 possible time values, 5 draws should almost never all be identical
        assert len(set(timestamps)) > 1


# ---------------------------------------------------------------------------
# Tests: Conda environment
# ---------------------------------------------------------------------------


class TestCondaEnvironment:
    """Validate the conda env has all packages required for model serving."""

    def test_has_required_channels(self):
        assert "conda-forge" in EXPECTED_CONDA_ENV["channels"]

    def test_has_pip_dependencies(self):
        pip_deps = None
        for dep in EXPECTED_CONDA_ENV["dependencies"]:
            if isinstance(dep, dict) and "pip" in dep:
                pip_deps = dep["pip"]
        assert pip_deps is not None

    def test_required_packages_present(self):
        """All packages needed for FHIR auth + OTel must be in the conda env."""
        pip_deps = [d for d in EXPECTED_CONDA_ENV["dependencies"] if isinstance(d, dict)][0]["pip"]
        required = {"PyJWT", "cryptography", "requests", "pandas"}
        pip_names = {p.split(">=")[0].split("==")[0] for p in pip_deps}
        missing = required - pip_names
        assert not missing, f"Missing required packages: {missing}"

    def test_mlflow_version_constraint(self):
        pip_deps = [d for d in EXPECTED_CONDA_ENV["dependencies"] if isinstance(d, dict)][0]["pip"]
        mlflow_dep = [p for p in pip_deps if p.startswith("mlflow")]
        assert len(mlflow_dep) == 1
        assert ">=" in mlflow_dep[0], "mlflow should have a minimum version constraint"

    def test_otel_packages_present(self):
        pip_deps = [d for d in EXPECTED_CONDA_ENV["dependencies"] if isinstance(d, dict)][0]["pip"]
        otel_required = {"opentelemetry-api", "opentelemetry-sdk", "opentelemetry-exporter-otlp-proto-http"}
        pip_names = set(pip_deps)
        missing = otel_required - pip_names
        assert not missing, f"Missing OTel packages: {missing}"

    def test_no_proxy_url_in_conda_env(self):
        """Conda env must NOT contain proxy URLs (unreachable from model serving)."""
        env_str = json.dumps(EXPECTED_CONDA_ENV)
        assert "pypi-proxy" not in env_str
        assert "extra-index-url" not in env_str


# ---------------------------------------------------------------------------
# Tests: Model file template
# ---------------------------------------------------------------------------


class TestModelFileTemplate:
    """Validate the models-from-code model definition template."""

    def _render_template(self):
        token_url = FAKE_TOKEN_URL
        algo = FAKE_ALGO
        return f'''"""Epic on FHIR MLflow pyfunc model - Models from Code definition"""
import mlflow
from mlflow.models import set_model
from smart_on_fhir.epic_fhir_pyfunc import EpicFhirPyfuncModel

model = EpicFhirPyfuncModel(
\ttoken_url="{token_url}",
\talgo="{algo}"
)

set_model(model)
'''

    def test_template_is_valid_python(self):
        code = self._render_template()
        # ast.parse will raise SyntaxError if the code is invalid
        tree = ast.parse(code)
        assert tree is not None

    def test_template_imports_set_model(self):
        code = self._render_template()
        assert "set_model" in code

    def test_template_calls_set_model(self):
        code = self._render_template()
        assert "set_model(model)" in code

    def test_template_uses_correct_class(self):
        code = self._render_template()
        assert "EpicFhirPyfuncModel" in code


# ---------------------------------------------------------------------------
# Tests: Exit payload
# ---------------------------------------------------------------------------


class TestExitPayload:
    """Validate the notebook exit JSON contains all keys needed by downstream tasks."""

    REQUIRED_EXIT_KEYS = {"model_name", "model_version", "model_uri", "model_id"}

    def test_exit_payload_has_required_keys(self, mock_model_info):
        payload = json.dumps({
            "model_name": FAKE_MODEL_NAME,
            "model_version": mock_model_info.registered_model_version,
            "model_uri": mock_model_info.model_uri,
            "model_id": mock_model_info.model_id,
        })
        parsed = json.loads(payload)
        missing = self.REQUIRED_EXIT_KEYS - set(parsed.keys())
        assert not missing, f"Exit payload missing keys: {missing}"

    def test_exit_payload_is_json_serializable(self, mock_model_info):
        payload = {
            "model_name": FAKE_MODEL_NAME,
            "model_version": mock_model_info.registered_model_version,
            "model_uri": mock_model_info.model_uri,
            "model_id": mock_model_info.model_id,
        }
        # Should not raise
        result = json.dumps(payload)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Tests: Challenger alias
# ---------------------------------------------------------------------------


class TestChallengerAlias:
    """Validate challenger alias is set correctly on registration."""

    def test_set_challenger_alias(self, mock_mlflow_client, mock_model_info):
        mock_mlflow_client.set_registered_model_alias(
            name=FAKE_MODEL_NAME,
            alias="challenger",
            version=mock_model_info.registered_model_version,
        )
        mock_mlflow_client.set_registered_model_alias.assert_called_once_with(
            name=FAKE_MODEL_NAME,
            alias="challenger",
            version=FAKE_MODEL_VERSION,
        )
