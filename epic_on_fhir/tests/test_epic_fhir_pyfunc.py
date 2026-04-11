"""Tests for smart_on_fhir.epic_fhir_pyfunc.EpicFhirPyfuncModel.

Covers model initialization, secret loading, predict() with various payloads,
NaN/None handling, and return type validation.
All tests mock the Epic API — no real API calls.
"""

import json
import os
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from smart_on_fhir.epic_fhir_pyfunc import EpicFhirPyfuncModel
from conftest import FAKE_TOKEN_URL, FAKE_ALGO, FAKE_BASE_URL


class TestModelInit:
    """Verify model constructor."""

    def test_default_base_url(self):
        model = EpicFhirPyfuncModel(token_url=FAKE_TOKEN_URL, algo=FAKE_ALGO)
        assert model.base_url.endswith("/")
        assert "FHIR/R4" in model.base_url

    def test_custom_base_url_trailing_slash(self):
        """base_url should always end with /."""
        model = EpicFhirPyfuncModel(
            token_url=FAKE_TOKEN_URL,
            algo=FAKE_ALGO,
            base_url="https://example.com/fhir",
        )
        assert model.base_url == "https://example.com/fhir/"

    def test_attributes_stored(self):
        model = EpicFhirPyfuncModel(token_url=FAKE_TOKEN_URL, algo=FAKE_ALGO)
        assert model.token_url == FAKE_TOKEN_URL
        assert model.algo == FAKE_ALGO


class TestGetSecrets:
    """Verify _get_secrets reads from environment variables."""

    def test_reads_env_vars(self, pyfunc_model, fake_secrets_env):
        """_get_secrets should return values from EPIC_* env vars."""
        client_id, private_key, kid = pyfunc_model._get_secrets()

        assert client_id == fake_secrets_env["client_id"]
        assert private_key == fake_secrets_env["private_key"]
        assert kid == fake_secrets_env["kid"]

    def test_returns_none_when_env_vars_missing(self, pyfunc_model):
        """_get_secrets should return None for missing env vars."""
        # Clear any existing env vars
        env_clean = {k: v for k, v in os.environ.items()
                     if k not in ("EPIC_CLIENT_ID", "EPIC_PRIVATE_KEY", "EPIC_KID")}
        with patch.dict(os.environ, env_clean, clear=True):
            client_id, private_key, kid = pyfunc_model._get_secrets()
            assert client_id is None
            assert private_key is None
            assert kid is None


class TestPredict:
    """Verify predict() with various payloads."""

    @pytest.fixture(autouse=True)
    def _setup_model(self, pyfunc_model, fake_secrets_env, mock_fhir_response):
        """Set up model with mocked API for all tests in this class."""
        self.model = pyfunc_model
        self.mock_response = mock_fhir_response
        # Mock the api object that load_context would create
        mock_api = MagicMock()
        mock_api.make_request.return_value = {
            "request": {"http_method": "get", "url": "test", "data": ""},
            "response": {
                "response_status_code": 200,
                "response_time_seconds": 0.25,
                "response_headers": {"Content-Type": "application/fhir+json"},
                "response_text": '{"resourceType": "Patient"}',
                "response_url": f"{FAKE_BASE_URL}Patient/123",
            },
        }
        self.model.api = mock_api

    def test_predict_get_returns_list(self):
        """GET request should return a list with one response dict."""
        df = pd.DataFrame([{
            "http_method": "get",
            "resource": "Patient",
            "action": "test-patient-id",
        }])
        result = self.model.predict(None, df)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["response_status_code"] == 200

    def test_predict_post_with_data(self):
        """POST with data should forward the data payload."""
        payload = json.dumps({"resourceType": "Observation", "status": "final"})
        df = pd.DataFrame([{
            "http_method": "post",
            "resource": "Observation",
            "action": "",
            "data": payload,
        }])
        result = self.model.predict(None, df)

        self.model.api.make_request.assert_called_once()
        call_kwargs = self.model.api.make_request.call_args
        assert call_kwargs.kwargs.get("data") == payload or call_kwargs[1].get("data") == payload

    def test_predict_nan_data_treated_as_none(self):
        """NaN in data column should be passed as None (not 'nan' string)."""
        df = pd.DataFrame([{
            "http_method": "get",
            "resource": "Patient",
            "action": "123",
        }])
        # Pandas will create NaN for the missing 'data' column
        assert "data" not in df.columns or pd.isna(df.iloc[0].get("data", float("nan")))

        result = self.model.predict(None, df)

        call_kwargs = self.model.api.make_request.call_args
        assert call_kwargs.kwargs.get("data") is None or call_kwargs[1].get("data") is None

    def test_predict_explicit_none_data(self):
        """Explicit None in data column should be passed as None."""
        df = pd.DataFrame([{
            "http_method": "get",
            "resource": "Patient",
            "action": "123",
            "data": None,
        }])
        result = self.model.predict(None, df)

        call_kwargs = self.model.api.make_request.call_args
        assert call_kwargs.kwargs.get("data") is None or call_kwargs[1].get("data") is None

    def test_predict_empty_dataframe(self):
        """Empty DataFrame should return empty DataFrame."""
        df = pd.DataFrame()
        result = self.model.predict(None, df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_predict_none_input(self):
        """None input should return empty DataFrame."""
        result = self.model.predict(None, None)
        assert isinstance(result, pd.DataFrame)

    def test_predict_missing_resource_returns_error(self):
        """Row with empty resource should return error dict."""
        df = pd.DataFrame([{
            "http_method": "get",
            "resource": "",
            "action": "123",
        }])
        result = self.model.predict(None, df)

        assert len(result) == 1
        assert "error" in str(result[0].get("response", ""))

    def test_predict_multiple_rows(self):
        """Multiple rows should return one result per row."""
        df = pd.DataFrame([
            {"http_method": "get", "resource": "Patient", "action": "123"},
            {"http_method": "get", "resource": "Patient", "action": "456"},
            {"http_method": "get", "resource": "Patient", "action": "789"},
        ])
        result = self.model.predict(None, df)

        assert isinstance(result, list)
        assert len(result) == 3

    def test_predict_api_exception_caught(self):
        """Exceptions from API calls should be caught and returned as error strings."""
        self.model.api.make_request.side_effect = ConnectionError("Network unreachable")

        df = pd.DataFrame([{
            "http_method": "get",
            "resource": "Patient",
            "action": "123",
        }])
        result = self.model.predict(None, df)

        assert len(result) == 1
        assert "Network unreachable" in str(result[0])

    def test_predict_default_http_method_is_get(self):
        """Missing http_method should default to 'get'."""
        df = pd.DataFrame([{
            "resource": "Patient",
            "action": "123",
        }])
        result = self.model.predict(None, df)

        call_kwargs = self.model.api.make_request.call_args
        assert call_kwargs.kwargs.get("http_method") == "get" or call_kwargs[1].get("http_method") == "get"
