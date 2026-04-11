"""Tests for smart_on_fhir.endpoint.EpicApiRequest.

Covers URL construction, HTTP method dispatch, and response dict structure.
All tests mock the HTTP layer — no real API calls.
"""

from unittest.mock import patch, MagicMock

import pytest

from smart_on_fhir.endpoint import EpicApiRequest
from conftest import FAKE_BASE_URL


class TestUrlConstruction:
    """Verify URLs are built correctly from base_url + resource + action."""

    def test_get_url_format(self, epic_api, mock_fhir_response):
        """GET Patient/<id> should hit base_url/Patient/<id>."""
        with patch("smart_on_fhir.endpoint.requests.get", return_value=mock_fhir_response) as mock_get:
            epic_api.make_request("get", "Patient", "test-patient-id")

            mock_get.assert_called_once()
            call_url = mock_get.call_args[0][0]
            assert call_url == f"{FAKE_BASE_URL}Patient/test-patient-id"

    def test_post_url_format(self, epic_api, mock_fhir_response):
        """POST Observation/ should hit base_url/Observation/."""
        with patch("smart_on_fhir.endpoint.requests.post", return_value=mock_fhir_response) as mock_post:
            epic_api.make_request("post", "Observation", "", data='{"test": true}')

            mock_post.assert_called_once()
            call_url = mock_post.call_args[0][0]
            assert call_url == f"{FAKE_BASE_URL}Observation/"

    def test_empty_action_trailing_slash(self, epic_api, mock_fhir_response):
        """Empty action should produce resource/ (trailing slash from action join)."""
        with patch("smart_on_fhir.endpoint.requests.get", return_value=mock_fhir_response) as mock_get:
            epic_api.make_request("get", "Patient", "")

            call_url = mock_get.call_args[0][0]
            assert call_url.endswith("Patient/")


class TestHttpMethodDispatch:
    """Verify correct HTTP method is called."""

    def test_get_calls_requests_get(self, epic_api, mock_fhir_response):
        with patch("smart_on_fhir.endpoint.requests.get", return_value=mock_fhir_response) as mock_get:
            epic_api.make_request("get", "Patient", "123")
            mock_get.assert_called_once()

    def test_post_calls_requests_post(self, epic_api, mock_fhir_response):
        with patch("smart_on_fhir.endpoint.requests.post", return_value=mock_fhir_response) as mock_post:
            epic_api.make_request("post", "Observation", "", data="{}")
            mock_post.assert_called_once()

    def test_post_passes_data(self, epic_api, mock_fhir_response):
        """POST should forward the data argument."""
        payload = '{"resourceType": "Observation"}'
        with patch("smart_on_fhir.endpoint.requests.post", return_value=mock_fhir_response) as mock_post:
            epic_api.make_request("post", "Observation", "", data=payload)

            call_kwargs = mock_post.call_args
            assert call_kwargs[1]["data"] == payload or call_kwargs.kwargs.get("data") == payload

    def test_get_passes_none_data(self, epic_api, mock_fhir_response):
        """GET should pass data=None by default."""
        with patch("smart_on_fhir.endpoint.requests.get", return_value=mock_fhir_response) as mock_get:
            epic_api.make_request("get", "Patient", "123")

            call_kwargs = mock_get.call_args
            assert call_kwargs[1].get("data") is None or call_kwargs.kwargs.get("data") is None


class TestResponseStructure:
    """Verify the response dict has expected keys."""

    def test_response_has_request_and_response_keys(self, epic_api, mock_fhir_response):
        """Return dict must have 'request' and 'response' top-level keys."""
        with patch("smart_on_fhir.endpoint.requests.get", return_value=mock_fhir_response):
            result = epic_api.make_request("get", "Patient", "123")

            assert "request" in result
            assert "response" in result

    def test_request_dict_keys(self, epic_api, mock_fhir_response):
        """Request dict must have http_method, url, data."""
        with patch("smart_on_fhir.endpoint.requests.get", return_value=mock_fhir_response):
            result = epic_api.make_request("get", "Patient", "123")

            req = result["request"]
            assert req["http_method"] == "get"
            assert "Patient/123" in req["url"]
            assert "data" in req

    def test_response_dict_keys(self, epic_api, mock_fhir_response):
        """Response dict must have status_code, time, headers, text, url."""
        with patch("smart_on_fhir.endpoint.requests.get", return_value=mock_fhir_response):
            result = epic_api.make_request("get", "Patient", "123")

            resp = result["response"]
            assert "response_status_code" in resp
            assert "response_time_seconds" in resp
            assert "response_headers" in resp
            assert "response_text" in resp
            assert "response_url" in resp

    def test_response_time_is_numeric(self, epic_api, mock_fhir_response):
        """response_time_seconds should be a float."""
        with patch("smart_on_fhir.endpoint.requests.get", return_value=mock_fhir_response):
            result = epic_api.make_request("get", "Patient", "123")

            assert isinstance(result["response"]["response_time_seconds"], (int, float))

    def test_empty_data_is_empty_string(self, epic_api, mock_fhir_response):
        """When data=None, request dict should have data=''."""
        with patch("smart_on_fhir.endpoint.requests.get", return_value=mock_fhir_response):
            result = epic_api.make_request("get", "Patient", "123")

            assert result["request"]["data"] == ""
