"""Tests for smart_on_fhir.auth.EpicApiAuth.

Covers JWT generation, token caching/refresh, and requests.auth.AuthBase integration.
All tests mock the Epic token endpoint — no real API calls.
"""

import datetime
import json
import zoneinfo
from unittest.mock import patch, MagicMock

import jwt
import pytest

from smart_on_fhir.auth import EpicApiAuth
from conftest import FAKE_CLIENT_ID, FAKE_PRIVATE_KEY, FAKE_KID, FAKE_ALGO, FAKE_TOKEN_URL


class TestJwtGeneration:
    """Verify the JWT assertion sent to Epic's token endpoint."""

    def test_jwt_payload_has_required_claims(self, epic_auth):
        """JWT payload must include iss, sub, aud, exp, iat, jti per Epic spec."""
        now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=zoneinfo.ZoneInfo("America/New_York"))
        exp = now + datetime.timedelta(minutes=5)

        with patch("smart_on_fhir.auth.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            epic_auth.generate_token(now=now, expiration=exp)

            # Extract the JWT from the POST data
            call_data = mock_post.call_args[1]["data"] if "data" in mock_post.call_args[1] else mock_post.call_args[0][1] if len(mock_post.call_args[0]) > 1 else mock_post.call_args.kwargs.get("data", mock_post.call_args[1].get("data"))
            client_assertion = call_data["client_assertion"]

            # Decode without verification (we just want to inspect claims)
            payload = jwt.decode(client_assertion, options={"verify_signature": False})

            assert payload["iss"] == FAKE_CLIENT_ID
            assert payload["sub"] == FAKE_CLIENT_ID
            assert payload["aud"] == FAKE_TOKEN_URL
            assert payload["exp"] == int(exp.timestamp())
            assert payload["iat"] == int(now.timestamp())
            assert "jti" in payload and len(payload["jti"]) > 0

    def test_jwt_header_has_required_fields(self, epic_auth):
        """JWT header must include kid, alg=RS384, typ=JWT."""
        with patch("smart_on_fhir.auth.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            epic_auth.generate_token()

            call_data = mock_post.call_args[1].get("data", {})
            client_assertion = call_data["client_assertion"]

            header = jwt.get_unverified_header(client_assertion)

            assert header["kid"] == FAKE_KID
            assert header["alg"] == FAKE_ALGO
            assert header["typ"] == "JWT"

    def test_grant_type_is_client_credentials(self, epic_auth):
        """POST body must use client_credentials grant type."""
        with patch("smart_on_fhir.auth.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            epic_auth.generate_token()

            call_data = mock_post.call_args[1].get("data", {})
            assert call_data["grant_type"] == "client_credentials"
            assert call_data["client_assertion_type"] == "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

    def test_jti_is_unique_per_call(self, epic_auth):
        """Each JWT must have a unique jti (nonce)."""
        jtis = []
        with patch("smart_on_fhir.auth.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            for _ in range(3):
                epic_auth.generate_token()
                call_data = mock_post.call_args[1].get("data", {})
                payload = jwt.decode(call_data["client_assertion"], options={"verify_signature": False})
                jtis.append(payload["jti"])

        assert len(set(jtis)) == 3, f"Expected 3 unique jtis, got {jtis}"


class TestTokenCaching:
    """Verify token is cached and refreshed correctly."""

    def test_token_is_cached(self, epic_auth, mock_token_response):
        """Second call within expiry should reuse cached token (single POST)."""
        now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=zoneinfo.ZoneInfo("America/New_York"))
        exp = now + datetime.timedelta(minutes=5)

        with patch("smart_on_fhir.auth.requests.post", return_value=mock_token_response) as mock_post:
            token1 = epic_auth.get_token(now=now, expiration=exp)
            # Second call at same time — should use cache
            token2 = epic_auth.get_token(now=now, expiration=exp)

            assert mock_post.call_count == 1
            assert token1 == token2

    def test_token_refreshed_when_expired(self, epic_auth, mock_token_response):
        """Token should be re-fetched when current time >= expiry."""
        tz = zoneinfo.ZoneInfo("America/New_York")
        now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        exp = now + datetime.timedelta(minutes=5)
        after_expiry = exp + datetime.timedelta(seconds=1)
        new_exp = after_expiry + datetime.timedelta(minutes=5)

        with patch("smart_on_fhir.auth.requests.post", return_value=mock_token_response) as mock_post:
            epic_auth.get_token(now=now, expiration=exp)
            epic_auth.get_token(now=after_expiry, expiration=new_exp)

            assert mock_post.call_count == 2


class TestAuthBaseIntegration:
    """Verify requests.auth.AuthBase __call__ integration."""

    def test_call_sets_authorization_header(self, epic_auth, mock_token_response):
        """__call__ should set Bearer token in Authorization header."""
        with patch("smart_on_fhir.auth.requests.post", return_value=mock_token_response):
            mock_request = MagicMock()
            mock_request.headers = {}

            result = epic_auth(mock_request)

            assert "Authorization" in result.headers
            assert result.headers["Authorization"].startswith("Bearer ")
            assert result.headers["Accept"] == "application/json"
            assert result.headers["Content-Type"] == "application/json"

    def test_can_connect_returns_bool(self, epic_auth, mock_token_response):
        """can_connect() should return True on 200, False otherwise."""
        with patch("smart_on_fhir.auth.requests.post", return_value=mock_token_response):
            assert epic_auth.can_connect() is True

        failed_resp = MagicMock()
        failed_resp.status_code = 401
        with patch("smart_on_fhir.auth.requests.post", return_value=failed_resp):
            auth2 = EpicApiAuth(
                client_id=FAKE_CLIENT_ID,
                private_key=FAKE_PRIVATE_KEY,
                kid=FAKE_KID,
                algo=FAKE_ALGO,
            )
            assert auth2.can_connect() is False
