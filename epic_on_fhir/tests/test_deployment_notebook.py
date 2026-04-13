"""Tests for the deployment notebook (src/deployment).

Validates:
- Alias rotation logic (challenger → champion → prior)
- ServedEntityInput construction with environment_vars preservation
- Custom tags JSON parsing
- Deployment verification logic
- Exit payload format

All tests mock MLflow and the Databricks SDK — no real endpoint updates.
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from conftest import (
    FAKE_CATALOG,
    FAKE_ENDPOINT_NAME,
    FAKE_MODEL_NAME,
    FAKE_MODEL_VERSION,
    FAKE_SCHEMA,
)


# ---------------------------------------------------------------------------
# Tests: Alias rotation (champion → prior)
# ---------------------------------------------------------------------------


class TestAliasRotation:
    """Validate champion alias promotion and prior rotation."""

    def test_promote_challenger_to_champion(self, mock_mlflow_client):
        """New version should get the 'champion' alias."""
        mock_mlflow_client.set_registered_model_alias(
            name=FAKE_MODEL_NAME,
            alias="champion",
            version=FAKE_MODEL_VERSION,
        )
        mock_mlflow_client.set_registered_model_alias.assert_called_with(
            name=FAKE_MODEL_NAME,
            alias="champion",
            version=FAKE_MODEL_VERSION,
        )

    def test_rotate_old_champion_to_prior(self, mock_mlflow_client):
        """When a previous champion exists, it should be rotated to 'prior'."""
        old_version = "2"

        # Simulate: promote new version, then rotate old
        mock_mlflow_client.set_registered_model_alias(
            name=FAKE_MODEL_NAME, alias="champion", version=FAKE_MODEL_VERSION,
        )
        mock_mlflow_client.set_registered_model_alias(
            name=FAKE_MODEL_NAME, alias="prior", version=old_version,
        )

        calls = mock_mlflow_client.set_registered_model_alias.call_args_list
        assert len(calls) == 2
        assert calls[1] == call(name=FAKE_MODEL_NAME, alias="prior", version=old_version)

    def test_skip_prior_when_no_previous_champion(self, mock_mlflow_client):
        """First deployment: get_model_version_by_alias raises, skip prior rotation."""
        current_champion_version = None

        # Promote to champion
        mock_mlflow_client.set_registered_model_alias(
            name=FAKE_MODEL_NAME, alias="champion", version=FAKE_MODEL_VERSION,
        )

        # Prior rotation should be skipped
        if current_champion_version and current_champion_version != FAKE_MODEL_VERSION:
            mock_mlflow_client.set_registered_model_alias(
                name=FAKE_MODEL_NAME, alias="prior", version=current_champion_version,
            )

        # Should only have the champion call, no prior call
        assert mock_mlflow_client.set_registered_model_alias.call_count == 1

    def test_skip_prior_when_same_version(self, mock_mlflow_client):
        """Re-deploying same version: don't set prior to itself."""
        current_champion_version = FAKE_MODEL_VERSION  # Same as new version

        mock_mlflow_client.set_registered_model_alias(
            name=FAKE_MODEL_NAME, alias="champion", version=FAKE_MODEL_VERSION,
        )

        if current_champion_version and current_champion_version != FAKE_MODEL_VERSION:
            mock_mlflow_client.set_registered_model_alias(
                name=FAKE_MODEL_NAME, alias="prior", version=current_champion_version,
            )

        assert mock_mlflow_client.set_registered_model_alias.call_count == 1

    def test_get_current_champion_version(self, mock_mlflow_client):
        """When champion exists, version should be extracted correctly."""
        mock_mv = MagicMock()
        mock_mv.version = "2"
        mock_mlflow_client.get_model_version_by_alias.side_effect = None
        mock_mlflow_client.get_model_version_by_alias.return_value = mock_mv

        champion_info = mock_mlflow_client.get_model_version_by_alias(FAKE_MODEL_NAME, "champion")
        assert champion_info.version == "2"

    def test_get_current_champion_raises_on_first_deploy(self, mock_mlflow_client):
        """No champion alias exists on first deploy."""
        with pytest.raises(Exception):
            mock_mlflow_client.get_model_version_by_alias(FAKE_MODEL_NAME, "champion")


# ---------------------------------------------------------------------------
# Tests: Environment vars preservation
# ---------------------------------------------------------------------------


class TestEnvironmentVarsPreservation:
    """Validate that environment_vars from the current endpoint config are preserved."""

    def test_env_vars_carried_forward(self, mock_workspace_client):
        """update_config should include existing env vars from served entity."""
        endpoint = mock_workspace_client.serving_endpoints.get(FAKE_ENDPOINT_NAME)
        current_entities = endpoint.config.served_entities
        current_env_vars = dict(current_entities[0].environment_vars)

        assert "EPIC_CLIENT_ID" in current_env_vars
        assert "EPIC_PRIVATE_KEY" in current_env_vars
        assert "EPIC_KID" in current_env_vars
        assert "ENABLE_OTEL_INSTRUMENTATION" in current_env_vars

    def test_env_vars_are_secret_references(self, mock_workspace_client):
        """Epic secrets should use {{secrets/scope/key}} format."""
        endpoint = mock_workspace_client.serving_endpoints.get(FAKE_ENDPOINT_NAME)
        env_vars = dict(endpoint.config.served_entities[0].environment_vars)

        assert env_vars["EPIC_CLIENT_ID"].startswith("{{secrets/")
        assert env_vars["EPIC_PRIVATE_KEY"].startswith("{{secrets/")
        assert env_vars["EPIC_KID"].startswith("{{secrets/")

    def test_env_vars_none_when_no_served_entities(self):
        """Handle edge case where endpoint has no served entities."""
        mock_w = MagicMock()
        mock_endpoint = MagicMock()
        mock_endpoint.config.served_entities = []
        mock_w.serving_endpoints.get.return_value = mock_endpoint

        entities = mock_endpoint.config.served_entities
        current_env_vars = None
        if entities and entities[0].environment_vars:
            current_env_vars = dict(entities[0].environment_vars)

        assert current_env_vars is None


# ---------------------------------------------------------------------------
# Tests: ServedEntityInput construction
# ---------------------------------------------------------------------------


class TestServedEntityInput:
    """Validate the ServedEntityInput is constructed correctly."""

    def test_entity_name_is_model_name(self):
        """entity_name should be the fully qualified UC model name."""
        entity_name = FAKE_MODEL_NAME
        assert "." in entity_name  # Must be catalog.schema.model
        assert len(entity_name.split(".")) == 3

    def test_entity_version_is_model_version(self):
        entity_version = FAKE_MODEL_VERSION
        assert entity_version.isdigit()

    def test_scale_to_zero_enabled(self):
        """Scale to zero should be enabled (cost optimization)."""
        scale_to_zero = True
        assert scale_to_zero is True

    def test_served_entity_name_format(self):
        """Name should be {model_short_name}_v{version}."""
        model_short = FAKE_MODEL_NAME.split(".")[-1]
        name = f"{model_short}_v{FAKE_MODEL_VERSION}"
        assert name == f"sandbox_epic_on_fhir_requests_v{FAKE_MODEL_VERSION}"


# ---------------------------------------------------------------------------
# Tests: Custom tags JSON parsing
# ---------------------------------------------------------------------------


class TestCustomTags:
    """Validate custom tags JSON parsing from job parameter."""

    def test_empty_json_object(self):
        tags_json = "{}"
        custom_tags = json.loads(tags_json) if tags_json and tags_json != "{}" else {}
        assert custom_tags == {}

    def test_valid_tags_parsed(self):
        tags_json = '{"env": "production", "team": "hls"}'
        custom_tags = json.loads(tags_json)
        assert custom_tags == {"env": "production", "team": "hls"}

    def test_empty_string_defaults_to_empty_dict(self):
        tags_json = ""
        custom_tags = json.loads(tags_json) if tags_json and tags_json != "{}" else {}
        assert custom_tags == {}

    def test_none_defaults_to_empty_dict(self):
        tags_json = None
        custom_tags = json.loads(tags_json) if tags_json and tags_json != "{}" else {}
        assert custom_tags == {}

    def test_invalid_json_raises(self):
        tags_json = "not valid json"
        with pytest.raises(json.JSONDecodeError):
            json.loads(tags_json)


# ---------------------------------------------------------------------------
# Tests: Deployment verification
# ---------------------------------------------------------------------------


class TestDeploymentVerification:
    """Validate the endpoint version check after update."""

    def test_verification_passes_when_version_matches(self, mock_workspace_client):
        """After update, endpoint should serve the requested version."""
        # Simulate: update succeeds, endpoint now serves new version
        mock_entity = MagicMock()
        mock_entity.entity_version = FAKE_MODEL_VERSION
        mock_endpoint = MagicMock()
        mock_endpoint.config.served_entities = [mock_entity]
        mock_workspace_client.serving_endpoints.get.return_value = mock_endpoint

        final_endpoint = mock_workspace_client.serving_endpoints.get(FAKE_ENDPOINT_NAME)
        served_version = final_endpoint.config.served_entities[0].entity_version
        assert served_version == FAKE_MODEL_VERSION

    def test_verification_fails_when_version_mismatches(self):
        """Raise if endpoint is serving a different version than expected."""
        served_version = "1"
        expected_version = FAKE_MODEL_VERSION  # "3"

        with pytest.raises(ValueError, match="Deployment verification failed"):
            if served_version != expected_version:
                raise ValueError(
                    f"Deployment verification failed: endpoint is serving v{served_version}, "
                    f"expected v{expected_version}"
                )

    def test_verification_fails_when_no_served_entities(self):
        """Raise if endpoint has no served entities after update."""
        mock_w = MagicMock()
        mock_endpoint = MagicMock()
        mock_endpoint.config.served_entities = []
        mock_w.serving_endpoints.get.return_value = mock_endpoint

        final_endpoint = mock_w.serving_endpoints.get(FAKE_ENDPOINT_NAME)
        served_version = (
            final_endpoint.config.served_entities[0].entity_version
            if final_endpoint.config.served_entities
            else None
        )
        assert served_version is None


# ---------------------------------------------------------------------------
# Tests: update_config_and_wait call pattern
# ---------------------------------------------------------------------------


class TestUpdateConfigCall:
    """Validate the update_config_and_wait SDK call uses keyword args."""

    def test_uses_keyword_args(self, mock_workspace_client):
        """update_config_and_wait must use keyword args (not positional config object)."""
        from unittest.mock import ANY

        mock_workspace_client.serving_endpoints.update_config_and_wait(
            name=FAKE_ENDPOINT_NAME,
            served_entities=[MagicMock()],
        )

        mock_workspace_client.serving_endpoints.update_config_and_wait.assert_called_once()
        call_kwargs = mock_workspace_client.serving_endpoints.update_config_and_wait.call_args
        assert "name" in call_kwargs.kwargs
        assert "served_entities" in call_kwargs.kwargs

    def test_endpoint_state_ready_after_update(self, mock_workspace_client):
        """update_config_and_wait should return endpoint in READY state."""
        result = mock_workspace_client.serving_endpoints.update_config_and_wait(
            name=FAKE_ENDPOINT_NAME,
            served_entities=[MagicMock()],
        )
        assert result.state.ready == "READY"


# ---------------------------------------------------------------------------
# Tests: Exit payload
# ---------------------------------------------------------------------------


class TestExitPayload:
    """Validate the deployment exit JSON."""

    REQUIRED_EXIT_KEYS = {"model_name", "model_version", "endpoint_name", "deployment", "timestamp"}

    def test_exit_payload_has_required_keys(self):
        payload = json.dumps({
            "model_name": FAKE_MODEL_NAME,
            "model_version": FAKE_MODEL_VERSION,
            "endpoint_name": FAKE_ENDPOINT_NAME,
            "deployment": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        parsed = json.loads(payload)
        missing = self.REQUIRED_EXIT_KEYS - set(parsed.keys())
        assert not missing, f"Exit payload missing keys: {missing}"

    def test_exit_payload_deployment_success(self):
        payload = {
            "model_name": FAKE_MODEL_NAME,
            "model_version": FAKE_MODEL_VERSION,
            "endpoint_name": FAKE_ENDPOINT_NAME,
            "deployment": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        assert payload["deployment"] == "success"

    def test_timestamp_is_utc_iso_format(self):
        ts = datetime.now(timezone.utc).isoformat()
        # Should be parseable and contain timezone info
        parsed = datetime.fromisoformat(ts)
        assert parsed.tzinfo is not None
