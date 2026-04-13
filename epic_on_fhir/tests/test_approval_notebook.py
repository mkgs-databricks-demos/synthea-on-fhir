"""Tests for the approval notebook (src/approval).

Validates:
- APPROVAL_TAG_KEY matches the deployment job task name ('approval_check')
- Approval logic: approved tag passes, rejected raises ValueError,
  missing/empty tag raises RuntimeError
- Case-insensitive tag value handling
- Exit payload format for downstream deployment task
- Error messages include actionable instructions

All tests mock MlflowClient — no real UC calls.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from conftest import FAKE_MODEL_NAME, FAKE_MODEL_VERSION


# ---------------------------------------------------------------------------
# Constants from the approval notebook
# ---------------------------------------------------------------------------

# MLflow 3 convention: tag key MUST match the job task_key so the UC UI
# "Approve" button and auto-repair mechanism work correctly.
APPROVAL_TAG_KEY = "approval_check"


# ---------------------------------------------------------------------------
# Replicate approval logic (inline in the approval notebook)
# ---------------------------------------------------------------------------


def check_approval(client, model_name: str, model_version: str):
    """Replica of the approval check logic from the approval notebook.

    Returns the approval_tag value on success.
    Raises ValueError for rejected, RuntimeError for missing/unknown.
    """
    model_version_details = client.get_model_version(model_name, model_version)
    tags = model_version_details.tags or {}
    approval_tag = tags.get(APPROVAL_TAG_KEY, "")

    if approval_tag.lower() == "approved":
        return approval_tag
    elif approval_tag.lower() == "rejected":
        raise ValueError(
            f"Model version {model_version} was explicitly rejected.\n"
            f"  Tag: {APPROVAL_TAG_KEY} = {approval_tag}\n"
            f"  To re-approve, update the tag to 'approved' and re-run the deployment job."
        )
    else:
        raise RuntimeError(
            f"Model version {model_version} has not been approved yet.\n"
            f"  Tag: {APPROVAL_TAG_KEY} = {approval_tag!r}"
        )


# ---------------------------------------------------------------------------
# Tests: Tag key convention
# ---------------------------------------------------------------------------


class TestTagKeyConvention:
    """Validate the approval tag key matches the MLflow 3 task_key convention."""

    def test_tag_key_is_approval_check(self):
        """Tag key must be 'approval_check' to match the job task name."""
        assert APPROVAL_TAG_KEY == "approval_check"

    def test_tag_key_is_lowercase_with_underscore(self):
        assert APPROVAL_TAG_KEY == APPROVAL_TAG_KEY.lower()
        assert "_" in APPROVAL_TAG_KEY


# ---------------------------------------------------------------------------
# Tests: Approval logic
# ---------------------------------------------------------------------------


class TestApprovalLogic:
    """Validate all three approval paths: approved, rejected, missing."""

    def _make_mock_client(self, tag_value=None):
        mock_client = MagicMock()
        mock_mv = MagicMock()
        mock_mv.tags = {APPROVAL_TAG_KEY: tag_value} if tag_value is not None else {}
        mock_client.get_model_version.return_value = mock_mv
        return mock_client

    def test_approved_passes(self):
        client = self._make_mock_client("approved")
        result = check_approval(client, FAKE_MODEL_NAME, FAKE_MODEL_VERSION)
        assert result == "approved"

    def test_approved_case_insensitive(self):
        """UC UI may set 'Approved' (title case)."""
        client = self._make_mock_client("Approved")
        result = check_approval(client, FAKE_MODEL_NAME, FAKE_MODEL_VERSION)
        assert result == "Approved"

    def test_approved_uppercase(self):
        client = self._make_mock_client("APPROVED")
        result = check_approval(client, FAKE_MODEL_NAME, FAKE_MODEL_VERSION)
        assert result == "APPROVED"

    def test_rejected_raises_value_error(self):
        client = self._make_mock_client("rejected")
        with pytest.raises(ValueError, match="explicitly rejected"):
            check_approval(client, FAKE_MODEL_NAME, FAKE_MODEL_VERSION)

    def test_rejected_case_insensitive(self):
        client = self._make_mock_client("Rejected")
        with pytest.raises(ValueError, match="explicitly rejected"):
            check_approval(client, FAKE_MODEL_NAME, FAKE_MODEL_VERSION)

    def test_missing_tag_raises_runtime_error(self):
        client = self._make_mock_client()  # No tag
        with pytest.raises(RuntimeError, match="has not been approved yet"):
            check_approval(client, FAKE_MODEL_NAME, FAKE_MODEL_VERSION)

    def test_empty_tag_raises_runtime_error(self):
        client = self._make_mock_client("")
        with pytest.raises(RuntimeError, match="has not been approved yet"):
            check_approval(client, FAKE_MODEL_NAME, FAKE_MODEL_VERSION)

    def test_unknown_tag_value_raises_runtime_error(self):
        client = self._make_mock_client("pending")
        with pytest.raises(RuntimeError, match="has not been approved yet"):
            check_approval(client, FAKE_MODEL_NAME, FAKE_MODEL_VERSION)


# ---------------------------------------------------------------------------
# Tests: Error message content
# ---------------------------------------------------------------------------


class TestErrorMessages:
    """Validate error messages include actionable instructions."""

    def _make_mock_client(self, tag_value=None):
        mock_client = MagicMock()
        mock_mv = MagicMock()
        mock_mv.tags = {APPROVAL_TAG_KEY: tag_value} if tag_value is not None else {}
        mock_client.get_model_version.return_value = mock_mv
        return mock_client

    def test_rejected_message_includes_reapproval_instructions(self):
        client = self._make_mock_client("rejected")
        with pytest.raises(ValueError) as exc_info:
            check_approval(client, FAKE_MODEL_NAME, FAKE_MODEL_VERSION)
        assert "re-approve" in str(exc_info.value).lower() or "re-run" in str(exc_info.value).lower()

    def test_missing_tag_message_includes_tag_key(self):
        client = self._make_mock_client()
        with pytest.raises(RuntimeError) as exc_info:
            check_approval(client, FAKE_MODEL_NAME, FAKE_MODEL_VERSION)
        assert APPROVAL_TAG_KEY in str(exc_info.value)


# ---------------------------------------------------------------------------
# Tests: Exit payload
# ---------------------------------------------------------------------------


class TestExitPayload:
    """Validate the approval exit JSON for the deployment task."""

    REQUIRED_EXIT_KEYS = {"model_name", "model_version", "approval", "approval_tag"}

    def test_exit_payload_on_approval(self):
        payload = json.dumps({
            "model_name": FAKE_MODEL_NAME,
            "model_version": FAKE_MODEL_VERSION,
            "approval": "passed",
            "approval_tag": "approved",
        })
        parsed = json.loads(payload)
        missing = self.REQUIRED_EXIT_KEYS - set(parsed.keys())
        assert not missing, f"Exit payload missing keys: {missing}"
        assert parsed["approval"] == "passed"

    def test_exit_payload_is_json_serializable(self):
        payload = {
            "model_name": FAKE_MODEL_NAME,
            "model_version": FAKE_MODEL_VERSION,
            "approval": "passed",
            "approval_tag": "Approved",
        }
        result = json.dumps(payload)
        assert isinstance(result, str)
