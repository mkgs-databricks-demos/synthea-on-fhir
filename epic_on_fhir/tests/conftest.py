"""Pytest configuration, fixtures, and path setup for Epic on FHIR tests.

Adds src/ to sys.path so tests can import from smart_on_fhir.
Provides fixtures for Spark, test data loading, fake secrets, and model instances.

Supports running from:
  - Workspace testing sidebar (full Databricks context)
  - CLI: `python -B -m pytest tests/` (may not have Spark/SDK context)
  - Local dev: `uv run pytest` (uses Databricks Connect)
"""

import os, sys, pathlib
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

# Add src/ to sys.path so `from smart_on_fhir import ...` works in tests
_src_dir = str(pathlib.Path(__file__).parent.parent / "src")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

# Skip writing .pyc files (workspace filesystem is read-only for bytecache)
sys.dont_write_bytecode = True

try:
    import pytest
    import json
    import csv
except ImportError:
    raise ImportError(
        "Test dependencies not found.\n\nRun tests using 'uv run pytest'. See http://docs.astral.sh/uv to learn more about uv."
    )

# Optional imports — only needed for Spark-dependent tests
try:
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
    _HAS_SPARK = True
except ImportError:
    _HAS_SPARK = False


# ---------------------------------------------------------------------------
# Spark / data fixtures (skipped when Spark is unavailable)
# ---------------------------------------------------------------------------

@pytest.fixture()
def spark():
    """Provide a SparkSession fixture for tests that need it."""
    if not _HAS_SPARK:
        pytest.skip("Spark not available in this environment")
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture()
def load_fixture(spark):
    """Load JSON or CSV from fixtures/ directory as a Spark DataFrame."""

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


# ---------------------------------------------------------------------------
# Epic on FHIR fixtures
# ---------------------------------------------------------------------------

# Fake secrets for testing (not real credentials)
FAKE_CLIENT_ID = "test-client-id-00000000-0000-0000-0000-000000000000"
FAKE_KID = "test-kid-12345"
FAKE_ALGO = "RS384"
FAKE_TOKEN_URL = "https://fhir.epic.com/interconnect-fhir-oauth/oauth2/token"
FAKE_BASE_URL = "https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4/"

# Generate a real RSA private key for JWT signing in tests (NOT a real credential).
# Uses cryptography library (already a project dependency) — 2048 bit for speed.
# Generated once at import time and reused across all tests.
from cryptography.hazmat.primitives.asymmetric import rsa as _rsa
from cryptography.hazmat.primitives import serialization as _serialization

_test_key = _rsa.generate_private_key(public_exponent=65537, key_size=2048)
FAKE_PRIVATE_KEY = _test_key.private_bytes(
    encoding=_serialization.Encoding.PEM,
    format=_serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=_serialization.NoEncryption(),
).decode()


@pytest.fixture()
def fake_secrets():
    """Return a dict of fake Epic OAuth2 secrets for testing."""
    return {
        "client_id": FAKE_CLIENT_ID,
        "private_key": FAKE_PRIVATE_KEY,
        "kid": FAKE_KID,
    }


@pytest.fixture()
def fake_secrets_env(fake_secrets):
    """Set fake secrets as environment variables (as model serving would)."""
    with patch.dict(os.environ, {
        "EPIC_CLIENT_ID": fake_secrets["client_id"],
        "EPIC_PRIVATE_KEY": fake_secrets["private_key"],
        "EPIC_KID": fake_secrets["kid"],
    }):
        yield fake_secrets


@pytest.fixture()
def epic_auth(fake_secrets):
    """Create an EpicApiAuth instance with fake secrets."""
    from smart_on_fhir.auth import EpicApiAuth
    return EpicApiAuth(
        client_id=fake_secrets["client_id"],
        private_key=fake_secrets["private_key"],
        kid=fake_secrets["kid"],
        algo=FAKE_ALGO,
        auth_location=FAKE_TOKEN_URL,
    )


@pytest.fixture()
def epic_api(epic_auth):
    """Create an EpicApiRequest instance with fake auth."""
    from smart_on_fhir.endpoint import EpicApiRequest
    return EpicApiRequest(auth=epic_auth, base_url=FAKE_BASE_URL)


@pytest.fixture()
def pyfunc_model():
    """Create an EpicFhirPyfuncModel instance (no secrets loaded)."""
    from smart_on_fhir.epic_fhir_pyfunc import EpicFhirPyfuncModel
    return EpicFhirPyfuncModel(
        token_url=FAKE_TOKEN_URL,
        algo=FAKE_ALGO,
    )


@pytest.fixture()
def mock_token_response():
    """Create a mock OAuth2 token response."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = json.dumps({"access_token": "fake-bearer-token-12345", "token_type": "bearer", "expires_in": 300})
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


@pytest.fixture()
def mock_fhir_response():
    """Create a mock FHIR API response (Patient GET)."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = json.dumps({"resourceType": "Patient", "id": "test-patient-id"})
    mock_resp.headers = {"Content-Type": "application/fhir+json"}
    mock_resp.url = f"{FAKE_BASE_URL}Patient/test-patient-id"
    mock_resp.elapsed = MagicMock()
    mock_resp.elapsed.microseconds = 250000  # 0.25 seconds
    return mock_resp


# ---------------------------------------------------------------------------
# Notebook workflow fixtures (for test_*_notebook.py files)
# ---------------------------------------------------------------------------

FAKE_MODEL_NAME = "hls_fde.sandbox_open_epic_smart_on_fhir.sandbox_epic_on_fhir_requests"
FAKE_MODEL_VERSION = "3"
FAKE_ENDPOINT_NAME = "sandbox_epic_on_fhir_requests"
FAKE_CATALOG = "hls_fde"
FAKE_SCHEMA = "sandbox_open_epic_smart_on_fhir"
FAKE_EXPERIMENT_NAME = "/Workspace/.experiments/[sandbox] epic_on_fhir_requests"


@pytest.fixture()
def mock_mlflow_client():
    """Mock MlflowClient for notebook workflow tests."""
    mock_client = MagicMock()

    # Default: model version with no tags, no aliases
    mock_mv = MagicMock()
    mock_mv.version = FAKE_MODEL_VERSION
    mock_mv.name = FAKE_MODEL_NAME
    mock_mv.tags = {}
    mock_mv.run_id = "fake-run-id-abc123"
    mock_mv.aliases = []
    mock_client.get_model_version.return_value = mock_mv

    # get_model_version_by_alias: raise by default (no champion)
    mock_client.get_model_version_by_alias.side_effect = Exception("RESOURCE_DOES_NOT_EXIST")

    # set_registered_model_alias: no-op
    mock_client.set_registered_model_alias.return_value = None

    # set_model_version_tag: no-op
    mock_client.set_model_version_tag.return_value = None

    return mock_client


@pytest.fixture()
def mock_workspace_client():
    """Mock WorkspaceClient for deployment notebook tests."""
    mock_w = MagicMock()

    # Mock serving endpoint with one served entity
    mock_entity = MagicMock()
    mock_entity.entity_name = FAKE_MODEL_NAME
    mock_entity.entity_version = "2"  # Previous version
    mock_entity.environment_vars = {
        "EPIC_CLIENT_ID": "{{secrets/epic_on_fhir_oauth_keys/client_id}}",
        "EPIC_PRIVATE_KEY": "{{secrets/epic_on_fhir_oauth_keys/private_key}}",
        "EPIC_KID": "{{secrets/epic_on_fhir_oauth_keys/kid}}",
        "ENABLE_OTEL_INSTRUMENTATION": "true",
    }

    mock_config = MagicMock()
    mock_config.served_entities = [mock_entity]
    mock_config.auto_capture_config = MagicMock()

    mock_endpoint = MagicMock()
    mock_endpoint.name = FAKE_ENDPOINT_NAME
    mock_endpoint.config = mock_config
    mock_endpoint.state = MagicMock()
    mock_endpoint.state.ready = "READY"

    mock_w.serving_endpoints.get.return_value = mock_endpoint
    mock_w.serving_endpoints.update_config_and_wait.return_value = mock_endpoint

    return mock_w


@pytest.fixture()
def mock_model_info():
    """Mock MLflow model info returned by log_model or get_model_info."""
    mock_info = MagicMock()
    mock_info.model_uri = f"models:/{FAKE_MODEL_NAME}/{FAKE_MODEL_VERSION}"
    mock_info.model_id = "fake-model-id-xyz789"
    mock_info.run_id = "fake-run-id-abc123"
    mock_info.registered_model_version = FAKE_MODEL_VERSION
    return mock_info


# ---------------------------------------------------------------------------
# Pytest session configuration
# ---------------------------------------------------------------------------

def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified.

    Only works when Databricks SDK is available and running in a notebook context.
    Silently skipped in CLI or other contexts.
    """
    if not _HAS_SPARK:
        return

    try:
        conf = WorkspaceClient().config
        if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
            return

        url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
        print("\u2601\ufe0f no compute specified, falling back to serverless compute", file=sys.stderr)
        print(f"  see {url} for manual configuration", file=sys.stdout)

        os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"
    except Exception:
        # Not running in a Databricks context — skip compute setup
        pass


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session.

    Gracefully handles missing Spark/SDK context (e.g., CLI runs).
    Spark-dependent tests will be skipped via the spark fixture.
    """
    with _allow_stderr_output(config):
        _enable_fallback_compute()

        if _HAS_SPARK:
            try:
                if hasattr(DatabricksSession.builder, "validateSession"):
                    DatabricksSession.builder.validateSession().getOrCreate()
                else:
                    DatabricksSession.builder.getOrCreate()
            except Exception:
                pass  # Spark init failed — Spark-dependent tests will be skipped
