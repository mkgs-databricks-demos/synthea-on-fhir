"""
MLflow pyfunc model for Epic on FHIR API requests.

Wraps EpicApiAuth and EpicApiRequest to make authenticated FHIR requests.
Secrets are fetched from Databricks secret scope at predict time.
"""

import os

import mlflow
from mlflow.pyfunc.utils import pyfunc
import pandas as pd

from smart_on_fhir.auth import EpicApiAuth
from smart_on_fhir.endpoint import EpicApiRequest


class EpicFhirPyfuncModel(mlflow.pyfunc.PythonModel):
    """
    MLflow pyfunc model that makes Epic on FHIR API requests.

    Input (each row): resource, action, http_method (default "get"), data (optional)
    Output: response dict with request/response details
    """

    def __init__(
        self,
        secret_scope_name: str,
        client_id_dbs_key: str,
        token_url: str,
        algo: str,
        base_url: str = "https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4/",
    ):
        self.secret_scope_name = secret_scope_name
        self.client_id_dbs_key = client_id_dbs_key
        self.token_url = token_url
        self.algo = algo
        self.base_url = base_url.rstrip("/") + "/"

    def _get_secrets(self):
        """Fetch secrets. Tries env vars (for model serving) then dbutils (for notebooks/jobs)."""
        # Model serving: secrets injected as env vars via {{secrets/scope/key}}
        client_id = os.environ.get("EPIC_CLIENT_ID")
        private_key = os.environ.get("EPIC_PRIVATE_KEY")
        kid = os.environ.get("EPIC_KID")
        if client_id and private_key and kid:
            return client_id, private_key, kid
        # Notebook/job: use dbutils
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession
            dbutils = DBUtils(SparkSession.builder.getOrCreate())
        except Exception:
            try:
                import dbutils
            except ImportError:
                raise RuntimeError(
                    "Secrets not in env (EPIC_CLIENT_ID, EPIC_PRIVATE_KEY, EPIC_KID) and dbutils unavailable."
                )
        client_id = dbutils.secrets.get(scope=self.secret_scope_name, key=self.client_id_dbs_key)
        private_key = dbutils.secrets.get(scope=self.secret_scope_name, key="private_key")
        kid = dbutils.secrets.get(scope=self.secret_scope_name, key="kid")
        return client_id, private_key, kid

    def _make_api(self):
        """Build EpicApiAuth and EpicApiRequest from secrets."""
        client_id, private_key, kid = self._get_secrets()
        auth = EpicApiAuth(
            client_id=client_id,
            private_key=private_key,
            kid=kid,
            algo=self.algo,
            auth_location=self.token_url,
        )
        return EpicApiRequest(auth=auth, base_url=self.base_url)

    def load_context(self, context: mlflow.pyfunc.PythonModelContext):
        """Load context."""
        import os
        import pandas as pd
        from smart_on_fhir.auth import EpicApiAuth
        from smart_on_fhir.endpoint import EpicApiRequest
        self.api = self._make_api()

    @pyfunc 
    def predict(self, context, model_input: pd.DataFrame, params=None) -> list[str]:
        """Make Epic FHIR request(s). Input columns: resource, action, http_method, data (optional)."""
        if model_input is None or len(model_input) == 0:
            return pd.DataFrame()

        api = self.api
        results = []

        for _, row in model_input.iterrows():
            resource = str(row.get("resource", ""))
            action = str(row.get("action", ""))
            http_method = str(row.get("http_method", "get")).lower()
            data = row.get("data") if pd.notna(row.get("data")) else None

            if not resource:
                results.append({"response": "error: resource required"})
                continue

            try:
                out = api.make_request(
                    http_method=http_method,
                    resource=resource,
                    action=action,
                    data=data,
                )
                response = out['response']
                results.append(response)
            except Exception as e:
                results.append({"response": str(e)})

        return results
