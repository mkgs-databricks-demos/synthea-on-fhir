"""MLflow pyfunc model for Epic on FHIR API requests.

Wraps EpicApiAuth and EpicApiRequest to make authenticated FHIR requests.
Secrets are fetched from Databricks secret scope at predict time.

OpenTelemetry instrumentation (metrics, traces) is initialized at module level
and bound to the model instance in load_context(). On Databricks model serving
with telemetry_config enabled, the OTLP exporters are pre-configured to ship
data to Unity Catalog Delta tables.

OTel SDK initialization is gated on the ENABLE_OTEL_INSTRUMENTATION env var
(set via bundle variable → serving endpoint env var). When disabled or absent,
only standard Python logging is emitted (auto-captured to _otel_logs by the
endpoint telemetry_config regardless of this flag).
"""

import logging
import os
from contextlib import nullcontext

import mlflow
import pandas as pd
from mlflow.pyfunc.utils import pyfunc

from smart_on_fhir.auth import EpicApiAuth
from smart_on_fhir.endpoint import EpicApiRequest

logger = logging.getLogger("epic_on_fhir")

# ---- OpenTelemetry initialization (per-worker) --------------------------------
# Gated on ENABLE_OTEL_INSTRUMENTATION env var (Preview feature).
# When "true", initializes OTLP exporters for custom metrics and traces.
# When "false" or absent, skips SDK setup entirely — Python logging still flows
# to _otel_logs via the endpoint telemetry_config.
_OTEL_REQUESTED = os.environ.get("ENABLE_OTEL_INSTRUMENTATION", "false").lower() == "true"

if _OTEL_REQUESTED:
    try:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.metrics import get_meter, set_meter_provider
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.trace import get_tracer, set_tracer_provider, StatusCode

        _resource = Resource.create({"worker.pid": str(os.getpid())})

        # Traces
        _tracer_provider = TracerProvider(resource=_resource)
        _tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        set_tracer_provider(_tracer_provider)

        # Metrics
        _metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter())
        _meter_provider = MeterProvider(metric_readers=[_metric_reader], resource=_resource)
        set_meter_provider(_meter_provider)

        _tracer = get_tracer("epic_on_fhir")
        _meter = get_meter("epic_on_fhir")

        _prediction_counter = _meter.create_counter(
            name="predict.call_count",
            description="Total predict() invocations",
            unit="1",
        )
        _request_counter = _meter.create_counter(
            name="fhir.request_count",
            description="FHIR API requests by resource and method",
            unit="1",
        )
        _error_counter = _meter.create_counter(
            name="fhir.error_count",
            description="FHIR API request errors",
            unit="1",
        )
        _request_duration = _meter.create_histogram(
            name="fhir.request_duration",
            description="FHIR API request duration (from Epic response)",
            unit="s",
        )

        _OTEL_ENABLED = True
        logger.info("OpenTelemetry instrumentation initialized (pid=%s)", os.getpid())

    except Exception:
        _tracer = None
        _OTEL_ENABLED = False
        StatusCode = None
        logger.warning("ENABLE_OTEL_INSTRUMENTATION=true but OTel SDK init failed — disabled")

else:
    _tracer = None
    _OTEL_ENABLED = False
    StatusCode = None
    logger.info(
        "OTel SDK instrumentation disabled (ENABLE_OTEL_INSTRUMENTATION=%s)",
        os.environ.get("ENABLE_OTEL_INSTRUMENTATION", "<not set>"),
    )


class EpicFhirPyfuncModel(mlflow.pyfunc.PythonModel):
    """
    MLflow pyfunc model that makes Epic on FHIR API requests.

    Input (each row): resource, action, http_method (default "get"), data (optional)
    Output: response dict with request/response details
    """

    def __init__(
        self,
        token_url: str,
        algo: str,
        base_url: str = "https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4/"
    ):
        self.token_url = token_url
        self.algo = algo
        self.base_url = base_url.rstrip("/") + "/"


    def _get_secrets(self):
        """Fetch secrets. Tries env vars (for model serving) then dbutils (for notebooks/jobs)."""
        # Model serving: secrets injected as env vars via {{secrets/scope/key}}
        client_id = os.environ.get("EPIC_CLIENT_ID")
        private_key = os.environ.get("EPIC_PRIVATE_KEY")
        kid = os.environ.get("EPIC_KID")
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
        """Load context and bind OTel instruments."""
        import os
        import pandas as pd
        from smart_on_fhir.auth import EpicApiAuth
        from smart_on_fhir.endpoint import EpicApiRequest
        self.api = self._make_api()

        # Bind OTel instruments initialized at module level (per-worker)
        if _OTEL_ENABLED:
            self.tracer = _tracer
            self.prediction_counter = _prediction_counter
            self.request_counter = _request_counter
            self.error_counter = _error_counter
            self.request_duration = _request_duration
        else:
            self.tracer = None

        logger.warning("EpicFhirPyfuncModel loaded (otel=%s)", _OTEL_ENABLED)


    @pyfunc 
    def predict(self, context, model_input: pd.DataFrame, params=None) -> list[str]:
        """Make Epic FHIR request(s). Input columns: resource, action, http_method, data (optional)."""
        if model_input is None or len(model_input) == 0:
            return pd.DataFrame()

        api = self.api
        results = []
        _use_otel = getattr(self, "tracer", None) is not None

        # Wrap entire predict() call in a span
        span_ctx = (
            self.tracer.start_as_current_span("EpicFhirPyfuncModel.predict")
            if _use_otel else nullcontext()
        )

        with span_ctx as predict_span:
            if _use_otel and predict_span:
                predict_span.set_attribute("input.row_count", len(model_input))
                predict_span.set_attribute("input.columns", str(list(model_input.columns)))
                self.prediction_counter.add(1, {"input.row_count": len(model_input)})

            logger.warning(
                "predict() called with %d row(s): resources=%s",
                len(model_input),
                list(model_input.get("resource", [])),
            )

            for idx, row in model_input.iterrows():
                resource = str(row.get("resource", ""))
                action = str(row.get("action", ""))
                http_method = str(row.get("http_method", "get")).lower()
                data = row.get("data") if pd.notna(row.get("data")) else None

                if not resource:
                    results.append({"response": "error: resource required"})
                    if _use_otel:
                        self.error_counter.add(1, {"error.type": "missing_resource"})
                    logger.error("Row %s: resource is required but was empty", idx)
                    continue

                # Wrap each FHIR API call in its own span
                req_span_ctx = (
                    self.tracer.start_as_current_span(f"fhir.{http_method} {resource}")
                    if _use_otel else nullcontext()
                )

                with req_span_ctx as req_span:
                    if _use_otel and req_span:
                        req_span.set_attribute("fhir.resource", resource)
                        req_span.set_attribute("fhir.action", action)
                        req_span.set_attribute("http.method", http_method)
                        req_span.set_attribute("fhir.has_data", data is not None)

                    try:
                        out = api.make_request(
                            http_method=http_method,
                            resource=resource,
                            action=action,
                            data=data,
                        )
                        response = out['response']
                        # Convert CaseInsensitiveDict headers to regular dict for JSON serialization
                        if 'response_headers' in response:
                            response['response_headers'] = dict(response['response_headers'])

                        status_code = response.get('response_status_code', 0)
                        duration = response.get('response_time_seconds', 0)

                        if _use_otel:
                            _attrs = {
                                "fhir.resource": resource,
                                "http.method": http_method,
                                "http.status_code": status_code,
                            }
                            self.request_counter.add(1, _attrs)
                            self.request_duration.record(duration, _attrs)

                            if req_span:
                                req_span.set_attribute("http.status_code", status_code)
                                req_span.set_attribute("fhir.response_time_seconds", duration)
                                if status_code >= 400:
                                    req_span.set_status(StatusCode.ERROR, f"HTTP {status_code}")
                                    self.error_counter.add(1, {**_attrs, "error.type": "http_error"})

                        logger.warning(
                            "fhir.%s %s/%s -> %d (%.3fs)",
                            http_method, resource, action, status_code, duration,
                        )
                        results.append(response)

                    except Exception as e:
                        results.append({"response": str(e)})
                        logger.error(
                            "fhir.%s %s/%s failed: %s: %s",
                            http_method, resource, action, type(e).__name__, e,
                        )
                        if _use_otel:
                            self.error_counter.add(1, {
                                "fhir.resource": resource,
                                "http.method": http_method,
                                "error.type": type(e).__name__,
                            })
                            if req_span:
                                req_span.set_status(StatusCode.ERROR, str(e))
                                req_span.record_exception(e)

        return results
