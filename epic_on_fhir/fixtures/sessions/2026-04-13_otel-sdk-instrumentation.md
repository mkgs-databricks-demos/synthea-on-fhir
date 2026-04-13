## Session: 2026-04-13 16:00 UTC

### OpenTelemetry SDK Instrumentation for Model Serving (Preview)

**Context**: The bundle already had `telemetry_config` on the serving endpoint (persisting OTel data to UC Delta tables) and `ENABLE_MLFLOW_TRACING` enabled. However, the pyfunc model only emitted standard Python logging (auto-captured to `_otel_logs`). No custom metrics or traces were being emitted to the `_otel_metrics` or `_otel_spans` tables.

**Change**: Added full OTel SDK instrumentation to the pyfunc model following the [Persist custom model serving data to Unity Catalog](https://docs.databricks.com/aws/en/machine-learning/model-serving/custom-model-serving-uc-logs) documentation pattern.

### Bundle Variable Toggle (Preview Feature Gate)

**Problem**: Endpoint telemetry is a Beta/Preview feature. The OTel SDK initialization should be opt-in per deployment target, not always-on.

**Solution**: Full chain from bundle variable → serving env var → pyfunc module-level check:

| Layer | Location | Mechanism |
| --- | --- | --- |
| Bundle variable | `databricks.yml` | `enable_otel_instrumentation` (default: `"false"`) |
| Serving env var | `epic_on_fhir_requests.serving.yml` | `ENABLE_OTEL_INSTRUMENTATION: "${var.enable_otel_instrumentation}"` |
| Pyfunc gate | `epic_fhir_pyfunc.py` (line 36) | `_OTEL_REQUESTED = os.environ.get("ENABLE_OTEL_INSTRUMENTATION", "false").lower() == "true"` |

**Target overrides**: Only `hls_fde_sandbox_prod` sets `enable_otel_instrumentation: "true"`. All other targets (`dev`, `sandbox_prod`, `prod`) inherit the default `"false"` and skip OTel SDK initialization entirely.

**Behavior when disabled**: Python `logging` calls remain in the pyfunc (WARNING level). These are auto-captured to `_otel_logs` by the endpoint `telemetry_config` regardless of the `ENABLE_OTEL_INSTRUMENTATION` flag. Only the custom metrics/traces (counters, histograms, spans) are gated.

### OTel Instrumentation Details

**Module-level initialization** (per-worker, following docs pattern):
- `Resource` with `worker.pid` attribute
- `TracerProvider` with `BatchSpanProcessor` → `OTLPSpanExporter` (HTTP)
- `MeterProvider` with `PeriodicExportingMetricReader` → `OTLPMetricExporter` (HTTP)
- OTLP exporters are pre-configured by Databricks on serving endpoints — no endpoint URLs needed

**Instruments created**:

| Instrument | Type | Description | Attributes |
| --- | --- | --- | --- |
| `predict.call_count` | Counter | Total `predict()` invocations | `input.row_count` |
| `fhir.request_count` | Counter | FHIR API requests | `fhir.resource`, `http.method`, `http.status_code` |
| `fhir.error_count` | Counter | Request errors (HTTP 4xx/5xx, exceptions, validation) | `fhir.resource`, `http.method`, `error.type` |
| `fhir.request_duration` | Histogram | Response time from Epic (seconds) | `fhir.resource`, `http.method`, `http.status_code` |

**Spans**:
- `EpicFhirPyfuncModel.predict` — top-level span wrapping entire predict() call, attributes: `input.row_count`, `input.columns`
- `fhir.{method} {resource}` — child span per FHIR request, attributes: `fhir.resource`, `fhir.action`, `http.method`, `fhir.has_data`, `http.status_code`, `fhir.response_time_seconds`. On error: `set_status(ERROR)` + `record_exception()`

**Python logging** (auto-captured to `_otel_logs`, always active):
- `WARNING`: OTel init status, model load, per-request result (`fhir.get Patient/... -> 200 (0.253s)`), predict() summary
- `ERROR`: Missing resource validation, request exceptions

**Graceful fallback**: If `_OTEL_REQUESTED` is `false` OR the OTel SDK import fails, `_OTEL_ENABLED = False` and all metric/trace code paths are skipped via `_use_otel` guard. Uses `contextlib.nullcontext()` as span context manager no-op.

### Conda Environment Update

Added three OTel packages to the model serving conda env (notebook cell 17). No proxy URLs added per custom instructions (model serving has its own package mirror).

| Package | Purpose |
| --- | --- |
| `opentelemetry-api` | OTel API interfaces (tracer, meter) |
| `opentelemetry-sdk` | SDK implementations (providers, processors, readers) |
| `opentelemetry-exporter-otlp-proto-http` | OTLP HTTP exporters for spans and metrics |

### Telemetry Data Flow (Post-Deployment)

With `enable_otel_instrumentation: "true"` and `telemetry_config` set on the endpoint:

| UC Table | Data Source | Requires OTel SDK? |
| --- | --- | --- |
| `epic_on_fhir_requests_otel_logs` | Python `logging` (WARNING+) | No — auto-captured |
| `epic_on_fhir_requests_otel_spans` | `_tracer.start_as_current_span()` | Yes |
| `epic_on_fhir_requests_otel_metrics` | Counters + histogram via `_meter` | Yes |

### No Changes Needed Elsewhere

- **Cell 22 (log and register model)**: Model file content just imports `EpicFhirPyfuncModel` and calls `set_model()`. OTel init is at module level in `epic_fhir_pyfunc.py`, bundled via `code_paths`.
- **Cell 18 (code_paths)**: Already includes `smart_on_fhir/` directory.
- **`update-serving-endpoint-config.ipynb`**: Passes through `environment_vars` from the existing endpoint config, so the new env var is preserved on config updates.

### Files Modified (Summary)

| File | Action |
| --- | --- |
| `src/smart_on_fhir/epic_fhir_pyfunc.py` | Added OTel SDK init (gated on env var), 4 instruments, 2 span levels, Python logging, graceful fallback |
| `databricks.yml` | Added `enable_otel_instrumentation` variable (default `"false"`), set to `"true"` in `hls_fde_sandbox_prod` target |
| `resources/epic_on_fhir_requests.serving.yml` | Added `ENABLE_OTEL_INSTRUMENTATION` env var referencing bundle variable |
| Notebook cell 17 (conda env) | Added `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp-proto-http` |

---
