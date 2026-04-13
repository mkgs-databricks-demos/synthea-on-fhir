# Epic on FHIR Bundle — Session Summaries

---

## Session: 2026-04-13 — Evaluation Notebook: Bundle Resource Wiring & UC Model Version Metrics

**Objective**: Fix evaluation notebook so that metrics and traces appear on the Unity Catalog registered model version page, not just in the MLflow experiment UI.

### Root Cause

The evaluation notebook had two problems:

1. **No experiment context**: `mlflow.start_run()` logged to whatever default experiment was active, not the bundle's experiment resource (which has `artifact_location` pointed at the `mlflow_artifacts` volume).
2. **No LoggedModel link**: `mlflow.log_metrics(metrics)` and `@mlflow.trace` logged to an experiment **run** — but the UC model version page shows metrics/traces linked to a **LoggedModel**, not a run. The `model_id` parameter is the bridge.

### How LoggedModel Links Work (MLflow 3)

| What | Where it appears | How to log |
| --- | --- | --- |
| Run metrics | Experiment UI only | `mlflow.log_metrics(metrics)` inside `start_run()` |
| LoggedModel metrics | UC model version page + experiment | `mlflow.log_metrics(metrics, model_id=model_id)` |
| LoggedModel traces | UC model version page + experiment | `mlflow.set_active_model(model_id=model_id)` then `@mlflow.trace` |

The `model_id` (LoggedModel ID) is obtained via `mlflow.models.get_model_info(model_uri).model_id` from the registered model version URI.

### Changes

#### 1. Evaluation Notebook (`src/evaluation.ipynb`)

**Cell 2 (Parameters)**: Added `mlflow_experiment_name` widget and assertion.

**Cell 5 (Set MLflow experiment context)**: Calls `mlflow.set_experiment(mlflow_experiment_name)` and prints verification (name, experiment ID, artifact location, lifecycle stage).

**Cell 6 (Load model and link to LoggedModel)**: After `mlflow.pyfunc.load_model()`, extracts `model_id` via `mlflow.models.get_model_info(model_uri).model_id`, then calls `mlflow.set_active_model(model_id=model_id)` so all subsequent `@mlflow.trace` traces link to the LoggedModel (and therefore the UC model version page).

**Cell 10 (Log metrics to model version)**: Now logs metrics in two places:
- `mlflow.log_metrics(metrics, model_id=model_id)` — appears on UC model version page
- `mlflow.start_run()` + `mlflow.log_metrics(metrics)` — appears in experiment UI

Also adds `mlflow.end_run()` between the two calls because `set_active_model()` implicitly starts a run, which would conflict with the explicit `start_run()`. The explicit run includes `run_name`, `model_name`, `model_version`, `model_id`, and `task` tags for traceability.

#### 2. Deployment Job YAML (`resources/epic_on_fhir_model_deployment.job.yml`)

- Added `mlflow_experiment_name` job parameter (default: `${resources.experiments.epic_on_fhir_requests_experiment.name}`)
- Wired to evaluation task's `base_parameters`: `mlflow_experiment_name: "{{job.parameters.mlflow_experiment_name}}"`

### Full Parameter Chain

```
bundle experiment resource → job parameter default → base_parameters → notebook widget → mlflow.set_experiment()
bundle model version → models:/ URI → get_model_info() → model_id → set_active_model() + log_metrics(model_id=...)
```

### Bug Fix: Implicit Run Conflict

**Problem**: `mlflow.set_active_model(model_id=model_id)` in cell 6 implicitly starts an MLflow run. When cell 10 calls `mlflow.start_run()`, it fails with "Run with UUID ... is already active".

**Fix**: Added `mlflow.end_run()` in cell 10 before the `mlflow.start_run()` block.

### Files Modified

| File | Changes |
| --- | --- |
| `src/evaluation.ipynb` | Cell 2: added `mlflow_experiment_name` widget; Cell 5: `mlflow.set_experiment()` with verification; Cell 6: `get_model_info()` + `set_active_model(model_id=...)`; Cell 10: `log_metrics(model_id=...)` + `end_run()` fix |
| `resources/epic_on_fhir_model_deployment.job.yml` | Added `mlflow_experiment_name` parameter and wired to evaluation task |

### Next Steps

1. Deploy and test full flow on target
2. Verify metrics appear on UC model version page after evaluation task completes
3. Verify traces appear on UC model version page (linked via `set_active_model`)
4. Confirm artifact URI points to bundle's `mlflow_artifacts` volume

---

## Session: MLflow 3 Deployment Job Implementation (2026-04-13)

**Objective**: Refactor epic-on-fhir model registration job to MLflow 3 deployment job pattern with evaluation, human-in-the-loop approval, and automated promotion.

**Changes Implemented**:

### 1. Registration Notebook (epic-on-fhir-requests-model.ipynb)
- **Cell 26 updated**: Sets only "challenger" alias (removed v1 special case)
- **Cells 27-36 deleted**: Removed validation, promotion, and endpoint update logic (moved to deployment notebooks)
- **New exit cell**: Outputs model metadata (name, version, URI, ID) for job chaining

**Architecture**: Registration notebook now only registers model and marks as "challenger". Deployment job handles validation and promotion.

### 2. Evaluation Notebook (src/evaluation.ipynb) — 11 cells
- **Parameters**: model_name, model_version (required deployment job params)
- **MLflow 3 patterns**:
  - `mlflow.set_active_model()` links traces to model version
  - `@mlflow.trace` for traced predictions
  - `mlflow.log_metrics()` logs to UC model version page
- **Validation**: GET Patient (200), POST Observation (201), POST AllergyIntolerance (201), JSON serialization
- **Metrics logged**: status codes, response times, pass/fail flags
- **Fails task** if any validation check does not pass

### 3. Approval Notebook (src/approval.ipynb) — 4 cells
- **Parameters**: model_name, model_version
- **Pattern**: Task name starts with "approval" (deployment job requirement)
- **Check**: Unity Catalog tag `deployment.approval = 'approved'` on model version
- **Fails task** if tag missing or not 'approved' (blocks deployment)

### 4. Deployment Notebook (src/deployment.ipynb) — 13 cells
- **Parameters**: model_name, model_version, endpoint_name, catalog, schema, tags
- **Alias rotation**:
  - Promotes challenger → champion
  - Rotates old champion → prior
- **Endpoint update**: Updates served_entities with new model version, waits for completion (10min timeout)
- **Telemetry**: Configures inference table logging
- **AI Gateway**: Creates route (if needed)
- **Tags**: Sets deployment metadata (version, timestamp, model_id, alias)
- **Verification**: Confirms endpoint is serving new version

### 5. Job YAML Split
- **`epic_on_fhir_model_registration.job.yml`**: Rewritten as single-task job running `epic-on-fhir-requests-model.ipynb` (registration only). Parameters: `secret_scope_name`, `client_id_dbs_key`, `algo`, `token_url`, `mlflow_experiment_name`, `pip_index_url`, `registered_model_name`.
- **`epic_on_fhir_model_deployment.job.yml`** (new): 3-task deployment pipeline (evaluation → approval_check → deployment). Parameters: `model_name`, `model_version` (required, no defaults), `catalog`, `schema`, `endpoint_name`, `tags`.
- Both use `max_concurrent_runs: 1` and match `resources/*job.yml` include pattern.
- Removed `updateAIGatewayOnly` conditional (merged into deployment task).

**Deployment Pattern**:
```
Registration Job → Runs notebook → Sets "challenger" alias
                                 ↓ New model version triggers deployment job
                          Evaluation Task → Validates model
                                 ↓
                        Approval Task → Checks UC tag
                                 ↓
                       Deployment Task → Promotes to "champion", updates endpoint
```

**Key Benefits**:
- MLflow 3 deployment job compliance
- Human-in-the-loop approval via UC tags
- Metrics visible on UC model version page
- Auto-trigger on new model version creation
- Prevents concurrent deployments
- Clear separation of concerns (registration vs. deployment)

**Next Steps**:
1. Deploy updated job via Databricks CLI or bundle
2. Test deployment flow: register model → set UC tag → observe job trigger
3. Monitor metrics on UC model version page
4. Verify endpoint update and telemetry config

---

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

## Session: 2026-04-11 10:00 UTC

### Job YAML Refactoring

**Problem**: The job YAML had several issues discovered during `databricks bundle validate`:
- Parameter defaults used `${var...}` instead of `${resources...}` for deployed resources
- Missing `registered_model_name` parameter (lost during earlier restructuring)
- Dependency logic bug: `update_endpoint_config` was excluded when `updateAIGatewayOnly=false`
- Per-task `environment_spec` blocks instead of shared job-level environment
- Wrong notebook paths (incorrect names, missing `.ipynb` extension)
- Undefined variables (`${var.environment}`, `${var.owner}`)

**Fixes applied**:

| Issue | Before | After |
| --- | --- | --- |
| `catalog` param | `${var.catalog}` | `${resources.schemas.epic_on_fhir_schema.catalog_name}` |
| `schema` param | `.schema_name` (invalid) | `${resources.schemas.epic_on_fhir_schema.name}` |
| `endpoint_name` param | `${var.name_prefix}epic-on-fhir-requests` | `${resources.model_serving_endpoints.epic_on_fhir_requests_endpoint.name}` |
| `registered_model_name` | Missing | Added: `catalog.schema.model` composed from resources |
| `model_name` param | Present but unused | Removed |
| Task environments | Per-task `environment_spec` with `client:` | Job-level `environments:` with `environment_version:` |
| `update_endpoint_config` deps | `outcome: "true"` on `check_update_mode` | Removed outcome filter, added `run_if: ALL_DONE` |
| `environment` base_param | `${var.environment}` (undefined) | `${bundle.target}` |
| `owner` base_param | `${var.owner}` (undefined) | `${var.tags_developer}` |
| Registration notebook path | `epic_on_fhir_requests_model_registration.ipynb` | `epic-on-fhir-requests-model.ipynb` |
| Update notebook path | `update-serving-endpoint-config` | `update-serving-endpoint-config.ipynb` |

**Dependency logic fix explained**: The `update_endpoint_config` task had `depends_on: [{task_key: check_update_mode, outcome: "true"}, {task_key: register_and_promote_model}]`. When `updateAIGatewayOnly=false`, the condition evaluates to `false`, so the dependency on `outcome: "true"` fails and the task is **excluded**. Fix: remove the outcome filter and add `run_if: ALL_DONE` so it runs in both branches (per Databricks docs: "Skipped upstream tasks are treated as successful when evaluating Run if conditions").

### Endpoint Existence Guard

**Problem**: On first deployment, Phase 2's `update_endpoint_config` task fails because the serving endpoint doesn't exist yet (Phase 1 couldn't create it without a model version). This causes `deploy.sh` to exit before Phase 3, so the endpoint is never created.

**Fix**: Added a guard cell (cell 4) to `update-serving-endpoint-config.ipynb` after the SDK init:

```python
from databricks.sdk.errors import NotFound, ResourceDoesNotExist

try:
    endpoint = w.serving_endpoints.get(endpoint_name)
    print(f"✓ Endpoint '{endpoint_name}' exists...")
except (NotFound, ResourceDoesNotExist):
    print(f"⚠ Endpoint '{endpoint_name}' does not exist yet...")
    dbutils.notebook.exit("SKIPPED: endpoint does not exist yet")
```

This lets the job task succeed so `deploy.sh` continues to Phase 3 (which creates the endpoint).

### Four-Phase Deployment (deploy.sh)

**Problem**: Even after the endpoint existence guard, Phase 2's `update_endpoint_config` task skips on first deploy, so AI Gateway/telemetry/tags are never applied via the SDK.

**Fix**: Added Phase 4 to `deploy.sh` — re-runs the job with `updateAIGatewayOnly=true` after Phase 3 creates the endpoint:

| Phase | Condition | Command |
| --- | --- | --- |
| 1 | Always | `databricks bundle deploy` |
| 2 | Always | `databricks bundle run epic_on_fhir_model_registration` |
| 3 | Only if Phase 1 failed | `databricks bundle deploy` |
| 4 | Only if Phase 1 failed | `databricks bundle run ... --params updateAIGatewayOnly=true` |

On subsequent deploys, Phase 1 succeeds so Phases 3 and 4 are skipped — the script remains idempotent.

### README Updates

Updated the deployment documentation to reflect the four-phase flow:
- "Three-Phase" → "Four-Phase Deployment" heading
- Added Phase 4 description and explanation
- Expanded flow diagram to show both job tasks, condition routing, and Phase 4 path
- Updated Resource #7 with conditional task table (`check_update_mode`, `register_and_promote_model`, `update_endpoint_config`)
- Updated Job Parameters table (removed stale params, added `endpoint_name`, `updateAIGatewayOnly`)
- Updated Package Dependencies to match shared job environment
- Added Phase 4 failure guidance to Troubleshooting section
- Added AI Gateway and OpenTelemetry to Monitoring section

### Assistant Instructions Update

Added to `.assistant_instructions.md`:
- **Notebook Paths in DAB YAML**: Databricks notebooks require the `.ipynb` extension when referenced in bundle YAML files. The workspace explorer may show notebooks without the extension, but the bundle validator expects it.

### Files Modified (Summary)

| File | Action |
| --- | --- |
| `resources/epic_on_fhir_model_registration.job.yml` | Refactored: resource refs for params, job-level environments, `run_if: ALL_DONE`, fixed paths and variables |
| `src/update-serving-endpoint-config.ipynb` | Added cell 4: endpoint existence guard with graceful exit |
| `deploy.sh` | Added Phase 4: conditional re-run with `updateAIGatewayOnly=true` |
| `README.md` | Updated: four-phase deployment, expanded flow diagram, new job params table, troubleshooting |
| `.assistant_instructions.md` | Added: notebook `.ipynb` extension requirement for DAB YAML |

---

## Session: 2026-04-11 09:00 UTC

### Serving Endpoint Config Update Limitation

**Problem**: User asked whether the bundle can update AI Gateway and telemetry on existing endpoints after first deployment.

**Finding**: The Serving Endpoint REST API splits configuration updates across multiple paths:

| API | What it updates |
| --- | --- |
| `POST /serving-endpoints` | Creates with full config (served_entities, traffic_config, telemetry_config, ai_gateway, tags) |
| `PUT /serving-endpoints/{name}/config` | Only served_entities and traffic_config |
| `PUT /serving-endpoints/{name}/ai-gateway` | AI Gateway settings only |
| `PATCH /serving-endpoints/{name}` | Tags only |

**Impact**: DAB bundle's serving resource sets all config on first deploy. On subsequent deploys it uses the config update API, which only handles served_entities and traffic_config — AI Gateway and telemetry require separate API calls.

**Solution**: Created `update-serving-endpoint-config.ipynb` notebook (12 cells) as a job task to apply full endpoint configuration via SDK calls:

| Cell | Purpose |
| --- | --- |
| 1-3 | Setup (parameters, SDK init) |
| 4 | Endpoint existence guard |
| 5-6 | AI Gateway create/update |
| 7-8 | Telemetry config |
| 9-10 | Tags |
| 11 | Rate limits |
| 12 | Verification |

### Files Modified (Summary)

| File | Action |
| --- | --- |
| `src/update-serving-endpoint-config.ipynb` | Created: 12-cell notebook for full endpoint config via SDK |
| `resources/epic_on_fhir_model_registration.job.yml` | Added `update_endpoint_config` task |
