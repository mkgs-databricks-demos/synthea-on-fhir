# Epic on FHIR Bundle — Session Summaries

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

### Created `update-serving-endpoint-config.ipynb`

New notebook (`src/update-serving-endpoint-config.ipynb`) using the Databricks SDK to update an existing serving endpoint without redeploying the model:

| Cell | Purpose |
| --- | --- |
| Parameters | Widgets: `endpoint_name`, `catalog`, `schema`, tag values (`component`, `environment`, `project`, `owner`) |
| AI Gateway | `w.serving_endpoints.update_ai_gateway()` — inference tables, usage tracking, guardrails, rate limits (100/min/user) |
| Telemetry | `w.serving_endpoints.update_config()` — OpenTelemetry traces/logs/metrics table names |
| Tags | `w.serving_endpoints.patch()` — component, environment, project, owner |
| Verification | `w.serving_endpoints.get()` — reads back and prints all config |

### Conditional Job Workflow

Updated `resources/epic_on_fhir_model_registration.job.yml` with `updateAIGatewayOnly` parameter and three-task conditional flow:

| Task | Condition | Purpose |
| --- | --- | --- |
| `check_update_mode` | `condition_task` evaluating `updateAIGatewayOnly == "true"` | Branch gate |
| `register_and_promote_model` | Runs only if condition is **false** | Full model registration + promotion |
| `update_endpoint_config` | Runs after model registration (condition false) OR directly (condition true) | AI Gateway, telemetry, tags update |

**Usage**: `updateAIGatewayOnly=false` (default) → full deployment. `updateAIGatewayOnly=true` → config-only update, skips model registration.

### Cleanup

Deleted stray `session_summary.md` created at bundle root — session summaries belong in `fixtures/session_summaries.md` per convention.

### Files Modified (Summary)

| File | Action |
| --- | --- |
| `src/update-serving-endpoint-config.ipynb` | Created — Databricks SDK notebook for AI Gateway, telemetry, and tag updates |
| `resources/epic_on_fhir_model_registration.job.yml` | Added `updateAIGatewayOnly` parameter, `check_update_mode` condition task, `update_endpoint_config` task |
| `session_summary.md` (bundle root) | Deleted — stray file, content merged here |

---

## Session: 2026-04-11 08:00 UTC

### SQL Endpoint Test Queries (via ai_query)

Created two SQL queries that call the `sandbox_epic_on_fhir_requests` model serving endpoint using `ai_query()` instead of the Python SDK. Uses the baseline pattern from [Simple Endpoint Test](#query-414613ad-b10c-40d4-b310-73a3e1645cd1) and translates the full flow from [epic-on-fhir-example](#notebook-2671959084702948).

#### Simple Endpoint Test (query)

**Fixes applied across multiple `/fix` iterations**:

| Issue | Root Cause | Fix |
| --- | --- | --- |
| `PARSE_SYNTAX_ERROR` at `"dataframe_split"` | Raw JSON object literal `{ "key": "value" }` is not valid SQL | Replaced with `named_struct()` for `ai_query` `request` parameter |
| `AI_FUNCTION_HTTP_PARSE_COLUMNS_ERROR` | Model returns 5 columns but `ai_query` defaults to single STRING | Added `returnType => 'STRUCT<response_headers:STRING, response_url:STRING, response_time_seconds:DOUBLE, response_status_code:INT, response_text:STRING>'` |
| `AI_FUNCTION_UNSUPPORTED_RETURN_TYPE` for `"StringType"` | Spark internal type name, not SQL syntax | Changed to `"STRING"` (valid SQL type), then to full STRUCT since model returns multiple columns |

**Final pattern**: CTE calls `ai_query` once, outer SELECT expands struct fields with `parse_json()` on JSON columns (`response_headers`, `response_text` as VARIANT).

**Parameters**: `catalog_name` (default: `hls_fde`), `schema_name` (default: `sandbox_open_epic_smart_on_fhir`) with `USE CATALOG/SCHEMA IDENTIFIER(:param)` at top.

#### Epic FHIR Endpoint Test Flow (query)

Full 7-step end-to-end test flow translated from the notebook into pure SQL:

| Step | Type | Resource | Description |
| --- | --- | --- | --- |
| 1 | GET | Patient | Search by `?identifier=EXTERNAL\|Z6129`, extract FHIR STU3 ID |
| 2 | GET | Patient/$summary | Clinical summary for extracted patient |
| 3 | GET | Encounter | Search encounters for patient, extract encounter_id |
| 4 | POST | Observation | Create Heart Rate vital sign (Epic flowsheet code 8) |
| 5 | GET | Observation | Read-back to verify creation |
| 6 | POST | AllergyIntolerance | Create Penicillin allergy (RxNorm 7980) |
| 7 | GET | AllergyIntolerance | Read-back to verify creation |

**Key SQL techniques**:
- `DECLARE OR REPLACE VARIABLE` + `SET VARIABLE` for chaining results between steps (patient_id → encounter_id → observation_id → allergy_id)
- `filter()` + `from_json()` to extract typed identifiers from VARIANT arrays
- `format_string()` + `concat()` to build FHIR JSON payloads for POST requests
- `parse_json(response_headers):Location` + `element_at(split(..., '/'), -1)` to extract resource IDs from POST 201 Location headers
- Summary statement with CASE expression reporting pass/fail for all 4 extraction steps

**13 total statements**, 7 Epic API calls (each called exactly once). Same `catalog_name`/`schema_name` parameters as Simple Endpoint Test.

### Patient ID: DSTU2 vs STU3

**Problem**: Initial SQL flow extracted `type.text = 'FHIR'` (DSTU2 ID: `TnOZ.elPXC6...`) from the Patient search, while the notebook's cell 16 overrides with the STU3 ID (`erXuFYUfucBZaryVksYEcMg3`).

**Fix**: Changed filter from `x -> x.type.text = 'FHIR'` to `x -> x.type.text = 'FHIR STU3'` to match the notebook's actual behavior. Epic resolves both internally, but STU3 is the canonical ID.

### AllergyIntolerance Read-Back: Epic 400 (Both Notebook & SQL)

**Problem**: GET `AllergyIntolerance/{id}` returns HTTP 400 with Epic error 59102: `"Error normalizing codeable concept"` at `/f:codeableconcept`.

**Investigation**:
- POST succeeds (201) with valid Location header in both notebook and SQL
- ID extraction verified correct (relative path `AllergyIntolerance/{id}`, `split('/')[-1]` gives correct ID)
- Tested with both DSTU2 and STU3 patient IDs — same 400 on read-back
- Payloads are semantically identical between notebook (`json.dumps`) and SQL (`format_string`)

**Finding**: The notebook was **never successfully reading back** AllergyIntolerance either. The `else` branch in cell 28 ran `json.dumps(new_allergy)` which failed with `"Object of type CaseInsensitiveDict is not JSON serializable"` — this **masked** the real Epic 400 error.

**Root Cause**: Epic sandbox server-side issue. The AllergyIntolerance resource is created (201) but Epic cannot serialize it back to FHIR JSON on read. Error 59102 = "Content invalid against the specification or a profile." This is an Epic FHIR server normalization bug, not a client-side issue.

**Fix to cell 28**: Convert `response_headers` (a `requests.CaseInsensitiveDict`) to a plain `dict` before `json.dumps` in the `else` branch, so the actual Epic OperationOutcome is visible. Cell 23 (Observation read-back) has the same latent bug but doesn't trigger because Observation GETs return 200.

### Files Modified (Summary)

| File | Action |
| --- | --- |
| Query: Simple Endpoint Test | Created — single `ai_query` call with `named_struct`, STRUCT returnType, VARIANT columns, parameterized catalog/schema |
| Query: Epic FHIR Endpoint Test Flow | Created — 13-statement flow with `DECLARE`/`SET VARIABLE` chaining, 7 API calls, pass/fail summary |
| `src/epic-on-fhir-example` cell 28 | Fixed `CaseInsensitiveDict` serialization in AllergyIntolerance read-back error path |

---

## Session: 2026-04-11 07:42 UTC

### Serverless Environment & Proxy Fixes

**Problem**: Job runs failed with package installation errors on serverless compute.

**Root Cause**: `--index-url` (later `--extra-index-url`) pointed at `pypi-proxy.dev.databricks.com`, which is unreachable from all Databricks-managed infrastructure (serverless, Apps, model serving). These environments use Databricks' own package mirror.

**Changes**:

| File | Change |
| --- | --- |
| `resources/epic_on_fhir_model_registration.job.yml` | Removed `--extra-index-url` from serverless dependencies; added explanatory comment |
| `resources/jwk_url.app.yml` | Removed `PIP_INDEX_URL` env var from app config |
| `src/jwk_url_app/requirements.txt` | Removed `--extra-index-url` directive |
| Notebook cell 17 (conda env) | Removed `--extra-index-url` from model serving pip list |

**Established rule**: Proxy is for **local dev only** (`pyproject.toml`, `.npmrc`). Never add proxy config to serverless, Apps, or model serving.

### Serverless Environment Version Update

Updated `serverless_environment_version` default from `4` to `5` in `databricks.yml`.

### MLflow NaN→None Fix

**Problem**: `MlflowException: Can not safely convert float64 to string` when the `data` column is NaN (GET requests have no body).

**Root Cause**: Pandas stores missing values as `float64` NaN. MLflow's schema enforcement can't convert `float64` to the `string (optional)` column type.

**Fix**: Added `.where(row.notna(), None)` to convert NaN to Python `None` (accepted by MLflow for optional columns).

| Notebook Cell | Change |
| --- | --- |
| Cell 30 ("Validate model with test payloads") | `pd.DataFrame([row])` → `pd.DataFrame([row.where(row.notna(), None)])` |
| Cell 36 ("Final traced prediction against champion") | `.iloc[[0]]` → `.iloc[[0]].where(lambda df: df.notna(), None)` |

### Registered Model Name — Bundle Prefix Alignment

**Problem**: The notebook constructed `registered_model_name` from `catalog_use.schema_use.epic_on_fhir_requests`, missing the DAB `name_prefix` (e.g., `[sandbox] `) applied to deployed resources.

**Fix**:

| Location | Change |
| --- | --- |
| Notebook cell 6 | Added `registered_model_name` widget (full 3-level UC namespace) |
| Notebook cell 7 | Reads widget directly instead of constructing from parts |
| Job YAML `parameters` | Added `registered_model_name` with default: `${resources.schemas...catalog_name}.${resources.schemas...name}.${resources.registered_models...name}` |
| Job YAML `base_parameters` | Added `registered_model_name: "{{job.parameters.registered_model_name}}"` |

### Removed Unused catalog_use / schema_use Widgets

**Verification**: Searched all 36 notebook cells — `catalog_use` and `schema_use` were only referenced in their own widget definition/retrieval cells (6 and 7). No downstream usage.

**Removed from**:
- Notebook cell 6 (widget definitions)
- Notebook cell 7 (widget retrieval)
- Job YAML `parameters` and `base_parameters`

**Result**: Notebook now has **7 widgets** (down from 9): `secret_scope_name`, `client_id_dbs_key`, `algo`, `token_url`, `mlflow_experiment_name`, `pip_index_url`, `registered_model_name`.

### Deploy Script Consistency Check

Audited `deploy.sh` against all other files. Found one stale comment in job YAML referencing "apps and model serving" for proxy usage — corrected to "local dev only".

All cross-file references verified consistent:
- Serverless environment version = 5
- No proxy in any Databricks-managed environment
- Job key matches between deploy.sh and job YAML
- Experiment paths standardized to `/Workspace/.experiments/`
- Notebook path has `.ipynb` extension

### README.md Overhaul

Rewrote the bundle README with:
- **New section**: "Deploy Script (`deploy.sh`)" — usage, three-phase flow diagram, job parameters table, package dependencies, proxy caveat
- **Fixed resource #7**: "Sample Job" → "Model Registration Job"
- **Fixed experiment path**: `/Workspace/experiments/` → `/Workspace/.experiments/`
- **Added `hls_fde_sandbox_prod`** to deployment targets table
- **Fixed secrets list**: Added `kid` and `public_key`, removed `jwk_set`
- **Fixed API example**: Updated request/response to match actual model schema
- **New sections**: "Proxy Configuration", "Deploy Script Failures" troubleshooting

### Test Suite Created

Created 4 pytest test files (54 tests total) compatible with the Databricks workspace testing sidebar:

| File | Tests | Coverage |
| --- | --- | --- |
| `tests/conftest.py` | — | Fixtures: fake secrets, dynamic RSA key gen, mock token/FHIR responses, epic_auth/epic_api/pyfunc_model. Defensive Spark/SDK init for CLI compatibility. |
| `tests/test_auth.py` | 8 | `EpicApiAuth`: JWT payload/header structure, grant_type, jti uniqueness, token caching, token refresh, AuthBase header injection, `can_connect()` |
| `tests/test_endpoint.py` | 11 | `EpicApiRequest`: URL construction (GET/POST/empty action), HTTP method dispatch, data passthrough, response dict structure and keys |
| `tests/test_epic_fhir_pyfunc.py` | 13 | `EpicFhirPyfuncModel`: init, `_get_secrets` from env vars, predict with GET/POST/NaN/None/empty/missing resource/multiple rows/exception/default method |
| `tests/test_payloads.py` | 19 | Payload generation: DataFrame structure, FHIR content validation, randomization, NaN→None conversion pattern (cells 30/36) |

**Key design decisions**:
- All tests use mocking — no real Epic API calls
- RSA key generated dynamically via `cryptography` library (hardcoded key was invalid for RS384)
- `conftest.py` handles missing Spark/SDK context gracefully (works from workspace sidebar, CLI, and local dev)
- Deleted stale `sample_taxis_test.py` (referenced non-existent `epic_on_fhir.taxis` module)

**Verification**: Full suite run via `python -B -m pytest` — 54 passed, 0 failed, 2.90s.

### Serving Endpoint — Tags & Telemetry

Added tags and telemetry best practices to `resources/epic_on_fhir_requests.serving.yml`.

**Tags** (cost tracking): Added `project`, `businessUnit`, `developer`, `requestedBy` tags using bundle variables. These propagate to `system.billing.usage` under `custom_tags` for cost attribution.

**Endpoint Telemetry** (Preview): Added `telemetry_config` to persist OpenTelemetry data to three Unity Catalog Delta tables in the same schema as other FHIR resources:

| Table | Content |
| --- | --- |
| `epic_on_fhir_requests_otel_logs` | Application logs (severity, body, attributes) |
| `epic_on_fhir_requests_otel_metrics` | Performance metrics |
| `epic_on_fhir_requests_otel_spans` | Distributed traces/spans (trace_id, span_id) |

**AI Gateway**: Added inference table config (request/response payload logging), usage tracking, and per-user rate limiting (60 calls/min).

**Other additions**:
- `ENABLE_MLFLOW_TRACING: "true"` env var on served entity
- `description` field on endpoint
- `traffic_config` with explicit routing

### Assistant Instructions Updated

Updated `.assistant_instructions.md` to document:
- Proxy is for local dev only (not Databricks-managed compute)
- Specific locations where proxy IS and IS NOT configured
- The `pip_index_url` widget/param flow for future flexibility
- Session summaries convention for all bundles

### Files Modified (Summary)

| File | Action |
| --- | --- |
| `databricks.yml` | Updated `serverless_environment_version` default: 4 → 5 |
| `resources/epic_on_fhir_model_registration.job.yml` | Removed proxy, removed catalog_use/schema_use params, added registered_model_name param |
| `resources/epic_on_fhir_requests.serving.yml` | Added tags, telemetry_config, ai_gateway (inference tables, usage tracking, rate limits) |
| `resources/jwk_url.app.yml` | Removed `PIP_INDEX_URL` env var |
| `src/jwk_url_app/requirements.txt` | Removed `--extra-index-url` |
| `README.md` | Comprehensive rewrite (deploy script docs, fixed references, new sections) |
| Notebook cell 6 | Removed catalog_use/schema_use widgets, added registered_model_name widget |
| Notebook cell 7 | Removed catalog_use/schema_use retrieval, reads registered_model_name from widget |
| Notebook cell 17 | Removed `--extra-index-url` from conda env |
| Notebook cell 30 | Added NaN→None conversion |
| Notebook cell 36 | Added NaN→None conversion |
| `tests/conftest.py` | Rewritten with Epic fixtures, dynamic RSA key, defensive init |
| `tests/test_auth.py` | Created (8 tests) |
| `tests/test_endpoint.py` | Created (11 tests) |
| `tests/test_epic_fhir_pyfunc.py` | Created (13 tests) |
| `tests/test_payloads.py` | Created (19 tests) |
| `tests/sample_taxis_test.py` | Deleted |
| `fixtures/session_summaries.md` | Created |
| `.assistant_instructions.md` | Updated proxy documentation, added session summaries convention |
