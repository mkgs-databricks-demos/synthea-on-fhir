# Epic on FHIR Bundle — Session Summaries

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
