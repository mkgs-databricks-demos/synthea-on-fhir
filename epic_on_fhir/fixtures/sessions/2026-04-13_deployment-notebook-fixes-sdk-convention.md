## Session: 2026-04-13 — Deployment Notebook Fixes & SDK Install Convention

**Objective**: Diagnose and fix the failing `deployment` task in the MLflow 3 deployment job, and establish a `%pip install` convention for notebooks using the Databricks Python SDK.

### Problem: Deployment Task Failure

The most recent deployment job run (job ID `880066388050939`) failed on the `deployment` task with:

> "Task deployment failed with message: An error occurred during the execution of a feature workload."

The `evaluation` and `approval_check` tasks passed — the failure was isolated to the deployment notebook.

### Root Cause Analysis

Four issues identified in `src/deployment.ipynb`:

#### Issue 1: Incorrect `update_config` SDK Call (CRITICAL — caused the failure)

**Cell 8 (Update serving endpoint version)** constructed an `EndpointCoreConfigInput` object and passed it as a positional argument:

```python
updated_config = EndpointCoreConfigInput(name=endpoint_name, served_entities=[...])
w.serving_endpoints.update_config(endpoint_name, updated_config)
```

The `databricks-sdk` method signature is:

```python
def update_config(self, name: str, *, served_entities=None, ...) -> Wait[ServingEndpointDetailed]
```

All config fields are **keyword-only** — the second positional arg maps to `auto_capture_config`, not a config object. This caused a `TypeError`.

**Fix**: Replaced with `update_config_and_wait()` using keyword args directly. Also reads existing `environment_vars` from the current served entity and carries them forward (Epic OAuth2 secrets, OTel flags).

#### Issue 2: Missing `environment_vars` Preservation

The original code built a new `ServedEntityInput` without copying `environment_vars` from the existing endpoint config. On update, the endpoint would lose its secrets:
- `EPIC_CLIENT_ID` (from secret scope)
- `EPIC_KID` (from secret scope)
- `EPIC_PRIVATE_KEY` (from secret scope)
- `ENABLE_MLFLOW_TRACING`
- `ENABLE_OTEL_INSTRUMENTATION`

**Fix**: Cell 8 now reads `current_entities[0].environment_vars` and passes them as `environment_vars=current_env_vars` on the new `ServedEntityInput`.

#### Issue 3: Redundant Polling Loop

**Cell 9 (Wait for endpoint update)** had a manual polling loop (`time.sleep(10)`, 10-minute timeout). Since cell 8 now uses `update_config_and_wait()` (which blocks until READY), the polling is redundant.

**Fix**: Replaced with a simple confirmation print.

#### Issue 4: Incorrect AI Gateway Route Creation

**Cell 11 (Configure AI Gateway route)** used `from databricks import agents` with `agents.create_route()` / `agents.get_route()`. The `databricks-agents` package is designed for GenAI agent use cases — not custom pyfunc model endpoints. The bundle's serving YAML already manages AI Gateway config declaratively via the `ai_gateway` block.

**Fix**: Replaced with a note that AI Gateway is managed by bundle YAML (`epic_on_fhir_requests.serving.yml`).

### SDK Install Convention

**Problem**: The deployment notebook imported `from databricks.sdk import WorkspaceClient` but never installed the latest SDK version. The serverless job environment specifies `databricks-sdk>=0.20.0`, but the actual SDK version may vary and older versions may not have the correct method signatures.

**Fix**: Added a `%pip install --upgrade databricks-sdk mlflow` + `dbutils.library.restartPython()` cell at the top of the notebook (cell 2, before Parameters).

**Convention established**: Any notebook that imports `databricks.sdk` must include this install cell. Audited all 7 notebooks in `src/`:

| Notebook | SDK Usage | Action |
| --- | --- | --- |
| `deployment` | `WorkspaceClient`, `ServedEntityInput` | **Added** install cell |
| `update-serving-endpoint-config` | `WorkspaceClient`, AI Gateway classes | Already had it |
| `evaluation` | mlflow only | N/A |
| `approval` | mlflow only | N/A |
| `epic-on-fhir-requests-model` | `databricks.connect` (local dev) | N/A |
| `epic-smart-on-fhir-class-examples` | `databricks.connect` (local dev) | N/A |
| `epic-sandbox-basic-auth` | No SDK | N/A |

Convention saved to `.assistant_instructions.md` for future sessions.

### Changes Made

#### `src/deployment.ipynb`

| Cell | Title | Change |
| --- | --- | --- |
| 2 (new) | Install latest Databricks SDK | Added `%pip install --upgrade databricks-sdk mlflow` + `restartPython()` |
| 4 | Imports and clients | Removed `EndpointCoreConfigInput` from import (no longer used) |
| 8 | Update serving endpoint version | Rewrote: reads existing `environment_vars`, uses `update_config_and_wait()` with keyword args, preserves env vars on new `ServedEntityInput` |
| 9 | Wait for endpoint update | Replaced polling loop with simple confirmation (redundant with `_and_wait`) |
| 11 | Configure AI Gateway route | Replaced `databricks-agents` code with note about bundle YAML management |

#### `.assistant_instructions.md`

- Added **Databricks SDK %pip Install Convention** section with rules, pattern, and current notebooks list

### Design Decisions

1. **`update_config_and_wait` over `update_config` + polling**: The SDK's `_and_wait` variant handles all polling, retries, and timeout logic internally. Eliminates custom polling code and ensures correct state detection.
2. **Preserve `environment_vars` explicitly**: The serving YAML defines env vars declaratively, but `update_config` replaces the entire served entity config. Without copying env vars forward, the endpoint loses its secrets on every deployment — causing the pyfunc to fail at runtime.
3. **Remove AI Gateway programmatic setup**: The bundle's `ai_gateway` YAML block is the single source of truth for inference table config, usage tracking, and rate limits. Programmatic setup via `databricks-agents` was redundant and incorrect (wrong package for custom models).
4. **`%pip install` convention**: Ensures notebooks always use the latest SDK, preventing method signature mismatches between the SDK version in the serverless environment and the code's assumptions.

### Files Modified

| File | Changes |
| --- | --- |
| `src/deployment.ipynb` | Added SDK install cell; fixed imports; rewrote endpoint update (keyword args + env vars); simplified wait; replaced AI Gateway cell |
| `.assistant_instructions.md` | Added SDK install convention |

### Next Steps

1. Deploy updated bundle to `hls_fde_sandbox_prod` target
2. Re-run the deployment job to verify the endpoint update succeeds
3. Confirm `environment_vars` are preserved after endpoint update
4. Verify endpoint serves the correct model version
