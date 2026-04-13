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
