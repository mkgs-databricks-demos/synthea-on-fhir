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
- **Check**: Unity Catalog tag `approval_check = 'approved'` on model version
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
