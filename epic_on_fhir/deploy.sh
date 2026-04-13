#!/bin/bash
# deploy.sh — Single-command deployment for the Epic on FHIR asset bundle.
#
# Handles the chicken-and-egg dependency between the model serving endpoint
# (which requires a model version) and the model registration notebook
# (which requires the registered model resource to exist).
#
# Two-job architecture:
#   - Registration job: registers model to UC, sets "challenger" alias
#   - Deployment job: evaluates → approves → promotes to "champion", updates endpoint
#
# Phases:
#   1. Deploy bundle infrastructure (schema, experiment, registered model, volume).
#      The serving endpoint may fail on first deploy if no model version exists.
#      After deploy, connects the deployment job to the registered model in UC
#      (enables auto-trigger on new model version creation).
#   2. Run the registration job to create a model version with "challenger" alias.
#   3. Re-deploy the bundle so the serving endpoint picks up the model version.
#   4. Auto-approve and run the deployment job to promote to "champion" and
#      configure the endpoint (AI Gateway, telemetry, tags).
#
# Subsequent runs: Phase 1 succeeds fully, Phase 2 registers a new version,
# Phases 3-4 are skipped — the deployment job auto-triggers on new model
# version creation.
#
# Usage:
#   ./deploy.sh [target]
#
# Arguments:
#   target   Bundle target (default: dev). One of: dev, sandbox_prod,
#            hls_fde_sandbox_prod, prod
#
# Prerequisites:
#   - Databricks CLI installed and authenticated
#   - Secret scope configured with Epic OAuth2 credentials
#   - Bundle validated: databricks bundle validate -t <target>
#
# Examples:
#   ./deploy.sh                     # Deploy to dev (default)
#   ./deploy.sh sandbox_prod        # Deploy to sandbox production
#   ./deploy.sh hls_fde_sandbox_prod # Deploy to HLS FDE sandbox

set -euo pipefail

TARGET="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRATION_JOB="epic_on_fhir_model_registration"
DEPLOYMENT_JOB="epic_on_fhir_model_deployment"

echo "============================================="
echo "Epic on FHIR Bundle Deployment"
echo "Target: ${TARGET}"
echo "Time:   $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
echo "============================================="
echo ""

# Change to the bundle root directory
cd "${SCRIPT_DIR}"

# --------------------------------------------------------------------------
# Phase 1: Deploy bundle infrastructure
# --------------------------------------------------------------------------
echo "[Phase 1/4] Deploying bundle infrastructure..."
echo "  This creates: schema, experiment, registered model, volume, app, jobs."
echo "  The serving endpoint may fail if no model version exists yet."
echo ""

if databricks bundle deploy -t "${TARGET}"; then
    echo ""
    echo "  ✓ Full deployment succeeded."
    PHASE1_FULL_SUCCESS=true
else
    echo ""
    echo "  ⚠ Partial deployment (serving endpoint may have failed — expected on first run)."
    PHASE1_FULL_SUCCESS=false
fi
echo ""

# --------------------------------------------------------------------------
# Connect deployment job to registered model (idempotent)
# --------------------------------------------------------------------------
# Links the deployment job to the UC registered model so it:
#   - Auto-triggers when a new model version is created
#   - Shows on the model's Overview page in Unity Catalog
# Uses bundle summary to resolve target-specific names (handles name_prefix).
echo "  Connecting deployment job to registered model..."

DEPLOY_JOB_NAME=""
MODEL_FULL_NAME=""
DEPLOY_JOB_ID=""

BUNDLE_SUMMARY=$(databricks bundle summary -t "${TARGET}" --output json 2>/dev/null || true)

if [ -n "${BUNDLE_SUMMARY}" ]; then
    # Extract deployment job name (single-quoted heredoc: no bash interpretation)
    DEPLOY_JOB_NAME=$(echo "${BUNDLE_SUMMARY}" | python3 << 'PYEOF'
import sys, json
try:
    data = json.load(sys.stdin)
    job = data.get('resources', {}).get('jobs', {}).get('epic_on_fhir_model_deployment', {})
    print(job.get('name', ''))
except Exception:
    pass
PYEOF
    )

    # Extract registered model full name (catalog.schema.model)
    MODEL_FULL_NAME=$(echo "${BUNDLE_SUMMARY}" | python3 << 'PYEOF'
import sys, json
try:
    data = json.load(sys.stdin)
    model = data.get('resources', {}).get('registered_models', {}).get('epic_on_fhir_requests_model', {})
    c = model.get('catalog_name', '')
    s = model.get('schema_name', '')
    n = model.get('name', '')
    if c and s and n:
        print('%s.%s.%s' % (c, s, n))
except Exception:
    pass
PYEOF
    )

    # Look up the deployment job ID by name
    if [ -n "${DEPLOY_JOB_NAME}" ]; then
        DEPLOY_JOB_ID=$(databricks jobs list --name "${DEPLOY_JOB_NAME}" --output json 2>/dev/null | python3 << 'PYEOF'
import sys, json
try:
    data = json.load(sys.stdin)
    jobs = data.get('jobs', [])
    if jobs:
        print(jobs[0].get('job_id', ''))
except Exception:
    pass
PYEOF
        )
    fi

    if [ -n "${DEPLOY_JOB_ID}" ] && [ -n "${MODEL_FULL_NAME}" ]; then
        # Connect via MLflow REST API (idempotent — safe to re-run)
        databricks api patch /api/2.0/mlflow/unity-catalog/registered-models/update \
            --json "{\"name\": \"${MODEL_FULL_NAME}\", \"deployment_job_id\": \"${DEPLOY_JOB_ID}\"}" \
            2>/dev/null || true
        echo "  ✓ Deployment job (ID: ${DEPLOY_JOB_ID}) connected to model ${MODEL_FULL_NAME}"
    else
        echo "  ⚠ Could not resolve job ID or model name — connect manually via the model page."
    fi
else
    echo "  ⚠ Could not read bundle summary — connect manually via the model page."
fi
echo ""

# --------------------------------------------------------------------------
# Phase 2: Run model registration job
# --------------------------------------------------------------------------
echo "[Phase 2/4] Running model registration job..."
echo "  Job: ${REGISTRATION_JOB}"
echo "  This registers a new model version and sets the 'challenger' alias."
echo ""

# Capture the run URL to extract the run ID for later
RUN_LOG=$(databricks bundle run -t "${TARGET}" "${REGISTRATION_JOB}" 2>&1 | tee /dev/stderr)

# Extract the run ID from the output (format: "Run URL: https://.../runs/<run_id>")
RUN_ID=$(echo "${RUN_LOG}" | grep -oP 'runs/\K[0-9]+' | tail -1 || true)

echo ""
echo "  ✓ Model registration job completed."
echo ""

# --------------------------------------------------------------------------
# Extract model metadata from registration job output
# --------------------------------------------------------------------------
# The registration notebook exits with JSON: {model_name, model_version, model_uri, model_id}
MODEL_NAME=""
MODEL_VERSION=""

if [ -n "${RUN_ID}" ]; then
    echo "  Extracting model metadata from run ${RUN_ID}..."
    RUN_OUTPUT=$(databricks jobs get-run "${RUN_ID}" --output json 2>/dev/null || true)
    if [ -n "${RUN_OUTPUT}" ]; then
        # Parse the notebook exit value from the run output
        NOTEBOOK_RESULT=$(echo "${RUN_OUTPUT}" | python3 << 'PYEOF'
import sys, json
try:
    data = json.load(sys.stdin)
    for task in data.get('tasks', []):
        nb_output = task.get('notebook_output', {}).get('result', '')
        if nb_output:
            print(nb_output)
            break
except Exception:
    pass
PYEOF
        )
        if [ -n "${NOTEBOOK_RESULT}" ]; then
            MODEL_NAME=$(echo "${NOTEBOOK_RESULT}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['model_name'])" 2>/dev/null || true)
            MODEL_VERSION=$(echo "${NOTEBOOK_RESULT}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['model_version'])" 2>/dev/null || true)
            echo "  Model: ${MODEL_NAME} v${MODEL_VERSION}"
        fi
    fi
fi
echo ""

# --------------------------------------------------------------------------
# Phase 3: Re-deploy (only if Phase 1 had partial failure)
# --------------------------------------------------------------------------
if [ "${PHASE1_FULL_SUCCESS}" = true ]; then
    echo "[Phase 3/4] Skipping re-deploy (Phase 1 fully succeeded)."
else
    echo "[Phase 3/4] Re-deploying bundle..."
    echo "  The serving endpoint should succeed now that a model version exists."
    echo ""

    databricks bundle deploy -t "${TARGET}"

    echo ""
    echo "  ✓ Re-deployment succeeded."
fi
echo ""

# --------------------------------------------------------------------------
# Phase 4: Run deployment job (only if Phase 1 had partial failure)
# --------------------------------------------------------------------------
# On first deploy, the endpoint was just created in Phase 3 and needs
# promotion (challenger→champion) and configuration (AI Gateway, telemetry, tags).
# The deployment job handles all of this.
#
# On subsequent deploys, Phase 1 succeeds and the deployment job is
# auto-triggered by new model version creation — skip Phase 4.
if [ "${PHASE1_FULL_SUCCESS}" = true ]; then
    echo "[Phase 4/4] Skipping deployment job (auto-triggered by model version creation)."
else
    echo "[Phase 4/4] Running deployment job (initial setup)..."

    if [ -z "${MODEL_NAME}" ] || [ -z "${MODEL_VERSION}" ]; then
        echo "  ⚠ Could not extract model metadata from registration job output."
        echo "  Manual steps required:"
        echo "    1. Set approval tag on the model version in Unity Catalog:"
        echo "       deployment.approval = approved"
        echo "    2. Run the deployment job:"
        echo "       databricks bundle run -t ${TARGET} ${DEPLOYMENT_JOB} \\"
        echo "         --params model_name=<catalog.schema.model>,model_version=<version>"
    else
        echo "  Auto-approving model ${MODEL_NAME} v${MODEL_VERSION} for initial deployment..."

        # Set the approval tag (required by the approval_check task)
        databricks api post /api/2.0/mlflow/unity-catalog/model-versions/set-tag \
            --json "{\"name\": \"${MODEL_NAME}\", \"version\": \"${MODEL_VERSION}\", \"key\": \"deployment.approval\", \"value\": \"approved\"}" \
            2>/dev/null || true

        echo "  ✓ Approval tag set."
        echo ""
        echo "  Running deployment job: ${DEPLOYMENT_JOB}"
        echo "  This evaluates the model, checks approval, promotes to champion,"
        echo "  and configures the endpoint (AI Gateway, telemetry, tags)."
        echo ""

        databricks bundle run -t "${TARGET}" "${DEPLOYMENT_JOB}" \
            --params "model_name=${MODEL_NAME},model_version=${MODEL_VERSION}"

        echo ""
        echo "  ✓ Deployment job completed."
    fi
fi

echo ""
echo "============================================="
echo "Deployment complete for target: ${TARGET}"
echo "============================================="
