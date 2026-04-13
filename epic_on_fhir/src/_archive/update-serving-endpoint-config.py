# Databricks notebook source
# DBTITLE 1,Update Serving Endpoint Configuration
# MAGIC %md
# MAGIC # Update Serving Endpoint Configuration
# MAGIC
# MAGIC This notebook updates the serving endpoint to serve the champion model version, configures AI Gateway, telemetry, and tags.
# MAGIC
# MAGIC Use this when:
# MAGIC - The endpoint already exists and you need to update these settings
# MAGIC - Deploying with `updateAIGatewayOnly=true` to skip model registration
# MAGIC - A new champion version was promoted and the endpoint needs to serve it

# COMMAND ----------

# DBTITLE 1,Install latest Databricks SDK
# MAGIC %pip install --upgrade databricks-sdk mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Parameters
# Clear persisted widget values so defaults match current target
dbutils.widgets.removeAll()

dbutils.widgets.text("endpoint_name", "sandbox_epic_on_fhir_requests", "Endpoint Name")
dbutils.widgets.text("catalog", "hls_fde", "Catalog")
dbutils.widgets.text("schema", "sandbox_open_epic_smart_on_fhir", "Schema")
dbutils.widgets.text("registered_model_name", "hls_fde.sandbox_open_epic_smart_on_fhir.sandbox_epic_on_fhir_requests", "Registered Model Name (catalog.schema.model)")
dbutils.widgets.text("component", "epic-on-fhir", "Tag: Component")
dbutils.widgets.text("environment", "hls_fde_sandbox_prod", "Tag: Environment")
dbutils.widgets.text("project", "Open Epic Smart on FHIR", "Tag: Project")
dbutils.widgets.text("owner", "matthew.giglia@databricks.com", "Tag: Owner")

endpoint_name = dbutils.widgets.get("endpoint_name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
registered_model_name = dbutils.widgets.get("registered_model_name")
tag_component = dbutils.widgets.get("component")
tag_environment = dbutils.widgets.get("environment")
tag_project = dbutils.widgets.get("project")
tag_owner = dbutils.widgets.get("owner")

# COMMAND ----------

# DBTITLE 1,Initialize Databricks SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointTag,
    ServedEntityInput,
    TrafficConfig,
    Route,
)

# AI Gateway classes — names vary across SDK versions
try:
    from databricks.sdk.service.serving import (
        AiGatewayGuardrails, AiGatewayInferenceTableConfig,
        AiGatewayRateLimit, AiGatewayRateLimitKey, AiGatewayUsageTrackingConfig,
    )
except ImportError:
    # Older SDK: AI Gateway may be under different names or unavailable
    AiGatewayGuardrails = AiGatewayInferenceTableConfig = None
    AiGatewayRateLimit = AiGatewayRateLimitKey = AiGatewayUsageTrackingConfig = None
    print("\u26a0 AI Gateway SDK classes not available \u2014 will use REST API fallback")

try:
    from databricks.sdk.service.serving import AiGatewayRateLimitRenewalPeriod as RateLimitRenewal
except ImportError:
    try:
        from databricks.sdk.service.serving import AiGatewayRateLimitRenewal as RateLimitRenewal
    except ImportError:
        RateLimitRenewal = None

w = WorkspaceClient()

import importlib.metadata
print(f"Databricks SDK version: {importlib.metadata.version('databricks-sdk')}")
print(f"Updating endpoint: {endpoint_name}")
print(f"Catalog: {catalog}, Schema: {schema}")

# COMMAND ----------

# DBTITLE 1,Check endpoint exists
# Guard: exit gracefully if the endpoint doesn't exist yet.
# On first deployment, Phase 1 (bundle deploy) may fail to create the serving
# endpoint because no model version exists. This notebook runs in Phase 2 as
# part of the job — if we fail here, Phase 3 (re-deploy) never runs.
# Exiting cleanly lets the job succeed so Phase 3 can create the endpoint
# with full config from the serving YAML.
from databricks.sdk.errors import NotFound, ResourceDoesNotExist

try:
    endpoint = w.serving_endpoints.get(endpoint_name)
    print(f"\u2713 Endpoint '{endpoint_name}' exists \u2014 proceeding with configuration updates")
except (NotFound, ResourceDoesNotExist):
    msg = (
        f"\u26a0 Endpoint '{endpoint_name}' does not exist yet. Skipping configuration updates.\n"
        f"  On first deployment, the endpoint will be created by 'databricks bundle deploy' (Phase 3)\n"
        f"  with full configuration from the serving YAML. This notebook is only needed for\n"
        f"  subsequent updates when the bundle deploy API cannot modify AI Gateway/telemetry."
    )
    print(msg)
    dbutils.notebook.exit("SKIPPED: endpoint does not exist yet")

# COMMAND ----------

# DBTITLE 1,Resolve champion model version
# Resolve the champion alias to a specific model version.
# This ensures the endpoint always serves the current champion,
# even when running with updateAIGatewayOnly=true.
import mlflow

champion_version = None
if registered_model_name:
    try:
        client = mlflow.MlflowClient()
        champion = client.get_model_version_by_alias(registered_model_name, "champion")
        champion_version = champion.version
        print(f"✓ Champion alias resolved: {registered_model_name} version {champion_version}")
    except Exception as e:
        print(f"⚠ Could not resolve champion alias: {e}")
        print("  Served entity version will not be updated.")
else:
    print("⚠ No registered_model_name provided — skipping served entity update")

# COMMAND ----------

# DBTITLE 1,Update AI Gateway Configuration
# Configure AI Gateway with inference tables, usage tracking, and rate limits
print("Updating AI Gateway configuration...")

if AiGatewayInferenceTableConfig is not None:
    # SDK path (0.67+)
    w.serving_endpoints.put_ai_gateway(
        name=endpoint_name,
        inference_table_config=AiGatewayInferenceTableConfig(
            catalog_name=catalog,
            schema_name=schema,
            table_name_prefix=f"{endpoint_name.replace('-', '_')}_payload",
            enabled=True,
        ),
        usage_tracking_config=AiGatewayUsageTrackingConfig(enabled=True),
        rate_limits=[
            AiGatewayRateLimit(
                key=AiGatewayRateLimitKey.USER,
                renewal_period=RateLimitRenewal.MINUTE,
                calls=100,
            )
        ],
    )
else:
    # REST API fallback for older SDK versions
    ep_prefix = endpoint_name.replace("-", "_")
    w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{endpoint_name}/ai-gateway", body={
        "inference_table_config": {
            "catalog_name": catalog, "schema_name": schema,
            "table_name_prefix": f"{ep_prefix}_payload", "enabled": True,
        },
        "usage_tracking_config": {"enabled": True},
        "rate_limits": [{"key": "user", "renewal_period": "minute", "calls": 100}],
    })

print("✓ AI Gateway configuration updated")

# COMMAND ----------

# DBTITLE 1,Update Telemetry Configuration
# Update served entity to champion version AND configure telemetry (Preview).
# Uses a single REST API call so telemetry_config can be included alongside
# served_entities (the API rejects telemetry-only updates without entities).
import time

def wait_for_endpoint_ready(w, endpoint_name, timeout_secs=300, poll_interval=5):
    """Wait until endpoint config update is no longer in progress."""
    waited = False
    for i in range(0, timeout_secs, poll_interval):
        ep_state = w.serving_endpoints.get(endpoint_name)
        if str(ep_state.state.config_update) != "EndpointStateConfigUpdate.IN_PROGRESS":
            return ep_state
        if not waited:
            print("Waiting for previous config update to complete...")
            waited = True
        time.sleep(poll_interval)
    print(f"\u26a0 Config update still in progress after {timeout_secs}s \u2014 proceeding anyway")
    return w.serving_endpoints.get(endpoint_name)

# Wait for any in-progress config update (e.g. from AI Gateway cell) to complete
endpoint = wait_for_endpoint_ready(w, endpoint_name)

table_prefix = endpoint_name.replace("-", "_")

if champion_version and registered_model_name:
    # Read current config to preserve environment_vars (secret references)
    current_entities = endpoint.config.served_entities if endpoint.config else []
    current_env_vars = {}
    if current_entities:
        current_env_vars = current_entities[0].environment_vars or {}
        print(f"  Preserving {len(current_env_vars)} environment variables from current config")

    served_entity_name = f"{endpoint_name}-{champion_version}"

    served_entity_dict = {
        "entity_name": registered_model_name,
        "entity_version": str(champion_version),
        "name": served_entity_name,
        "workload_size": "Small",
        "scale_to_zero_enabled": True,
        "environment_vars": current_env_vars,
    }
    traffic_config_dict = {
        "routes": [{"served_model_name": served_entity_name, "traffic_percentage": 100}]
    }
    telemetry_config_dict = {
        "otel_traces_enabled": True,
        "otel_logs_enabled": True,
        "otel_metrics_enabled": True,
        "otel_traces_table_name": f"{catalog}.{schema}.{table_prefix}_traces",
        "otel_logs_table_name": f"{catalog}.{schema}.{table_prefix}_logs",
        "otel_metrics_table_name": f"{catalog}.{schema}.{table_prefix}_metrics",
    }

    # Try combined update: served entity + telemetry in one call
    try:
        print(f"Updating served entity to champion v{champion_version} with telemetry...")
        w.api_client.do(
            "PUT",
            f"/api/2.0/serving-endpoints/{endpoint_name}/config",
            body={
                "served_entities": [served_entity_dict],
                "traffic_config": traffic_config_dict,
                "telemetry_config": telemetry_config_dict,
            },
        )
        print(f"\u2713 Served entity updated to {registered_model_name} v{champion_version}")
        print("\u2713 Telemetry configuration updated (Preview)")
    except Exception as e:
        # Telemetry may not be supported yet \u2014 fall back to SDK for served entity only.
        # The combined call may have partially triggered a config update, so wait first.
        print(f"\u26a0 Combined update failed: {e}")
        print("  Falling back to SDK update (without telemetry)...")
        wait_for_endpoint_ready(w, endpoint_name)
        served_entity = ServedEntityInput(
            entity_name=registered_model_name,
            entity_version=str(champion_version),
            name=served_entity_name,
            workload_size="Small",
            scale_to_zero_enabled=True,
            environment_vars=current_env_vars,
        )
        traffic_config = TrafficConfig(
            routes=[Route(served_model_name=served_entity_name, traffic_percentage=100)]
        )
        w.serving_endpoints.update_config(
            name=endpoint_name,
            served_entities=[served_entity],
            traffic_config=traffic_config,
        )
        print(f"\u2713 Served entity updated to {registered_model_name} v{champion_version}")
        print("  Telemetry will be set by bundle deploy if supported.")
else:
    print("\u26a0 No champion version available \u2014 served entity unchanged")

# COMMAND ----------

# DBTITLE 1,Update Tags
# Update endpoint tags
tags = [
    EndpointTag(key="component", value=tag_component),
    EndpointTag(key="environment", value=tag_environment),
    EndpointTag(key="project", value=tag_project),
    EndpointTag(key="owner", value=tag_owner)
]

print("Updating endpoint tags...")
w.serving_endpoints.patch(
    name=endpoint_name,
    add_tags=tags
)
print("✓ Endpoint tags updated")

# COMMAND ----------

# DBTITLE 1,Verification
# Verify the updates
endpoint = w.serving_endpoints.get(endpoint_name)

print("\n=== Endpoint Configuration ===")
print(f"Name: {endpoint.name}")
print(f"State: {endpoint.state.config_update}")

if endpoint.config and endpoint.config.served_entities:
    for se in endpoint.config.served_entities:
        print(f"\n\u2713 Served entity: {se.entity_name} v{se.entity_version}")
        print(f"  - Name: {se.name}")
        print(f"  - Workload: {se.workload_size}, Scale to zero: {se.scale_to_zero_enabled}")

if endpoint.ai_gateway:
    print("\n\u2713 AI Gateway configured")
    if endpoint.ai_gateway.inference_table_config:
        itc = endpoint.ai_gateway.inference_table_config
        print(f"  - Inference tables: {itc.catalog_name}.{itc.schema_name}.{itc.table_name_prefix}_*")
    if endpoint.ai_gateway.usage_tracking_config:
        print(f"  - Usage tracking: {endpoint.ai_gateway.usage_tracking_config.enabled}")
    if endpoint.ai_gateway.rate_limits:
        for rl in endpoint.ai_gateway.rate_limits:
            print(f"  - Rate limit: {rl.calls} calls per {rl.renewal_period} per {rl.key}")

# Telemetry is a Preview feature \u2014 SDK doesn't expose it, so check via REST API
try:
    resp = w.api_client.do("GET", f"/api/2.0/serving-endpoints/{endpoint_name}")
    tc = resp.get("config", {}).get("telemetry_config")
    if tc:
        print("\n\u2713 Telemetry configured (Preview)")
        if tc.get("otel_traces_table_name"):
            print(f"  - Traces: {tc['otel_traces_table_name']}")
        if tc.get("otel_logs_table_name"):
            print(f"  - Logs: {tc['otel_logs_table_name']}")
        if tc.get("otel_metrics_table_name"):
            print(f"  - Metrics: {tc['otel_metrics_table_name']}")
    else:
        print("\n\u26a0 Telemetry not configured (Preview feature may not be enabled)")
except Exception as e:
    print(f"\n\u26a0 Could not verify telemetry: {e}")

if endpoint.tags:
    print("\n\u2713 Tags:")
    for tag in endpoint.tags:
        print(f"  - {tag.key}: {tag.value}")
