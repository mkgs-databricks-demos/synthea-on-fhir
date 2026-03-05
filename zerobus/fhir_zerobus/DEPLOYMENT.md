# FHIR Bundle Zerobus App - Deployment Guide

## Overview
This Databricks Asset Bundle deploys a FastAPI application that ingests FHIR bundles via REST API and streams them to Unity Catalog using the Zerobus SDK.

## What is Zerobus?

**Databricks Zerobus** is a high-performance, low-latency streaming ingestion service that enables real-time data writes directly to Unity Catalog Delta tables. Unlike traditional streaming solutions (Kafka, Kinesis), Zerobus provides:

* **Microsecond-level latency** - Near-instant data availability
* **No infrastructure overhead** - No streaming clusters to manage
* **Direct Delta writes** - Data lands immediately in Unity Catalog
* **Schema validation** - Automatic validation against target table
* **Built-in recovery** - Automatic retry and error handling

This application uses Zerobus to stream FHIR bundles in JSON format to a Unity Catalog table with a VARIANT column, enabling flexible schema-on-read analytics.

**Learn more:** [Zerobus Overview Documentation](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)

## Bundle Structure
```
fhir_zerobus/
├── databricks.yml                          # Main bundle configuration
├── resources/
│   ├── zerobus_app.app.yml                # App resource definition
│   ├── zerobus.secret_scope.yml           # Secret scope configuration
│   ├── fhir_zerobus_etl.pipeline.yml      # DLT pipeline (optional)
│   └── fhir_bundle_table_setup.job.yml    # Table setup job
└── src/
    └── zerobus_app/
        ├── app.py                          # FastAPI application
        ├── config.py                       # Configuration
        ├── app.yaml                        # App runtime config
        ├── requirements.txt                # Python dependencies
        └── static/
            └── index.html                  # React frontend dashboard
```

## Prerequisites

1. **Databricks CLI** installed and authenticated:
   ```bash
   databricks auth login --host https://fe-sandbox-himss2026.cloud.databricks.com
   ```

2. **Target table exists** in Unity Catalog:
   - Table: `himss.redox.fhir_bundle_zerobus`
   - Schema includes: `bundle_uuid`, `fhir` (VARIANT), `source_system`, `event_timestamp`, `ingest_datetime`

3. **Zerobus credentials** stored in secret scope:
   - Scope name: `fhir_zerobus_credentials` (configurable via `${var.secret_scope_name}`)
   - Required secrets: `client_id`, `client_secret`, `zerobus_endpoint`, `workspace_url`, `fhir_bundle_table_name`
   - See [SECRET_SCOPE_SETUP.md](SECRET_SCOPE_SETUP.md) for instructions

4. **Service Principal** (optional for production):
   - Automatically created by Databricks Apps if not specified
   - Needs read access to secret scope
   - Needs write access to target Unity Catalog table

## Deployment Targets

The bundle supports four deployment targets:

| Target | Workspace | Catalog | Schema | Purpose |
|--------|-----------|---------|--------|---------|
| **dev** | fe-vm-mkgs-databricks-demos | mkgs_dev | redox | Development |
| **himss2026** | fe-sandbox-himss2026 | himss | redox | HIMSS demo |
| **prod** | fe-vm-mkgs-databricks-demos | mkgs | redox | Production |
| **free_edition** | dbc-e5684c0a-20fa | mkgs | redox | Free tier |

## Deployment Steps

### 1. Validate the Bundle
Validate the bundle configuration before deployment:

```bash
# For himss2026 target (current workspace)
databricks bundle validate -t himss2026

# For other targets
databricks bundle validate -t dev
databricks bundle validate -t prod
```

### 2. Deploy the Bundle
Deploy the app to the workspace:

```bash
# Deploy to himss2026 target
databricks bundle deploy -t himss2026

# Or to other targets
databricks bundle deploy -t dev
databricks bundle deploy -t prod
```

This command will:
- Create the app resource in the workspace
- Upload the source code from `src/zerobus_app/`
- Configure environment variables from secret scope
- Set up permissions based on the target
- Create a service principal for the app (if needed)

### 3. Start the App
The app starts automatically after deployment. To check status:

```bash
# Check app status
databricks bundle run fhir_zerobus_ingest_app -t himss2026

# View app logs
databricks apps logs fhir-zerobus-ingest-himss2026
```

### 4. Get the App URL
Retrieve the app URL from the bundle summary or workspace UI:

```bash
# Get bundle summary
databricks bundle summary -t himss2026
```

Look for the `Apps` section to get the full app URL.

## Configuration

### Environment Variables
The app automatically receives these from the secret scope (configured in `resources/zerobus_app.app.yml`):
- `ZEROBUS_CLIENT_ID` - Zerobus OAuth2 M2M client ID
- `ZEROBUS_CLIENT_SECRET` - Zerobus OAuth2 M2M client secret
- `ZEROBUS_SERVER_ENDPOINT` - Zerobus server endpoint URL
- `WORKSPACE_URL` - Databricks workspace URL
- `FHIR_BUNDLE_TABLE_NAME` - Target Unity Catalog table name

### Table Configuration
The app writes to different tables per target via bundle variables:
- **dev**: `mkgs_dev.redox.fhir_bundle_zerobus`
- **himss2026**: `himss.redox.fhir_bundle_zerobus`
- **prod**: `mkgs.redox.fhir_bundle_zerobus`

## Testing the App

Once deployed and started, test the endpoint:

### Using the React Frontend
Navigate to the app URL in your browser to access the React dashboard:
- View real-time health status
- Read API documentation
- Copy code examples with one click

### Using cURL
```bash
# Get your Databricks token
export DATABRICKS_TOKEN="<your-token>"

# Get the app URL
export APP_URL="https://<app-url>"

# Send a test FHIR bundle
curl -X POST "${APP_URL}/api/v1/ingest/fhir-bundle" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "resourceType": "Bundle",
    "type": "transaction",
    "entry": [
      {
        "resource": {
          "resourceType": "Patient",
          "id": "example",
          "name": [{"family": "Smith", "given": ["John"]}]
        }
      }
    ]
  }'
```

Expected response:
```json
{
  "status": "ok",
  "bundle_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2024-03-05T12:00:00Z"
}
```

### Using Python
```python
import requests
import json

token = "<your-databricks-token>"
app_url = "https://<app-url>"

bundle = {
    "resourceType": "Bundle",
    "type": "transaction",
    "entry": [{
        "resource": {
            "resourceType": "Patient",
            "id": "example",
            "name": [{"family": "Doe", "given": ["Jane"]}]
        }
    }]
}

response = requests.post(
    f"{app_url}/api/v1/ingest/fhir-bundle",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    },
    json=bundle
)

print(f"Status: {response.status_code}")
print(f"Response: {response.json()}")
```

## Verify Data Ingestion

Query the target table to verify data was written:

```sql
SELECT 
  bundle_uuid,
  ingest_datetime,
  source_system,
  event_timestamp,
  fhir:resourceType::string as resource_type,
  fhir
FROM himss.redox.fhir_bundle_zerobus
ORDER BY ingest_datetime DESC
LIMIT 10;
```

## Permissions

### App Permissions
Configured in `resources/zerobus_app.app.yml`:
- **CAN_MANAGE**: matthew.giglia@databricks.com
- **CAN_USE**: users group (all targets)

### Secret Scope Permissions
The app's service principal automatically gets READ permission on the secret scope.

### Unity Catalog Permissions
Grant these manually if using a custom service principal:
```sql
GRANT USAGE ON CATALOG himss TO `<service-principal-app-id>`;
GRANT USAGE ON SCHEMA himss.redox TO `<service-principal-app-id>`;
GRANT SELECT, MODIFY ON TABLE himss.redox.fhir_bundle_zerobus TO `<service-principal-app-id>`;
```

## Updating the App

To update the app after code changes:

```bash
# 1. Validate changes
databricks bundle validate -t himss2026

# 2. Deploy updated bundle
databricks bundle deploy -t himss2026

# The app will automatically restart with new code
```

## Monitoring

### View App Logs
```bash
databricks apps logs fhir-zerobus-ingest-himss2026 --follow
```

### Check Health Status

**For humans** - Visit the health page in your browser:
```
https://<app-url>/health
```

**For monitoring/scripts** - Use the JSON endpoint:
```bash
curl https://<app-url>/health/json
```

Response:
```json
{
  "status": "healthy",
  "zerobus_stream": "healthy",
  "timestamp": "2024-03-05T12:00:00Z"
}
```

### Monitor Table Ingestion
```sql
-- Record count over time
SELECT 
  DATE_TRUNC('hour', ingest_datetime) as hour,
  COUNT(*) as record_count
FROM himss.redox.fhir_bundle_zerobus
GROUP BY hour
ORDER BY hour DESC;

-- Resource type distribution
SELECT 
  fhir:resourceType::string as resource_type,
  COUNT(*) as count
FROM himss.redox.fhir_bundle_zerobus
GROUP BY resource_type
ORDER BY count DESC;
```

## Troubleshooting

### Common Issues

1. **App won't start**
   - Check logs: `databricks apps logs fhir-zerobus-ingest-himss2026`
   - Verify secrets exist in scope: `databricks secrets list-secrets --scope fhir_zerobus_credentials`
   - Confirm zerobus endpoint is accessible

2. **Authentication failures**
   - Verify secrets are populated with correct values
   - Check service principal has READ access to secret scope
   - Ensure Databricks token is valid when testing

3. **Table write failures**
   - Verify table exists: `DESCRIBE TABLE himss.redox.fhir_bundle_zerobus`
   - Check service principal has `MODIFY` permission on table
   - Verify table schema matches app record structure

4. **Bundle validation errors**
   - Review `databricks.yml` syntax
   - Check resource file inclusion paths match actual files
   - Verify variable references are correct

5. **React frontend not loading**
   - Verify `static/index.html` exists in `src/zerobus_app/`
   - Check app logs for static file mounting errors
   - Access `/docs` endpoint to verify app is running

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main dashboard (health status, docs, examples, Zerobus info) |
| `/health` | GET | Professional health status page (HTML) with auto-refresh |
| `/health/json` | GET | Health check JSON endpoint for monitoring/load balancers |
| `/api/v1/ingest/fhir-bundle` | POST | Ingest FHIR bundle (requires auth) |
| `/docs` | GET | Interactive Swagger API documentation |

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Databricks Apps Documentation](https://docs.databricks.com/en/dev-tools/databricks-apps/)
- [Zerobus SDK Documentation](https://docs.databricks.com/ingestion/zerobus/)
- [Unity Catalog Permissions](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)

## Support

For issues or questions:
- Primary developer: matthew.giglia@databricks.com
- Project: Redox Zerobus
- Business unit: Healthcare and Life Sciences
