# FHIR Bundle Zerobus App - Deployment Guide

## Overview
This Databricks Asset Bundle deploys a FastAPI application that ingests FHIR bundles via REST API and streams them to Unity Catalog using the Zerobus SDK.

## Bundle Structure
```
fhir_zerobus/
├── databricks.yml                          # Main bundle configuration
├── resources/
│   ├── fhir_bundle_app.app.yml            # App resource definition
│   ├── fhir_zerobus_etl.pipeline.yml      # DLT pipeline (if applicable)
│   └── sample_job.job.yml                 # Sample job (if applicable)
└── src/
    └── zerobus_app/
        ├── app.py                          # FastAPI application
        ├── config.py                       # Configuration
        ├── requirements.txt                # Python dependencies
        └── __init__.py
```

## Prerequisites

1. **Databricks CLI** installed and authenticated:
   ```bash
   databricks auth login --host https://fe-sandbox-himss2026.cloud.databricks.com
   ```

2. **Target table exists** in Unity Catalog:
   - Table: `himss.redox.fhir_bundle_zerobus`
   - Schema includes: `bundle_uuid`, `fhir` (VARIANT), `source_system`, `event_timestamp`, `ingest_datetime`

3. **Service Principal** with appropriate permissions:
   - Read/write access to target table
   - Zerobus endpoint permissions

## Deployment Targets

The bundle supports four deployment targets:

 Target | Workspace | Catalog | Schema | Purpose |
--------|-----------|---------|--------|---------|
 **dev** | fe-vm-mkgs-databricks-demos | mkgs_dev | redox | Development |
 **himss2026** | fe-sandbox-himss2026 | himss | redox | HIMSS demo |
 **prod** | fe-vm-mkgs-databricks-demos | mkgs | redox | Production |
 **free_edition** | dbc-e5684c0a-20fa | mkgs | redox | Free tier |

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
- Configure permissions based on the target
- Generate a service principal for the app (auto-injected as `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`)

### 3. Start the App
After deployment, start the app using the CLI:

```bash
# Start the app
databricks apps deploy fhir-bundle-ingest-app-himss2026 --source-code-path ./src/zerobus_app

# Check app status
databricks apps get fhir-bundle-ingest-app-himss2026

# View app logs
databricks apps logs fhir-bundle-ingest-app-himss2026
```

### 4. Get the App URL
Retrieve the app URL from the bundle summary:

```bash
databricks bundle summary -t himss2026
```

Look for the `Apps` section to get the full app URL.

## Configuration

### Environment Variables
The app automatically receives these from Databricks Apps:
- `DATABRICKS_CLIENT_ID` - Service principal client ID (auto-injected)
- `DATABRICKS_CLIENT_SECRET` - Service principal secret (auto-injected)

Default values in `config.py`:
- `ZEROBUS_SERVER_ENDPOINT`: `7474651703425732.zerobus.us-east-1.cloud.databricks.com`
- `WORKSPACE_URL`: `https://fe-sandbox-himss2026.cloud.databricks.com/`
- `TABLE_NAME`: `himss.redox.fhir_bundle_zerobus`

To override these, set environment variables when deploying or configure them in the app settings.

### Table Configuration
The app writes to different tables per target via bundle variables:
- **dev**: `mkgs_dev.redox.fhir_bundle_zerobus`
- **himss2026**: `himss.redox.fhir_bundle_zerobus`
- **prod**: `mkgs.redox.fhir_bundle_zerobus`

## Testing the App

Once deployed and started, test the endpoint:

```bash
# Get the app URL from bundle summary
APP_URL=$(databricks bundle summary -t himss2026 | grep -A 1 "fhir_bundle_ingest_app" | grep URL | awk '{print $2}')

# Send a test FHIR bundle
curl -X POST "${APP_URL}/ingest/fhir-bundle" \
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
  "bundle_uuid": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Verify Data Ingestion

Query the target table to verify data was written:

```sql
SELECT 
  bundle_uuid,
  ingest_datetime,
  source_system,
  event_timestamp,
  fhir:resourceType::string as resource_type
FROM himss.redox.fhir_bundle_zerobus
ORDER BY ingest_datetime DESC
LIMIT 10;
```

## Permissions

### App Permissions
Configured in `resources/fhir_bundle_app.app.yml`:
- **CAN_MANAGE**: matthew.giglia@databricks.com
- **CAN_USE**: users group (himss2026 target)

### Service Principal Permissions
The app's service principal needs:
1. **Zerobus permissions**: Write to the zerobus endpoint
2. **Unity Catalog permissions**: 
   - `USE CATALOG` on target catalog
   - `USE SCHEMA` on target schema
   - `SELECT` and `MODIFY` on target table

Grant these manually or via bundle:
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

# 3. Redeploy the app to apply changes
databricks apps deploy fhir-bundle-ingest-app-himss2026 --source-code-path ./src/zerobus_app
```

## Monitoring

### View App Logs
```bash
databricks apps logs fhir-bundle-ingest-app-himss2026
```

### Check App Status
```bash
databricks apps get fhir-bundle-ingest-app-himss2026
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

-- Recent errors (if any)
SELECT *
FROM himss.redox.fhir_bundle_zerobus
WHERE fhir:issue IS NOT NULL
ORDER BY ingest_datetime DESC;
```

## Troubleshooting

### Common Issues

1. **App won't start**
   - Check logs: `databricks apps logs fhir-bundle-ingest-app-himss2026`
   - Verify service principal has correct permissions
   - Confirm zerobus endpoint is accessible

2. **Authentication failures**
   - Verify `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` are injected
   - Check service principal exists and is active
   - Confirm service principal has zerobus permissions

3. **Table write failures**
   - Verify table exists: `DESCRIBE TABLE himss.redox.fhir_bundle_zerobus`
   - Check service principal has `MODIFY` permission on table
   - Verify table schema matches app record structure

4. **Bundle validation errors**
   - Review `databricks.yml` syntax
   - Check resource file inclusion paths
   - Verify variable references are correct

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Databricks Apps Documentation](https://docs.databricks.com/en/dev-tools/databricks-apps/)
- [Zerobus SDK Documentation](https://docs.databricks.com/ingestion/zerobus/)
- [Unity Catalog Permissions](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/)

## Support

For issues or questions:
- Primary developer: matthew.giglia@databricks.com
- Project: Open Epic Smart on FHIR
- Business unit: Healthcare and Life Sciences
