# Secret Scope Setup Guide

## Overview
The `fhir_zerobus_credentials` secret scope stores sensitive credentials for the FHIR Zerobus application, including Zerobus client credentials and configuration values.

## Secret Scope Configuration

**Scope Name**: `fhir_zerobus_credentials` (configurable via `${var.secret_scope_name}`)  
**Backend Type**: Databricks-managed  
**Protection**: `prevent_destroy: true` (prevents accidental deletion)

## Permissions by Target

### Dev
- **MANAGE**: `matthew.giglia@databricks.com`, `${var.run_as_user}`

### HIMSS2026, Prod, Free Edition
- **MANAGE**: `matthew.giglia@databricks.com`, higher level service principal
- **READ**: Zerobus service principal (for app authentication)

## Required Secrets

The following secrets must be stored in the scope:

| Secret Key | Description | Used By |
|------------|-------------|---------|
| `client_id` | Zerobus OAuth2 M2M client ID | App authentication |
| `client_secret` | Zerobus OAuth2 M2M client secret | App authentication |
| `zerobus_endpoint` | Zerobus server endpoint URL | App configuration |
| `workspace_url` | Databricks workspace URL | App configuration |
| `fhir_bundle_table_name` | Target Unity Catalog table name | App configuration |

## Deployment Steps

### 1. Deploy the Secret Scope

The secret scope is automatically created when you deploy the bundle:

```bash
# Deploy to himss2026 target
databricks bundle deploy -t himss2026

# Or to other targets
databricks bundle deploy -t dev
databricks bundle deploy -t prod
```

This creates the secret scope with the configured permissions.

### 2. Add Secrets to the Scope

After deployment, populate the secrets using the Databricks CLI:

```bash
# Set the target environment
export SCOPE_NAME=fhir_zerobus_credentials

# Add Zerobus client credentials
databricks secrets put-secret \
  --scope ${SCOPE_NAME} \
  --key client_id \
  --string-value "<your-zerobus-client-id>"

databricks secrets put-secret \
  --scope ${SCOPE_NAME} \
  --key client_secret \
  --string-value "<your-zerobus-client-secret>"

# Add configuration values
databricks secrets put-secret \
  --scope ${SCOPE_NAME} \
  --key zerobus_endpoint \
  --string-value "7474651703425732.zerobus.us-east-1.cloud.databricks.com"

databricks secrets put-secret \
  --scope ${SCOPE_NAME} \
  --key workspace_url \
  --string-value "https://fe-sandbox-himss2026.cloud.databricks.com"

databricks secrets put-secret \
  --scope ${SCOPE_NAME} \
  --key fhir_bundle_table_name \
  --string-value "himss.redox.fhir_bundle_zerobus"
```

### 3. Verify Secrets

List all secrets in the scope (values are hidden):

```bash
databricks secrets list-secrets --scope ${SCOPE_NAME}
```

Expected output:
```
Key                      Last Updated
client_id                2024-03-04T08:00:00.000Z
client_secret            2024-03-04T08:00:00.000Z
zerobus_endpoint         2024-03-04T08:00:00.000Z
workspace_url            2024-03-04T08:00:00.000Z
fhir_bundle_table_name   2024-03-04T08:00:00.000Z
```

## How Secrets Are Used

### In Databricks Apps (app.yaml)
The app configuration automatically maps secrets to environment variables:

```yaml
env:
  - name: ZEROBUS_CLIENT_ID
    valueFrom: zerobus_client_id
  - name: ZEROBUS_CLIENT_SECRET
    valueFrom: zerobus_client_secret
  - name: ZEROBUS_SERVER_ENDPOINT
    valueFrom: zerobus_endpoint
  - name: WORKSPACE_URL
    valueFrom: workspace_url
  - name: FHIR_BUNDLE_TABLE_NAME
    valueFrom: fhir_bundle_table_name
```

These environment variables are read by `config.py`:

```python
import os

ZEROBUS_SERVER_ENDPOINT = os.getenv("ZEROBUS_SERVER_ENDPOINT")
CLIENT_ID = os.getenv("ZEROBUS_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZEROBUS_CLIENT_SECRET")
WORKSPACE_URL = os.getenv("WORKSPACE_URL")
FHIR_BUNDLE_TABLE_NAME = os.getenv("FHIR_BUNDLE_TABLE_NAME")
```

### In Notebooks (Python)
```python
# Get secrets using dbutils
client_id = dbutils.secrets.get(scope="fhir_zerobus_credentials", key="client_id")
client_secret = dbutils.secrets.get(scope="fhir_zerobus_credentials", key="client_secret")
```

### In Jobs
Configure secrets as environment variables in job definitions:

```yaml
tasks:
  - task_key: my_task
    spark_env_vars:
      CLIENT_ID: "{{secrets/fhir_zerobus_credentials/client_id}}"
      CLIENT_SECRET: "{{secrets/fhir_zerobus_credentials/client_secret}}"
```

## Managing Secrets

### Update a Secret
```bash
databricks secrets put-secret \
  --scope fhir_zerobus_credentials \
  --key client_secret \
  --string-value "<new-secret-value>"
```

After updating secrets, restart the app:
```bash
databricks bundle deploy -t himss2026
```

### Delete a Secret
```bash
databricks secrets delete-secret \
  --scope fhir_zerobus_credentials \
  --key client_secret
```

**Note**: The scope itself is protected by `prevent_destroy: true` in the bundle configuration.

### List All Scopes
```bash
databricks secrets list-scopes
```

## Permissions Management

### Grant Additional Access
```bash
# Grant READ access to a service principal
databricks secrets put-acl \
  --scope fhir_zerobus_credentials \
  --principal <service-principal-app-id> \
  --permission READ

# Grant MANAGE access to a user
databricks secrets put-acl \
  --scope fhir_zerobus_credentials \
  --principal user@example.com \
  --permission MANAGE
```

### List ACLs
```bash
databricks secrets list-acls --scope fhir_zerobus_credentials
```

## Security Best Practices

1. **Never commit secrets to git** - Use the secret scope exclusively
2. **Use READ permission** for application service principals
3. **Use MANAGE permission** for administrators only
4. **Rotate secrets regularly** - Update client credentials periodically
5. **Audit secret access** - Monitor who accesses sensitive credentials
6. **Use service principals** for production deployments, not user credentials
7. **Environment separation** - Use different secret values per target (dev/prod)

## Troubleshooting

### Secret Scope Not Found
**Issue**: `Error: Secret scope 'fhir_zerobus_credentials' does not exist`

**Solution**: Deploy the bundle first to create the scope:
```bash
databricks bundle deploy -t himss2026
```

### Permission Denied
**Issue**: `Error: Principal does not have MANAGE permission`

**Solution**: Ensure you have MANAGE permission or ask an administrator:
```bash
databricks secrets put-acl \
  --scope fhir_zerobus_credentials \
  --principal matthew.giglia@databricks.com \
  --permission MANAGE
```

### App Can't Access Secrets
**Issue**: App throws authentication errors

**Solution**: 
1. Verify service principal has READ access to the scope:
   ```bash
   databricks secrets list-acls --scope fhir_zerobus_credentials
   ```
2. Ensure secrets are populated with correct values:
   ```bash
   databricks secrets list-secrets --scope fhir_zerobus_credentials
   ```
3. Check app logs for specific error messages:
   ```bash
   databricks apps logs fhir-zerobus-ingest-himss2026
   ```
4. Verify `resources/zerobus_app.app.yml` references correct secret keys

## Environment-Specific Secrets

For different environments, you can:

**Option 1**: Use different scope names per target (modify `variables.secret_scope_name` in databricks.yml)
```yaml
targets:
  dev:
    variables:
      secret_scope_name: fhir_zerobus_credentials_dev
  prod:
    variables:
      secret_scope_name: fhir_zerobus_credentials_prod
```

**Option 2**: Use same scope with environment-specific values
```bash
# Deploy to dev and set dev values
databricks bundle deploy -t dev
databricks secrets put-secret --scope fhir_zerobus_credentials --key fhir_bundle_table_name --string-value "mkgs_dev.redox.fhir_bundle_zerobus"

# Deploy to prod and set prod values
databricks bundle deploy -t prod
databricks secrets put-secret --scope fhir_zerobus_credentials --key fhir_bundle_table_name --string-value "mkgs.redox.fhir_bundle_zerobus"
```

## Additional Resources

- [Databricks Secrets Documentation](https://docs.databricks.com/security/secrets/)
- [Secret Scopes in Asset Bundles](https://docs.databricks.com/dev-tools/bundles/resources.html#secret-scopes)
- [Databricks CLI Secrets Commands](https://docs.databricks.com/dev-tools/cli/secrets-cli.html)
- [Databricks Apps Configuration](https://docs.databricks.com/en/dev-tools/databricks-apps/)

## Support

For secret management issues:
- Primary developer: matthew.giglia@databricks.com
- Project: Redox Zerobus
- Business unit: Healthcare and Life Sciences
