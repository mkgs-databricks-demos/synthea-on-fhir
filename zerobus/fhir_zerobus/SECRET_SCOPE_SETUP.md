# Secret Scope Setup Guide

## Overview
The `redox_oauth_keys` secret scope stores sensitive credentials for the FHIR Zerobus application, including Zerobus client credentials and other OAuth keys.

## Secret Scope Configuration

**Scope Name**: `redox_oauth_keys` (configurable via `${var.secret_scope_name}`)
**Backend Type**: Databricks-managed
**Protection**: `prevent_destroy: true` (prevents accidental deletion)

## Permissions by Target

### Dev
- **MANAGE**: `matthew.giglia@databricks.com`, `${var.run_as_user}`

### HIMSS2026, Prod, Free Edition
- **MANAGE**: `matthew.giglia@databricks.com`, higher level service principal
- **READ**: Zerobus service principal (for app authentication)

## Required Secrets

The following secrets should be stored in the scope:

 Secret Key | Description | Used By |
------------|-------------|---------|
 `client_id` | Zerobus OAuth client ID | App authentication |
 `client_secret` | Zerobus OAuth client secret | App authentication |
 `zerobus_endpoint` | Zerobus server endpoint | App configuration |
 `workspace_url` | Databricks workspace URL | App configuration |

## Deployment Steps

### 1. Deploy the Secret Scope

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
export TARGET=himss2026
export SCOPE_NAME=redox_oauth_keys

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
```

### 3. Verify Secrets

List all secrets in the scope (values are hidden):

```bash
databricks secrets list-secrets --scope ${SCOPE_NAME}
```

Expected output:
```
Key                 Last Updated
client_id           2024-03-04T08:00:00.000Z
client_secret       2024-03-04T08:00:00.000Z
zerobus_endpoint    2024-03-04T08:00:00.000Z
workspace_url       2024-03-04T08:00:00.000Z
```

## Using Secrets in Code

### In Notebooks (Python)
```python
# Get secrets using dbutils
client_id = dbutils.secrets.get(scope="redox_oauth_keys", key="client_id")
client_secret = dbutils.secrets.get(scope="redox_oauth_keys", key="client_secret")
```

### In Jobs
Configure secrets as environment variables in job definitions:

```yaml
tasks:
  - task_key: my_task
    spark_env_vars:
      CLIENT_ID: "{{secrets/redox_oauth_keys/client_id}}"
      CLIENT_SECRET: "{{secrets/redox_oauth_keys/client_secret}}"
```

### In Databricks Apps
Update `config.py` to read from secrets:

```python
import os

# Try environment variables first, fallback to secrets
try:
    from databricks.sdk.runtime import dbutils
    CLIENT_ID = dbutils.secrets.get(scope="redox_oauth_keys", key="client_id")
    CLIENT_SECRET = dbutils.secrets.get(scope="redox_oauth_keys", key="client_secret")
except:
    # Fallback to environment variables (when running as Databricks App)
    CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
    CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")
```

## Managing Secrets

### Update a Secret
```bash
databricks secrets put-secret \
  --scope redox_oauth_keys \
  --key client_secret \
  --string-value "<new-secret-value>"
```

### Delete a Secret
```bash
databricks secrets delete-secret \
  --scope redox_oauth_keys \
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
  --scope redox_oauth_keys \
  --principal <service-principal-app-id> \
  --permission READ

# Grant MANAGE access to a user
databricks secrets put-acl \
  --scope redox_oauth_keys \
  --principal user@example.com \
  --permission MANAGE
```

### List ACLs
```bash
databricks secrets list-acls --scope redox_oauth_keys
```

## Security Best Practices

1. **Never commit secrets to git** - Use the secret scope exclusively
2. **Use READ permission** for application service principals
3. **Use MANAGE permission** for administrators only
4. **Rotate secrets regularly** - Update client credentials periodically
5. **Audit secret access** - Monitor who accesses sensitive credentials
6. **Use service principals** for production deployments, not user credentials

## Troubleshooting

### Secret Scope Not Found
**Issue**: `Error: Secret scope 'redox_oauth_keys' does not exist`

**Solution**: Deploy the bundle first to create the scope:
```bash
databricks bundle deploy -t himss2026
```

### Permission Denied
**Issue**: `Error: Principal does not have MANAGE permission`

**Solution**: Ensure you have MANAGE permission or ask an administrator:
```bash
databricks secrets put-acl \
  --scope redox_oauth_keys \
  --principal matthew.giglia@databricks.com \
  --permission MANAGE
```

### App Can't Access Secrets
**Issue**: App throws authentication errors

**Solution**: 
1. Verify service principal has READ access to the scope
2. Ensure secrets are populated with correct values
3. Check app configuration reads from correct scope/keys

## Environment-Specific Secrets

For different environments, you can:

**Option 1**: Use different scope names per target (modify `variables.secret_scope_name` in databricks.yml)
```yaml
targets:
  dev:
    variables:
      secret_scope_name: redox_oauth_keys_dev
  prod:
    variables:
      secret_scope_name: redox_oauth_keys_prod
```

**Option 2**: Use same scope with environment-specific key prefixes
```bash
# Dev secrets
databricks secrets put-secret --scope redox_oauth_keys --key dev_client_id --string-value "..."
databricks secrets put-secret --scope redox_oauth_keys --key dev_client_secret --string-value "..."

# Prod secrets
databricks secrets put-secret --scope redox_oauth_keys --key prod_client_id --string-value "..."
databricks secrets put-secret --scope redox_oauth_keys --key prod_client_secret --string-value "..."
```

## Additional Resources

- [Databricks Secrets Documentation](https://docs.databricks.com/security/secrets/)
- [Secret Scopes in Asset Bundles](https://docs.databricks.com/dev-tools/bundles/resources.html#secret-scopes)
- [Databricks CLI Secrets Commands](https://docs.databricks.com/dev-tools/cli/secrets-cli.html)

## Support

For secret management issues:
- Primary developer: matthew.giglia@databricks.com
- Project: Open Epic Smart on FHIR
