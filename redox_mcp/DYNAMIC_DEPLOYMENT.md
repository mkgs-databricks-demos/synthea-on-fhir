# Dynamic Resource Deployment with pyDABs

This bundle now supports dynamic resource deployment based on the target environment.

## How It Works

1. **deployment_config.json** - Defines which resources to deploy per target
2. **resources/deployment.yml.py** - Python file that conditionally generates resources
3. **databricks.yml** - Updated to include the dynamic deployment file

## Current Configuration

### Dev Target (default)
- `deploy_app: false` - App is NOT deployed (faster iteration)

### Free Edition Target  
- `deploy_app: false` - App is NOT deployed (potential limitations)

### Prod Target
- `deploy_app: true` - App IS deployed (full production environment)

### HIMSS2026 Target
- `deploy_app: true` - App IS deployed (demo environment)

## Usage

Deploy to different targets:

```bash
# Deploy to dev (app will NOT be deployed)
databricks bundle deploy -t dev

# Deploy to prod (app WILL be deployed)
databricks bundle deploy -t prod

# Deploy to himss2026 (app WILL be deployed)
databricks bundle deploy -t himss2026
```

## Adding More Conditional Resources

To add more conditional resources, edit `deployment_config.json`:

```json
{
  "dev": {
    "deploy_app": false,
    "deploy_pipeline": true,
    "deploy_job": false
  },
  "prod": {
    "deploy_app": true,
    "deploy_pipeline": true,
    "deploy_job": true
  }
}
```

Then update `resources/deployment.yml.py` to include the new resource logic.

## Validating Configuration

To validate the bundle configuration:

```bash
databricks bundle validate -t dev
databricks bundle validate -t prod
```

To see what will be deployed:

```bash
databricks bundle validate -t dev --output json | jq '.resources.apps'
databricks bundle validate -t prod --output json | jq '.resources.apps'
```

## Files Created

- `/deployment_config.json` - Configuration file
- `/resources/deployment.yml.py` - Dynamic resource generator
- `/databricks.yml` - Updated to use deployment.yml.py (line 9)

## Original App File

The original `resources/redox_mcp_serving.app.yml` is still present but no longer included in the bundle. You can delete it or keep it for reference.
