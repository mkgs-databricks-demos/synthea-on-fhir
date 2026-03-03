# Dynamic Resource Deployment with Python DABs

This bundle uses **Python support for Databricks Asset Bundles** to dynamically deploy resources based on the target environment. This is the official Databricks approach for conditional resource deployment.

## Architecture

The bundle uses Python DABs to define resources programmatically:

1. **databricks.yml** - Standard YAML configuration with `python:` section
2. **deployment_config.json** - Defines which resources to deploy per target
3. **resources/__init__.py** - Python module that loads resources conditionally
4. **requirements.txt** - Python dependencies (databricks-bundles package)
5. **.venv/** - Virtual environment (created during setup)

## How It Works

During bundle deployment:
1. DABs CLI activates the `.venv` virtual environment
2. Calls the `load_resources()` function from `resources/__init__.py`
3. Function reads `deployment_config.json` to determine what to deploy
4. Conditionally creates resource objects based on the target
5. Returns resources to be merged with YAML-defined resources

## Current Configuration

| Target | deploy_app | Description |
|--------|------------|-------------|
| dev (default) | ❌ false | Dev environment - app deployment disabled for faster iteration |
| free_edition | ❌ false | Free edition - app deployment disabled due to potential limitations |
| prod | ✅ true | Production environment - full app deployment enabled |
| himss2026 | ✅ true | HIMSS 2026 demo environment - full app deployment enabled |

## Setup (One-Time)

Before first deployment, set up the Python environment:

```bash
# Run the setup script
./setup.sh
```

This creates a virtual environment (`.venv/`) and installs the required `databricks-bundles` package.

### Manual Setup (Alternative)

```bash
# Create virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Deploy to Different Targets

```bash
# Deploy to dev (app will NOT be deployed)
databricks bundle deploy -t dev

# Deploy to prod (app WILL be deployed)
databricks bundle deploy -t prod

# Deploy to himss2026 (app WILL be deployed)
databricks bundle deploy -t himss2026

# Pass additional arguments
databricks bundle deploy -t prod --force-lock
```

### Validate Before Deploying

```bash
# Validate bundle configuration
databricks bundle validate -t dev

# View what will be deployed (requires jq)
databricks bundle validate -t prod --output json | jq '.resources'
```

### Run Deployed Resources

```bash
# After deployment, run a job
databricks bundle run <job_name> -t prod
```

## Adding More Conditional Resources

### 1. Update deployment_config.json

Add new deployment flags:

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

### 2. Update resources/__init__.py

Add logic in the `load_resources()` function:

```python
def load_resources(bundle: Bundle) -> Resources:
    # ... existing code ...
    
    # Conditionally add pipeline resource
    if target_config.get("deploy_pipeline", False):
        print("[PyDABs] ✓ Including pipeline resource")
        from databricks.sdk.service.pipelines import PipelineSpec
        
        resources.pipelines["my_pipeline"] = PipelineSpec(
            name="my-pipeline",
            # ... pipeline configuration ...
        )
    
    # Conditionally add job resource
    if target_config.get("deploy_job", False):
        print("[PyDABs] ✓ Including job resource")
        from databricks.sdk.service.jobs import Job
        
        resources.jobs["my_job"] = Job(
            name="my-job",
            # ... job configuration ...
        )
    
    return resources
```

## Advantages of Python DABs

* **Native Integration**: Official Databricks approach, fully supported
* **Type Safety**: Use Python SDK types for resource definitions
* **Dynamic Logic**: Access bundle context, environment variables, read files
* **Coexistence**: Python and YAML resources work together seamlessly
* **No Preprocessing**: Resources generated during deployment, not before
* **Better IDE Support**: Auto-completion and type checking in Python

## CI/CD Integration

Python DABs works seamlessly with CI/CD pipelines:

```yaml
# GitHub Actions example
- name: Set up Python
  uses: actions/setup-python@v4
  with:
    python-version: '3.11'

- name: Install dependencies
  run: |
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

- name: Deploy Bundle
  run: |
    source .venv/bin/activate
    databricks bundle deploy -t ${{ env.TARGET }}
```

## File Structure

```
redox_mcp/
├── databricks.yml           # Main bundle config with python: section
├── deployment_config.json   # Deployment configuration per target
├── requirements.txt         # Python dependencies
├── setup.sh                 # Setup script (creates .venv)
├── .venv/                   # Virtual environment (gitignored)
├── resources/
│   ├── __init__.py         # Dynamic resource loader
│   ├── *.volume.yml        # Static YAML resources
│   ├── *.schema.yml
│   └── *.secret_scope.yml
└── src/
    └── redox_mcp_serving_app/
```

## Original Files

The original `resources/redox_mcp_serving.app.yml` is still present for reference but no longer included in the bundle. You can delete it if you'd like, or keep it for reference.

## Troubleshooting

### Error: "No module named 'databricks.bundles'"

Make sure you've run the setup:
```bash
./setup.sh
```

Or manually install dependencies:
```bash
pip install -r requirements.txt
```

### Error: Virtual environment not found

The `databricks.yml` specifies `.venv` as the virtual environment path. Make sure it exists:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Resources not deploying as expected

Check the deployment logs for `[PyDABs]` messages that show which resources are being loaded:
```bash
databricks bundle deploy -t prod
```

### Verify configuration

Check what target configuration is being used:
```bash
cat deployment_config.json
```

## Documentation

* [Official Python DABs Documentation](https://docs.databricks.com/aws/en/dev-tools/bundles/python/)
* [databricks-bundles API Reference](https://databricks.github.io/cli/python/)
* [Databricks Asset Bundles Overview](https://docs.databricks.com/aws/en/dev-tools/bundles/)

## Notes

* The `.venv/` directory should be added to `.gitignore`
* The virtual environment is only used during deployment, not at runtime
* Python resources and YAML resources are merged together during deployment
* The `load_resources()` function only runs during `databricks bundle deploy`, not at runtime
