# Dynamic Resource Deployment with Python DABs

This bundle uses **Python support for Databricks Asset Bundles** to dynamically deploy resources based on runtime environment checks. This is the official Databricks approach for conditional resource deployment.

## Architecture

The bundle uses Python DABs to define resources programmatically:

1. **databricks.yml** - Standard YAML configuration with `python:` section
2. **resources/__init__.py** - Python module that loads resources conditionally based on runtime checks
3. **resources/dynamic_deployment.py** - Core logic for conditional deployment
4. **requirements.txt** - Python dependencies (databricks-bundles package)
5. **.venv/** - Virtual environment (created during setup)

## How It Works

During bundle deployment:
1. DABs CLI activates the `.venv` virtual environment
2. Calls the `load_resources()` function from `resources/__init__.py`
3. Function performs runtime checks:
   - Does the secret scope exist?
   - Does the secret key exist in the scope?
   - Does the binary file exist in the volume?
4. Conditionally creates resource objects based on these checks
5. Returns resources to be merged with YAML-defined resources

## Deployment Logic

The bundle automatically determines what to deploy based on runtime conditions:

| Resource | Deployment Condition |
|----------|---------------------|
| **Secret Scope** | Deployed if scope does not exist |
| **Redox MCP App** | Deployed if:<br>✅ Secret scope exists<br>✅ Secret key exists in scope<br>✅ Binary file exists in volume |

This ensures:
* First-time deployments create the necessary secret scope
* App only deploys when all prerequisites are met
* No manual configuration needed - everything is automatic

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

### Deploy

```bash
# Deploy to any target - resources are deployed based on runtime checks
databricks bundle deploy -t dev
databricks bundle deploy -t prod
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

### 1. Create a new YAML resource file

Add your resource definition in `resources/` directory:

```yaml
# resources/my_pipeline.yml
resources:
  pipelines:
    my_pipeline:
      name: ${var.catalog}_${var.schema}_pipeline
      # ... pipeline configuration ...
```

### 2. Update resources/dynamic_deployment.py

Add a new method to check conditions and deploy the resource:

```python
def deploy_pipeline_if_ready(
    self,
    pipeline_yaml_path: Optional[str] = None
) -> bool:
    """Deploy pipeline if conditions are met."""
    
    # Add your condition checks here
    if not self._check_some_condition():
        logger.warning("Condition not met. Skipping pipeline deployment.")
        return False
    
    # Load and deploy the resource
    try:
        yaml_path = self._get_yaml_path(pipeline_yaml_path, "my_pipeline.yml")
        config = self._load_yaml_config(yaml_path)
        
        if 'pipelines' not in config.get('resources', {}):
            raise ValueError("No 'pipelines' found in resources configuration")
        
        self.resources.pipelines = config['resources']['pipelines']
        logger.info("Pipeline added to deployment resources.")
        return True
        
    except Exception as e:
        logger.error("Error deploying pipeline: %s", e)
        return False
```

### 3. Update resources/__init__.py

Call your new method in the `load_resources()` function:

```python
def load_resources(bundle: Bundle) -> Resources:
    deployer = DynamicResources(bundle)
    
    # Deploy secret scope if missing
    deployer.deploy_secret_scope_if_missing()
    
    # Deploy app if ready
    deployer.deploy_app_if_ready(binary_filename="redox-mcp-server")
    
    # Deploy your new resource
    deployer.deploy_pipeline_if_ready()
    
    return deployer.get_resources()
```

## Advantages of Python DABs

* **Native Integration**: Official Databricks approach, fully supported
* **Type Safety**: Use Python SDK types for resource definitions
* **Runtime Checks**: Inspect workspace state before deploying resources
* **Dynamic Logic**: Access bundle context, environment variables, read files
* **Coexistence**: Python and YAML resources work together seamlessly
* **No Preprocessing**: Resources generated during deployment, not before
* **Better IDE Support**: Auto-completion and type checking in Python
* **Automatic Decision Making**: No manual configuration - deployment adapts to environment

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
├── databricks.yml                      # Main bundle config with python: section
├── requirements.txt                    # Python dependencies
├── setup.sh                            # Setup script (creates .venv)
├── .venv/                              # Virtual environment (gitignored)
├── resources/
│   ├── __init__.py                    # Entry point - calls DynamicResources
│   ├── dynamic_deployment.py          # Core conditional deployment logic
│   ├── redox_oauth.secret_scope.yml   # Secret scope resource definition
│   ├── redox_mcp_serving.app.yml      # App resource definition
│   ├── bin.volume.yml                 # Volume resource (always deployed)
│   └── redox.schema.yml               # Schema resource (always deployed)
└── src/
    └── redox_mcp_serving_app/
```

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

Check the deployment logs for conditional deployment messages:
```bash
databricks bundle deploy -t prod
```

Look for log messages indicating:
* Which prerequisites are met/not met
* Which resources are being added/skipped
* Any errors during resource loading

### Secret scope already exists

This is expected behavior! The bundle checks if the secret scope exists and only creates it if missing. You'll see a log message:
```
Secret scope 'your-scope-name' already exists. Skipping deployment.
```

### App not deploying

The app requires three prerequisites:
1. ✅ Secret scope must exist
2. ✅ Secret key must exist in the scope
3. ✅ Binary file must exist in the volume

Check the logs to see which prerequisite is missing.

## Documentation

* [Official Python DABs Documentation](https://docs.databricks.com/aws/en/dev-tools/bundles/python/)
* [databricks-bundles API Reference](https://databricks.github.io/cli/python/)
* [Databricks Asset Bundles Overview](https://docs.databricks.com/aws/en/dev-tools/bundles/)

## Notes

* The `.venv/` directory should be added to `.gitignore`
* The virtual environment is only used during deployment, not at runtime
* Python resources and YAML resources are merged together during deployment
* The `load_resources()` function only runs during `databricks bundle deploy`, not at runtime
* Deployment decisions are made automatically based on workspace state - no manual configuration needed
