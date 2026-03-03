# Quick Start: Python DABs Dynamic Deployment

## 🚀 First Time Setup

Run this once to set up the Python environment:

```bash
cd /Workspace/Users/matthew.giglia@databricks.com/synthea-on-fhir/redox_mcp
./setup.sh
```

## 📦 Deploy

Deploy to any target - resources are automatically deployed based on runtime checks:

```bash
# Deploy to dev
databricks bundle deploy -t dev

# Deploy to prod
databricks bundle deploy -t prod

# Deploy to himss2026
databricks bundle deploy -t himss2026
```

## 🔍 What Gets Deployed?

Deployment is automatic based on runtime conditions:

| Resource | Deployment Condition |
|----------|---------------------|
| **Secret Scope** | Deployed if scope doesn't exist yet |
| **Redox MCP App** | Deployed if:<br>✅ Secret scope exists<br>✅ Secret key exists<br>✅ Binary file exists in volume |

No manual configuration needed - the bundle inspects your workspace and decides what to deploy!

## 📚 Full Documentation

See `DYNAMIC_DEPLOYMENT.md` for complete documentation.

## 🔧 Key Files

* `databricks.yml` - Bundle config with Python section
* `resources/__init__.py` - Entry point for dynamic deployment
* `resources/dynamic_deployment.py` - Core conditional deployment logic
* `requirements.txt` - Python dependencies
* `setup.sh` - Setup script

## 🎯 How It Works

1. **Runtime Checks**: During deployment, the bundle checks your workspace:
   - Does the secret scope exist?
   - Does the secret key exist in the scope?
   - Does the binary file exist in the volume?

2. **Automatic Decisions**: Based on these checks, resources are automatically included or skipped

3. **Idempotent**: Safe to run multiple times - only deploys what's needed
