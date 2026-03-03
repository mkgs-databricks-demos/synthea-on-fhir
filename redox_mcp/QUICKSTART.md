# Quick Start: Python DABs Dynamic Deployment

## 🚀 First Time Setup

Run this once to set up the Python environment:

```bash
cd /Workspace/Users/matthew.giglia@databricks.com/synthea-on-fhir/redox_mcp
./setup.sh
```

## 📦 Deploy

Deploy to different targets:

```bash
# Dev (app NOT deployed)
databricks bundle deploy -t dev

# Prod (app DEPLOYED)
databricks bundle deploy -t prod

# HIMSS 2026 (app DEPLOYED)
databricks bundle deploy -t himss2026
```

## 🔍 What Gets Deployed?

| Target | App Deployed? |
|--------|---------------|
| dev | ❌ No |
| free_edition | ❌ No |
| prod | ✅ Yes |
| himss2026 | ✅ Yes |

## ⚙️ Configuration

Edit `deployment_config.json` to change what gets deployed per target.

## 📚 Full Documentation

See `DYNAMIC_DEPLOYMENT.md` for complete documentation.

## 🔧 Key Files

* `databricks.yml` - Bundle config (lines 12-17 contain Python section)
* `deployment_config.json` - Deployment rules per target
* `resources/__init__.py` - Dynamic resource loader
* `requirements.txt` - Python dependencies
* `setup.sh` - Setup script
