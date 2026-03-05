# FHIR to Zerobus Ingest Application

A FastAPI application that provides real-time streaming ingestion of HL7 FHIR Bundles to Unity Catalog via Databricks Zerobus SDK.

## Overview

This asset bundle deploys a production-ready FHIR ingestion service with:

* **FastAPI REST endpoint** - Accepts FHIR bundles via HTTP POST
* **React.js frontend** - Interactive dashboard showing health status and API documentation
* **Zerobus streaming** - Real-time ingestion to Unity Catalog with automatic recovery
* **Databricks Apps Gateway** - Built-in authentication and authorization
* **DLT pipeline** - Optional downstream processing of ingested FHIR data

## Architecture

```
FHIR Bundle (JSON) 
    ↓ HTTP POST
FastAPI App (/api/v1/ingest/fhir-bundle)
    ↓ Zerobus SDK
Unity Catalog Table (VARIANT column)
    ↓ DLT Pipeline
Processed FHIR Resources
```

## Bundle Structure

```
fhir_zerobus/
├── databricks.yml                        # Main bundle configuration
├── README.md                             # This file
├── DEPLOYMENT.md                         # Deployment guide
├── SECRET_SCOPE_SETUP.md                 # Secret configuration guide
├── resources/
│   ├── zerobus_app.app.yml              # Databricks App definition
│   ├── zerobus.secret_scope.yml         # Secret scope configuration
│   ├── fhir_bundle_table_setup.job.yml  # Table setup job
│   └── fhir_zerobus_etl.pipeline.yml    # DLT pipeline for processing
└── src/
    ├── zerobus_app/                      # FastAPI application
    │   ├── app.py                        # Main application logic
    │   ├── config.py                     # Configuration management
    │   ├── app.yaml                      # App runtime config
    │   ├── requirements.txt              # Python dependencies
    │   └── static/
    │       └── index.html                # React frontend
    └── fhir_zerobus_etl/                 # DLT pipeline source
        └── transformations/              # FHIR processing logic
```

## Features

### FastAPI Application
* **Health check endpoint** (`/health`) - Monitor app and Zerobus stream status
* **Ingestion endpoint** (`/api/v1/ingest/fhir-bundle`) - POST FHIR bundles as JSON
* **Interactive API docs** (`/docs`) - Swagger UI for testing
* **React dashboard** (`/`) - Status monitoring and code examples

### Zerobus Integration
* **JSON streaming** - Direct VARIANT column ingestion
* **High throughput** - Up to 10,000 in-flight records
* **Automatic recovery** - Resilient to transient failures
* **Metadata tracking** - UUID, timestamp, source system per bundle

### Unity Catalog Table Schema
```sql
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.fhir_bundle_zerobus (
  bundle_uuid STRING,
  fhir VARIANT,
  source_system STRING,
  event_timestamp BIGINT,
  ingest_datetime TIMESTAMP
);
```

## Quick Start

### 1. Prerequisites
* Databricks CLI authenticated
* Unity Catalog with target catalog/schema
* Zerobus endpoint credentials
* Service principal with appropriate permissions

### 2. Configure Secrets
See [SECRET_SCOPE_SETUP.md](SECRET_SCOPE_SETUP.md) for detailed instructions.

```bash
# Create secret scope and add credentials
databricks secrets create-scope fhir_zerobus_credentials
databricks secrets put-secret --scope fhir_zerobus_credentials --key client_id
databricks secrets put-secret --scope fhir_zerobus_credentials --key client_secret
databricks secrets put-secret --scope fhir_zerobus_credentials --key zerobus_endpoint
databricks secrets put-secret --scope fhir_zerobus_credentials --key workspace_url
databricks secrets put-secret --scope fhir_zerobus_credentials --key fhir_bundle_table_name
```

### 3. Deploy
See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment instructions.

```bash
# Validate bundle
databricks bundle validate -t himss2026

# Deploy to target environment
databricks bundle deploy -t himss2026
```

### 4. Test the App
Once deployed, navigate to the app URL to see the React dashboard, or use the API directly:

```bash
curl -X POST "https://<app-url>/api/v1/ingest/fhir-bundle" \
  -H "Authorization: Bearer <databricks-token>" \
  -H "Content-Type: application/json" \
  -d '{"resourceType": "Bundle", "type": "transaction", "entry": [...]}'
```

## Deployment Targets

| Target | Workspace | Catalog | Schema | Purpose |
|--------|-----------|---------|--------|---------|
| **dev** | fe-vm-mkgs-databricks-demos | mkgs_dev | redox | Development |
| **prod** | fe-vm-mkgs-databricks-demos | mkgs | redox | Production |
| **himss2026** | fe-sandbox-himss2026 | himss | redox | HIMSS demo |
| **free_edition** | dbc-e5684c0a-20fa | mkgs | redox | Free tier |

## Documentation

* [DEPLOYMENT.md](DEPLOYMENT.md) - Complete deployment and operational guide
* [SECRET_SCOPE_SETUP.md](SECRET_SCOPE_SETUP.md) - Secret management instructions
* [Databricks Apps Documentation](https://docs.databricks.com/en/dev-tools/databricks-apps/)
* [Zerobus SDK Documentation](https://docs.databricks.com/ingestion/zerobus/)
* [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/)

## Support

* **Primary Developer**: matthew.giglia@databricks.com
* **Project**: Redox Zerobus
* **Business Unit**: Healthcare and Life Sciences
* **Target Audience**: Healthcare Providers and Health Plans
