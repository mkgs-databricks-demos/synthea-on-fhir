# config.py
import os

# Zerobus / Databricks settings (use Databricks secrets in production)
ZEROBUS_SERVER_ENDPOINT = os.getenv("ZEROBUS_SERVER_ENDPOINT")
CLIENT_ID = os.getenv("ZEROBUS_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZEROBUS_CLIENT_SECRET")
WORKSPACE_URL = os.getenv("WORKSPACE_URL")

FHIR_BUNDLE_TABLE_NAME = os.getenv("FHIR_BUNDLE_TABLE_NAME")
