# config.py
import os

# Zerobus / Databricks settings (use Databricks secrets in production)
ZEROBUS_SERVER_ENDPOINT = os.getenv(
    "ZEROBUS_SERVER_ENDPOINT",
    "7474651703425732.zerobus.us-east-1.cloud.databricks.com",
)
WORKSPACE_URL = os.getenv(
    "WORKSPACE_URL",
    "https://fe-sandbox-himss2026.cloud.databricks.com/",
)
FHIR_BUNDLE_TABLE_NAME = os.getenv(
    "TABLE_NAME",
    "himss.redox.fhir_bundle_zerobus",
)

CLIENT_ID = os.getenv("ZEROBUS_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZEROBUS_CLIENT_SECRET")
