import json
import os
from pathlib import Path

# Load deployment configuration
config_file = Path(__file__).parent.parent / "deployment_config.json"
with open(config_file) as f:
    config = json.load(f)

# Get target from environment variable (set by DABs during deployment)
target = os.environ.get("DATABRICKS_BUNDLE_TARGET", "dev")
target_config = config.get(target, {})

# Initialize resources dictionary
resources = {}

# Conditionally add app resource based on configuration
if target_config.get("deploy_app", False):
    resources["apps"] = {
        "redox_mcp_serving_app": {
            "name": "mcp-redox",
            "source_code_path": "../src/redox_mcp_serving_app/",
            "description": "An application to serve the Redox MCP Server on Databricks",
            "lifecycle": {
                "prevent_destroy": False
            },
            "resources": [
                {
                    "name": "redox_public_key",
                    "description": "Redox Authentication Public Key",
                    "secret": {
                        "scope": "${var.secret_scope_name}",
                        "key": "public_key",
                        "permission": "READ"
                    }
                },
                {
                    "name": "redox_kid",
                    "description": "Redox Authentication KID",
                    "secret": {
                        "scope": "${var.secret_scope_name}",
                        "key": "kid",
                        "permission": "READ"
                    }
                },
                {
                    "name": "redox_private_key",
                    "description": "Redox Authentication Private Key",
                    "secret": {
                        "scope": "${var.secret_scope_name}",
                        "key": "private_key",
                        "permission": "READ"
                    }
                },
                {
                    "name": "redox_client_id",
                    "description": "Redox Authentication Client ID",
                    "secret": {
                        "scope": "${var.secret_scope_name}",
                        "key": "${var.client_id_dbs_key}",
                        "permission": "READ"
                    }
                },
                {
                    "name": "bin_volume",
                    "description": "Volume containing the Redox MCP Linux x86-64 binary",
                    "uc_securable": {
                        "securable_type": "VOLUME",
                        "securable_full_name": "${resources.volumes.source_bin.catalog_name}.${resources.volumes.source_bin.schema_name}.${resources.volumes.source_bin.name}",
                        "permission": "READ_VOLUME"
                    }
                }
            ]
        }
    }

# Return the resources (or empty dict if nothing to deploy)
{"resources": resources} if resources else {}
