"""
Resources initialization for dynamic DABs deployment.
This module loads bundle resources conditionally based on deployment_config.json.
"""
import json
import os
from pathlib import Path
from databricks.bundles.core import Bundle, Resources


def load_resources(bundle: Bundle) -> Resources:
    """
    Load bundle resources conditionally based on target configuration.
    
    This function is called by Databricks CLI during bundle deployment.
    It reads deployment_config.json and conditionally adds resources
    based on the target environment.
    """
    # Load deployment configuration
    config_file = Path(__file__).parent.parent / "deployment_config.json"
    with open(config_file) as f:
        config = json.load(f)
    
    # Get target from bundle context
    target = bundle.target
    target_config = config.get(target, {})
    
    print(f"[PyDABs] Loading resources for target: {target}")
    print(f"[PyDABs] Configuration: {json.dumps(target_config, indent=2)}")
    
    # Initialize empty resources
    resources = Resources()
    
    # Conditionally add app resource
    if target_config.get("deploy_app", False):
        print("[PyDABs] ✓ Including app resource")
        
        from databricks.sdk.service.apps import App
        
        # Define the app resource
        resources.apps["redox_mcp_serving_app"] = App(
            name="mcp-redox",
            source_code_path="../src/redox_mcp_serving_app/",
            description="An application to serve the Redox MCP Server on Databricks",
            resources=[
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
        )
    else:
        print("[PyDABs] ✗ Skipping app resource")
    
    # Add more conditional resources here as needed
    # Example:
    # if target_config.get("deploy_pipeline", False):
    #     print("[PyDABs] ✓ Including pipeline resource")
    #     resources.pipelines["my_pipeline"] = Pipeline(...)
    
    return resources
