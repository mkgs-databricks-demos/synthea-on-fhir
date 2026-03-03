"""
Resources initialization for dynamic DABs deployment.

This module loads bundle resources conditionally based on runtime checks
of secret scopes, keys, and volume files.
"""
import json
import logging
from pathlib import Path
from typing import Optional

import yaml
from databricks.bundles.core import Bundle, Resources, Variable, variables
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist

# Configure logging
logger = logging.getLogger(__name__)


@variables
class Variables:
    """Bundle variables for dynamic deployment."""
    
    secret_scope_name: Variable[str]
    client_id_dbs_key: Variable[str]
    run_as_user: Variable[str]
    redox_binary_filename: Variable[str]


class DynamicResources:
    """Manages dynamic resource loading for DABs deployment.
    
    This class handles conditional deployment of Databricks resources based on
    runtime checks of secret scopes, keys, and volume files.
    """
    
    # Class constants for default file paths and resource keys
    DEFAULT_SECRET_SCOPE_YAML = "redox_oauth.secret_scope.yml"
    DEFAULT_APP_YAML = "redox_mcp_serving.app.yml"
    DEFAULT_VOLUME_KEY = "source_bin"
    
    def __init__(self, bundle: Bundle, config_path: Optional[str] = None) -> None:
        """Initialize DynamicResources with configuration.
        
        Args:
            bundle: The Databricks bundle instance
            config_path: Optional path to an external configuration JSON file.
                        Not required for normal bundle operation.
        """
        self.bundle = bundle
        self.config_path = Path(config_path) if config_path else None
        self.config = self._load_config()
        self.resources = Resources()
        self.secret_scope_name = self.bundle.resolve_variable(Variables.secret_scope_name)
        self.client_id_dbs_key = self.bundle.resolve_variable(Variables.client_id_dbs_key)
        self.run_as_user = self.bundle.resolve_variable(Variables.run_as_user)
        self.redox_binary_filename = self.bundle.resolve_variable(Variables.redox_binary_filename)
        self.workspace_client = WorkspaceClient()
        self._resources_dir = Path(__file__).parent

    def deploy_secret_scope_if_missing(
        self, 
        scope_yaml_path: Optional[str] = None
    ) -> bool:
        """Deploy the secret scope from YAML file if it doesn't exist or is managed by bundle.
        
        Loads the resource configuration from redox_oauth.secret_scope.yml 
        in the same directory as this file. Deploys if:
        - The secret scope doesn't exist in the workspace, OR
        - The secret scope is part of this bundle's resources (managed by bundle)
        
        Args:
            scope_yaml_path: Path to the secret scope YAML configuration file. 
                           Defaults to redox_oauth.secret_scope.yml in the same 
                           directory as this Python file.
            
        Returns:
            True if scope was deployed, False if it already exists (and not managed 
            by bundle) or deployment failed
        """
        scope_exists, _ = self._check_secret_scope_and_key()
        is_managed_by_bundle = self._is_secret_scope_in_bundle()
        
        # If scope exists and is NOT managed by this bundle, skip deployment
        if scope_exists and not is_managed_by_bundle:
            logger.info(
                "Secret scope '%s' already exists and is not managed by this bundle. Skipping deployment.",
                self.secret_scope_name
            )
            return False
        
        # Deploy if scope doesn't exist OR if it's managed by this bundle
        if is_managed_by_bundle:
            logger.info(
                "Secret scope '%s' is managed by this bundle. Proceeding with deployment/update.",
                self.secret_scope_name
            )
        else:
            logger.info(
                "Secret scope '%s' does not exist. Proceeding with deployment.",
                self.secret_scope_name
            )
        
        try:
            yaml_path = self._get_yaml_path(scope_yaml_path, self.DEFAULT_SECRET_SCOPE_YAML)
            scope_config = self._load_yaml_config(yaml_path)
            
            # Extract and add the secret scope resources to the bundle
            if 'resources' not in scope_config:
                raise ValueError("Invalid YAML structure: 'resources' section not found")
            
            if 'secret_scopes' not in scope_config['resources']:
                raise ValueError("No 'secret_scopes' found in resources configuration")
            
            self.resources.secret_scopes = scope_config['resources']['secret_scopes']
            logger.info(
                "Secret scope '%s' added to deployment resources.",
                self.secret_scope_name
            )
            return True
            
        except (FileNotFoundError, ValueError) as e:
            logger.error("Error deploying secret scope: %s", e)
            return False
        except Exception as e:
            logger.exception("Unexpected error deploying secret scope: %s", e)
            return False

    def deploy_app_if_ready(
        self,
        binary_filename: Optional[str] = None,
        app_yaml_path: Optional[str] = None,
        volume_key: str = DEFAULT_VOLUME_KEY
    ) -> bool:
        """Deploy the Redox MCP serving app if all prerequisites are met.
        
        Deploys only if: secret scope exists, secret key exists, and binary 
        file exists in volume.
        
        Args:
            binary_filename: Name of the binary file to check for in the volume.
                           If None, uses the bundle variable redox_binary_filename.
            app_yaml_path: Path to the app YAML configuration file.
                          Defaults to redox_mcp_serving.app.yml in the same 
                          directory as this Python file.
            volume_key: The key of the volume resource in the bundle 
                       (default: "source_bin")
            
        Returns:
            True if app was deployed, False if prerequisites not met or 
            deployment failed
        """
        # Use bundle variable if binary_filename not provided
        if binary_filename is None:
            binary_filename = self.redox_binary_filename
        
        # Check all three prerequisites
        scope_exists, key_exists = self._check_secret_scope_and_key()
        file_exists = self._check_file_in_volume(binary_filename, volume_key)
        
        # Log status of prerequisites
        if not scope_exists:
            logger.warning(
                "Secret scope '%s' does not exist. Skipping app deployment.",
                self.secret_scope_name
            )
            return False
        
        if not key_exists:
            logger.warning(
                "Secret key '%s' not found in scope '%s'. Skipping app deployment.",
                self.client_id_dbs_key,
                self.secret_scope_name
            )
            return False
        
        if not file_exists:
            logger.warning(
                "Binary file '%s' not found in volume '%s'. Skipping app deployment.",
                binary_filename,
                volume_key
            )
            return False
        
        # All prerequisites met, proceed with deployment
        logger.info("All prerequisites met. Proceeding with app deployment.")
        
        try:
            yaml_path = self._get_yaml_path(app_yaml_path, self.DEFAULT_APP_YAML)
            app_config = self._load_yaml_config(yaml_path)
            
            # Extract and add the app resources to the bundle
            if 'resources' not in app_config:
                raise ValueError("Invalid YAML structure: 'resources' section not found")
            
            if 'apps' not in app_config['resources']:
                raise ValueError("No 'apps' found in resources configuration")
            
            self.resources.apps = app_config['resources']['apps']
            logger.info("Redox MCP serving app added to deployment resources.")
            return True
            
        except (FileNotFoundError, ValueError) as e:
            logger.error("Error deploying app: %s", e)
            return False
        except Exception as e:
            logger.exception("Unexpected error deploying app: %s", e)
            return False
    
    def get_resources(self) -> Resources:
        """Return the configured resources.
        
        Returns:
            Resources object containing all dynamically added resources
        """
        return self.resources
    
    # Private methods
    
    def _check_secret_scope_and_key(self) -> tuple[bool, bool]:
        """Check if the secret scope exists and if the client ID key is present.
        
        Returns:
            Tuple of (scope_exists, key_exists) where:
                - scope_exists: True if the secret scope exists
                - key_exists: True if the scope exists and contains the client ID key
        """
        scope_exists = False
        key_exists = False
        
        try:
            # Check if scope exists by listing all scopes
            scopes = self.workspace_client.secrets.list_scopes()
            scope_exists = any(scope.name == self.secret_scope_name for scope in scopes)
            
            if scope_exists:
                # Check if the key exists in the scope
                secrets = self.workspace_client.secrets.list_secrets(
                    scope=self.secret_scope_name
                )
                key_exists = any(secret.key == self.client_id_dbs_key for secret in secrets)
        
        except (NotFound, ResourceDoesNotExist) as e:
            logger.debug("Secret scope or key not found: %s", e)
            return False, False
        except Exception as e:
            logger.error("Error checking secret scope: %s", e)
            return False, False
        
        return scope_exists, key_exists

    def _is_secret_scope_in_bundle(self) -> bool:
        """Check if the secret scope is part of this bundle's resources.
        
        Returns:
            True if the secret scope is managed by this bundle, False otherwise
        """
        try:
            # Check if bundle has secret_scopes resources
            if not hasattr(self.bundle.resources, 'secret_scopes'):
                return False
            
            # Check if our specific secret scope is in the bundle's resources
            secret_scopes = self.bundle.resources.secret_scopes
            if not secret_scopes:
                return False
            
            # Check if any secret scope in the bundle matches our scope name
            for scope_key, scope_resource in secret_scopes.items():
                if hasattr(scope_resource, 'name') and scope_resource.name == self.secret_scope_name:
                    logger.debug(
                        "Secret scope '%s' found in bundle resources with key '%s'",
                        self.secret_scope_name,
                        scope_key
                    )
                    return True
            
            return False
            
        except Exception as e:
            logger.debug("Error checking if secret scope is in bundle: %s", e)
            return False

    def _check_file_in_volume(
        self,
        filename: str,
        volume_key: str = DEFAULT_VOLUME_KEY
    ) -> bool:
        """Check if a file exists in the deployed volume.
        
        Retrieves volume information from the bundle's resources dynamically.
        
        Args:
            filename: Name of the file to check for in the volume
            volume_key: The key of the volume resource in the bundle 
                       (default: "source_bin")
            
        Returns:
            True if the file exists, False otherwise
        """
        try:
            # Get the volume resource from the bundle's resources
            if not hasattr(self.bundle.resources, 'volumes'):
                raise ValueError("No volumes found in bundle resources")
            
            if volume_key not in self.bundle.resources.volumes:
                raise ValueError(
                    f"Volume resource '{volume_key}' not found in bundle resources"
                )
            
            source_bin_volume = self.bundle.resources.volumes[volume_key]
            
            # Extract catalog, schema, and volume name from the volume resource
            catalog_name = source_bin_volume.catalog_name
            schema_name = source_bin_volume.schema_name
            volume_name = source_bin_volume.name
            
            # Construct the full volume path
            volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{filename}"
            
            # Check if the file exists using the workspace client
            try:
                self.workspace_client.files.get_status(volume_path)
                logger.info("File found: %s", volume_path)
                return True
            except (NotFound, ResourceDoesNotExist):
                logger.info("File not found: %s", volume_path)
                return False
                
        except ValueError as e:
            logger.error("Configuration error checking file in volume: %s", e)
            return False
        except Exception as e:
            logger.error("Error checking file in volume: %s", e)
            return False
    
    def _load_config(self) -> dict:
        """Load external configuration from JSON file if provided.
        
        This is optional and not required for normal bundle operation.
        
        Returns:
            Dictionary containing configuration, or empty dict if no config provided
        """
        if self.config_path is None:
            logger.info("No config file provided. Using default configuration.")
            return {}
        
        if not self.config_path.exists():
            logger.warning(
                "Configuration file not found: %s. Using default configuration.",
                self.config_path
            )
            return {}
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in config file: %s", e)
            return {}
        except Exception as e:
            logger.error("Error loading config file: %s. Using default configuration.", e)
            return {}
    
    def _get_yaml_path(
        self,
        provided_path: Optional[str],
        default_filename: str
    ) -> Path:
        """Get the YAML file path, using provided path or default.
        
        Args:
            provided_path: Optional path provided by caller
            default_filename: Default filename to use in resources directory
            
        Returns:
            Path object for the YAML file
        """
        if provided_path is None:
            return self._resources_dir / default_filename
        return Path(provided_path)
    
    def _load_yaml_config(self, yaml_path: Path) -> dict:
        """Load and parse a YAML configuration file.
        
        Args:
            yaml_path: Path to the YAML file
            
        Returns:
            Dictionary containing the parsed YAML content
            
        Raises:
            FileNotFoundError: If the YAML file doesn't exist
            ValueError: If the YAML content is invalid
        """
        if not yaml_path.exists():
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")
        
        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                
            if not config:
                raise ValueError(f"Empty or invalid YAML file: {yaml_path}")
                
            return config
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax in {yaml_path}: {e}") from e
