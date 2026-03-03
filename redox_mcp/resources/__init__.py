import logging
from databricks.bundles.core import (
    Bundle,
    Resources,
    load_resources_from_current_package_module,
)

# Configure logging to ensure debug messages are visible
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_resources(bundle: Bundle) -> Resources:
    """
    'load_resources' function is referenced in databricks.yml and
    is responsible for loading bundle resources defined in Python code.
    This function is called by Databricks CLI during bundle deployment.
    After deployment, this function is not used.
    
    This implementation:
    1. Loads static resources from YAML files in the resources folder
    2. Attempts dynamic deployment if authentication is available
    3. Merges dynamic resources into the base resources
    """
    
    # First, load all static resources from YAML files in the resources folder
    logger.info("Loading base resources from YAML files...")
    base_resources = load_resources_from_current_package_module()
    
    # Attempt dynamic deployment with graceful fallback
    try:
        # Import DynamicResources here to avoid Spark session errors during module load
        from .dynamic_deployment import DynamicResources
        
        logger.info("=" * 60)
        logger.info("STARTING DYNAMIC DEPLOYMENT")
        logger.info("=" * 60)
        deployer = DynamicResources(bundle)
        
        # Deploy secret scope if missing
        logger.info("Checking if secret scope should be deployed...")
        scope_deployed = deployer.deploy_secret_scope_if_missing()
        logger.info("Secret scope deployment result: %s", "DEPLOYED" if scope_deployed else "SKIPPED")
        
        # Deploy app if all prerequisites are met
        # Uses the bundle variable redox_binary_filename for the binary filename
        logger.info("Checking if app should be deployed...")
        app_deployed = deployer.deploy_app_if_ready()
        logger.info("App deployment result: %s", "DEPLOYED" if app_deployed else "SKIPPED")
        
        # Get the dynamically added resources
        dynamic_resources = deployer.get_resources()
        
        # Merge dynamic resources into base resources
        logger.info("Merging dynamic resources into base resources...")
        if hasattr(dynamic_resources, 'secret_scopes') and dynamic_resources.secret_scopes:
            if not hasattr(base_resources, 'secret_scopes'):
                base_resources.secret_scopes = {}
            scope_count = len(dynamic_resources.secret_scopes)
            base_resources.secret_scopes.update(dynamic_resources.secret_scopes)
            logger.info("✓ Merged %d secret scope(s): %s", scope_count, list(dynamic_resources.secret_scopes.keys()))
        else:
            logger.info("No secret scopes to merge from dynamic deployment")
        
        if hasattr(dynamic_resources, 'apps') and dynamic_resources.apps:
            if not hasattr(base_resources, 'apps'):
                base_resources.apps = {}
            app_count = len(dynamic_resources.apps)
            base_resources.apps.update(dynamic_resources.apps)
            logger.info("✓ Merged %d app(s): %s", app_count, list(dynamic_resources.apps.keys()))
        else:
            logger.info("No apps to merge from dynamic deployment")
        
        logger.info("=" * 60)
        logger.info("DYNAMIC DEPLOYMENT COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except ImportError as e:
        # Import failed - likely due to missing dependencies or Spark session
        logger.warning(
            "Dynamic deployment skipped due to import error: %s. "
            "This may occur when cluster is terminated or dependencies are missing. "
            "Proceeding with static resources only.",
            str(e)
        )
    except ValueError as e:
        # Authentication failed - this is expected during bundle deployment
        logger.warning(
            "Dynamic deployment skipped due to authentication: %s. "
            "This is normal during 'databricks bundle deploy' from CLI. "
            "To enable dynamic deployment, ensure ~/.databrickscfg is configured "
            "or run a post-deployment setup job.",
            str(e)
        )
    except Exception as e:
        # Other errors - log but don't fail the deployment
        logger.warning(
            "Dynamic deployment failed: %s. Proceeding with static resources only.",
            str(e),
            exc_info=True
        )
    
    return base_resources
