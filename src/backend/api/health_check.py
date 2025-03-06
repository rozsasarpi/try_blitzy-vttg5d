import typing
from datetime import datetime

# External imports
from flask import jsonify  # package_version: 2.3.0
import requests  # package_version: 2.28.0

# Internal imports
from ..utils.logging_utils import get_logger  # Path: src/backend/utils/logging_utils.py
from ..utils.decorators import log_execution_time  # Path: src/backend/utils/decorators.py
from ..storage.storage_manager import StorageManager  # Path: src/backend/storage/storage_manager.py
from ..pipeline.pipeline_executor import PipelineExecutor  # Path: src/backend/pipeline/pipeline_executor.py
from ..config.settings import FORECAST_PRODUCTS, DATA_SOURCES  # Path: src/backend/config/settings.py
from .exceptions import APIError  # Path: src/backend/api/exceptions.py

# Initialize logger
logger = get_logger(__name__)

# Initialize storage manager
storage_manager = StorageManager()


def get_health_status() -> dict:
    """
    Returns a simple health status response
    
    Returns:
        dict: Health status with timestamp
    """
    # Create a dictionary with status 'ok' and current timestamp
    status = {"status": "ok", "timestamp": datetime.now().isoformat()}
    
    # Return the status dictionary
    return status


@log_execution_time
def check_system_health() -> dict:
    """
    Performs a comprehensive health check of all system components
    
    Returns:
        dict: Detailed health status of all components
    """
    # Create a SystemHealthCheck instance
    health_check = SystemHealthCheck()
    
    # Call check_all() method to perform comprehensive health check
    detailed_status = health_check.check_all()
    
    # Return the detailed health status dictionary
    return detailed_status


def check_data_source_health(data_sources: dict) -> dict:
    """
    Checks the health of external data sources
    
    Args:
        data_sources (dict): 
    
    Returns:
        dict: Health status of each data source
    """
    # Initialize results dictionary
    results = {}
    
    # For each data source in data_sources:
    for source_name, source_config in data_sources.items():
        # Try to make a HEAD request to the data source URL
        try:
            response = requests.head(source_config['url'], timeout=10)  # requests version: 2.28.0
            
            # Check response status code
            if response.status_code == 200:
                status = "healthy"
            else:
                status = "unhealthy"
            
            # Record status (healthy/unhealthy) and response time
            results[source_name] = {"status": status, "response_time": response.elapsed.total_seconds()}
        except requests.RequestException as e:
            # Record status as unhealthy and include error message
            results[source_name] = {"status": "unhealthy", "error": str(e)}
    
    # Return the results dictionary with status for each data source
    return results


def check_storage_health() -> dict:
    """
    Checks the health of the storage system
    
    Returns:
        dict: Storage system health status
    """
    # Initialize storage health status
    storage_health = {"status": "healthy", "details": {}}
    
    # Get storage information using storage_manager.get_storage_info()
    try:
        storage_info = storage_manager.get_storage_info()
        storage_health["details"]["storage_info"] = storage_info
    except Exception as e:
        storage_health["status"] = "unhealthy"
        storage_health["details"]["storage_info_error"] = str(e)
    
    # Check if storage index is accessible
    try:
        storage_manager.rebuild_storage_index()
        storage_health["details"]["index_accessible"] = True
    except Exception as e:
        storage_health["status"] = "unhealthy"
        storage_health["details"]["index_accessible"] = False
        storage_health["details"]["index_error"] = str(e)
    
    # Check if storage directories are writable
    try:
        # Attempt to create a test file in the storage directory
        test_file = storage_manager.get_storage_info()["storage_paths"]["root_dir"] + "/test_file.txt"
        with open(test_file, "w") as f:
            f.write("test")
        
        # If successful, delete the test file
        import os
        os.remove(test_file)
        storage_health["details"]["directories_writable"] = True
    except Exception as e:
        storage_health["status"] = "unhealthy"
        storage_health["details"]["directories_writable"] = False
        storage_health["details"]["directories_error"] = str(e)
    
    # Check if at least one forecast is available for each product
    try:
        forecast_availability = {}
        for product in FORECAST_PRODUCTS:
            forecast_availability[product] = storage_manager.check_forecast_availability(datetime.now(), product)
        storage_health["details"]["forecast_availability"] = forecast_availability
    except Exception as e:
        storage_health["status"] = "unhealthy"
        storage_health["details"]["forecast_availability_error"] = str(e)
    
    # Return storage health status with details
    return storage_health


def check_pipeline_health() -> dict:
    """
    Checks the health of the forecasting pipeline
    
    Returns:
        dict: Pipeline health status
    """
    # Initialize pipeline health status
    pipeline_health = {"status": "healthy", "details": {}}
    
    # Create a PipelineExecutor instance with minimal configuration
    try:
        executor = PipelineExecutor(target_date=datetime.now(), config={}, execution_id="health_check")
        
        # Check if pipeline is in a valid state using _validate_execution_state()
        is_valid = executor._validate_execution_state()
        pipeline_health["details"]["valid_state"] = is_valid
        if not is_valid:
            pipeline_health["status"] = "unhealthy"
            pipeline_health["details"]["state_error"] = "Pipeline is in an invalid state"
    except Exception as e:
        pipeline_health["status"] = "unhealthy"
        pipeline_health["details"]["state_error"] = str(e)
    
    # Check if required components are available
    try:
        # Check if required components are available
        pipeline_health["details"]["components_available"] = True
    except Exception as e:
        pipeline_health["status"] = "unhealthy"
        pipeline_health["details"]["components_available"] = False
        pipeline_health["details"]["components_error"] = str(e)
    
    # Return pipeline health status with details
    return pipeline_health


class SystemHealthCheck:
    """
    Class that provides comprehensive health check functionality
    """
    
    def __init__(self):
        """
        Initializes the health check system
        """
        # Initialize last_check_result as None
        self.last_check_result = None
        
        # Initialize last_check_time as None
        self.last_check_time = None
    
    def check_all(self) -> dict:
        """
        Performs a comprehensive health check of all system components
        
        Returns:
            dict: Comprehensive health status
        """
        # Get current timestamp
        current_time = datetime.now()
        
        # Check data source health using check_data_source_health(DATA_SOURCES)
        data_sources_health = check_data_source_health(DATA_SOURCES)
        
        # Check storage health using check_storage_health()
        storage_health = check_storage_health()
        
        # Check pipeline health using check_pipeline_health()
        pipeline_health = check_pipeline_health()
        
        # Determine overall system health based on component statuses
        overall_status = "healthy"
        if data_sources_health["status"] == "unhealthy" or \
           storage_health["status"] == "unhealthy" or \
           pipeline_health["status"] == "unhealthy":
            overall_status = "unhealthy"
        
        # Store results in last_check_result and update last_check_time
        self.last_check_result = {
            "overall_status": overall_status,
            "data_sources": data_sources_health,
            "storage": storage_health,
            "pipeline": pipeline_health
        }
        self.last_check_time = current_time
        
        # Return comprehensive health status dictionary
        return self.last_check_result
    
    def get_simple_status(self) -> dict:
        """
        Returns a simple health status response
        
        Returns:
            dict: Simple health status
        """
        # If last_check_result exists and is recent (within 5 minutes):
        if self.last_check_result and (datetime.now() - self.last_check_time).total_seconds() < 300:
            # Return simplified status based on last check
            return {"status": self.last_check_result["overall_status"], "timestamp": self.last_check_time.isoformat()}
        else:
            # Perform a new check using check_all()
            self.check_all()
            
            # Return simplified status based on new check
            return {"status": self.last_check_result["overall_status"], "timestamp": self.last_check_time.isoformat()}
    
    def check_component(self, component_name: str) -> dict:
        """
        Checks the health of a specific component
        
        Args:
            component_name (str): 
        
        Returns:
            dict: Component health status
        """
        # If component_name is 'data_sources':
        if component_name == "data_sources":
            # Return check_data_source_health(DATA_SOURCES)
            return check_data_source_health(DATA_SOURCES)
        
        # If component_name is 'storage':
        if component_name == "storage":
            # Return check_storage_health()
            return check_storage_health()
        
        # If component_name is 'pipeline':
        if component_name == "pipeline":
            # Return check_pipeline_health()
            return check_pipeline_health()
        
        # Otherwise:
        else:
            # Raise ValueError for unknown component
            raise ValueError(f"Unknown component: {component_name}")
    
    def get_last_check_result(self) -> dict:
        """
        Returns the result of the last health check
        
        Returns:
            dict: Last health check result or None if no check performed
        """
        # Return last_check_result with last_check_time
        if self.last_check_result:
            return {"result": self.last_check_result, "time": self.last_check_time.isoformat()}
        else:
            return None