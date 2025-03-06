"""Core executor module for the Electricity Market Price Forecasting System's pipeline.
Provides high-level interfaces for executing the forecasting pipeline with configuration management,
error handling, and fallback mechanisms. This module serves as the main entry point for running the forecasting process.
"""

import typing
import uuid
import time
import copy
from datetime import datetime

# Internal imports
from .exceptions import PipelineError, PipelineExecutionError, PipelineConfigurationError
from .pipeline_logger import log_pipeline_start, log_pipeline_completion, log_pipeline_failure, log_fallback_activation
from .forecasting_pipeline import ForecastingPipeline
from ..utils.decorators import log_execution_time, log_exceptions
from ..utils.logging_utils import get_logger
from ..config.settings import FORECAST_PRODUCTS, DATA_SOURCES

# Global logger
logger = get_logger(__name__)

# Define default configuration
DEFAULT_CONFIG = {"data_sources": DATA_SOURCES, "products": FORECAST_PRODUCTS, "fallback": {"enabled": True, "max_search_days": 7}, "validation": {"schema": True, "completeness": True, "plausibility": True}, "storage": {"format": "parquet", "compression": "snappy"}}


@log_execution_time
@log_exceptions
def execute_forecasting_pipeline(target_date: datetime, config: dict) -> dict:
    """Main entry point for executing the forecasting pipeline with custom configuration

    Args:
        target_date (datetime.datetime): The target date for which to generate forecasts
        config (dict): Configuration dictionary for the pipeline

    Returns:
        dict: Dictionary with pipeline execution results and metadata
    """
    # Generate a unique execution ID using uuid
    execution_id = str(uuid.uuid4())

    # Validate the provided configuration
    if not validate_config(config):
        raise PipelineConfigurationError("Invalid pipeline configuration", "execute_forecasting_pipeline", "config")

    # Merge provided config with default configuration
    merged_config = merge_configs(config, get_default_config())

    # Create a PipelineExecutor instance with target_date, merged config, and execution_id
    executor = PipelineExecutor(target_date, merged_config, execution_id)

    # Execute the pipeline by calling executor.execute()
    try:
        success = executor.execute()
    except Exception as e:
        log_pipeline_failure("forecasting_pipeline", execution_id, time.time(), e)
        raise PipelineExecutionError(f"Pipeline execution failed: {str(e)}", "forecasting_pipeline", execution_id, target_date) from e

    # Return the execution results from executor.get_results()
    results = executor.get_results()
    return results


@log_execution_time
def execute_with_default_config(target_date: datetime) -> dict:
    """Execute the forecasting pipeline with default configuration

    Args:
        target_date (datetime.datetime): The target date for which to generate forecasts

    Returns:
        dict: Dictionary with pipeline execution results and metadata
    """
    # Call execute_forecasting_pipeline with target_date and default configuration
    try:
        results = execute_forecasting_pipeline(target_date, get_default_config())
    except Exception as e:
        log_pipeline_failure("forecasting_pipeline", "default_config", time.time(), e)
        raise PipelineExecutionError(f"Pipeline execution failed with default config: {str(e)}", "forecasting_pipeline", "default_config", target_date) from e

    # Return the execution results
    return results


def get_default_config() -> dict:
    """Get the default pipeline configuration

    Returns:
        dict: Default configuration dictionary
    """
    # Return a deep copy of the DEFAULT_CONFIG dictionary to prevent modification of the global default
    return copy.deepcopy(DEFAULT_CONFIG)


def validate_config(config: dict) -> bool:
    """Validate pipeline configuration for required fields and valid values

    Args:
        config (dict): Configuration dictionary

    Returns:
        bool: True if configuration is valid, False otherwise
    """
    # Check if config is a dictionary
    if not isinstance(config, dict):
        logger.error("Configuration must be a dictionary")
        return False

    # Verify required sections exist (data_sources, products, fallback, validation, storage)
    required_sections = ["data_sources", "products", "fallback", "validation", "storage"]
    for section in required_sections:
        if section not in config:
            logger.error(f"Missing required configuration section: {section}")
            return False

    # Validate data_sources configuration
    if not isinstance(config["data_sources"], dict):
        logger.error("data_sources configuration must be a dictionary")
        return False

    # Validate products list (must be non-empty)
    if not isinstance(config["products"], list) or not config["products"]:
        logger.error("products configuration must be a non-empty list")
        return False

    # Validate fallback configuration (enabled flag, max_search_days)
    if not isinstance(config["fallback"], dict):
        logger.error("fallback configuration must be a dictionary")
        return False
    if "enabled" not in config["fallback"] or not isinstance(config["fallback"]["enabled"], bool):
        logger.error("fallback.enabled must be a boolean")
        return False
    if "max_search_days" not in config["fallback"] or not isinstance(config["fallback"]["max_search_days"], int):
        logger.error("fallback.max_search_days must be an integer")
        return False

    # Validate validation configuration (schema, completeness, plausibility flags)
    if not isinstance(config["validation"], dict):
        logger.error("validation configuration must be a dictionary")
        return False
    if "schema" not in config["validation"] or not isinstance(config["validation"]["schema"], bool):
        logger.error("validation.schema must be a boolean")
        return False
    if "completeness" not in config["validation"] or not isinstance(config["validation"]["completeness"], bool):
        logger.error("validation.completeness must be a boolean")
        return False
    if "plausibility" not in config["validation"] or not isinstance(config["validation"]["plausibility"], bool):
        logger.error("validation.plausibility must be a boolean")
        return False

    # Validate storage configuration (format, compression)
    if not isinstance(config["storage"], dict):
        logger.error("storage configuration must be a dictionary")
        return False
    if "format" not in config["storage"] or not isinstance(config["storage"]["format"], str):
        logger.error("storage.format must be a string")
        return False
    if "compression" not in config["storage"] or not isinstance(config["storage"]["compression"], str):
        logger.error("storage.compression must be a string")
        return False

    # Return True if all validations pass, False otherwise
    return True


def merge_configs(user_config: dict, default_config: dict) -> dict:
    """Merge user-provided configuration with default configuration

    Args:
        user_config (dict): User-provided configuration dictionary
        default_config (dict): Default configuration dictionary

    Returns:
        dict: Merged configuration dictionary
    """
    # Start with a deep copy of the default configuration
    merged_config = copy.deepcopy(default_config)

    # Recursively update with user-provided configuration values
    def update_recursive(d, u):
        for k, v in u.items():
            if isinstance(v, dict):
                d[k] = update_recursive(d.get(k, {}), v)
            else:
                d[k] = v
        return d

    merged_config = update_recursive(merged_config, user_config)

    # Return the merged configuration dictionary
    return merged_config


class PipelineExecutor:
    """Class for executing the forecasting pipeline with configuration management"""

    def __init__(self, target_date: datetime, config: dict, execution_id: str):
        """Initialize the pipeline executor with target date and configuration

        Args:
            target_date (datetime.datetime): The target date for which to generate forecasts
            config (dict): Configuration dictionary for the pipeline
            execution_id (str): Unique identifier for this execution
        """
        # Store target_date for forecasting
        self.target_date = target_date

        # Store config for pipeline configuration
        self.config = config

        # Store execution_id for tracking (generate one if not provided)
        self.execution_id = execution_id

        # Initialize results dictionary with execution_id and status='pending'
        self.results = {"execution_id": self.execution_id, "status": "pending"}

        # Set executed flag to False initially
        self.executed = False

        # Create ForecastingPipeline instance with target_date, config, and execution_id
        self.pipeline = ForecastingPipeline(self.target_date, self.config, self.execution_id)

        # Log initialization of pipeline executor
        logger.info(f"Initialized PipelineExecutor for {target_date} with execution ID {execution_id}")

    @log_execution_time
    @log_exceptions
    def execute(self) -> bool:
        """Execute the forecasting pipeline

        Returns:
            bool: True if pipeline executed successfully, False otherwise
        """
        # Check if pipeline has already been executed
        if self.executed:
            raise PipelineError("Pipeline has already been executed")

        # Log start of pipeline execution with configuration
        log_pipeline_start("forecasting_pipeline", self.execution_id, self.config)

        # Record start time for performance measurement
        start_time = time.time()

        # Execute the pipeline by calling pipeline.run()
        try:
            success = self.pipeline.run()
        except Exception as e:
            self.results["status"] = "failure"
            log_pipeline_failure("forecasting_pipeline", self.execution_id, start_time, e, self.results)
            raise PipelineExecutionError(f"Pipeline execution failed: {str(e)}", "forecasting_pipeline", self.execution_id, self.target_date) from e

        # Get pipeline results and update results dictionary
        self.results.update(self.pipeline.get_results())

        # Calculate execution time and add to results
        execution_time = time.time() - start_time
        self.results["execution_time"] = execution_time

        # Set executed flag to True
        self.executed = True

        # Log completion of pipeline execution with status
        log_pipeline_completion("forecasting_pipeline", self.execution_id, start_time, self.results)

        # Return True if pipeline executed successfully, False if fallback was used
        return success

    def get_results(self) -> dict:
        """Get the results of the pipeline execution

        Returns:
            dict: Dictionary with pipeline execution results
        """
        # If pipeline has been executed, merge with pipeline.get_results()
        if self.executed:
            self.results.update(self.pipeline.get_results())

        # Return the complete results dictionary
        return self.results

    def was_fallback_used(self) -> bool:
        """Check if fallback mechanism was used during pipeline execution

        Returns:
            bool: True if fallback was used, False otherwise
        """
        # If pipeline has been executed, return pipeline.was_fallback_used()
        if self.executed:
            return self.pipeline.was_fallback_used()

        # If pipeline has not been executed, return False
        return False

    def _validate_execution_state(self) -> bool:
        """Validate that the pipeline is in a valid state for execution

        Returns:
            bool: True if state is valid, False otherwise
        """
        # Check if target_date is a valid datetime object
        if not isinstance(self.target_date, datetime):
            logger.error("Target date must be a datetime object")
            return False

        # Verify that config is a valid dictionary
        if not isinstance(self.config, dict):
            logger.error("Config must be a dictionary")
            return False

        # Ensure execution_id is a valid string
        if not isinstance(self.execution_id, str):
            logger.error("Execution ID must be a string")
            return False

        # Return True if all validations pass, False otherwise
        return True