"""
Initialization module for the configuration package of the Electricity Market Price Forecasting System.

This module exports settings, logging configuration, and schema definitions for use throughout 
the application. It also initializes the logging system and ensures required directories exist.
"""

import os
from pathlib import Path

# Import configuration modules to expose their contents
from . import settings
from . import logging_config
from . import schema_config

# Direct exports for convenience
from .logging_config import setup_logging
from .settings import (
    BASE_DIR, 
    ENVIRONMENT, 
    DEBUG, 
    TIMEZONE,
    FORECAST_SCHEDULE_TIME,
    FORECAST_PRODUCTS,
    FORECAST_HORIZON_HOURS,
    PROBABILISTIC_SAMPLE_COUNT,
    STORAGE_ROOT_DIR,
    STORAGE_LATEST_DIR,
    STORAGE_INDEX_FILE,
    DATA_SOURCES,
    API_HOST,
    API_PORT
)
from .schema_config import (
    FORECAST_BASE_SCHEMA,
    FORECAST_OUTPUT_SCHEMA,
    LOAD_FORECAST_SCHEMA,
    HISTORICAL_PRICE_SCHEMA,
    GENERATION_FORECAST_SCHEMA
)

def initialize_config():
    """
    Initializes the configuration package by setting up logging and ensuring required directories exist.
    
    This function should be called when the application starts to ensure proper configuration.
    It sets up the logging system and creates necessary storage directories.
    """
    # Set up logging system
    logging_config.setup_logging()
    
    # Load environment variables if not already loaded
    settings.load_environment_variables()
    
    # Ensure storage directories exist
    storage_dirs = [
        STORAGE_ROOT_DIR,
        STORAGE_LATEST_DIR
    ]
    for directory in storage_dirs:
        Path(directory).mkdir(parents=True, exist_ok=True)
    
    # Log configuration initialization
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"Configuration initialized for environment: {ENVIRONMENT}")
    logger.info(f"Forecast schedule time set to: {FORECAST_SCHEDULE_TIME} {TIMEZONE}")
    logger.info(f"Storage configured at: {STORAGE_ROOT_DIR}")


# Define what should be exported from this module
__all__ = [
    "settings", "logging_config", "schema_config", "setup_logging", 
    "initialize_config", "BASE_DIR", "ENVIRONMENT", "DEBUG", "TIMEZONE",
    "FORECAST_SCHEDULE_TIME", "FORECAST_PRODUCTS", "FORECAST_HORIZON_HOURS",
    "PROBABILISTIC_SAMPLE_COUNT", "STORAGE_ROOT_DIR", "STORAGE_LATEST_DIR",
    "STORAGE_INDEX_FILE", "DATA_SOURCES", "API_HOST", "API_PORT",
    "FORECAST_BASE_SCHEMA", "FORECAST_OUTPUT_SCHEMA", "LOAD_FORECAST_SCHEMA",
    "HISTORICAL_PRICE_SCHEMA", "GENERATION_FORECAST_SCHEMA"
]

# Initialize configuration when this module is imported
initialize_config()