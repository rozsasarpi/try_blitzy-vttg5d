"""
Configuration module for the logging system of the Electricity Market Price Forecasting System.

This module defines the logging setup, configuration loading, and provides utilities
for configuring loggers with appropriate levels and handlers. It supports both
file-based and console logging with different levels based on the environment.
"""

import os
import logging
import logging.config
import logging.handlers
import yaml
from pathlib import Path

from .settings import BASE_DIR, LOG_LEVEL, DEBUG, ENVIRONMENT

# Constants
LOG_DIR = os.path.join(BASE_DIR, 'logs')
LOG_CONFIG_FILE = os.path.join(BASE_DIR, 'logging_config.yaml')
DEFAULT_LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Mapping of string log levels to logging module constants
LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

# Default log levels for different components
COMPONENT_LOG_LEVELS = {
    "data_ingestion": logging.INFO,
    "feature_engineering": logging.INFO,
    "forecasting_engine": logging.INFO,
    "forecast_validation": logging.INFO,
    "storage": logging.INFO,
    "fallback": logging.INFO,
    "scheduler": logging.INFO,
    "pipeline": logging.INFO,
    "api": logging.INFO
}

def ensure_log_directory():
    """
    Ensures that the log directory exists, creating it if necessary.
    """
    if not os.path.exists(LOG_DIR):
        Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    # No logging here since this is called during logging setup

def get_log_level(level_name):
    """
    Converts string log level to logging module constant.
    
    Args:
        level_name (str): Name of the log level (e.g., 'INFO', 'DEBUG')
        
    Returns:
        int: Logging level constant from logging module
    """
    level_name = level_name.upper()
    return LOG_LEVELS.get(level_name, logging.INFO)

def configure_console_handler():
    """
    Creates and configures a console handler for logging.
    
    Returns:
        logging.StreamHandler: Configured console handler
    """
    console_handler = logging.StreamHandler()
    # In development mode, use DEBUG level for console, otherwise use INFO
    console_handler.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)
    console_handler.setFormatter(formatter)
    return console_handler

def configure_file_handler(filename, level):
    """
    Creates and configures a rotating file handler for logging.
    
    Args:
        filename (str): Name of the log file
        level (int): Logging level for the handler
        
    Returns:
        logging.handlers.RotatingFileHandler: Configured file handler
    """
    log_file = os.path.join(LOG_DIR, filename)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5 if ENVIRONMENT == 'development' else 10,
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)
    file_handler.setFormatter(formatter)
    return file_handler

def override_log_levels(config):
    """
    Overrides log levels in configuration based on environment settings.
    
    Args:
        config (dict): Logging configuration dictionary
        
    Returns:
        dict: Updated configuration with overridden log levels
    """
    # Get the global log level from settings
    global_level = get_log_level(LOG_LEVEL)
    
    # Override root logger level
    if 'root' in config:
        config['root']['level'] = global_level
    
    # Override specific logger levels
    if 'loggers' in config:
        for logger_name, logger_config in config['loggers'].items():
            # Check if this logger corresponds to a component with a defined log level
            if logger_name in COMPONENT_LOG_LEVELS:
                logger_config['level'] = COMPONENT_LOG_LEVELS[logger_name]
            else:
                # Use global level for other loggers
                logger_config['level'] = global_level
    
    return config

def setup_logging():
    """
    Sets up the logging configuration for the entire application.
    
    This function first checks for a YAML configuration file. If found, it loads and uses
    that configuration with environment-specific overrides. Otherwise, it sets up a basic
    configuration with console and file handlers.
    """
    # Ensure log directory exists
    ensure_log_directory()
    
    # Check if config file exists
    if os.path.exists(LOG_CONFIG_FILE):
        # Load YAML config
        with open(LOG_CONFIG_FILE, 'rt') as f:
            config = yaml.safe_load(f.read())
        
        # Apply environment-specific overrides
        config = override_log_levels(config)
        
        # Configure logging using dictionary
        logging.config.dictConfig(config)
        logging.info(f"Configured logging from {LOG_CONFIG_FILE}")
    else:
        # Set up basic configuration
        root_logger = logging.getLogger()
        root_logger.setLevel(get_log_level(LOG_LEVEL))
        
        # Clear existing handlers (to avoid duplicates)
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Add console handler
        root_logger.addHandler(configure_console_handler())
        
        # Add file handlers
        root_logger.addHandler(configure_file_handler('app.log', logging.INFO))
        root_logger.addHandler(configure_file_handler('error.log', logging.ERROR))
        
        # Configure component-specific loggers
        for component, level in COMPONENT_LOG_LEVELS.items():
            component_logger = logging.getLogger(component)
            component_logger.setLevel(level)
        
        logging.info("Configured logging with basic configuration")