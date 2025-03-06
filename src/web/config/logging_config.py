"""
Logging configuration for the Electricity Market Price Forecasting System's web visualization component.

This module provides configuration and utilities for setting up logging across the web application.
It supports different log levels, console and file handlers, and can be configured through
environment variables or a YAML configuration file.
"""

import os
import logging
import logging.config
import logging.handlers
import yaml
from pathlib import Path

from .settings import BASE_DIR, LOG_LEVEL, DEBUG, ENVIRONMENT

# Directory where log files will be stored
LOG_DIR = os.path.join(BASE_DIR, 'logs')

# Path to the logging configuration file (YAML)
LOG_CONFIG_FILE = os.path.join(BASE_DIR, 'logging_config.yaml')

# Default format for log messages
DEFAULT_LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'

# Default date format for log timestamps
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
    'dashboard': logging.INFO,
    'data': logging.INFO,
    'callbacks': logging.INFO,
    'components': logging.INFO,
    'layouts': logging.INFO,
    'utils': logging.INFO,
    'services': logging.INFO
}


def ensure_log_directory():
    """
    Ensures that the log directory exists, creating it if necessary.
    """
    try:
        path = Path(LOG_DIR)
        path.mkdir(parents=True, exist_ok=True)
        print(f"Log directory ensured: {LOG_DIR}")  # Use print as logging is not configured yet
    except Exception as e:
        print(f"Error creating log directory: {e}")
        # Fallback to temp directory if necessary
        global LOG_DIR
        LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
        print(f"Using fallback log directory: {LOG_DIR}")


def get_log_level(level_name):
    """
    Converts string log level to logging module constant.
    
    Args:
        level_name: String representation of log level
        
    Returns:
        Logging level constant from logging module
    """
    if not level_name:
        return logging.INFO
        
    level_name = level_name.upper()
    return LOG_LEVELS.get(level_name, logging.INFO)


def configure_console_handler():
    """
    Creates and configures a console handler for logging.
    
    Returns:
        Configured console handler
    """
    handler = logging.StreamHandler()
    # Use DEBUG level in development, INFO otherwise
    handler.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)
    handler.setFormatter(formatter)
    return handler


def configure_file_handler(filename, level):
    """
    Creates and configures a rotating file handler for logging.
    
    Args:
        filename: Base name for the log file
        level: Logging level for the handler
        
    Returns:
        Configured file handler
    """
    log_file = os.path.join(LOG_DIR, filename)
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5 if ENVIRONMENT == 'development' else 10
    )
    handler.setLevel(level)
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)
    handler.setFormatter(formatter)
    return handler


def override_log_levels(config):
    """
    Overrides log levels in configuration based on environment settings.
    
    Args:
        config: Logging configuration dictionary
        
    Returns:
        Updated configuration with overridden log levels
    """
    # Override root logger level
    root_level = get_log_level(LOG_LEVEL)
    if 'root' in config:
        config['root']['level'] = root_level
    
    # Override component logger levels
    if 'loggers' in config:
        for component, level in COMPONENT_LOG_LEVELS.items():
            component_name = f"web.{component}"
            if component_name in config['loggers']:
                if DEBUG:
                    # Use DEBUG level in debug mode regardless of component setting
                    config['loggers'][component_name]['level'] = logging.DEBUG
                else:
                    config['loggers'][component_name]['level'] = level
    
    return config


def get_logger(name):
    """
    Gets a configured logger for a specific component.
    
    Args:
        name: Name for the logger (typically module or component name)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure if logger doesn't have handlers already
    if not logger.handlers:
        # Determine appropriate log level based on component name
        component_name = name.split('.')[-1] if '.' in name else name
        level = logging.DEBUG if DEBUG else COMPONENT_LOG_LEVELS.get(component_name, logging.INFO)
        
        # Configure logger
        logger.setLevel(level)
        
        # Add console handler
        logger.addHandler(configure_console_handler())
        
        # Add file handler for component-specific logs
        logger.addHandler(configure_file_handler(f"{component_name}.log", level))
        
        # Don't propagate to root logger
        logger.propagate = False
    
    return logger


def configure_logging():
    """
    Sets up the logging configuration for the web application.
    """
    # Ensure log directory exists
    ensure_log_directory()
    
    try:
        # Check if config file exists
        if os.path.exists(LOG_CONFIG_FILE):
            try:
                # Load config from YAML file
                with open(LOG_CONFIG_FILE, 'r') as f:
                    config = yaml.safe_load(f)
                
                # Apply environment-specific overrides
                config = override_log_levels(config)
                
                # Configure logging from dictionary
                logging.config.dictConfig(config)
                
                # Get a logger to indicate successful configuration
                root_logger = logging.getLogger()
                root_logger.info(f"Configured logging from file: {LOG_CONFIG_FILE}")
                root_logger.info(f"Log directory: {LOG_DIR}")
                root_logger.info(f"Root log level: {logging.getLevelName(root_logger.level)}")
                root_logger.info(f"Environment: {ENVIRONMENT}, Debug mode: {DEBUG}")
            
            except Exception as e:
                # Fallback to basic configuration if YAML file is invalid
                print(f"Error loading logging config file: {e}")
                # Configure basic logging
                logging.basicConfig(
                    level=get_log_level(LOG_LEVEL),
                    format=DEFAULT_LOG_FORMAT,
                    datefmt=DEFAULT_DATE_FORMAT,
                    handlers=[
                        logging.StreamHandler(),
                        logging.handlers.RotatingFileHandler(
                            os.path.join(LOG_DIR, 'web.log'),
                            maxBytes=10 * 1024 * 1024,
                            backupCount=5 if ENVIRONMENT == 'development' else 10
                        )
                    ]
                )
                logging.info("Configured logging with basic settings due to config file error")
        else:
            # Basic configuration if no config file exists
            logging.basicConfig(
                level=get_log_level(LOG_LEVEL),
                format=DEFAULT_LOG_FORMAT,
                datefmt=DEFAULT_DATE_FORMAT,
                handlers=[
                    logging.StreamHandler(),
                    logging.handlers.RotatingFileHandler(
                        os.path.join(LOG_DIR, 'web.log'),
                        maxBytes=10 * 1024 * 1024,
                        backupCount=5 if ENVIRONMENT == 'development' else 10
                    )
                ]
            )
            logging.info("Configured logging with basic settings")
            logging.info(f"Log directory: {LOG_DIR}")
            logging.info(f"Environment: {ENVIRONMENT}, Debug mode: {DEBUG}")
    
    except Exception as e:
        # Last resort fallback
        print(f"Critical error configuring logging: {e}")
        logging.basicConfig(level=logging.INFO)
        logging.error(f"Failed to configure logging properly: {e}")