"""
Settings configuration for the Electricity Market Price Forecasting System web visualization component.

This file defines application-wide settings, constants, and configuration parameters used
throughout the Dash-based visualization interface.
"""

import os
from pathlib import Path
import datetime
import pytz  # version 2023.3

# Base directory of the application
BASE_DIR = Path(__file__).resolve().parent.parent

# Debug mode - defaults to False for security
DEBUG = os.getenv('DEBUG', 'False').lower() in ('true', '1', 't')

# Current environment - defaults to development
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

# Logging level - defaults to INFO
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Secret key for security features
SECRET_KEY = os.getenv('SECRET_KEY', 'default-dev-key-change-in-production')

# API base URL
API_BASE_URL = os.getenv('API_BASE_URL', 'http://localhost:8000/api')

# API timeout in seconds
FORECAST_API_TIMEOUT = int(os.getenv('FORECAST_API_TIMEOUT', 30))

# Maximum number of forecast days to display (72 hours = 3 days)
MAX_FORECAST_DAYS = 3

# Default date and time formats
DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_TIME_FORMAT = '%H:%M'
DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# CST timezone for consistent time handling
CST_TIMEZONE = pytz.timezone('America/Chicago')

# Time when forecasts are generated daily (7 AM CST)
FORECAST_GENERATION_TIME = '07:00'

# Dashboard refresh interval in seconds (default: 5 minutes)
DASHBOARD_REFRESH_INTERVAL_SECONDS = int(os.getenv('DASHBOARD_REFRESH_INTERVAL_SECONDS', 300))

# Enable responsive UI features
ENABLE_RESPONSIVE_UI = os.getenv('ENABLE_RESPONSIVE_UI', 'True').lower() in ('true', '1', 't')

# Caching configuration
CACHE_ENABLED = os.getenv('CACHE_ENABLED', 'True').lower() in ('true', '1', 't')
CACHE_TIMEOUT = int(os.getenv('CACHE_TIMEOUT', 300))
CACHE_DIR = os.path.join(BASE_DIR, 'cache')

# Server configuration
ALLOWED_HOSTS = os.getenv('ALLOWED_HOSTS', 'localhost,127.0.0.1').split(',')
SERVER_PORT = int(os.getenv('SERVER_PORT', 8050))
SERVER_HOST = os.getenv('SERVER_HOST', '0.0.0.0')

# Authentication
ENABLE_AUTH = os.getenv('ENABLE_AUTH', 'False').lower() in ('true', '1', 't')

# Fallback indicator
FALLBACK_INDICATOR_ENABLED = True

# Analytics
ENABLE_ANALYTICS = os.getenv('ENABLE_ANALYTICS', 'False').lower() in ('true', '1', 't')


def get_abs_path(relative_path: str) -> Path:
    """
    Returns an absolute path from a path relative to the base directory.
    
    Args:
        relative_path: A path relative to the base directory
        
    Returns:
        An absolute Path object
    """
    return BASE_DIR / relative_path


def get_env_bool(name: str, default: bool) -> bool:
    """
    Gets a boolean value from an environment variable with a default.
    
    Args:
        name: The name of the environment variable
        default: The default value if the environment variable is not set
        
    Returns:
        Boolean value from environment or default
    """
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in ('true', '1', 't')


def get_env_int(name: str, default: int) -> int:
    """
    Gets an integer value from an environment variable with a default.
    
    Args:
        name: The name of the environment variable
        default: The default value if the environment variable is not set
        
    Returns:
        Integer value from environment or default
    """
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def get_env_list(name: str, default: list) -> list:
    """
    Gets a list of values from a comma-separated environment variable.
    
    Args:
        name: The name of the environment variable
        default: The default value if the environment variable is not set
        
    Returns:
        List of values from environment or default
    """
    value = os.getenv(name)
    if value is None:
        return default
    return [item.strip() for item in value.split(',')]


def is_production() -> bool:
    """
    Checks if the current environment is production.
    
    Returns:
        True if in production environment, False otherwise
    """
    return ENVIRONMENT == 'production'


def is_development() -> bool:
    """
    Checks if the current environment is development.
    
    Returns:
        True if in development environment, False otherwise
    """
    return ENVIRONMENT == 'development'


def get_forecast_generation_time() -> datetime.time:
    """
    Returns the forecast generation time as a datetime.time object.
    
    Returns:
        Daily forecast generation time (7 AM CST)
    """
    hour, minute = map(int, FORECAST_GENERATION_TIME.split(':'))
    return datetime.time(hour=hour, minute=minute)