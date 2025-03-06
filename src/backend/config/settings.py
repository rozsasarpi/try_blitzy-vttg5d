"""
Central configuration module for the Electricity Market Price Forecasting System.

This module contains environment-specific settings, constants, and configuration 
parameters used throughout the application for forecast generation, storage, and 
visualization.
"""

import os
from pathlib import Path
import datetime
import pytz  # version: 2023.3
from dotenv import load_dotenv  # version: 1.0.0

# Base directory of the project (3 levels up from this file: src/)
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Path to environment variables file
ENV_FILE = os.path.join(BASE_DIR, '.env')

# Load environment variables from .env file if it exists
def load_environment_variables():
    """
    Loads environment variables from .env file if it exists.
    
    Returns:
        bool: True if .env file was loaded, False otherwise
    """
    if os.path.exists(ENV_FILE):
        load_dotenv(ENV_FILE)
        return True
    return False

# Call function to load environment variables
load_environment_variables()

# Environment settings
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
DEBUG = os.getenv('DEBUG', 'True').lower() in ('true', '1', 't')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Timezone settings (CST for 7 AM scheduling)
TIMEZONE = pytz.timezone('America/Chicago')
FORECAST_SCHEDULE_TIME = datetime.time(7, 0, 0)

# Forecasting parameters
FORECAST_PRODUCTS = ['DALMP', 'RTLMP', 'RegUp', 'RegDown', 'RRS', 'NSRS']
FORECAST_HORIZON_HOURS = 72
PROBABILISTIC_SAMPLE_COUNT = 100

# Storage paths
STORAGE_ROOT_DIR = os.path.join(BASE_DIR, 'data', 'forecasts')
STORAGE_LATEST_DIR = os.path.join(STORAGE_ROOT_DIR, 'latest')
STORAGE_INDEX_FILE = os.path.join(STORAGE_ROOT_DIR, 'index.parquet')

# External data source configuration
DATA_SOURCES = {
    "load_forecast": {
        "url": os.getenv('LOAD_FORECAST_URL', 'http://example.com/api/load-forecast'),
        "api_key": os.getenv('LOAD_FORECAST_API_KEY', '')
    },
    "historical_prices": {
        "url": os.getenv('HISTORICAL_PRICES_URL', 'http://example.com/api/historical-prices'),
        "api_key": os.getenv('HISTORICAL_PRICES_API_KEY', '')
    },
    "generation_forecast": {
        "url": os.getenv('GENERATION_FORECAST_URL', 'http://example.com/api/generation-forecast'),
        "api_key": os.getenv('GENERATION_FORECAST_API_KEY', '')
    }
}

# API settings
API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('API_PORT', 5000))

def get_storage_path_for_date(date):
    """
    Generates a storage path for a specific date.
    
    Args:
        date (datetime.date): Date to generate path for
        
    Returns:
        str: Path to the storage directory for the specified date
    """
    year = date.strftime('%Y')
    month = date.strftime('%m')
    return os.path.join(STORAGE_ROOT_DIR, year, month)

def get_environment_settings():
    """
    Returns environment-specific settings.
    
    Returns:
        dict: Dictionary of environment-specific settings
    """
    # Base settings for all environments
    settings = {
        'debug': DEBUG,
        'log_level': LOG_LEVEL,
        'storage_root': STORAGE_ROOT_DIR,
    }
    
    # Environment-specific settings
    if ENVIRONMENT == 'development':
        settings.update({
            'fallback_enabled': True,
            'validation_strict': False,
            'log_format': 'detailed',
        })
    elif ENVIRONMENT == 'staging':
        settings.update({
            'fallback_enabled': True,
            'validation_strict': True,
            'log_format': 'standard',
        })
    elif ENVIRONMENT == 'production':
        settings.update({
            'fallback_enabled': True,
            'validation_strict': True,
            'log_format': 'standard',
        })
    
    return settings