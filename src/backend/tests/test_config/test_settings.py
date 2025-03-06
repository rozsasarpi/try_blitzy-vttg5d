"""
Unit tests for the settings module of the Electricity Market Price Forecasting System.

This file verifies that configuration settings are properly loaded, environment
variables are correctly processed, and path generation functions work as expected.
"""

import os
import datetime
import pytz
import tempfile
from unittest.mock import patch

import pytest

from src.backend.config import settings


def test_base_dir_exists():
    """Test that BASE_DIR points to a valid directory."""
    assert settings.BASE_DIR.exists()
    assert settings.BASE_DIR.is_dir()


@patch('os.getenv', return_value=None)
def test_environment_default(mock_getenv):
    """Test that ENVIRONMENT has a default value when not set."""
    # Mock os.getenv to return None specifically for 'ENVIRONMENT'
    mock_getenv.side_effect = lambda key, default=None: default if key == 'ENVIRONMENT' else os.getenv(key, default)
    
    # Import settings with mocked os.getenv
    import importlib
    from src.backend.config import settings as test_settings
    importlib.reload(test_settings)
    
    # Assert that ENVIRONMENT equals 'development'
    assert test_settings.ENVIRONMENT == 'development'


def test_debug_conversion():
    """Test that DEBUG correctly converts string values to boolean."""
    test_cases = [
        ('True', True),
        ('true', True),
        ('1', True),
        ('t', True),
        ('False', False),
        ('false', False),
        ('0', False),
        ('f', False),
        ('anything_else', False)
    ]
    
    for input_str, expected_bool in test_cases:
        with patch('os.getenv') as mock_getenv:
            # For each test case, mock os.getenv to return the input string
            mock_getenv.side_effect = lambda key, default=None: input_str if key == 'DEBUG' else os.getenv(key, default)
            
            # Import settings with mocked os.getenv
            import importlib
            from src.backend.config import settings as test_settings
            importlib.reload(test_settings)
            
            # Assert that DEBUG equals the expected boolean value
            assert test_settings.DEBUG == expected_bool


def test_timezone_is_cst():
    """Test that TIMEZONE is set to Central Standard Time."""
    assert settings.TIMEZONE.zone == 'America/Chicago'
    
    # Create a datetime in the timezone to check if it's in Central time
    dt = datetime.datetime.now(settings.TIMEZONE)
    tz_name = dt.strftime('%Z')
    assert tz_name in ['CST', 'CDT'], f"Expected CST or CDT, got {tz_name}"


def test_forecast_schedule_time():
    """Test that FORECAST_SCHEDULE_TIME is set to 7 AM."""
    assert settings.FORECAST_SCHEDULE_TIME.hour == 7
    assert settings.FORECAST_SCHEDULE_TIME.minute == 0
    assert settings.FORECAST_SCHEDULE_TIME.second == 0


def test_forecast_products_list():
    """Test that FORECAST_PRODUCTS contains all required products."""
    assert isinstance(settings.FORECAST_PRODUCTS, list)
    assert 'DALMP' in settings.FORECAST_PRODUCTS
    assert 'RTLMP' in settings.FORECAST_PRODUCTS
    
    # Check for ancillary service products
    ancillary_products = ['RegUp', 'RegDown', 'RRS', 'NSRS']
    for product in ancillary_products:
        assert product in settings.FORECAST_PRODUCTS


def test_forecast_horizon_hours():
    """Test that FORECAST_HORIZON_HOURS is set to 72."""
    assert settings.FORECAST_HORIZON_HOURS == 72
    assert isinstance(settings.FORECAST_HORIZON_HOURS, int)


def test_probabilistic_sample_count():
    """Test that PROBABILISTIC_SAMPLE_COUNT is set to 100."""
    assert settings.PROBABILISTIC_SAMPLE_COUNT == 100
    assert isinstance(settings.PROBABILISTIC_SAMPLE_COUNT, int)


def test_storage_directories():
    """Test that storage directories are correctly defined."""
    assert 'forecasts' in settings.STORAGE_ROOT_DIR
    assert 'latest' in settings.STORAGE_LATEST_DIR
    assert 'index.parquet' in settings.STORAGE_INDEX_FILE


def test_data_sources_configuration():
    """Test that DATA_SOURCES contains all required sources."""
    assert isinstance(settings.DATA_SOURCES, dict)
    assert 'load_forecast' in settings.DATA_SOURCES
    assert 'historical_prices' in settings.DATA_SOURCES
    assert 'generation_forecast' in settings.DATA_SOURCES
    
    for source_name, source_config in settings.DATA_SOURCES.items():
        assert 'url' in source_config
        assert 'api_key' in source_config


@patch('os.path.exists', return_value=True)
@patch('dotenv.load_dotenv', return_value=True)
def test_load_environment_variables(mock_load_dotenv, mock_path_exists):
    """Test that load_environment_variables correctly loads from .env file."""
    result = settings.load_environment_variables()
    
    mock_path_exists.assert_called_once_with(settings.ENV_FILE)
    mock_load_dotenv.assert_called_once_with(settings.ENV_FILE)
    assert result is True


@patch('os.path.exists', return_value=False)
def test_load_environment_variables_no_file(mock_path_exists):
    """Test that load_environment_variables handles missing .env file."""
    result = settings.load_environment_variables()
    
    mock_path_exists.assert_called_once_with(settings.ENV_FILE)
    assert result is False


def test_get_storage_path_for_date():
    """Test that get_storage_path_for_date generates correct paths."""
    test_date = datetime.date(2023, 6, 1)
    path = settings.get_storage_path_for_date(test_date)
    
    assert '2023' in path
    assert '06' in path
    assert path.startswith(settings.STORAGE_ROOT_DIR)


@patch('src.backend.config.settings.ENVIRONMENT', 'development')
def test_get_environment_settings_development():
    """Test that get_environment_settings returns correct development settings."""
    env_settings = settings.get_environment_settings()
    
    assert env_settings['debug'] == settings.DEBUG
    assert env_settings['fallback_enabled'] is True
    assert env_settings['validation_strict'] is False
    assert env_settings['log_format'] == 'detailed'


@patch('src.backend.config.settings.ENVIRONMENT', 'production')
def test_get_environment_settings_production():
    """Test that get_environment_settings returns correct production settings."""
    env_settings = settings.get_environment_settings()
    
    assert env_settings['debug'] == settings.DEBUG
    assert env_settings['fallback_enabled'] is True
    assert env_settings['validation_strict'] is True
    assert env_settings['log_format'] == 'standard'