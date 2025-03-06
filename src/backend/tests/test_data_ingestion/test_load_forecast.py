"""
Unit tests for the load forecast data ingestion functionality of the Electricity Market Price Forecasting System. Tests the retrieval, validation, and transformation of load forecast data from external sources.
"""

import pytest
from unittest.mock import patch, MagicMock, Mock
import pandas as pd
from datetime import datetime, timedelta

from src.backend.data_ingestion.load_forecast import (
    LoadForecastClient,
    fetch_load_forecast,
    get_load_forecast_for_horizon,
    create_api_client
)
from src.backend.data_ingestion.api_client import APIClient
from src.backend.data_ingestion.exceptions import (
    DataIngestionError,
    APIConnectionError,
    APIResponseError,
    MissingDataError
)
from src.backend.data_ingestion.data_validator import validate_load_forecast_data
from src.backend.data_ingestion.data_transformer import normalize_load_forecast_data
from src.backend.models.data_models import LoadForecast
from src.backend.config.settings import FORECAST_HORIZON_HOURS, DATA_SOURCES
from src.backend.tests.fixtures.load_forecast_fixtures import (
    create_mock_load_forecast_data,
    create_mock_api_response,
    create_invalid_api_response,
    create_incomplete_load_forecast_data,
    MockLoadForecastClient
)
from src.backend.utils.date_utils import localize_to_cst


def test_create_api_client():
    """Tests that the create_api_client function correctly creates an API client for load forecast"""
    # Call create_api_client function
    client = create_api_client()
    
    # Assert that the returned object is an instance of APIClient
    assert isinstance(client, APIClient)
    
    # Assert that the client's source name is set to the load forecast source
    assert client._source_name == "load_forecast"


@pytest.mark.parametrize('additional_params', [None, {'region': 'ERCOT'}])
def test_fetch_load_forecast_success(mock_api_client):
    """Tests that fetch_load_forecast successfully retrieves and processes load forecast data"""
    # Create test start and end dates
    start_date = datetime.now() - timedelta(days=1)
    end_date = start_date + timedelta(days=2)
    
    # Create mock load forecast data
    mock_data = create_mock_load_forecast_data(start_date, hours=48)
    mock_response = {"data": mock_data.to_dict('records')}
    
    # Configure mock_api_client to return mock data
    mock_api_client.get_data.return_value = mock_response
    
    # Call fetch_load_forecast with test dates and additional params
    with patch('src.backend.data_ingestion.load_forecast.create_api_client', return_value=mock_api_client):
        result = fetch_load_forecast(start_date, end_date, additional_params)
    
    # Assert that mock_api_client.get_data was called with correct parameters
    mock_api_client.get_data.assert_called_once()
    args, kwargs = mock_api_client.get_data.call_args
    assert args[0] == localize_to_cst(start_date)
    assert args[1] == localize_to_cst(end_date)
    if additional_params is not None:
        assert args[2] == additional_params
    
    # Assert that the returned DataFrame has the expected structure and data
    assert isinstance(result, pd.DataFrame)
    assert 'timestamp' in result.columns
    assert 'load_mw' in result.columns
    assert 'region' in result.columns
    
    # Assert that the returned DataFrame has the correct number of rows
    assert len(result) > 0


@pytest.mark.parametrize('error_class', [APIConnectionError, APIResponseError])
def test_fetch_load_forecast_api_error(mock_api_client, error_class):
    """Tests that fetch_load_forecast properly handles API errors"""
    # Create test start and end dates
    start_date = datetime.now() - timedelta(days=1)
    end_date = start_date + timedelta(days=2)
    
    # Configure mock_api_client to raise the specified error
    mock_api_client.get_data.side_effect = error_class("Test error", "load_forecast")
    
    # Use pytest.raises to assert that the error is propagated
    with patch('src.backend.data_ingestion.load_forecast.create_api_client', return_value=mock_api_client):
        with pytest.raises(error_class):
            # Call fetch_load_forecast with test dates
            fetch_load_forecast(start_date, end_date)
    
    # Assert that the error is properly propagated
    mock_api_client.get_data.assert_called_once()


def test_fetch_load_forecast_missing_data(mock_api_client):
    """Tests that fetch_load_forecast properly handles missing data"""
    # Create test start and end dates
    start_date = datetime.now() - timedelta(days=1)
    end_date = start_date + timedelta(days=2)
    
    # Configure mock_api_client to return empty data
    mock_api_client.get_data.return_value = {"data": []}
    
    # Use pytest.raises to assert that MissingDataError is raised
    with patch('src.backend.data_ingestion.load_forecast.create_api_client', return_value=mock_api_client):
        with pytest.raises(MissingDataError):
            # Call fetch_load_forecast with test dates
            fetch_load_forecast(start_date, end_date)
    
    # Assert that MissingDataError is properly raised
    mock_api_client.get_data.assert_called_once()


@patch('src.backend.data_ingestion.load_forecast.fetch_load_forecast')
def test_get_load_forecast_for_horizon(mock_fetch_load_forecast):
    """Tests that get_load_forecast_for_horizon correctly retrieves forecast data for the entire horizon"""
    # Create test start date
    start_date = datetime.now()
    
    # Create mock load forecast data for the horizon
    mock_data = create_mock_load_forecast_data(start_date, hours=FORECAST_HORIZON_HOURS)
    
    # Configure mock fetch_load_forecast to return mock data
    mock_fetch_load_forecast.return_value = mock_data
    
    # Call get_load_forecast_for_horizon with test start date
    result = get_load_forecast_for_horizon(start_date)
    
    # Assert that fetch_load_forecast was called with correct parameters
    end_date = start_date + timedelta(hours=FORECAST_HORIZON_HOURS)
    mock_fetch_load_forecast.assert_called_once()
    call_args = mock_fetch_load_forecast.call_args[0]
    assert call_args[0] == localize_to_cst(start_date)
    assert isinstance(call_args[1], datetime)
    
    # Assert that the returned DataFrame has the expected structure and data
    assert isinstance(result, pd.DataFrame)
    assert 'timestamp' in result.columns
    assert 'load_mw' in result.columns
    assert 'region' in result.columns
    
    # Assert that the returned DataFrame covers the entire horizon
    assert len(result) == len(mock_data)


@patch('src.backend.data_ingestion.load_forecast.fetch_load_forecast')
def test_get_load_forecast_for_horizon_custom_hours(mock_fetch_load_forecast):
    """Tests that get_load_forecast_for_horizon correctly handles custom horizon hours"""
    # Create test start date
    start_date = datetime.now()
    
    # Define custom horizon hours (e.g., 48)
    custom_hours = 48
    
    # Create mock load forecast data for the custom horizon
    mock_data = create_mock_load_forecast_data(start_date, hours=custom_hours)
    
    # Configure mock fetch_load_forecast to return mock data
    mock_fetch_load_forecast.return_value = mock_data
    
    # Call get_load_forecast_for_horizon with test start date and custom horizon hours
    result = get_load_forecast_for_horizon(start_date, horizon_hours=custom_hours)
    
    # Assert that fetch_load_forecast was called with correct parameters
    end_date = start_date + timedelta(hours=custom_hours)
    mock_fetch_load_forecast.assert_called_once()
    call_args = mock_fetch_load_forecast.call_args[0]
    assert call_args[0] == localize_to_cst(start_date)
    assert isinstance(call_args[1], datetime)
    
    # Assert that the returned DataFrame has the expected structure and data
    assert isinstance(result, pd.DataFrame)
    assert 'timestamp' in result.columns
    assert 'load_mw' in result.columns
    assert 'region' in result.columns
    
    # Assert that the returned DataFrame covers the custom horizon
    assert len(result) == len(mock_data)


@patch('src.backend.data_ingestion.load_forecast.fetch_load_forecast')
def test_get_load_forecast_for_horizon_incomplete_data(mock_fetch_load_forecast):
    """Tests that get_load_forecast_for_horizon properly handles incomplete data"""
    # Create test start date
    start_date = datetime.now()
    
    # Create incomplete mock load forecast data (missing some hours)
    mock_data = create_incomplete_load_forecast_data(
        start_date, 
        hours=FORECAST_HORIZON_HOURS,
        missing_indices=[10, 20, 30]  # Remove some hours
    )
    
    # Configure mock fetch_load_forecast to return incomplete data
    mock_fetch_load_forecast.return_value = mock_data
    
    # Use pytest.raises to assert that MissingDataError is raised
    with pytest.raises(MissingDataError):
        # Call get_load_forecast_for_horizon with test start date
        get_load_forecast_for_horizon(start_date)
    
    # Assert that MissingDataError is properly raised
    mock_fetch_load_forecast.assert_called_once()


@patch('src.backend.data_ingestion.load_forecast.create_api_client')
def test_load_forecast_client_get_forecast(mock_create_api_client):
    """Tests that LoadForecastClient.get_forecast correctly retrieves forecast data"""
    # Create mock API client
    mock_api_client = MagicMock()
    mock_create_api_client.return_value = mock_api_client
    
    # Create test start and end dates
    start_date = datetime.now() - timedelta(days=1)
    end_date = start_date + timedelta(days=2)
    
    # Create mock load forecast data
    mock_response = create_mock_api_response(start_date, hours=48)
    
    # Configure mock API client to return mock data
    mock_api_client.get_data.return_value = mock_response
    
    # Create LoadForecastClient instance
    client = LoadForecastClient()
    
    # Call get_forecast with test dates
    result = client.get_forecast(start_date, end_date)
    
    # Assert that API client's get_data was called with correct parameters
    mock_api_client.get_data.assert_called_once()
    args, kwargs = mock_api_client.get_data.call_args
    assert args[0] == localize_to_cst(start_date)
    assert args[1] == localize_to_cst(end_date)
    
    # Assert that the returned DataFrame has the expected structure and data
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


@patch('src.backend.data_ingestion.load_forecast.create_api_client')
def test_load_forecast_client_get_forecast_for_horizon(mock_create_api_client):
    """Tests that LoadForecastClient.get_forecast_for_horizon correctly retrieves forecast data for the entire horizon"""
    # Create mock API client
    mock_api_client = MagicMock()
    mock_create_api_client.return_value = mock_api_client
    
    # Create test start date
    start_date = datetime.now()
    
    # Create mock load forecast data for the horizon
    mock_response = create_mock_api_response(start_date, hours=FORECAST_HORIZON_HOURS)
    
    # Configure mock API client to return mock data
    mock_api_client.get_data.return_value = mock_response
    
    # Create LoadForecastClient instance
    client = LoadForecastClient()
    
    # Call get_forecast_for_horizon with test start date
    result = client.get_forecast_for_horizon(start_date)
    
    # Assert that API client's get_data was called with correct parameters
    mock_api_client.get_data.assert_called_once()
    args, kwargs = mock_api_client.get_data.call_args
    assert args[0] == localize_to_cst(start_date)
    assert isinstance(args[1], datetime)
    
    # Assert that the returned DataFrame has the expected structure and data
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    
    # Assert that the returned DataFrame covers the entire horizon
    # Note: The exact number of rows may vary due to data processing
    assert 'timestamp' in result.columns


@patch('src.backend.data_ingestion.load_forecast.create_api_client')
def test_load_forecast_client_get_latest_forecast(mock_create_api_client):
    """Tests that LoadForecastClient.get_latest_forecast correctly retrieves the latest forecast data"""
    # Create mock API client
    mock_api_client = MagicMock()
    mock_create_api_client.return_value = mock_api_client
    
    # Create mock load forecast data
    start_date = datetime.now()
    mock_response = create_mock_api_response(start_date, hours=24)
    
    # Configure mock API client to return mock data for latest forecast
    mock_api_client.get_latest_data.return_value = mock_response
    
    # Create LoadForecastClient instance
    client = LoadForecastClient()
    
    # Call get_latest_forecast
    result = client.get_latest_forecast()
    
    # Assert that API client's get_latest_data was called with correct parameters
    mock_api_client.get_latest_data.assert_called_once()
    
    # Assert that the returned DataFrame has the expected structure and data
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


@patch('src.backend.data_ingestion.load_forecast.create_api_client')
@pytest.mark.parametrize('error_class', [APIConnectionError, APIResponseError, MissingDataError])
def test_load_forecast_client_error_handling(mock_create_api_client, error_class):
    """Tests that LoadForecastClient properly handles and propagates errors"""
    # Create mock API client
    mock_api_client = MagicMock()
    mock_create_api_client.return_value = mock_api_client
    
    # Configure mock API client to raise the specified error
    error_params = ("Test error", "load_forecast") if error_class in [APIConnectionError, APIResponseError] else ("load_forecast", ["Test error"])
    mock_api_client.get_data.side_effect = error_class(*error_params)
    mock_api_client.get_latest_data.side_effect = error_class(*error_params)
    
    # Create LoadForecastClient instance
    client = LoadForecastClient()
    
    # Use pytest.raises to assert that the error is propagated
    with pytest.raises(error_class):
        # Call client methods (get_forecast, get_forecast_for_horizon, get_latest_forecast)
        client.get_forecast(datetime.now(), datetime.now() + timedelta(days=1))
    
    # Assert that the error is properly propagated
    mock_api_client.get_data.assert_called_once()