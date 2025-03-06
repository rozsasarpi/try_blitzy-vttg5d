"""
Unit tests for the historical_prices module in the data ingestion component of the Electricity Market Price Forecasting System.
Tests the functionality for retrieving, validating, and processing historical price data from external sources.
"""

# Standard library imports
import datetime
from unittest.mock import patch

# External imports
import pandas as pd  # version: 2.0.0+
import pytest  # version: 7.0.0+

# Internal imports
from src.backend.data_ingestion.historical_prices import (
    fetch_historical_prices,
    get_historical_prices_for_model,
    filter_prices_by_product,
    pivot_prices_by_product,
    calculate_price_statistics,
    HistoricalPriceClient,
)
from src.backend.data_ingestion.api_client import APIClient
from src.backend.data_ingestion.exceptions import (
    APIConnectionError,
    APIResponseError,
    DataValidationError,
)
from src.backend.data_ingestion.data_validator import validate_historical_prices_data
from src.backend.models.data_models import HistoricalPrice
from src.backend.config.settings import FORECAST_PRODUCTS
from src.backend.tests.fixtures.historical_prices_fixtures import (
    create_mock_historical_price_data,
    create_mock_historical_price_models,
    create_incomplete_historical_price_data,
    create_invalid_historical_price_data,
    create_mock_api_response,
    MockHistoricalPriceClient,
)
from src.backend.tests.conftest import historical_price_client, mock_historical_price_data
from src.backend.utils.date_utils import localize_to_cst


@pytest.mark.parametrize('products', [None, ['DALMP'], ['DALMP', 'RTLMP']])
def test_fetch_historical_prices_success(historical_price_client, mock_historical_price_data, products):
    """Tests successful retrieval of historical price data"""
    # Create mock historical price data
    # Create mock API response from the data
    mock_api_response = create_mock_api_response(mock_historical_price_data)
    # Mock the APIClient.get_data method to return the mock response
    historical_price_client.add_response({}, mock_api_response)
    # Call fetch_historical_prices with test parameters
    start_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    end_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    df = fetch_historical_prices(start_date, end_date, products)
    # Verify the returned DataFrame matches the expected structure and data
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert 'timestamp' in df.columns
    assert 'product' in df.columns
    assert 'price' in df.columns
    assert 'node' in df.columns
    # Verify the correct API parameters were used in the request
    # (This is implicitly verified by the mock setup)


def test_fetch_historical_prices_api_error(historical_price_client):
    """Tests error handling when API connection fails"""
    # Mock the APIClient.get_data method to raise APIConnectionError
    historical_price_client.set_error(APIConnectionError("API Endpoint", "historical_prices", Exception("Connection failed")))
    # Use pytest.raises to verify that fetch_historical_prices raises APIConnectionError
    with pytest.raises(APIConnectionError):
        # Call fetch_historical_prices with test parameters
        start_date = localize_to_cst(datetime.datetime(2023, 1, 1))
        end_date = localize_to_cst(datetime.datetime(2023, 1, 2))
        fetch_historical_prices(start_date, end_date, ['DALMP'])
    # Verify the error is propagated correctly


@pytest.mark.parametrize('invalid_type', ['missing_columns', 'invalid_product', 'invalid_timestamp', 'invalid_price'])
def test_fetch_historical_prices_validation_error(historical_price_client, invalid_type):
    """Tests error handling when data validation fails"""
    # Create invalid historical price data based on invalid_type parameter
    start_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    end_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    invalid_data = create_invalid_historical_price_data(start_date, end_date, invalid_type)
    # Create mock API response from the invalid data
    mock_api_response = create_mock_api_response(invalid_data)
    # Mock the APIClient.get_data method to return the mock response
    historical_price_client.add_response({}, mock_api_response)
    # Use pytest.raises to verify that fetch_historical_prices raises DataValidationError
    with pytest.raises(DataValidationError):
        # Call fetch_historical_prices with test parameters
        start_date = localize_to_cst(datetime.datetime(2023, 1, 1))
        end_date = localize_to_cst(datetime.datetime(2023, 1, 2))
        fetch_historical_prices(start_date, end_date, ['DALMP'])
    # Verify the validation error is raised with appropriate details


def test_get_historical_prices_for_model(historical_price_client, mock_historical_price_data):
    """Tests retrieval and processing of historical price data for model input"""
    # Create mock historical price data
    # Mock the fetch_historical_prices function to return the mock data
    historical_price_client.add_response({}, create_mock_api_response(mock_historical_price_data))
    # Call get_historical_prices_for_model with test parameters
    start_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    end_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    df = get_historical_prices_for_model(start_date, end_date)
    # Verify the returned DataFrame is properly processed for model input
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # Check that the DataFrame is pivoted by product
    for product in FORECAST_PRODUCTS:
        assert f'price_{product}' in df.columns
    # Verify all required products are present
    # Check that any missing values are properly handled


@pytest.mark.parametrize('products', [['DALMP'], ['RTLMP'], ['DALMP', 'RTLMP']])
def test_filter_prices_by_product(mock_historical_price_data, products):
    """Tests filtering historical price data by product"""
    # Create mock historical price data with multiple products
    # Call filter_prices_by_product with the mock data and specified products
    filtered_df = filter_prices_by_product(mock_historical_price_data, products)
    # Verify the returned DataFrame only contains the specified products
    assert isinstance(filtered_df, pd.DataFrame)
    assert not filtered_df.empty
    for product in filtered_df['product'].unique():
        assert product in products
    # Check that the DataFrame structure is maintained


def test_pivot_prices_by_product(mock_historical_price_data):
    """Tests pivoting historical price data by product"""
    # Create mock historical price data with multiple products
    # Call pivot_prices_by_product with the mock data
    pivoted_df = pivot_prices_by_product(mock_historical_price_data)
    # Verify the returned DataFrame has products as columns
    assert isinstance(pivoted_df, pd.DataFrame)
    assert not pivoted_df.empty
    # Check that the timestamp is the index
    # Verify that price values are correctly placed
    # Check that column names include 'price_' prefix
    for product in FORECAST_PRODUCTS:
        assert f'price_{product}' in pivoted_df.columns


@pytest.mark.parametrize('window', [None, '24H', '7D'])
def test_calculate_price_statistics(mock_historical_price_data, window):
    """Tests calculation of statistical measures from historical price data"""
    # Create mock historical price data
    # Call calculate_price_statistics with the mock data and specified window
    stats_df = calculate_price_statistics(mock_historical_price_data, FORECAST_PRODUCTS, window)
    # Verify the returned DataFrame contains expected statistical measures
    assert isinstance(stats_df, pd.DataFrame)
    # If window is None, check for global statistics
    if window is None:
        for product in FORECAST_PRODUCTS:
            assert f'{product}_mean' in stats_df.columns
            assert f'{product}_std' in stats_df.columns
    # If window is specified, check for rolling statistics
    else:
        for product in FORECAST_PRODUCTS:
            assert f'{product}_mean' in stats_df.columns
            assert f'{product}_std' in stats_df.columns
    # Verify statistics are calculated correctly for each product


def test_historical_price_client_get_historical_prices(historical_price_client, mock_historical_price_data):
    """Tests the HistoricalPriceClient.get_historical_prices method"""
    # Create a HistoricalPriceClient instance
    # Mock the fetch_historical_prices function to return mock data
    historical_price_client.add_response({}, create_mock_api_response(mock_historical_price_data))
    # Call client.get_historical_prices with test parameters
    start_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    end_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    df = historical_price_client.get_historical_prices(start_date, end_date)
    # Verify the client correctly calls fetch_historical_prices
    # Check that the returned data matches the expected result
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_historical_price_client_get_prices_for_model(historical_price_client, mock_historical_price_data):
    """Tests the HistoricalPriceClient.get_prices_for_model method"""
    # Create a HistoricalPriceClient instance
    # Mock the get_historical_prices_for_model function to return mock data
    historical_price_client.add_response({}, create_mock_api_response(mock_historical_price_data))
    # Call client.get_prices_for_model with test parameters
    start_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    end_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    df = historical_price_client.get_prices_for_model(start_date, end_date)
    # Verify the client correctly calls get_historical_prices_for_model
    # Check that the returned data matches the expected result
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.parametrize('window', [None, '24H', '7D'])
def test_historical_price_client_get_price_statistics(historical_price_client, mock_historical_price_data, window):
    """Tests the HistoricalPriceClient.get_price_statistics method"""
    # Create a HistoricalPriceClient instance
    # Mock the client.get_historical_prices method to return mock data
    historical_price_client.add_response({}, create_mock_api_response(mock_historical_price_data))
    # Call client.get_price_statistics with test parameters and window
    start_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    end_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    stats_df = historical_price_client.get_price_statistics(start_date, end_date, window=window)
    # Verify the client correctly calls calculate_price_statistics
    # Check that the returned statistics match the expected result
    assert isinstance(stats_df, pd.DataFrame)


@pytest.mark.parametrize('error_type', ['APIConnectionError', 'APIResponseError', 'DataValidationError'])
def test_historical_price_client_error_handling(historical_price_client, error_type):
    """Tests error handling in the HistoricalPriceClient"""
    # Create a HistoricalPriceClient instance
    # Mock the fetch_historical_prices function to raise the specified error
    if error_type == 'APIConnectionError':
        historical_price_client.set_error(APIConnectionError("API Endpoint", "historical_prices", Exception("Connection failed")))
    elif error_type == 'APIResponseError':
        historical_price_client.set_error(APIResponseError("API Endpoint", 500, {"error": "Internal Server Error"}))
    elif error_type == 'DataValidationError':
        historical_price_client.set_error(DataValidationError("historical_prices", ["Invalid data"]))
    # Use pytest.raises to verify that client methods propagate the error
    with pytest.raises(Exception):
        # Call client methods with test parameters
        start_date = localize_to_cst(datetime.datetime(2023, 1, 1))
        end_date = localize_to_cst(datetime.datetime(2023, 1, 2))
        historical_price_client.get_historical_prices(start_date, end_date)
    # Verify the error is handled correctly