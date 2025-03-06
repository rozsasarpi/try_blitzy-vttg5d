# src/web/tests/test_services/test_api_service.py
import pytest  # pytest: 7.0.0+
from unittest.mock import patch, MagicMock, Mock  # unittest.mock
import pandas as pd  # pandas: 2.0.0+
from datetime import datetime, timedelta  # datetime

from src.web.services.api_service import ForecastService, get_forecast_by_date, get_latest_forecast, get_forecast_range, check_api_health, clear_cache, process_forecast_data, is_using_fallback  # Service class being tested
from src.web.data.forecast_client import ForecastClient  # Client class used by the service
from src.web.data.cache_manager import forecast_cache_manager  # Cache manager used by the service
from src.web.config.settings import CACHE_ENABLED  # Configuration setting for caching
from src.web.tests.fixtures.forecast_fixtures import create_sample_forecast_dataframe, create_sample_visualization_dataframe, create_sample_fallback_dataframe  # Create test data for unit tests

def test_forecast_service_init():
    """Tests the initialization of the ForecastService class"""
    # Create a ForecastService instance
    service = ForecastService()

    # Assert that the client is properly initialized
    assert isinstance(service._client, ForecastClient)

    # Assert that the logger is properly initialized
    assert service.logger is not None

def test_get_forecast_by_date():
    """Tests the get_forecast_by_date function"""
    # Mock the ForecastClient.get_forecast_by_date method
    with patch.object(ForecastClient, 'get_forecast_by_date', return_value=create_sample_forecast_dataframe()) as mock_client_method:
        # Mock the process_forecast_data function
        with patch('src.web.services.api_service.process_forecast_data', return_value=create_sample_visualization_dataframe()) as mock_process_data:
            # Call get_forecast_by_date with test parameters
            product = "DALMP"
            date = datetime.now()
            result = get_forecast_by_date(product, date)

            # Assert that the client method was called with correct parameters
            mock_client_method.assert_called_once_with(product, date)

            # Assert that process_forecast_data was called with the result
            mock_process_data.assert_called_once()

            # Assert that the function returns the expected result
            assert isinstance(result, pd.DataFrame)

def test_get_forecast_by_date_with_cache():
    """Tests the get_forecast_by_date function with caching enabled"""
    # Mock the forecast_cache_manager.get_forecast method to return a cached result
    with patch('src.web.services.api_service.forecast_cache_manager.get_forecast', return_value=create_sample_forecast_dataframe()) as mock_cache_get:
        # Mock the ForecastClient.get_forecast_by_date method
        with patch.object(ForecastClient, 'get_forecast_by_date') as mock_client_method:
            # Call get_forecast_by_date with use_cache=True
            product = "DALMP"
            date = datetime.now()
            get_forecast_by_date(product, date, use_cache=True)

            # Assert that the cache manager was checked first
            mock_cache_get.assert_called_once()

            # Assert that the client method was not called
            mock_client_method.assert_not_called()

def test_get_forecast_by_date_cache_miss():
    """Tests the get_forecast_by_date function with cache miss"""
    # Mock the forecast_cache_manager.get_forecast method to return None (cache miss)
    with patch('src.web.services.api_service.forecast_cache_manager.get_forecast', return_value=None) as mock_cache_get:
        # Mock the ForecastClient.get_forecast_by_date method to return a result
        with patch.object(ForecastClient, 'get_forecast_by_date', return_value=create_sample_forecast_dataframe()) as mock_client_method:
            # Mock the forecast_cache_manager.cache_forecast method
            with patch('src.web.services.api_service.forecast_cache_manager.cache_forecast') as mock_cache_set:
                # Call get_forecast_by_date with use_cache=True
                product = "DALMP"
                date = datetime.now()
                get_forecast_by_date(product, date, use_cache=True)

                # Assert that the cache manager was checked first
                mock_cache_get.assert_called_once()

                # Assert that the client method was called after cache miss
                mock_client_method.assert_called_once()

                # Assert that the result was cached
                mock_cache_set.assert_called_once()

def test_get_latest_forecast():
    """Tests the get_latest_forecast function"""
    # Mock the ForecastClient.get_latest_forecast method
    with patch.object(ForecastClient, 'get_latest_forecast', return_value=create_sample_forecast_dataframe()) as mock_client_method:
        # Mock the process_forecast_data function
        with patch('src.web.services.api_service.process_forecast_data', return_value=create_sample_visualization_dataframe()) as mock_process_data:
            # Call get_latest_forecast with test parameters
            product = "DALMP"
            result = get_latest_forecast(product)

            # Assert that the client method was called with correct parameters
            mock_client_method.assert_called_once_with(product)

            # Assert that process_forecast_data was called with the result
            mock_process_data.assert_called_once()

            # Assert that the function returns the expected result
            assert isinstance(result, pd.DataFrame)

def test_get_forecast_range():
    """Tests the get_forecast_range function"""
    # Mock the ForecastClient.get_forecasts_by_date_range method
    with patch.object(ForecastClient, 'get_forecasts_by_date_range', return_value=create_sample_forecast_dataframe()) as mock_client_method:
        # Mock the process_forecast_data function
        with patch('src.web.services.api_service.process_forecast_data', return_value=create_sample_visualization_dataframe()) as mock_process_data:
            # Call get_forecast_range with test parameters
            product = "DALMP"
            start_date = datetime.now()
            end_date = start_date + timedelta(days=1)
            result = get_forecast_range(product, start_date, end_date)

            # Assert that the client method was called with correct parameters
            mock_client_method.assert_called_once_with(product, start_date, end_date)

            # Assert that process_forecast_data was called with the result
            mock_process_data.assert_called_once()

            # Assert that the function returns the expected result
            assert isinstance(result, pd.DataFrame)

def test_check_api_health():
    """Tests the check_api_health function"""
    # Mock the ForecastClient.check_api_health method to return True
    with patch.object(ForecastClient, 'check_api_health', return_value=True) as mock_client_method:
        # Call check_api_health
        result = check_api_health()

        # Assert that the client method was called
        mock_client_method.assert_called_once()

        # Assert that the function returns True
        assert result is True

    # Mock the client method to return False
    with patch.object(ForecastClient, 'check_api_health', return_value=False) as mock_client_method:
        # Assert that the function returns False
        assert check_api_health() is False

def test_check_api_health_exception():
    """Tests the check_api_health function when an exception occurs"""
    # Mock the ForecastClient.check_api_health method to raise an exception
    with patch.object(ForecastClient, 'check_api_health', side_effect=Exception("API Error")) as mock_client_method:
        # Call check_api_health
        result = check_api_health()

        # Assert that the function catches the exception and returns False
        assert result is False

def test_clear_cache():
    """Tests the clear_cache function"""
    # Mock the forecast_cache_manager.clear_cache method
    with patch('src.web.services.api_service.forecast_cache_manager.clear_cache') as mock_cache_clear:
        # Call clear_cache
        clear_cache()

        # Assert that the cache manager method was called
        mock_cache_clear.assert_called_once()

    # Call clear_cache with a specific product
    with patch('src.web.services.api_service.forecast_cache_manager.clear_cache') as mock_cache_clear:
        product = "DALMP"
        clear_cache(product)

        # Assert that the cache manager method was called with the product
        mock_cache_clear.assert_called_once_with(product)

def test_process_forecast_data():
    """Tests the process_forecast_data function"""
    # Create a sample forecast dataframe
    sample_df = create_sample_forecast_dataframe()

    # Mock the validate_forecast_dataframe function
    with patch('src.web.services.api_service.validate_forecast_dataframe', return_value=(True, {})) as mock_validate:
        # Mock the prepare_dataframe_for_visualization function
        with patch('src.web.services.api_service.prepare_dataframe_for_visualization', return_value=create_sample_visualization_dataframe()) as mock_prepare:
            # Mock the add_unit_information function
            with patch('src.web.services.api_service.add_unit_information', return_value=create_sample_visualization_dataframe()) as mock_add_unit:
                # Call process_forecast_data with the sample dataframe
                result = process_forecast_data(sample_df)

                # Assert that validate_forecast_dataframe was called
                mock_validate.assert_called_once_with(sample_df)

                # Assert that prepare_dataframe_for_visualization was called with correct percentiles
                mock_prepare.assert_called_once()

                # Assert that add_unit_information was called
                mock_add_unit.assert_called_once()

                # Assert that the function returns the expected result
                assert isinstance(result, pd.DataFrame)

def test_process_forecast_data_validation_error():
    """Tests the process_forecast_data function when validation fails"""
    # Create a sample forecast dataframe
    sample_df = create_sample_forecast_dataframe()

    # Mock the validate_forecast_dataframe function to return (False, error_details)
    error_details = {"error": "Validation failed"}
    with patch('src.web.services.api_service.validate_forecast_dataframe', return_value=(False, error_details)):
        # Call process_forecast_data with the sample dataframe
        with pytest.raises(ValueError):
            process_forecast_data(sample_df)

def test_is_using_fallback():
    """Tests the is_using_fallback function"""
    # Create a sample dataframe with is_fallback=True
    sample_df_true = create_sample_fallback_dataframe()

    # Call is_using_fallback with the dataframe
    result_true = is_using_fallback(sample_df_true)

    # Assert that the function returns True
    assert result_true is True

    # Create a sample dataframe with is_fallback=False
    sample_df_false = create_sample_forecast_dataframe()

    # Assert that the function returns False
    result_false = is_using_fallback(sample_df_false)
    assert result_false is False

    # Create a dataframe without is_fallback column
    sample_df_no_column = create_sample_forecast_dataframe()
    sample_df_no_column = sample_df_no_column.drop(columns=['is_fallback'])

    # Assert that the function returns False
    result_no_column = is_using_fallback(sample_df_no_column)
    assert result_no_column is False

def test_forecast_service_get_forecast_by_date():
    """Tests the ForecastService.get_forecast_by_date method"""
    # Create a mock ForecastClient
    mock_client = MagicMock()

    # Create a ForecastService instance with the mock client
    service = ForecastService(client=mock_client)

    # Mock the client's get_forecast_by_date method
    mock_client.get_forecast_by_date.return_value = create_sample_forecast_dataframe()

    # Call service.get_forecast_by_date with test parameters
    product = "DALMP"
    date = datetime.now()
    service.get_forecast_by_date(product, date)

    # Assert that the client method was called with correct parameters
    mock_client.get_forecast_by_date.assert_called_once_with(product, date)

    # Assert that the method returns the expected result
    assert isinstance(service.get_forecast_by_date(product, date), pd.DataFrame)

def test_forecast_service_get_latest_forecast():
    """Tests the ForecastService.get_latest_forecast method"""
    # Create a mock ForecastClient
    mock_client = MagicMock()

    # Create a ForecastService instance with the mock client
    service = ForecastService(client=mock_client)

    # Mock the client's get_latest_forecast method
    mock_client.get_latest_forecast.return_value = create_sample_forecast_dataframe()

    # Call service.get_latest_forecast with test parameters
    product = "DALMP"
    service.get_latest_forecast(product)

    # Assert that the client method was called with correct parameters
    mock_client.get_latest_forecast.assert_called_once_with(product)

    # Assert that the method returns the expected result
    assert isinstance(service.get_latest_forecast(product), pd.DataFrame)

def test_forecast_service_get_forecast_range():
    """Tests the ForecastService.get_forecast_range method"""
    # Create a mock ForecastClient
    mock_client = MagicMock()

    # Create a ForecastService instance with the mock client
    service = ForecastService(client=mock_client)

    # Mock the client's get_forecasts_by_date_range method
    mock_client.get_forecasts_by_date_range.return_value = create_sample_forecast_dataframe()

    # Call service.get_forecast_range with test parameters
    product = "DALMP"
    start_date = datetime.now()
    end_date = start_date + timedelta(days=1)
    service.get_forecast_range(product, start_date, end_date)

    # Assert that the client method was called with correct parameters
    mock_client.get_forecasts_by_date_range.assert_called_once_with(product, start_date, end_date)

    # Assert that the method returns the expected result
    assert isinstance(service.get_forecast_range(product, start_date, end_date), pd.DataFrame)

def test_forecast_service_check_api_health():
    """Tests the ForecastService.check_api_health method"""
    # Create a mock ForecastClient
    mock_client = MagicMock()

    # Create a ForecastService instance with the mock client
    service = ForecastService(client=mock_client)

    # Mock the client's check_api_health method to return True
    mock_client.check_api_health.return_value = True

    # Call service.check_api_health
    result = service.check_api_health()

    # Assert that the client method was called
    mock_client.check_api_health.assert_called_once()

    # Assert that the method returns True
    assert result is True

def test_forecast_service_is_using_fallback():
    """Tests the ForecastService.is_using_fallback method"""
    # Create a ForecastService instance
    service = ForecastService()

    # Create a sample dataframe with is_fallback=True
    sample_df_true = create_sample_fallback_dataframe()

    # Call service.is_using_fallback with the dataframe
    result_true = service.is_using_fallback(sample_df_true)

    # Assert that the method returns True
    assert result_true is True

    # Create a sample dataframe with is_fallback=False
    sample_df_false = create_sample_forecast_dataframe()
    result_false = service.is_using_fallback(sample_df_false)

    # Assert that the method returns False
    assert result_false is False

def test_forecast_service_close():
    """Tests the ForecastService.close method"""
    # Create a mock ForecastClient
    mock_client = MagicMock()

    # Create a ForecastService instance with the mock client
    service = ForecastService(client=mock_client)

    # Call service.close
    service.close()

    # Assert that the client's close method was called
    mock_client.close.assert_called_once()

def test_error_handling():
    """Tests error handling in the API service functions"""
    # Mock the ForecastClient.get_forecast_by_date method to raise an exception
    with patch.object(ForecastClient, 'get_forecast_by_date', side_effect=Exception("API Error")):
        # Mock the handle_data_loading_error function
        with patch('src.web.services.api_service.handle_data_loading_error') as mock_handle_error:
            # Call get_forecast_by_date
            product = "DALMP"
            date = datetime.now()
            get_forecast_by_date(product, date)

            # Assert that handle_data_loading_error was called with the exception
            mock_handle_error.assert_called_once()

    # Repeat for other API service functions
    with patch.object(ForecastClient, 'get_latest_forecast', side_effect=Exception("API Error")):
        with patch('src.web.services.api_service.handle_data_loading_error') as mock_handle_error:
            product = "DALMP"
            get_latest_forecast(product)
            mock_handle_error.assert_called_once()

    with patch.object(ForecastClient, 'get_forecasts_by_date_range', side_effect=Exception("API Error")):
        with patch('src.web.services.api_service.handle_data_loading_error') as mock_handle_error:
            product = "DALMP"
            start_date = datetime.now()
            end_date = start_date + timedelta(days=1)
            get_forecast_range(product, start_date, end_date)
            mock_handle_error.assert_called_once()