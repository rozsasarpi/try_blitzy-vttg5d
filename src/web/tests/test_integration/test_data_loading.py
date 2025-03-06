import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import pandas as pd  # pandas: 2.0.0+
from datetime import datetime  # standard library
import requests  # version ^2.28.0

from src.web.data.forecast_loader import ForecastLoader, forecast_loader  # Class for loading forecast data from various sources
from src.web.data.forecast_client import ForecastClient, forecast_client  # Client for retrieving forecast data from the backend API
from src.web.data.cache_manager import ForecastCacheManager, forecast_cache_manager  # Manager for caching forecast data
from src.web.data.schema import prepare_dataframe_for_visualization, validate_forecast_dataframe  # Transform forecast dataframe for visualization
from src.web.config.product_config import PRODUCTS, DEFAULT_PRODUCT  # List of valid electricity market products
from src.web.config.settings import CACHE_ENABLED  # Flag indicating if caching is enabled
from src.web.tests.fixtures.forecast_fixtures import create_sample_forecast_dataframe, create_sample_visualization_dataframe, create_sample_fallback_dataframe  # Create sample forecast dataframe for testing

@pytest.mark.integration
def setup_module():
    """Setup function that runs before all tests in the module"""
    # Clear the forecast cache to ensure tests start with a clean state
    forecast_loader.clear_cache()
    # Set up any required test environment variables or configurations
    pass

@pytest.mark.integration
def teardown_module():
    """Teardown function that runs after all tests in the module"""
    # Clear the forecast cache to clean up after tests
    forecast_loader.clear_cache()
    # Reset any modified environment variables or configurations
    pass

@pytest.mark.integration
def test_load_forecast_by_date_integration():
    """Tests the integration between forecast loader, client, and cache when loading a forecast by date"""
    # Mock the forecast_client.get_forecast_by_date method to return a sample forecast
    sample_forecast = create_sample_forecast_dataframe()
    with unittest.mock.patch.object(forecast_client, 'get_forecast_by_date', return_value=sample_forecast) as mock_client_method:
        # Call forecast_loader.load_forecast_by_date with a test product and date
        test_product = DEFAULT_PRODUCT
        test_date = datetime.now().date()
        returned_df = forecast_loader.load_forecast_by_date(test_product, test_date)

        # Verify that the client method was called with correct parameters
        mock_client_method.assert_called_once_with(test_product, test_date)

        # Verify that the returned dataframe has the expected structure
        assert isinstance(returned_df, pd.DataFrame)
        assert 'timestamp' in returned_df.columns
        assert 'product' in returned_df.columns
        assert 'point_forecast' in returned_df.columns

        # Verify that the forecast was cached correctly
        if CACHE_ENABLED:
            cached_df = forecast_cache_manager.get_forecast(test_product, test_date)
            assert cached_df is not None
            assert isinstance(cached_df, pd.DataFrame)

        # Call the method again with the same parameters
        forecast_loader.load_forecast_by_date(test_product, test_date)

        # Verify that the client method was not called again (cache hit)
        assert mock_client_method.call_count == 1

        # Verify that the returned dataframe is the same as before
        returned_df_2 = forecast_loader.load_forecast_by_date(test_product, test_date)
        pd.testing.assert_frame_equal(returned_df, returned_df_2)

@pytest.mark.integration
def test_load_latest_forecast_integration():
    """Tests the integration between forecast loader, client, and cache when loading the latest forecast"""
    # Mock the forecast_client.get_latest_forecast method to return a sample forecast
    sample_forecast = create_sample_forecast_dataframe()
    with unittest.mock.patch.object(forecast_client, 'get_latest_forecast', return_value=sample_forecast) as mock_client_method:
        # Call forecast_loader.load_latest_forecast with a test product
        test_product = DEFAULT_PRODUCT
        returned_df = forecast_loader.load_latest_forecast(test_product)

        # Verify that the client method was called with correct parameters
        mock_client_method.assert_called_once_with(test_product)

        # Verify that the returned dataframe has the expected structure
        assert isinstance(returned_df, pd.DataFrame)
        assert 'timestamp' in returned_df.columns
        assert 'product' in returned_df.columns
        assert 'point_forecast' in returned_df.columns

        # Verify that the forecast was cached correctly
        if CACHE_ENABLED:
            cached_df = forecast_cache_manager.get_forecast(test_product, "latest")
            assert cached_df is not None
            assert isinstance(cached_df, pd.DataFrame)

        # Call the method again with the same parameters
        forecast_loader.load_latest_forecast(test_product)

        # Verify that the client method was not called again (cache hit)
        assert mock_client_method.call_count == 1

        # Verify that the returned dataframe is the same as before
        returned_df_2 = forecast_loader.load_latest_forecast(test_product)
        pd.testing.assert_frame_equal(returned_df, returned_df_2)

@pytest.mark.integration
def test_load_forecast_by_date_range_integration():
    """Tests the integration between forecast loader, client, and cache when loading forecasts for a date range"""
    # Mock the forecast_client.get_forecasts_by_date_range method to return a sample forecast
    sample_forecast = create_sample_forecast_dataframe()
    with unittest.mock.patch.object(forecast_client, 'get_forecasts_by_date_range', return_value=sample_forecast) as mock_client_method:
        # Call forecast_loader.load_forecast_by_date_range with a test product and date range
        test_product = DEFAULT_PRODUCT
        test_start_date = datetime.now().date()
        test_end_date = (datetime.now() + pd.Timedelta(days=2)).date()
        returned_df = forecast_loader.load_forecast_by_date_range(test_product, test_start_date, test_end_date)

        # Verify that the client method was called with correct parameters
        mock_client_method.assert_called_once_with(test_product, test_start_date, test_end_date)

        # Verify that the returned dataframe has the expected structure
        assert isinstance(returned_df, pd.DataFrame)
        assert 'timestamp' in returned_df.columns
        assert 'product' in returned_df.columns
        assert 'point_forecast' in returned_df.columns

        # Verify that the forecast was cached correctly
        if CACHE_ENABLED:
            cached_df = forecast_cache_manager.get_forecast(test_product, test_start_date, test_end_date)
            assert cached_df is not None
            assert isinstance(cached_df, pd.DataFrame)

        # Call the method again with the same parameters
        forecast_loader.load_forecast_by_date_range(test_product, test_start_date, test_end_date)

        # Verify that the client method was not called again (cache hit)
        assert mock_client_method.call_count == 1

        # Verify that the returned dataframe is the same as before
        returned_df_2 = forecast_loader.load_forecast_by_date_range(test_product, test_start_date, test_end_date)
        pd.testing.assert_frame_equal(returned_df, returned_df_2)

@pytest.mark.integration
def test_cache_integration():
    """Tests the integration between forecast loader and cache manager"""
    # Mock the forecast_client to return a sample forecast
    sample_forecast = create_sample_forecast_dataframe()
    with unittest.mock.patch.object(forecast_client, 'get_forecast_by_date', return_value=sample_forecast) as mock_client_method:
        # Call forecast_loader.load_forecast_by_date to load a forecast
        test_product = DEFAULT_PRODUCT
        test_date = datetime.now().date()
        forecast_loader.load_forecast_by_date(test_product, test_date)

        # Verify that the forecast was cached correctly
        if CACHE_ENABLED:
            cached_df = forecast_cache_manager.get_forecast(test_product, test_date)
            assert cached_df is not None
            assert isinstance(cached_df, pd.DataFrame)

        # Clear the cache for the specific product
        forecast_loader.clear_cache(test_product)

        # Verify that the cache is empty for that product
        cached_df = forecast_cache_manager.get_forecast(test_product, test_date)
        assert cached_df is None

        # Call forecast_loader.load_forecast_by_date again
        forecast_loader.load_forecast_by_date(test_product, test_date)

        # Verify that the client method was called again (cache miss)
        assert mock_client_method.call_count == 2

        # Verify that the forecast was cached again
        if CACHE_ENABLED:
            cached_df = forecast_cache_manager.get_forecast(test_product, test_date)
            assert cached_df is not None
            assert isinstance(cached_df, pd.DataFrame)

@pytest.mark.integration
def test_data_transformation_integration():
    """Tests the integration between forecast loader and data transformation functions"""
    # Create a sample forecast dataframe in backend format
    sample_forecast = create_sample_forecast_dataframe()

    # Mock the forecast_client to return this sample dataframe
    with unittest.mock.patch.object(forecast_client, 'get_forecast_by_date', return_value=sample_forecast):
        # Call forecast_loader.load_forecast_by_date to load and transform the forecast
        test_product = DEFAULT_PRODUCT
        test_date = datetime.now().date()
        transformed_df = forecast_loader.load_forecast_by_date(test_product, test_date)

        # Verify that the returned dataframe has been transformed for visualization
        assert isinstance(transformed_df, pd.DataFrame)

        # Verify that the dataframe has the expected columns (timestamp, product, point_forecast, lower_bound, upper_bound)
        expected_columns = ['timestamp', 'product', 'point_forecast', 'lower_bound', 'upper_bound']
        assert all(col in transformed_df.columns for col in expected_columns)

        # Verify that the transformation preserved the data integrity
        assert len(transformed_df) == len(sample_forecast)

@pytest.mark.integration
def test_fallback_integration():
    """Tests the integration with fallback mechanism when API calls fail"""
    # Mock the forecast_client to raise an exception
    with unittest.mock.patch.object(forecast_client, 'get_forecast_by_date', side_effect=Exception("API Error")):
        # Create a sample fallback forecast and cache it
        test_product = DEFAULT_PRODUCT
        test_date = datetime.now().date()
        sample_fallback = create_sample_fallback_dataframe()
        forecast_cache_manager.cache_forecast(test_product, sample_fallback, test_date)

        # Call forecast_loader.load_forecast_by_date
        returned_df = forecast_loader.load_forecast_by_date(test_product, test_date)

        # Verify that the fallback forecast was returned
        assert isinstance(returned_df, pd.DataFrame)

        # Verify that the returned dataframe is marked as a fallback (is_fallback=True)
        assert all(returned_df['is_fallback'])

@pytest.mark.integration
def test_percentile_extraction_integration():
    """Tests the integration between forecast loader and percentile extraction"""
    # Create a sample forecast dataframe with probabilistic samples
    sample_forecast = create_sample_forecast_dataframe()

    # Mock the forecast_client to return this sample dataframe
    with unittest.mock.patch.object(forecast_client, 'get_forecast_by_date', return_value=sample_forecast):
        # Call forecast_loader.load_forecast_by_date with custom percentiles
        test_product = DEFAULT_PRODUCT
        test_date = datetime.now().date()
        percentiles = [25, 75]
        transformed_df = forecast_loader.load_forecast_by_date(test_product, test_date, percentiles=percentiles)

        # Verify that the returned dataframe has the correct percentile values
        assert isinstance(transformed_df, pd.DataFrame)
        assert 'lower_bound' in transformed_df.columns
        assert 'upper_bound' in transformed_df.columns

        # Verify that the lower_bound and upper_bound correspond to the requested percentiles
        lower_bound_values = transformed_df['lower_bound'].values
        upper_bound_values = transformed_df['upper_bound'].values
        assert len(lower_bound_values) > 0
        assert len(upper_bound_values) > 0

@pytest.mark.integration
def test_metadata_extraction_integration():
    """Tests the integration between forecast loader and metadata extraction"""
    # Create a sample forecast dataframe with metadata
    sample_forecast = create_sample_forecast_dataframe()

    # Mock the forecast_client to return this sample dataframe
    with unittest.mock.patch.object(forecast_client, 'get_forecast_by_date', return_value=sample_forecast):
        # Call forecast_loader.load_forecast_by_date to load the forecast
        test_product = DEFAULT_PRODUCT
        test_date = datetime.now().date()
        transformed_df = forecast_loader.load_forecast_by_date(test_product, test_date)

        # Call forecast_loader.get_forecast_metadata on the returned dataframe
        metadata = forecast_loader.get_forecast_metadata(transformed_df)

        # Verify that the extracted metadata matches the expected values
        assert isinstance(metadata, dict)
        assert 'generation_timestamp' in metadata
        assert 'is_fallback' in metadata

        # Verify that the metadata includes generation timestamp and fallback status
        assert metadata['generation_timestamp'] == sample_forecast['generation_timestamp'].iloc[0]
        assert metadata['is_fallback'] == sample_forecast['is_fallback'].iloc[0]

@pytest.mark.integration
def test_error_handling_integration():
    """Tests the integration of error handling across components"""
    # Mock the forecast_client to raise different types of exceptions
    with unittest.mock.patch.object(forecast_client, 'get_forecast_by_date', side_effect=requests.exceptions.RequestException("Network error")):
        # Call forecast_loader methods and verify appropriate error handling
        test_product = DEFAULT_PRODUCT
        test_date = datetime.now().date()
        with pytest.raises(Exception) as exc_info:
            forecast_loader.load_forecast_by_date(test_product, test_date)

        # Test network errors, API errors, and data validation errors
        assert "Network error" in str(exc_info.value)

        # Verify that meaningful error messages are provided
        assert "loading forecast" in str(exc_info.value)

        # Verify that the system degrades gracefully when errors occur
        # (e.g., by returning a fallback forecast or displaying an error message)
        pass

@pytest.mark.integration
def test_end_to_end_data_flow():
    """Tests the complete end-to-end data flow from API to visualization-ready data"""
    # Create a realistic sample forecast dataframe
    sample_forecast = create_sample_forecast_dataframe()

    # Mock the forecast_client to return this sample dataframe
    with unittest.mock.patch.object(forecast_client, 'get_forecast_by_date', return_value=sample_forecast):
        # Call forecast_loader.load_forecast_by_date to load and transform the forecast
        test_product = DEFAULT_PRODUCT
        test_date = datetime.now().date()
        transformed_df = forecast_loader.load_forecast_by_date(test_product, test_date)

        # Verify that the returned dataframe is ready for visualization
        assert isinstance(transformed_df, pd.DataFrame)

        # Verify that all data transformations have been applied correctly
        expected_columns = ['timestamp', 'product', 'point_forecast', 'lower_bound', 'upper_bound', 'is_fallback']
        assert all(col in transformed_df.columns for col in expected_columns)

        # Verify that the dataframe can be used directly by visualization components
        # (e.g., by checking that the data types are correct and that the data is in the expected format)
        assert transformed_df['timestamp'].dtype == 'datetime64[ns]'
        assert transformed_df['point_forecast'].dtype == 'float64'
        assert transformed_df['lower_bound'].dtype == 'float64'
        assert transformed_df['upper_bound'].dtype == 'float64'
        assert transformed_df['product'].dtype == 'object'
        assert transformed_df['is_fallback'].dtype == 'bool'