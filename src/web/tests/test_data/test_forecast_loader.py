"""
Unit tests for the forecast_loader module which is responsible for loading
electricity market price forecasts from various sources. Tests cover loading
forecasts by date, date range, latest forecasts, error handling, and caching
behavior.
"""

import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import pandas as pd  # pandas: 2.0.0+
from datetime import datetime  # standard library
from typing import Dict

from src.web.data.forecast_loader import ForecastLoader, load_forecast_by_date, load_latest_forecast, load_forecast_by_date_range, validate_product
from src.web.data.forecast_loader import DEFAULT_PERCENTILES
from src.web.data.forecast_client import ForecastClient  # ForecastClient: 
from src.web.data.cache_manager import ForecastCacheManager  # ForecastCacheManager: 
from src.web.config.product_config import PRODUCTS, DEFAULT_PRODUCT  # PRODUCTS: 
from src.web.tests.fixtures.forecast_fixtures import create_sample_forecast_dataframe, create_sample_visualization_dataframe, create_sample_fallback_dataframe  # create_sample_forecast_dataframe: 
from src.web.data.schema import prepare_dataframe_for_visualization  # prepare_dataframe_for_visualization: 

class MockForecastClient:
    """Mock implementation of ForecastClient for testing"""

    def __init__(self, mock_data: Dict, should_fail: bool):
        """Initializes the MockForecastClient"""
        self.mock_data = mock_data
        self.should_fail = should_fail

    def get_forecast_by_date(self, product: str, date: str, format: str):
        """Mock implementation of get_forecast_by_date"""
        if self.should_fail:
            raise Exception("Mock client failure")
        if (product, date) in self.mock_data:
            return self.mock_data[(product, date)]
        return pd.DataFrame()

    def get_latest_forecast(self, product: str, format: str):
        """Mock implementation of get_latest_forecast"""
        if self.should_fail:
            raise Exception("Mock client failure")
        if product in self.mock_data:
            return self.mock_data[product]
        return pd.DataFrame()

    def get_forecasts_by_date_range(self, product: str, start_date: str, end_date: str, format: str):
        """Mock implementation of get_forecasts_by_date_range"""
        if self.should_fail:
            raise Exception("Mock client failure")
        if (product, start_date, end_date) in self.mock_data:
            return self.mock_data[(product, start_date, end_date)]
        return pd.DataFrame()

class MockForecastCacheManager:
    """Mock implementation of ForecastCacheManager for testing"""

    def __init__(self, cache_data: Dict):
        """Initializes the MockForecastCacheManager"""
        self.cache_data = cache_data
        self.hit_count = 0
        self.miss_count = 0

    def get_forecast(self, product: str, date: str = None, end_date: str = None):
        """Mock implementation of get_forecast"""
        key = f"{product}_{date}_{end_date}"
        if key in self.cache_data:
            self.hit_count += 1
            return self.cache_data[key]
        self.miss_count += 1
        return None

    def cache_forecast(self, product: str, forecast_df: pd.DataFrame, date: str = None, end_date: str = None, timeout: int = None):
        """Mock implementation of cache_forecast"""
        key = f"{product}_{date}_{end_date}"
        self.cache_data[key] = forecast_df
        return True

    def clear_cache(self, product: str = None):
        """Mock implementation of clear_cache"""
        if product is None:
            cleared = len(self.cache_data)
            self.cache_data.clear()
            return cleared
        else:
            keys_to_remove = [k for k in self.cache_data if k.startswith(product)]
            for k in keys_to_remove:
                del self.cache_data[k]
            return len(keys_to_remove)

    def get_stats(self):
        """Mock implementation of get_stats"""
        return {
            "hit_count": self.hit_count,
            "miss_count": self.miss_count,
            "cache_size": len(self.cache_data),
        }

@pytest.mark.unit
def test_forecast_loader_initialization():
    """Tests that the ForecastLoader class initializes correctly"""
    # Create a ForecastLoader instance
    forecast_loader = ForecastLoader()

    # Assert that the instance is not None
    assert forecast_loader is not None

    # Assert that the instance has the expected attributes and methods
    assert hasattr(forecast_loader, 'load_forecast_by_date')
    assert hasattr(forecast_loader, 'load_latest_forecast')
    assert hasattr(forecast_loader, 'load_forecast_by_date_range')
    assert hasattr(forecast_loader, 'extract_forecast_percentiles')
    assert hasattr(forecast_loader, 'get_forecast_metadata')
    assert hasattr(forecast_loader, 'check_forecast_availability')
    assert hasattr(forecast_loader, 'clear_cache')

@pytest.mark.unit
def test_validate_product_valid():
    """Tests that validate_product accepts valid product names"""
    # For each product in PRODUCTS list
    for product in PRODUCTS:
        # Call validate_product with the product name
        # Assert that no exception is raised
        validate_product(product)

@pytest.mark.unit
def test_validate_product_invalid():
    """Tests that validate_product rejects invalid product names"""
    # Call validate_product with an invalid product name
    # Assert that ValueError is raised with appropriate message
    with pytest.raises(ValueError, match=f"Invalid product: invalid. Must be one of: {', '.join(PRODUCTS)}"):
        validate_product("invalid")

@pytest.mark.unit
def test_load_forecast_by_date_success():
    """Tests successful loading of forecast by date"""
    # Create mock ForecastClient and ForecastCacheManager
    mock_data = {("DALMP", "2023-01-01"): create_sample_forecast_dataframe()}
    mock_client = MockForecastClient(mock_data, False)
    mock_cache = MockForecastCacheManager({})

    # Configure mock client to return sample forecast data
    # Create ForecastLoader with mocked dependencies
    with unittest.mock.patch("src.web.data.forecast_loader.get_forecast_by_date", side_effect=mock_client.get_forecast_by_date):
        with unittest.mock.patch("src.web.data.forecast_loader.forecast_cache_manager", new=mock_cache):
            # Call load_forecast_by_date with valid parameters
            df = load_forecast_by_date("DALMP", "2023-01-01")

            # Assert that the returned dataframe has expected structure
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert "timestamp" in df.columns
            assert "product" in df.columns

            # Verify that client and cache methods were called correctly
            assert mock_cache.hit_count == 0
            assert mock_cache.miss_count == 1

@pytest.mark.unit
def test_load_forecast_by_date_from_cache():
    """Tests loading of forecast by date from cache"""
    # Create mock ForecastClient and ForecastCacheManager
    cached_df = create_sample_visualization_dataframe()
    mock_client = MockForecastClient({}, False)
    mock_cache = MockForecastCacheManager({f"DALMP_2023-01-01_None": cached_df})

    # Configure mock cache to return cached forecast data
    with unittest.mock.patch("src.web.data.forecast_loader.get_forecast_by_date", side_effect=mock_client.get_forecast_by_date):
        with unittest.mock.patch("src.web.data.forecast_loader.forecast_cache_manager", new=mock_cache):
            # Call load_forecast_by_date with valid parameters
            df = load_forecast_by_date("DALMP", "2023-01-01")

            # Assert that the returned dataframe matches the cached data
            pd.testing.assert_frame_equal(df, cached_df)

            # Verify that client methods were not called
            assert mock_client.get_forecast_by_date("DALMP", "2023-01-01", "json") == pd.DataFrame()

            # Verify that cache get_forecast was called correctly
            assert mock_cache.hit_count == 1
            assert mock_cache.miss_count == 0

@pytest.mark.unit
def test_load_forecast_by_date_client_error():
    """Tests error handling when client fails to retrieve forecast"""
    # Create mock ForecastClient and ForecastCacheManager
    mock_client = MockForecastClient({}, True)
    mock_cache = MockForecastCacheManager({})

    # Configure mock client to raise an exception
    with unittest.mock.patch("src.web.data.forecast_loader.get_forecast_by_date", side_effect=mock_client.get_forecast_by_date):
        with unittest.mock.patch("src.web.data.forecast_loader.forecast_cache_manager", new=mock_cache):
            # Call load_forecast_by_date with valid parameters
            with pytest.raises(Exception, match="Mock client failure"):
                load_forecast_by_date("DALMP", "2023-01-01")

            # Assert that appropriate exception is raised or handled
            assert mock_cache.hit_count == 0
            assert mock_cache.miss_count == 0

@pytest.mark.unit
def test_load_latest_forecast_success():
    """Tests successful loading of latest forecast"""
    # Create mock ForecastClient and ForecastCacheManager
    mock_data = {"DALMP": create_sample_forecast_dataframe()}
    mock_client = MockForecastClient(mock_data, False)
    mock_cache = MockForecastCacheManager({})

    # Configure mock client to return sample forecast data
    with unittest.mock.patch("src.web.data.forecast_loader.get_latest_forecast", side_effect=mock_client.get_latest_forecast):
        with unittest.mock.patch("src.web.data.forecast_loader.forecast_cache_manager", new=mock_cache):
            # Call load_latest_forecast with valid parameters
            df = load_latest_forecast("DALMP")

            # Assert that the returned dataframe has expected structure
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert "timestamp" in df.columns
            assert "product" in df.columns

            # Verify that client and cache methods were called correctly
            assert mock_cache.hit_count == 0
            assert mock_cache.miss_count == 1

@pytest.mark.unit
def test_load_forecast_by_date_range_success():
    """Tests successful loading of forecast by date range"""
    # Create mock ForecastClient and ForecastCacheManager
    mock_data = {("DALMP", "2023-01-01", "2023-01-02"): create_sample_forecast_dataframe()}
    mock_client = MockForecastClient(mock_data, False)
    mock_cache = MockForecastCacheManager({})

    # Configure mock client to return sample forecast data
    with unittest.mock.patch("src.web.data.forecast_loader.get_forecasts_by_date_range", side_effect=mock_client.get_forecasts_by_date_range):
        with unittest.mock.patch("src.web.data.forecast_loader.forecast_cache_manager", new=mock_cache):
            # Call load_forecast_by_date_range with valid parameters
            df = load_forecast_by_date_range("DALMP", "2023-01-01", "2023-01-02")

            # Assert that the returned dataframe has expected structure
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert "timestamp" in df.columns
            assert "product" in df.columns

            # Verify that client and cache methods were called correctly
            assert mock_cache.hit_count == 0
            assert mock_cache.miss_count == 1

@pytest.mark.unit
def test_extract_forecast_percentiles():
    """Tests extraction of percentile values from forecast dataframe"""
    # Create sample forecast dataframe with probabilistic samples
    sample_df = create_sample_forecast_dataframe()

    # Create ForecastLoader instance
    forecast_loader = ForecastLoader()

    # Call extract_forecast_percentiles with sample dataframe
    percentile_df = forecast_loader.extract_forecast_percentiles(sample_df)

    # Assert that the returned dataframe has percentile columns
    assert "percentile_10" in percentile_df.columns
    assert "percentile_90" in percentile_df.columns

    # Verify that percentile values are within expected ranges
    assert percentile_df["percentile_10"].min() >= 0
    assert percentile_df["percentile_90"].max() <= 100

@pytest.mark.unit
def test_get_forecast_metadata():
    """Tests extraction of metadata from forecast dataframe"""
    # Create sample forecast dataframe with metadata
    sample_df = create_sample_forecast_dataframe()

    # Create ForecastLoader instance
    forecast_loader = ForecastLoader()

    # Call get_forecast_metadata with sample dataframe
    metadata = forecast_loader.get_forecast_metadata(sample_df)

    # Assert that the returned metadata contains expected fields
    assert "generation_timestamp" in metadata
    assert "is_fallback" in metadata
    assert "start_time" in metadata
    assert "end_time" in metadata
    assert "hours" in metadata
    assert "products" in metadata

    # Verify that metadata values match the input dataframe
    assert metadata["is_fallback"] == False
    assert metadata["products"] == ["DALMP"]

@pytest.mark.unit
def test_check_forecast_availability():
    """Tests checking for forecast availability"""
    # Create mock ForecastClient
    mock_client = MockForecastClient({}, False)

    # Configure mock client to return data for some dates and not others
    with unittest.mock.patch("src.web.data.forecast_loader.get_available_forecast_dates", return_value=["2023-01-01"]):
        # Create ForecastLoader with mocked dependencies
        forecast_loader = ForecastLoader()

        # Call check_forecast_availability with available date
        # Assert that it returns True
        assert forecast_loader.check_forecast_availability("DALMP", "2023-01-01") == True

        # Call check_forecast_availability with unavailable date
        # Assert that it returns False
        assert forecast_loader.check_forecast_availability("DALMP", "2023-01-02") == False

@pytest.mark.unit
def test_clear_cache():
    """Tests clearing of forecast cache"""
    # Create mock ForecastCacheManager
    mock_cache = MockForecastCacheManager({"DALMP_2023-01-01": create_sample_forecast_dataframe()})

    # Configure mock cache manager to return cache statistics
    with unittest.mock.patch("src.web.data.forecast_loader.forecast_cache_manager", new=mock_cache):
        # Create ForecastLoader with mocked dependencies
        forecast_loader = ForecastLoader()

        # Call clear_cache with no parameters
        # Verify that cache_manager.clear_cache was called correctly
        forecast_loader.clear_cache()
        assert len(mock_cache.cache_data) == 0

        # Call clear_cache with specific product
        # Verify that cache_manager.clear_cache was called with product parameter
        mock_cache.cache_data = {"DALMP_2023-01-01": create_sample_forecast_dataframe(), "RTLMP_2023-01-01": create_sample_forecast_dataframe()}
        forecast_loader.clear_cache("DALMP")
        assert "DALMP_2023-01-01" not in mock_cache.cache_data
        assert "RTLMP_2023-01-01" in mock_cache.cache_data

@pytest.mark.integration
def test_integration_load_and_cache():
    """Integration test for loading and caching forecast data"""
    # Create real ForecastLoader instance with mocked client
    mock_data = {("DALMP", "2023-01-01"): create_sample_forecast_dataframe()}
    mock_client = MockForecastClient(mock_data, False)
    mock_cache = MockForecastCacheManager({})

    # Configure mock client to return sample forecast data
    with unittest.mock.patch("src.web.data.forecast_loader.get_forecast_by_date", side_effect=mock_client.get_forecast_by_date):
        with unittest.mock.patch("src.web.data.forecast_loader.forecast_cache_manager", new=mock_cache):
            forecast_loader = ForecastLoader()

            # Call load_forecast_by_date to load and cache data
            forecast_loader.load_forecast_by_date("DALMP", "2023-01-01")

            # Call load_forecast_by_date again with same parameters
            forecast_loader.load_forecast_by_date("DALMP", "2023-01-01")

            # Verify that second call uses cached data
            assert mock_client.get_forecast_by_date("DALMP", "2023-01-01", "json") == pd.DataFrame()

            # Verify that client method was called only once
            assert mock_cache.hit_count == 1
            assert mock_cache.miss_count == 1

@pytest.mark.unit
def test_fallback_handling():
    """Tests handling of fallback forecasts"""
    # Create sample fallback forecast dataframe
    fallback_df = create_sample_fallback_dataframe()

    # Create mock ForecastClient configured to return fallback data
    mock_client = MockForecastClient({("DALMP", "2023-01-01"): fallback_df}, False)
    mock_cache = MockForecastCacheManager({})

    # Configure ForecastLoader with mocked dependencies
    with unittest.mock.patch("src.web.data.forecast_loader.get_forecast_by_date", side_effect=mock_client.get_forecast_by_date):
        with unittest.mock.patch("src.web.data.forecast_loader.forecast_cache_manager", new=mock_cache):
            forecast_loader = ForecastLoader()

            # Call load_forecast_by_date with valid parameters
            df = forecast_loader.load_forecast_by_date("DALMP", "2023-01-01")

            # Assert that the returned dataframe has is_fallback=True
            assert df["is_fallback"].iloc[0] == True

            # Verify that metadata indicates fallback status
            metadata = forecast_loader.get_forecast_metadata(df)
            assert metadata["is_fallback"] == True