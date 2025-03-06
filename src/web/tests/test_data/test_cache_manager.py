import pytest  # version 7.0.0+
from unittest import mock  # standard library
import pandas as pd  # version 2.0.0+
import datetime  # standard library

from src.web.data.cache_manager import generate_forecast_cache_key  # Generate unique cache keys for forecast data
from src.web.data.cache_manager import cache_forecast  # Store forecast data in cache
from src.web.data.cache_manager import get_cached_forecast  # Retrieve forecast data from cache
from src.web.data.cache_manager import clear_forecast_cache  # Clear forecast cache entries
from src.web.data.cache_manager import get_forecast_cache_stats  # Get statistics about forecast cache usage
from src.web.data.cache_manager import is_forecast_cache_valid  # Check if a forecast cache entry is valid
from src.web.data.cache_manager import ForecastCacheManager  # Class for managing forecast data caching
from src.web.config.settings import CACHE_ENABLED  # Flag indicating if caching is enabled
from src.web.config.settings import CACHE_TIMEOUT  # Timeout in seconds for cached data
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe  # Create sample forecast dataframe for testing
from src.web.data.schema import validate_forecast_dataframe  # Validate forecast data against schema


def test_generate_forecast_cache_key():
    """Tests that unique cache keys are generated for different forecast parameters"""
    # Generate a cache key for a specific product and date
    key1 = generate_forecast_cache_key(product="DALMP", date="2023-11-20")
    # Generate another cache key with the same parameters
    key2 = generate_forecast_cache_key(product="DALMP", date="2023-11-20")
    # Assert that both keys are identical
    assert key1 == key2

    # Generate a key with different product
    key3 = generate_forecast_cache_key(product="RTLMP", date="2023-11-20")
    # Assert that it's different from the first key
    assert key3 != key1

    # Generate a key with different date
    key4 = generate_forecast_cache_key(product="DALMP", date="2023-11-21")
    # Assert that it's different from the first key
    assert key4 != key1

    # Generate a key with different format
    key5 = generate_forecast_cache_key(product="DALMP", date="2023-11-20", format_str="csv")
    # Assert that it's different from the first key
    assert key5 != key1

    # Test with datetime objects instead of strings
    date_obj = datetime.date(2023, 11, 20)
    key6 = generate_forecast_cache_key(product="DALMP", date=date_obj)
    # Assert that keys are consistent regardless of date format
    assert key6 == key1


def test_cache_forecast():
    """Tests storing forecast data in the cache"""
    # Create a sample forecast dataframe
    sample_df = create_sample_visualization_dataframe()
    # Generate a cache key
    key = generate_forecast_cache_key(product="DALMP", date="2023-11-20")
    # Cache the forecast data
    success = cache_forecast(key, sample_df)
    # Assert that caching was successful
    assert success is True

    # Retrieve the cached forecast
    retrieved_df = get_cached_forecast(key)
    # Assert that retrieved data matches original data
    pd.testing.assert_frame_equal(retrieved_df, sample_df)

    # Test with invalid dataframe
    invalid_df = pd.DataFrame({"invalid": [1, 2, 3]})
    # Assert that caching fails for invalid data
    success = cache_forecast(key, invalid_df)
    assert success is False


def test_get_cached_forecast():
    """Tests retrieving forecast data from the cache"""
    # Create a sample forecast dataframe
    sample_df = create_sample_visualization_dataframe()
    # Generate a cache key
    key = generate_forecast_cache_key(product="DALMP", date="2023-11-20")
    # Cache the forecast data
    cache_forecast(key, sample_df)

    # Retrieve the cached forecast
    retrieved_df = get_cached_forecast(key)
    # Assert that retrieved data matches original data
    pd.testing.assert_frame_equal(retrieved_df, sample_df)

    # Test with non-existent key
    non_existent_key = generate_forecast_cache_key(product="RTLMP", date="2023-11-21")
    retrieved_df = get_cached_forecast(non_existent_key)
    # Assert that None is returned for non-existent key
    assert retrieved_df is None

    # Test with expired cache entry
    expired_key = generate_forecast_cache_key(product="DALMP", date="2023-11-22")
    cache_forecast(expired_key, sample_df, timeout=1)
    # Mock time to advance beyond timeout
    with mock.patch("src.web.data.cache_manager.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = datetime.datetime.now() + datetime.timedelta(seconds=2)
        retrieved_df = get_cached_forecast(expired_key)
    # Assert that None is returned for expired entry
    assert retrieved_df is None


def test_clear_forecast_cache():
    """Tests clearing the forecast cache"""
    # Create multiple sample forecast dataframes for different products
    dalmp_df = create_sample_visualization_dataframe(product="DALMP")
    rtlmp_df = create_sample_visualization_dataframe(product="RTLMP")
    regup_df = create_sample_visualization_dataframe(product="RegUp")

    # Cache all the forecast data
    dalmp_key = generate_forecast_cache_key(product="DALMP", date="2023-11-20")
    rtlmp_key = generate_forecast_cache_key(product="RTLMP", date="2023-11-20")
    regup_key = generate_forecast_cache_key(product="RegUp", date="2023-11-20")
    cache_forecast(dalmp_key, dalmp_df)
    cache_forecast(rtlmp_key, rtlmp_df)
    cache_forecast(regup_key, regup_df)

    # Verify that cache contains the forecasts
    assert get_cached_forecast(dalmp_key) is not None
    assert get_cached_forecast(rtlmp_key) is not None
    assert get_cached_forecast(regup_key) is not None

    # Clear cache for a specific product
    clear_forecast_cache(product="DALMP")
    # Verify that only that product's forecasts are cleared
    assert get_cached_forecast(dalmp_key) is None
    assert get_cached_forecast(rtlmp_key) is not None
    assert get_cached_forecast(regup_key) is not None

    # Clear entire cache
    clear_forecast_cache()
    # Verify that cache is empty
    assert get_cached_forecast(rtlmp_key) is None
    assert get_cached_forecast(regup_key) is None


def test_get_forecast_cache_stats():
    """Tests retrieving statistics about the forecast cache"""
    # Clear the cache to start with known state
    clear_forecast_cache()
    # Get initial cache stats
    initial_stats = get_forecast_cache_stats()
    # Verify that cache is empty
    assert initial_stats["entry_count"] == 0
    assert initial_stats["hit_count"] == 0
    assert initial_stats["miss_count"] == 0

    # Cache multiple forecast dataframes
    dalmp_df = create_sample_visualization_dataframe(product="DALMP")
    rtlmp_df = create_sample_visualization_dataframe(product="RTLMP")
    dalmp_key = generate_forecast_cache_key(product="DALMP", date="2023-11-20")
    rtlmp_key = generate_forecast_cache_key(product="RTLMP", date="2023-11-20")
    cache_forecast(dalmp_key, dalmp_df)
    cache_forecast(rtlmp_key, rtlmp_df)

    # Get updated cache stats
    updated_stats = get_forecast_cache_stats()
    # Verify that stats show correct number of entries
    assert updated_stats["entry_count"] == 2
    # Verify that stats show entries by product
    assert updated_stats["products"]["DALMP"] == 1
    assert updated_stats["products"]["RTLMP"] == 1

    # Retrieve a cached forecast to increment hit counter
    get_cached_forecast(dalmp_key)
    final_stats = get_forecast_cache_stats()
    # Verify that hit/miss counters are working
    assert final_stats["hit_count"] == 1
    assert final_stats["miss_count"] == 0


def test_is_forecast_cache_valid():
    """Tests checking if a forecast cache entry is valid"""
    # Create a sample forecast dataframe
    sample_df = create_sample_visualization_dataframe()
    # Generate a cache key
    key = generate_forecast_cache_key(product="DALMP", date="2023-11-20")
    # Cache the forecast data
    cache_forecast(key, sample_df)

    # Check if cache entry is valid
    is_valid = is_forecast_cache_valid(key)
    # Assert that it's valid
    assert is_valid is True

    # Mock time to advance beyond timeout
    with mock.patch("src.web.data.cache_manager.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = datetime.datetime.now() + datetime.timedelta(seconds=CACHE_TIMEOUT + 1)
        # Check if cache entry is valid again
        is_valid = is_forecast_cache_valid(key)
    # Assert that it's now invalid
    assert is_valid is False

    # Test with non-existent key
    non_existent_key = generate_forecast_cache_key(product="RTLMP", date="2023-11-21")
    is_valid = is_forecast_cache_valid(non_existent_key)
    # Assert that non-existent key is invalid
    assert is_valid is False


class TestForecastCacheManager:
    """Test class for the ForecastCacheManager functionality"""

    def setup_method(self, method):
        """Set up test environment before each test method"""
        # Clear the forecast cache
        clear_forecast_cache()
        # Create a ForecastCacheManager instance
        self.cache_manager = ForecastCacheManager()
        # Create sample test data
        self.sample_df = create_sample_visualization_dataframe()
        # Set up any mocks needed for testing
        pass

    def teardown_method(self, method):
        """Clean up after each test method"""
        # Clear the forecast cache
        clear_forecast_cache()
        # Reset any mocks
        pass

    def test_cache_forecast_method(self):
        """Tests the cache_forecast method of ForecastCacheManager"""
        # Create a sample forecast dataframe
        sample_df = create_sample_visualization_dataframe()
        # Call cache_forecast method with the dataframe
        success = self.cache_manager.cache_forecast(product="DALMP", forecast_df=sample_df, date="2023-11-20")
        # Assert that caching was successful
        assert success is True

        # Verify that data can be retrieved from cache
        retrieved_df = self.cache_manager.get_forecast(product="DALMP", date="2023-11-20")
        pd.testing.assert_frame_equal(retrieved_df, sample_df)

    def test_get_forecast_method(self):
        """Tests the get_forecast method of ForecastCacheManager"""
        # Create a sample forecast dataframe
        sample_df = create_sample_visualization_dataframe()
        # Cache the forecast
        self.cache_manager.cache_forecast(product="DALMP", forecast_df=sample_df, date="2023-11-20")

        # Call get_forecast method with same parameters
        retrieved_df = self.cache_manager.get_forecast(product="DALMP", date="2023-11-20")
        # Assert that retrieved data matches original data
        pd.testing.assert_frame_equal(retrieved_df, sample_df)

        # Call with parameters not in cache
        retrieved_df = self.cache_manager.get_forecast(product="RTLMP", date="2023-11-21")
        # Assert that None is returned
        assert retrieved_df is None

    def test_clear_cache_method(self):
        """Tests the clear_cache method of ForecastCacheManager"""
        # Cache multiple forecast dataframes for different products
        dalmp_df = create_sample_visualization_dataframe(product="DALMP")
        rtlmp_df = create_sample_visualization_dataframe(product="RTLMP")
        self.cache_manager.cache_forecast(product="DALMP", forecast_df=dalmp_df, date="2023-11-20")
        self.cache_manager.cache_forecast(product="RTLMP", forecast_df=rtlmp_df, date="2023-11-20")

        # Call clear_cache for a specific product
        self.cache_manager.clear_cache(product="DALMP")
        # Verify that only that product's forecasts are cleared
        assert self.cache_manager.get_forecast(product="DALMP", date="2023-11-20") is None
        assert self.cache_manager.get_forecast(product="RTLMP", date="2023-11-20") is not None

        # Call clear_cache with no parameters
        self.cache_manager.clear_cache()
        # Verify that all forecasts are cleared
        assert self.cache_manager.get_forecast(product="RTLMP", date="2023-11-20") is None

    def test_get_stats_method(self):
        """Tests the get_stats method of ForecastCacheManager"""
        # Cache multiple forecast dataframes
        dalmp_df = create_sample_visualization_dataframe(product="DALMP")
        rtlmp_df = create_sample_visualization_dataframe(product="RTLMP")
        self.cache_manager.cache_forecast(product="DALMP", forecast_df=dalmp_df, date="2023-11-20")
        self.cache_manager.cache_forecast(product="RTLMP", forecast_df=rtlmp_df, date="2023-11-20")

        # Call get_stats method
        stats = self.cache_manager.get_stats()
        # Verify that stats contain expected information
        assert stats["entry_count"] == 2
        # Verify counts of entries by product
        assert stats["products"]["DALMP"] == 1
        assert stats["products"]["RTLMP"] == 1
        # Verify hit/miss counters
        assert stats["hit_count"] == 0
        assert stats["miss_count"] == 0

    def test_product_tracking(self):
        """Tests that ForecastCacheManager correctly tracks keys by product"""
        # Cache forecasts for multiple products
        dalmp_df = create_sample_visualization_dataframe(product="DALMP")
        rtlmp_df = create_sample_visualization_dataframe(product="RTLMP")
        self.cache_manager.cache_forecast(product="DALMP", forecast_df=dalmp_df, date="2023-11-20")
        self.cache_manager.cache_forecast(product="RTLMP", forecast_df=rtlmp_df, date="2023-11-20")

        # Inspect the _product_keys dictionary
        product_keys = self.cache_manager._product_keys
        # Verify that keys are correctly tracked by product
        assert "DALMP" in product_keys
        assert "RTLMP" in product_keys
        assert len(product_keys["DALMP"]) == 1
        assert len(product_keys["RTLMP"]) == 1

        # Clear cache for one product
        self.cache_manager.clear_cache(product="DALMP")
        # Verify that only that product's keys are removed
        assert "DALMP" not in self.cache_manager._product_keys
        assert "RTLMP" in self.cache_manager._product_keys


@pytest.mark.parametrize("cache_enabled", [False])
def test_cache_disabled(cache_enabled):
    """Tests that caching functions properly handle disabled cache"""
    # Mock CACHE_ENABLED to be False
    with mock.patch("src.web.data.cache_manager.CACHE_ENABLED", cache_enabled):
        # Create a sample forecast dataframe
        sample_df = create_sample_visualization_dataframe()
        # Generate a cache key
        key = generate_forecast_cache_key(product="DALMP", date="2023-11-20")
        # Attempt to cache the forecast
        success = cache_forecast(key, sample_df)
        # Assert that caching returns False
        assert success is False

        # Attempt to retrieve from cache
        retrieved_df = get_cached_forecast(key)
        # Assert that None is returned
        assert retrieved_df is None

        # Check if cache is valid
        is_valid = is_forecast_cache_valid(key)
        # Assert that it returns False when cache is disabled
        assert is_valid is False


def test_cache_timeout():
    """Tests that cache entries expire after the timeout period"""
    # Create a sample forecast dataframe
    sample_df = create_sample_visualization_dataframe()
    # Generate a cache key
    key = generate_forecast_cache_key(product="DALMP", date="2023-11-20")
    # Cache the forecast with a short timeout
    cache_forecast(key, sample_df, timeout=1)

    # Immediately retrieve the forecast
    retrieved_df = get_cached_forecast(key)
    # Assert that it's retrieved successfully
    pd.testing.assert_frame_equal(retrieved_df, sample_df)

    # Mock time to advance beyond timeout
    with mock.patch("src.web.data.cache_manager.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = datetime.datetime.now() + datetime.timedelta(seconds=2)
        # Attempt to retrieve again
        retrieved_df = get_cached_forecast(key)
    # Assert that None is returned due to expiration
    assert retrieved_df is None


def test_cache_forecast_decorator():
    """Tests the cache_forecast_decorator functionality"""
    # Define a test function that returns a forecast dataframe
    @mock.patch("src.web.data.cache_manager.datetime")
    def get_forecast_data(mock_datetime, product, date):
        mock_datetime.datetime.now.return_value = datetime.datetime(2023, 1, 1)
        return create_sample_visualization_dataframe(product=product)

    # Decorate it with cache_forecast_decorator
    from src.web.data.cache_manager import cache_forecast_decorator
    cached_get_forecast_data = cache_forecast_decorator()(get_forecast_data)

    # Call the function with specific parameters
    result1 = cached_get_forecast_data(product="DALMP", date="2023-11-20")
    # Verify that result is correct
    assert isinstance(result1, pd.DataFrame)

    # Call again with same parameters
    result2 = cached_get_forecast_data(product="DALMP", date="2023-11-20")
    # Verify that result is from cache (check hit counter)
    assert isinstance(result2, pd.DataFrame)
    assert id(result1) == id(result2)

    # Call with different parameters
    result3 = cached_get_forecast_data(product="RTLMP", date="2023-11-21")
    # Verify that new result is calculated, not from cache
    assert isinstance(result3, pd.DataFrame)
    assert id(result1) != id(result3)