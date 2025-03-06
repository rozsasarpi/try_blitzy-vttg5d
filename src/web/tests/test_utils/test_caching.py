import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import time  # standard library
import os  # standard library
import tempfile  # standard library
import pandas  # pandas: 2.0.0+

from src.web.utils.caching import cache  # Function to test
from src.web.utils.caching import disk_cache  # Function to test
from src.web.utils.caching import get_from_cache  # Function to test
from src.web.utils.caching import store_in_cache  # Function to test
from src.web.utils.caching import clear_cache  # Function to test
from src.web.utils.caching import clear_disk_cache  # Function to test
from src.web.utils.caching import get_cache_stats  # Function to test
from src.web.utils.caching import invalidate_cache_entry  # Function to test
from src.web.utils.caching import CacheManager  # Class to test
from src.web.config.settings import CACHE_ENABLED  # Flag indicating if caching is enabled
from src.web.config.settings import CACHE_TIMEOUT  # Timeout in seconds for cached data
from src.web.config.settings import CACHE_DIR  # Directory for storing cached data
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe  # Create sample forecast dataframe for testing caching

TEST_CACHE_KEY = 'test_cache_key'
TEST_CACHE_VALUE = 'test_cache_value'
TEST_CACHE_TIMEOUT = 10


class MockFunction:
    """Helper class for testing cache decorators"""

    def __init__(self):
        """Initializes the MockFunction with zero call count"""
        self.call_count = 0
        self.call_args = []

    def __call__(self, *args, **kwargs):
        """Makes the class instance callable and tracks calls"""
        self.call_count += 1
        self.call_args.append((args, kwargs))
        return TEST_CACHE_VALUE

    def reset(self):
        """Resets the call count and arguments"""
        self.call_count = 0
        self.call_args = []


@pytest.mark.parametrize('cache_func', [cache, disk_cache])
def test_cache_decorator_basic(cache_func):
    """Tests that the cache decorator correctly caches function results"""
    mock_func = MockFunction()
    cached_function = cache_func()(mock_func)

    # Call the decorated function multiple times with the same arguments
    result1 = cached_function()
    result2 = cached_function()
    result3 = cached_function()

    # Assert that the function was only called once (subsequent calls use cache)
    assert mock_func.call_count == 1
    assert result1 == TEST_CACHE_VALUE
    assert result2 == TEST_CACHE_VALUE
    assert result3 == TEST_CACHE_VALUE

    # Call the function with different arguments
    result4 = cached_function(1, 2)

    # Assert that the function was called again (new arguments bypass cache)
    assert mock_func.call_count == 2
    assert result4 == TEST_CACHE_VALUE


@pytest.mark.parametrize('cache_func', [cache, disk_cache])
def test_cache_decorator_timeout(cache_func):
    """Tests that cached values expire after the specified timeout"""
    mock_func = MockFunction()
    cached_function = cache_func(timeout=1)(mock_func)

    # Call the decorated function to cache the result
    result1 = cached_function()
    assert mock_func.call_count == 1
    assert result1 == TEST_CACHE_VALUE

    # Wait longer than the timeout period
    time.sleep(1.1)

    # Call the function again with the same arguments
    result2 = cached_function()

    # Assert that the function was called again (cache expired)
    assert mock_func.call_count == 2
    assert result2 == TEST_CACHE_VALUE


def test_get_from_cache():
    """Tests retrieving values from the cache"""
    # Store a test value in the cache with store_in_cache
    store_in_cache(TEST_CACHE_KEY, TEST_CACHE_VALUE)

    # Retrieve the value using get_from_cache
    retrieved_value = get_from_cache(TEST_CACHE_KEY)

    # Assert that the retrieved value matches the stored value
    assert retrieved_value == TEST_CACHE_VALUE

    # Try to retrieve a non-existent key
    non_existent_value = get_from_cache('non_existent_key')

    # Assert that None is returned for non-existent keys
    assert non_existent_value is None


def test_store_in_cache():
    """Tests storing values in the cache"""
    # Store a test value in the cache with store_in_cache
    success = store_in_cache(TEST_CACHE_KEY, TEST_CACHE_VALUE)

    # Verify the return value indicates success
    assert success is True

    # Retrieve the value to confirm it was stored
    retrieved_value = get_from_cache(TEST_CACHE_KEY)
    assert retrieved_value == TEST_CACHE_VALUE

    # Store a different value with the same key
    new_value = 'new_test_value'
    store_in_cache(TEST_CACHE_KEY, new_value)

    # Verify the value was updated in the cache
    retrieved_value = get_from_cache(TEST_CACHE_KEY)
    assert retrieved_value == new_value


def test_clear_cache():
    """Tests clearing the entire cache"""
    # Store multiple values in the cache
    store_in_cache('key1', 'value1')
    store_in_cache('key2', 'value2')

    # Clear the entire cache using clear_cache
    clear_cache()

    # Try to retrieve the previously stored values
    value1 = get_from_cache('key1')
    value2 = get_from_cache('key2')

    # Assert that None is returned for all keys (cache was cleared)
    assert value1 is None
    assert value2 is None


def test_clear_specific_cache_entry():
    """Tests clearing a specific cache entry"""
    # Store multiple values in the cache with different keys
    store_in_cache('key1', 'value1')
    store_in_cache('key2', 'value2')
    store_in_cache('key3', 'value3')

    # Clear a specific cache entry using clear_cache with a key
    clear_cache('key2')

    # Verify that the specified entry is removed
    assert get_from_cache('key2') is None

    # Verify that other entries remain in the cache
    assert get_from_cache('key1') == 'value1'
    assert get_from_cache('key3') == 'value3'


def test_get_cache_stats():
    """Tests retrieving cache statistics"""
    # Clear the cache to start with a clean state
    clear_cache()

    # Store multiple values in the cache
    store_in_cache('key1', 'value1')
    store_in_cache('key2', 'value2')

    # Retrieve some values to generate hits
    get_from_cache('key1')  # Hit
    get_from_cache('key2')  # Hit

    # Try to retrieve non-existent values to generate misses
    get_from_cache('key3')  # Miss
    get_from_cache('key4')  # Miss

    # Get cache statistics using get_cache_stats
    stats = get_cache_stats()

    # Assert that the statistics contain expected fields (entries, size, hit_rate)
    assert 'hit_count' in stats
    assert 'miss_count' in stats
    assert 'hit_rate' in stats
    assert 'entry_count' in stats
    assert 'expired_count' in stats
    assert 'estimated_size_bytes' in stats

    # Verify that the hit and miss counts match expected values
    assert stats['hit_count'] == 2
    assert stats['miss_count'] == 2
    assert stats['entry_count'] == 2


def test_invalidate_cache_entry():
    """Tests invalidating a specific cache entry"""
    # Store a value in the cache
    store_in_cache(TEST_CACHE_KEY, TEST_CACHE_VALUE)

    # Invalidate the cache entry using invalidate_cache_entry
    success = invalidate_cache_entry(TEST_CACHE_KEY)

    # Verify the return value indicates success
    assert success is True

    # Try to retrieve the invalidated entry
    retrieved_value = get_from_cache(TEST_CACHE_KEY)

    # Assert that None is returned (entry was invalidated)
    assert retrieved_value is None

    # Try to invalidate a non-existent entry
    success = invalidate_cache_entry('non_existent_key')

    # Verify the return value indicates failure
    assert success is False


@pytest.mark.parametrize('cache_func', [cache, disk_cache])
def test_cache_with_complex_data(cache_func):
    """Tests caching with complex data structures like pandas DataFrames"""
    # Create a sample DataFrame using create_sample_visualization_dataframe
    sample_df = create_sample_visualization_dataframe()

    def get_dataframe():
        return sample_df

    # Apply the cache decorator to the function
    cached_function = cache_func()(get_dataframe)

    # Call the function multiple times
    df1 = cached_function()
    df2 = cached_function()

    # Assert that the function was only called once
    assert get_dataframe.__code__ != cached_function.__code__

    # Verify that the returned DataFrame matches the original
    pandas.testing.assert_frame_equal(df1, sample_df)
    pandas.testing.assert_frame_equal(df2, sample_df)


def test_cache_manager_basic():
    """Tests basic functionality of the CacheManager class"""
    # Create a new CacheManager instance
    manager = CacheManager()

    # Set a value in the cache using manager.set
    manager.set(TEST_CACHE_KEY, TEST_CACHE_VALUE)

    # Retrieve the value using manager.get
    retrieved_value = manager.get(TEST_CACHE_KEY)

    # Assert that the retrieved value matches the stored value
    assert retrieved_value == TEST_CACHE_VALUE

    # Try to retrieve a non-existent key
    non_existent_value = manager.get('non_existent_key')

    # Assert that None is returned for non-existent keys
    assert non_existent_value is None


def test_cache_manager_timeout():
    """Tests timeout functionality of the CacheManager class"""
    # Create a new CacheManager instance
    manager = CacheManager()

    # Set a value in the cache with a short timeout
    manager.set(TEST_CACHE_KEY, TEST_CACHE_VALUE, timeout=1)

    # Verify the value can be retrieved immediately
    assert manager.get(TEST_CACHE_KEY) == TEST_CACHE_VALUE

    # Wait longer than the timeout period
    time.sleep(1.1)

    # Try to retrieve the value again
    retrieved_value = manager.get(TEST_CACHE_KEY)

    # Assert that None is returned (cache expired)
    assert retrieved_value is None


def test_cache_manager_clear():
    """Tests clearing functionality of the CacheManager class"""
    # Create a new CacheManager instance
    manager = CacheManager()

    # Set multiple values in the cache
    manager.set('key1', 'value1')
    manager.set('key2', 'value2')

    # Clear the entire cache using manager.clear
    manager.clear()

    # Try to retrieve the previously stored values
    value1 = manager.get('key1')
    value2 = manager.get('key2')

    # Assert that None is returned for all keys (cache was cleared)
    assert value1 is None
    assert value2 is None


def test_cache_manager_invalidate():
    """Tests invalidation functionality of the CacheManager class"""
    # Create a new CacheManager instance
    manager = CacheManager()

    # Set multiple values in the cache
    manager.set('key1', 'value1')
    manager.set('key2', 'value2')
    manager.set('key3', 'value3')

    # Invalidate a specific entry using manager.invalidate
    manager.invalidate('key2')

    # Verify that the specified entry is removed
    assert manager.get('key2') is None

    # Verify that other entries remain in the cache
    assert manager.get('key1') == 'value1'
    assert manager.get('key3') == 'value3'


def test_cache_manager_stats():
    """Tests statistics functionality of the CacheManager class"""
    # Create a new CacheManager instance
    manager = CacheManager()

    # Set multiple values in the cache
    manager.set('key1', 'value1')
    manager.set('key2', 'value2')

    # Retrieve some values to generate hits
    manager.get('key1')  # Hit
    manager.get('key2')  # Hit

    # Try to retrieve non-existent values to generate misses
    manager.get('key3')  # Miss
    manager.get('key4')  # Miss

    # Get cache statistics using manager.get_stats
    stats = manager.get_stats()

    # Assert that the statistics contain expected fields
    assert 'hit_count' in stats
    assert 'miss_count' in stats
    assert 'hit_rate' in stats
    assert 'entry_count' in stats
    assert 'expired_count' in stats
    assert 'estimated_size_bytes' in stats

    # Verify that the hit and miss counts match expected values
    assert stats['hit_count'] == 2
    assert stats['miss_count'] == 2
    assert stats['entry_count'] == 2


def test_disk_cache_file_creation():
    """Tests that disk_cache creates cache files in the specified directory"""
    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Patch CACHE_DIR to use the temporary directory
        with unittest.mock.patch('src.web.utils.caching.CACHE_DIR', temp_dir):
            @disk_cache()
            def cached_function():
                return TEST_CACHE_VALUE

            # Call the function to trigger cache file creation
            result = cached_function()

            # Verify that a cache file was created in the temporary directory
            cache_files = [f for f in os.listdir(temp_dir) if f.endswith('.pkl')]
            assert len(cache_files) == 1

            # Verify the file contains the expected cached data
            cache_file_path = os.path.join(temp_dir, cache_files[0])
            with open(cache_file_path, 'rb') as f:
                cached_data = f.read()
            assert cached_data != b''


def test_clear_disk_cache():
    """Tests clearing disk cache files"""
    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Patch CACHE_DIR to use the temporary directory
        with unittest.mock.patch('src.web.utils.caching.CACHE_DIR', temp_dir):
            # Create multiple cache files in the directory
            for i in range(3):
                file_path = os.path.join(temp_dir, f'test_file_{i}.pkl')
                with open(file_path, 'wb') as f:
                    f.write(b'test data')

            # Call clear_disk_cache to remove the files
            removed_count = clear_disk_cache()

            # Verify that the cache files were removed
            assert len(os.listdir(temp_dir)) == 0

            # Verify the return value indicates the correct number of files removed
            assert removed_count == 3


def test_cache_disabled():
    """Tests that caching functions properly handle disabled caching"""
    # Patch CACHE_ENABLED to False
    with unittest.mock.patch('src.web.utils.caching.CACHE_ENABLED', False):
        mock_func = MockFunction()

        @cache()(mock_func)
        def cached_function():
            return mock_func()

        # Call the decorated function multiple times with the same arguments
        cached_function()
        cached_function()
        cached_function()

        # Assert that the function was called every time (caching disabled)
        assert mock_func.call_count == 3

        # Try to store and retrieve values directly
        store_in_cache(TEST_CACHE_KEY, TEST_CACHE_VALUE)
        retrieved_value = get_from_cache(TEST_CACHE_KEY)

        # Verify that values are not cached when caching is disabled
        assert retrieved_value is None