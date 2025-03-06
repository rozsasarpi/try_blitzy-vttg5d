"""
Unit tests for the storage_manager.py module, which provides the main interface for the
forecast storage system. Tests cover all public functions of the storage manager, including
saving, retrieving, and managing forecast data, with appropriate mocking of dependencies.
"""

import pytest  # pytest: 7.0.0+
import pandas  # pandas: 1.3.0+
from unittest.mock import patch  # unittest.mock: standard library
from datetime import datetime  # datetime: standard library
from pathlib import Path  # pathlib: standard library

from src.backend.storage import storage_manager  # Module under test - main interface for forecast storage
from src.backend.storage import exceptions  # Custom exceptions for storage operations
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data  # Create mock forecast data for testing
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid forecast products


@pytest.mark.parametrize('is_fallback', [True, False])
def test_save_forecast(is_fallback):
    """Tests that save_forecast correctly saves a forecast dataframe"""
    # Create a mock forecast dataframe
    mock_df = create_mock_forecast_data()
    # Create a timestamp for the forecast
    forecast_timestamp = datetime(2023, 1, 1, 0, 0, 0)
    # Define a product to save
    product = 'DALMP'

    # Mock the store_forecast function from dataframe_store
    with patch('src.backend.storage.storage_manager.store_forecast') as mock_store_forecast:
        # Set the return value of the mock function
        mock_store_forecast.return_value = Path('/mocked/file/path.parquet')

        # Call save_forecast with the mock dataframe, timestamp, product, and is_fallback flag
        file_path = storage_manager.save_forecast(mock_df, forecast_timestamp, product, is_fallback)

        # Assert that store_forecast was called with the correct arguments
        mock_store_forecast.assert_called_once_with(mock_df, forecast_timestamp, product, is_fallback)

        # Assert that the function returns the expected path
        assert file_path == Path('/mocked/file/path.parquet')


def test_get_forecast():
    """Tests that get_forecast correctly retrieves a forecast dataframe"""
    # Create a timestamp for the forecast
    forecast_timestamp = datetime(2023, 1, 1, 0, 0, 0)
    # Define a product to retrieve
    product = 'DALMP'

    # Create a mock dataframe to be returned by load_forecast
    mock_df = create_mock_forecast_data()

    # Mock the load_forecast function from dataframe_store to return the mock dataframe
    with patch('src.backend.storage.storage_manager.load_forecast') as mock_load_forecast:
        # Set the return value of the mock function
        mock_load_forecast.return_value = mock_df

        # Call get_forecast with the timestamp and product
        retrieved_df = storage_manager.get_forecast(forecast_timestamp, product)

        # Assert that load_forecast was called with the correct arguments
        mock_load_forecast.assert_called_once_with(forecast_timestamp, product)

        # Assert that the function returns the expected dataframe
        assert retrieved_df.equals(mock_df)


def test_get_forecast_not_found():
    """Tests that get_forecast correctly handles the case when a forecast is not found"""
    # Create a timestamp for the forecast
    forecast_timestamp = datetime(2023, 1, 1, 0, 0, 0)
    # Define a product to retrieve
    product = 'DALMP'

    # Mock the load_forecast function from dataframe_store to raise DataFrameNotFoundError
    with patch('src.backend.storage.storage_manager.load_forecast') as mock_load_forecast:
        # Set the side effect of the mock function to raise DataFrameNotFoundError
        mock_load_forecast.side_effect = exceptions.DataFrameNotFoundError("Forecast not found", product, forecast_timestamp)

        # Use pytest.raises to assert that get_forecast raises DataFrameNotFoundError
        with pytest.raises(exceptions.DataFrameNotFoundError):
            # Call get_forecast with the timestamp and product inside the context manager
            storage_manager.get_forecast(forecast_timestamp, product)

        # Assert that load_forecast was called with the correct arguments
        mock_load_forecast.assert_called_once_with(forecast_timestamp, product)


def test_get_latest_forecast():
    """Tests that get_latest_forecast correctly retrieves the latest forecast for a product"""
    # Define a product to retrieve
    product = 'DALMP'

    # Create a mock dataframe to be returned by load_latest_forecast
    mock_df = create_mock_forecast_data()

    # Mock the load_latest_forecast function from dataframe_store to return the mock dataframe
    with patch('src.backend.storage.storage_manager.load_latest_forecast') as mock_load_latest_forecast:
        # Set the return value of the mock function
        mock_load_latest_forecast.return_value = mock_df

        # Call get_latest_forecast with the product
        retrieved_df = storage_manager.get_latest_forecast(product)

        # Assert that load_latest_forecast was called with the correct arguments
        mock_load_latest_forecast.assert_called_once_with(product)

        # Assert that the function returns the expected dataframe
        assert retrieved_df.equals(mock_df)


def test_get_latest_forecast_not_found():
    """Tests that get_latest_forecast correctly handles the case when no forecast is found"""
    # Define a product to retrieve
    product = 'DALMP'

    # Mock the load_latest_forecast function from dataframe_store to raise DataFrameNotFoundError
    with patch('src.backend.storage.storage_manager.load_latest_forecast') as mock_load_latest_forecast:
        # Set the side effect of the mock function to raise DataFrameNotFoundError
        mock_load_latest_forecast.side_effect = exceptions.DataFrameNotFoundError("Forecast not found", product, datetime.now())

        # Use pytest.raises to assert that get_latest_forecast raises DataFrameNotFoundError
        with pytest.raises(exceptions.DataFrameNotFoundError):
            # Call get_latest_forecast with the product inside the context manager
            storage_manager.get_latest_forecast(product)

        # Assert that load_latest_forecast was called with the correct arguments
        mock_load_latest_forecast.assert_called_once_with(product)


@pytest.mark.parametrize('product', [None, 'DALMP'])
def test_get_forecasts_for_period(product):
    """Tests that get_forecasts_for_period correctly retrieves forecasts for a time period"""
    # Create start and end dates for the period
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    end_date = datetime(2023, 1, 2, 0, 0, 0)

    # Create a mock dictionary of forecasts to be returned by get_forecasts_by_date_range
    mock_forecasts = {
        datetime(2023, 1, 1, 0, 0, 0): create_mock_forecast_data(),
        datetime(2023, 1, 1, 12, 0, 0): create_mock_forecast_data(),
    }

    # Mock the get_forecasts_by_date_range function from dataframe_store to return the mock dictionary
    with patch('src.backend.storage.storage_manager.get_forecasts_by_date_range') as mock_get_forecasts_by_date_range:
        # Set the return value of the mock function
        mock_get_forecasts_by_date_range.return_value = mock_forecasts

        # Call get_forecasts_for_period with the start date, end date, and product
        retrieved_forecasts = storage_manager.get_forecasts_for_period(start_date, end_date, product)

        # Assert that get_forecasts_by_date_range was called with the correct arguments
        mock_get_forecasts_by_date_range.assert_called_once_with(start_date, end_date, product)

        # Assert that the function returns the expected dictionary of forecasts
        assert retrieved_forecasts == mock_forecasts


@pytest.mark.parametrize('success', [True, False])
def test_remove_forecast(success):
    """Tests that remove_forecast correctly removes a forecast from storage"""
    # Create a timestamp for the forecast
    forecast_timestamp = datetime(2023, 1, 1, 0, 0, 0)
    # Define a product to remove
    product = 'DALMP'

    # Mock the delete_forecast function from dataframe_store to return the success parameter
    with patch('src.backend.storage.storage_manager.delete_forecast') as mock_delete_forecast:
        # Set the return value of the mock function
        mock_delete_forecast.return_value = success

        # Call remove_forecast with the timestamp and product
        result = storage_manager.remove_forecast(forecast_timestamp, product)

        # Assert that delete_forecast was called with the correct arguments
        mock_delete_forecast.assert_called_once_with(forecast_timestamp, product)

        # Assert that the function returns the expected success value
        assert result == success


@pytest.mark.parametrize('exists', [True, False])
def test_check_forecast_availability(exists):
    """Tests that check_forecast_availability correctly checks if a forecast is available"""
    # Create a timestamp for the forecast
    forecast_timestamp = datetime(2023, 1, 1, 0, 0, 0)
    # Define a product to check
    product = 'DALMP'

    # Mock the check_forecast_exists function from dataframe_store to return the exists parameter
    with patch('src.backend.storage.storage_manager.check_forecast_exists') as mock_check_forecast_exists:
        # Set the return value of the mock function
        mock_check_forecast_exists.return_value = exists

        # Call check_forecast_availability with the timestamp and product
        result = storage_manager.check_forecast_availability(forecast_timestamp, product)

        # Assert that check_forecast_exists was called with the correct arguments
        mock_check_forecast_exists.assert_called_once_with(forecast_timestamp, product)

        # Assert that the function returns the expected exists value
        assert result == exists


@pytest.mark.parametrize('mark_as_fallback', [True, False])
def test_duplicate_forecast(mark_as_fallback):
    """Tests that duplicate_forecast correctly creates a copy of a forecast with a new timestamp"""
    # Create source and target timestamps for the forecast
    source_timestamp = datetime(2023, 1, 1, 0, 0, 0)
    target_timestamp = datetime(2023, 1, 2, 0, 0, 0)
    # Define a product to duplicate
    product = 'DALMP'

    # Mock the copy_forecast function from dataframe_store to return a mock path
    with patch('src.backend.storage.storage_manager.copy_forecast') as mock_copy_forecast:
        # Set the return value of the mock function
        mock_copy_forecast.return_value = Path('/mocked/file/path.parquet')

        # Call duplicate_forecast with the source timestamp, target timestamp, product, and mark_as_fallback flag
        file_path = storage_manager.duplicate_forecast(source_timestamp, target_timestamp, product, mark_as_fallback)

        # Assert that copy_forecast was called with the correct arguments
        mock_copy_forecast.assert_called_once_with(source_timestamp, target_timestamp, product, mark_as_fallback)

        # Assert that the function returns the expected path
        assert file_path == Path('/mocked/file/path.parquet')


def test_get_forecast_info():
    """Tests that get_forecast_info correctly retrieves metadata about a specific forecast"""
    # Create a timestamp for the forecast
    forecast_timestamp = datetime(2023, 1, 1, 0, 0, 0)
    # Define a product to get info for
    product = 'DALMP'

    # Create a mock metadata dictionary to be returned by get_forecast_metadata
    mock_metadata = {'key1': 'value1', 'key2': 'value2'}

    # Mock the get_forecast_metadata function from dataframe_store to return the mock metadata
    with patch('src.backend.storage.storage_manager.get_forecast_metadata') as mock_get_forecast_metadata:
        # Set the return value of the mock function
        mock_get_forecast_metadata.return_value = mock_metadata

        # Call get_forecast_info with the timestamp and product
        metadata = storage_manager.get_forecast_info(forecast_timestamp, product)

        # Assert that get_forecast_metadata was called with the correct arguments
        mock_get_forecast_metadata.assert_called_once_with(forecast_timestamp, product)

        # Assert that the function returns the expected metadata
        assert metadata == mock_metadata


def test_get_latest_forecasts_info():
    """Tests that get_latest_forecasts_info correctly retrieves metadata about the latest forecasts"""
    # Create a mock metadata dictionary to be returned by get_latest_forecast_metadata
    mock_metadata = {'DALMP': {'key1': 'value1'}, 'RTLMP': {'key2': 'value2'}}

    # Mock the get_latest_forecast_metadata function from index_manager to return the mock metadata
    with patch('src.backend.storage.storage_manager.get_latest_forecast_metadata') as mock_get_latest_forecast_metadata:
        # Set the return value of the mock function
        mock_get_latest_forecast_metadata.return_value = mock_metadata

        # Call get_latest_forecasts_info
        metadata = storage_manager.get_latest_forecasts_info()

        # Assert that get_latest_forecast_metadata was called
        mock_get_latest_forecast_metadata.assert_called_once()

        # Assert that the function returns the expected metadata
        assert metadata == mock_metadata


@pytest.mark.parametrize('retention_days', [None, 30])
def test_maintain_storage(retention_days):
    """Tests that maintain_storage correctly performs maintenance operations"""
    # Mock the clean_old_forecasts function from file_utils
    with patch('src.backend.storage.storage_manager.clean_old_forecasts') as mock_clean_old_forecasts, \
            patch('src.backend.storage.storage_manager.clean_index') as mock_clean_index:
        # Set the return value of the mock function
        mock_clean_old_forecasts.return_value = 5
        mock_clean_index.return_value = {'removed_entries': 3, 'remaining_entries': 10}

        # Call maintain_storage with the retention_days parameter
        stats = storage_manager.maintain_storage(retention_days)

        # Assert that clean_old_forecasts was called with the correct arguments
        mock_clean_old_forecasts.assert_called_once_with(retention_days if retention_days is not None else 90)

        # Assert that clean_index was called
        mock_clean_index.assert_called_once()

        # Assert that the function returns a dictionary with the expected statistics
        assert stats == {
            'removed_files': 5,
            'removed_index_entries': 3,
            'remaining_index_entries': 10,
            'retention_days': retention_days if retention_days is not None else 90
        }


def test_rebuild_storage_index():
    """Tests that rebuild_storage_index correctly rebuilds the storage index"""
    # Mock the rebuild_index function from index_manager to return the mock statistics
    with patch('src.backend.storage.storage_manager.rebuild_index') as mock_rebuild_index:
        # Set the return value of the mock function
        mock_rebuild_index.return_value = {'files_processed': 15, 'index_entries': 15}

        # Call rebuild_storage_index
        stats = storage_manager.rebuild_storage_index()

        # Assert that rebuild_index was called
        mock_rebuild_index.assert_called_once()

        # Assert that the function returns the expected statistics
        assert stats == {'files_processed': 15, 'index_entries': 15}


def test_get_storage_info():
    """Tests that get_storage_info correctly retrieves information about the storage system"""
    # Mock the get_storage_statistics function from dataframe_store to return a mock statistics dictionary
    with patch('src.backend.storage.storage_manager.get_storage_statistics') as mock_get_storage_statistics, \
            patch('src.backend.storage.storage_manager.get_index_statistics') as mock_get_index_statistics, \
            patch('src.backend.storage.storage_manager.get_schema_info') as mock_get_schema_info:
        # Set the return value of the mock function
        mock_get_storage_statistics.return_value = {'total_forecasts': 100, 'storage_space_mb': 500}
        mock_get_index_statistics.return_value = {'total_entries': 100, 'index_size_mb': 10}
        mock_get_schema_info.return_value = {'schema_version': '1.0'}

        # Call get_storage_info
        info = storage_manager.get_storage_info()

        # Assert that all mocked functions were called
        mock_get_storage_statistics.assert_called_once()
        mock_get_index_statistics.assert_called_once()
        mock_get_schema_info.assert_called_once()

        # Assert that the function returns a dictionary with the expected information
        assert 'storage_stats' in info
        assert 'index_stats' in info
        assert 'schema_info' in info
        assert 'storage_paths' in info
        assert 'products' in info
        assert info['storage_stats'] == {'total_forecasts': 100, 'storage_space_mb': 500}
        assert info['index_stats'] == {'total_entries': 100, 'index_size_mb': 10}
        assert info['schema_info'] == {'schema_version': '1.0'}


def test_initialize_storage_new():
    """Tests that initialize_storage correctly initializes a new storage system"""
    # Mock os.path.exists to return False for the storage root directory
    with patch('os.path.exists') as mock_exists, \
            patch('src.backend.storage.storage_manager.get_base_storage_path') as mock_get_base_storage_path, \
            patch('src.backend.storage.storage_manager.get_index_file_path') as mock_get_index_file_path, \
            patch('src.backend.storage.storage_manager.rebuild_storage_index') as mock_rebuild_storage_index:
        # Set the return value of the mock function
        mock_exists.side_effect = [False, False]  # Root dir doesn't exist, index file doesn't exist
        mock_get_base_storage_path.return_value = Path('/mocked/storage/root')
        mock_get_index_file_path.return_value = Path('/mocked/storage/index.parquet')
        mock_rebuild_storage_index.return_value = {'files_processed': 0, 'index_entries': 0}

        # Call initialize_storage
        initialized = storage_manager.initialize_storage()

        # Assert that get_base_storage_path was called
        mock_get_base_storage_path.assert_called_once()

        # Assert that get_index_file_path was called
        mock_get_index_file_path.assert_called_once()

        # Assert that rebuild_storage_index was called
        mock_rebuild_storage_index.assert_called_once()

        # Assert that the function returns True (initialization performed)
        assert initialized is True


def test_initialize_storage_existing():
    """Tests that initialize_storage correctly handles an already initialized storage system"""
    # Mock os.path.exists to return True for both the storage root directory and index file
    with patch('os.path.exists') as mock_exists, \
            patch('src.backend.storage.storage_manager.get_base_storage_path') as mock_get_base_storage_path, \
            patch('src.backend.storage.storage_manager.get_index_file_path') as mock_get_index_file_path, \
            patch('src.backend.storage.storage_manager.rebuild_storage_index') as mock_rebuild_storage_index:
        # Set the return value of the mock function
        mock_exists.return_value = True  # Root dir exists, index file exists
        mock_get_base_storage_path.return_value = Path('/mocked/storage/root')
        mock_get_index_file_path.return_value = Path('/mocked/storage/index.parquet')

        # Call initialize_storage
        initialized = storage_manager.initialize_storage()

        # Assert that get_base_storage_path was called
        mock_get_base_storage_path.assert_called_once()

        # Assert that get_index_file_path was called
        mock_get_index_file_path.assert_called_once()

        # Assert that rebuild_storage_index was not called
        mock_rebuild_storage_index.assert_not_called()

        # Assert that the function returns False (no initialization needed)
        assert initialized is False


@pytest.mark.integration
def test_integration_save_and_get_forecast(temp_storage_path):
    """Integration test that verifies saving and retrieving a forecast works end-to-end"""
    # Create a mock forecast dataframe
    mock_df = create_mock_forecast_data()
    # Create a timestamp for the forecast
    forecast_timestamp = datetime(2023, 1, 1, 0, 0, 0)
    # Define a product to use
    product = 'DALMP'

    # Set up environment to use the temporary storage path
    with patch('src.backend.config.settings.STORAGE_ROOT_DIR', str(temp_storage_path)):
        # Initialize the storage system
        storage_manager.initialize_storage()

        # Save the forecast using save_forecast
        storage_manager.save_forecast(mock_df, forecast_timestamp, product)

        # Retrieve the forecast using get_forecast
        retrieved_df = storage_manager.get_forecast(forecast_timestamp, product)

        # Assert that the retrieved forecast matches the original
        assert retrieved_df.equals(mock_df)