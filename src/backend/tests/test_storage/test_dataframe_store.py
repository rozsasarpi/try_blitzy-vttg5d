# src/backend/tests/test_storage/test_dataframe_store.py
"""
Unit tests for the dataframe_store module in the storage component of the Electricity Market Price Forecasting System.
Tests the functionality for storing, loading, retrieving, and managing forecast dataframes with proper validation, error handling, and fallback mechanisms.
"""

import pytest  # pytest: 7.0.0+
import os  # standard library
import pathlib  # standard library
import datetime  # standard library
from mock import patch  # mock: 4.0.0+

import pandas as pd  # pandas: 2.0.0+
import numpy as np  # numpy: 1.24.0+

from src.backend.storage import dataframe_store  # Module under test
from src.backend.storage.exceptions import StorageError, SchemaValidationError, FileOperationError, DataFrameNotFoundError, DataIntegrityError  # Exceptions
from src.backend.storage.schema_definitions import validate_forecast_schema  # Schema validation function
from src.backend.storage.path_resolver import get_forecast_file_path, get_latest_file_path  # Path resolver functions
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data, create_invalid_forecast_data  # Mock forecast data
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid forecast products


class TestDataFrameStore:
    """Test class for dataframe_store module functionality"""

    def setup_method(self, method):
        """Setup method run before each test"""
        # Create a temporary directory for test forecasts
        self.temp_dir = pathlib.Path("test_data")
        self.temp_dir.mkdir(exist_ok=True)

        # Store the original settings
        self.original_storage_root = os.environ.get("STORAGE_ROOT_DIR")

        # Set up any required mocks
        os.environ["STORAGE_ROOT_DIR"] = str(self.temp_dir)

        # Initialize test data
        self.test_date = datetime.datetime.now()
        self.test_product = "DALMP"

    def teardown_method(self, method):
        """Teardown method run after each test"""
        # Clean up any created test files
        for file in self.temp_dir.glob("*"):
            file.unlink()
        self.temp_dir.rmdir()

        # Restore original settings
        if self.original_storage_root:
            os.environ["STORAGE_ROOT_DIR"] = self.original_storage_root
        else:
            del os.environ["STORAGE_ROOT_DIR"]

        # Remove any mocks
        # Clean up test data
        self.test_date = None
        self.test_product = None

    def test_store_forecast_valid(self):
        """Tests that store_forecast successfully stores a valid forecast dataframe"""
        # Create a test date (e.g., datetime.now())
        test_date = datetime.datetime.now()

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create a valid mock forecast dataframe using create_mock_forecast_data
        mock_df = create_mock_forecast_data()

        # Call store_forecast with the mock dataframe, timestamp, and product
        file_path = dataframe_store.store_forecast(mock_df, test_date, test_product)

        # Assert that the function returns a valid Path object
        assert isinstance(file_path, pathlib.Path)

        # Assert that the returned path exists on the filesystem
        assert file_path.exists()

        # Load the stored dataframe and verify it matches the original
        loaded_df = pd.read_parquet(file_path)
        pd.testing.assert_frame_equal(loaded_df, mock_df)

        # Clean up by deleting the test forecast
        os.remove(file_path)

    def test_store_forecast_invalid_schema(self):
        """Tests that store_forecast raises SchemaValidationError for invalid data"""
        # Create a test date (e.g., datetime.now())
        test_date = datetime.datetime.now()

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create an invalid mock forecast dataframe using create_invalid_forecast_data
        invalid_df = create_invalid_forecast_data()

        # Use pytest.raises to assert that SchemaValidationError is raised when calling store_forecast
        with pytest.raises(SchemaValidationError) as excinfo:
            dataframe_store.store_forecast(invalid_df, test_date, test_product)

        # Verify the error contains validation details
        assert "Forecast dataframe failed schema validation" in str(excinfo.value)

    def test_store_forecast_invalid_product(self):
        """Tests that store_forecast raises an error for invalid product"""
        # Create a test date (e.g., datetime.now())
        test_date = datetime.datetime.now()

        # Define an invalid product (e.g., 'INVALID_PRODUCT')
        invalid_product = 'INVALID_PRODUCT'

        # Create a valid mock forecast dataframe using create_mock_forecast_data
        mock_df = create_mock_forecast_data()

        # Use pytest.raises to assert that an error is raised when calling store_forecast
        with pytest.raises(StorageError) as excinfo:
            dataframe_store.store_forecast(mock_df, test_date, invalid_product)

        # Verify the error message mentions the invalid product
        assert f"Invalid product: {invalid_product}" in str(excinfo.value)

    @patch('src.backend.storage.dataframe_store.save_dataframe')
    def test_store_forecast_file_operation_error(self, mock_save_dataframe):
        """Tests that store_forecast handles file operation errors"""
        # Create a test date (e.g., datetime.now())
        test_date = datetime.datetime.now()

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create a valid mock forecast dataframe using create_mock_forecast_data
        mock_df = create_mock_forecast_data()

        # Mock the save_dataframe function to raise an exception
        mock_save_dataframe.side_effect = Exception("Test file operation error")

        # Use pytest.raises to assert that FileOperationError is raised when calling store_forecast
        with pytest.raises(FileOperationError) as excinfo:
            dataframe_store.store_forecast(mock_df, test_date, test_product)

        # Verify the error contains details about the file operation
        assert "Failed to save dataframe" in str(excinfo.value)

    def test_load_forecast_success(self):
        """Tests that load_forecast successfully loads a stored forecast"""
        # Create a test date (e.g., datetime.now())
        test_date = datetime.datetime.now()

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create and store a mock forecast dataframe
        mock_df = create_mock_forecast_data()
        file_path = dataframe_store.store_forecast(mock_df, test_date, test_product)

        # Call load_forecast with the same timestamp and product
        loaded_df = dataframe_store.load_forecast(test_date, test_product)

        # Assert that the loaded dataframe matches the original
        pd.testing.assert_frame_equal(loaded_df, mock_df)

        # Clean up by deleting the test forecast
        os.remove(file_path)

    def test_load_forecast_not_found(self):
        """Tests that load_forecast raises DataFrameNotFoundError when forecast doesn't exist"""
        # Create a test date that won't have a forecast (e.g., datetime(2000, 1, 1))
        test_date = datetime.datetime(2000, 1, 1)

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Use pytest.raises to assert that DataFrameNotFoundError is raised when calling load_forecast
        with pytest.raises(DataFrameNotFoundError) as excinfo:
            dataframe_store.load_forecast(test_date, test_product)

        # Verify the error contains details about the missing forecast
        assert f"Forecast not found for {test_product} at {test_date}" in str(excinfo.value)

    @patch('src.backend.storage.dataframe_store.check_storage_integrity')
    def test_load_forecast_integrity_error(self, mock_check_storage_integrity):
        """Tests that load_forecast raises DataIntegrityError for corrupted data"""
        # Create a test date (e.g., datetime.now())
        test_date = datetime.datetime.now()

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create and store a mock forecast dataframe
        mock_df = create_mock_forecast_data()
        file_path = dataframe_store.store_forecast(mock_df, test_date, test_product)

        # Mock the check_storage_integrity function to return (False, {'error': 'Test integrity error'})
        mock_check_storage_integrity.return_value = (False, {'error': 'Test integrity error'})

        # Use pytest.raises to assert that DataIntegrityError is raised when calling load_forecast
        with pytest.raises(DataIntegrityError) as excinfo:
            dataframe_store.load_forecast(test_date, test_product)

        # Verify the error contains details about the integrity issues
        assert "Forecast data failed integrity check" in str(excinfo.value)

        # Clean up by deleting the test forecast
        os.remove(file_path)

    def test_load_latest_forecast(self):
        """Tests that load_latest_forecast loads the most recent forecast"""
        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create and store a mock forecast dataframe
        mock_df = create_mock_forecast_data()
        dataframe_store.store_forecast(mock_df, self.test_date, test_product)

        # Call load_latest_forecast with the product
        loaded_df = dataframe_store.load_latest_forecast(test_product)

        # Assert that the loaded dataframe is not None
        assert loaded_df is not None

        # Verify the loaded dataframe has the expected structure
        assert isinstance(loaded_df, pd.DataFrame)

        # Clean up by deleting the test forecast
        file_path = get_forecast_file_path(self.test_date, test_product)
        os.remove(file_path)

    @patch('src.backend.storage.dataframe_store.get_latest_file_path')
    def test_load_latest_forecast_not_found(self, mock_get_latest_file_path):
        """Tests that load_latest_forecast raises DataFrameNotFoundError when no forecasts exist"""
        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Mock get_latest_file_path to return a non-existent path
        mock_get_latest_file_path.return_value = pathlib.Path("non_existent_path")

        # Use pytest.raises to assert that DataFrameNotFoundError is raised when calling load_latest_forecast
        with pytest.raises(DataFrameNotFoundError) as excinfo:
            dataframe_store.load_latest_forecast(test_product)

        # Verify the error contains details about the missing forecast
        assert f"Latest forecast not found for {test_product}" in str(excinfo.value)

    def test_delete_forecast(self):
        """Tests that delete_forecast successfully removes a forecast"""
        # Create a test date (e.g., datetime.now())
        test_date = datetime.datetime.now()

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create and store a mock forecast dataframe
        mock_df = create_mock_forecast_data()
        file_path = dataframe_store.store_forecast(mock_df, test_date, test_product)

        # Verify the forecast exists using check_forecast_exists
        assert dataframe_store.check_forecast_exists(test_date, test_product)

        # Call delete_forecast with the timestamp and product
        result = dataframe_store.delete_forecast(test_date, test_product)

        # Assert that the function returns True
        assert result is True

        # Verify the forecast no longer exists using check_forecast_exists
        assert not dataframe_store.check_forecast_exists(test_date, test_product)

    def test_delete_forecast_not_found(self):
        """Tests that delete_forecast returns False when forecast doesn't exist"""
        # Create a test date that won't have a forecast (e.g., datetime(2000, 1, 1))
        test_date = datetime.datetime(2000, 1, 1)

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Call delete_forecast with the timestamp and product
        result = dataframe_store.delete_forecast(test_date, test_product)

        # Assert that the function returns False
        assert result is False

    def test_get_forecasts_by_date_range(self):
        """Tests that get_forecasts_by_date_range retrieves forecasts within a date range"""
        # Create a start date and end date spanning multiple days
        start_date = datetime.datetime(2023, 1, 1)
        end_date = datetime.datetime(2023, 1, 3)

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create and store multiple mock forecast dataframes with different dates
        mock_df1 = create_mock_forecast_data()
        dataframe_store.store_forecast(mock_df1, datetime.datetime(2023, 1, 1), test_product)
        mock_df2 = create_mock_forecast_data()
        dataframe_store.store_forecast(mock_df2, datetime.datetime(2023, 1, 2), test_product)
        mock_df3 = create_mock_forecast_data()
        dataframe_store.store_forecast(mock_df3, datetime.datetime(2023, 1, 3), test_product)

        # Call get_forecasts_by_date_range with the date range and product
        forecasts = dataframe_store.get_forecasts_by_date_range(start_date, end_date, test_product)

        # Assert that the function returns a dictionary
        assert isinstance(forecasts, dict)

        # Verify the dictionary contains the expected forecasts
        assert len(forecasts) == 3
        assert datetime.datetime(2023, 1, 1) in forecasts
        assert datetime.datetime(2023, 1, 2) in forecasts
        assert datetime.datetime(2023, 1, 3) in forecasts

        # Clean up by deleting the test forecasts
        file_path1 = get_forecast_file_path(datetime.datetime(2023, 1, 1), test_product)
        os.remove(file_path1)
        file_path2 = get_forecast_file_path(datetime.datetime(2023, 1, 2), test_product)
        os.remove(file_path2)
        file_path3 = get_forecast_file_path(datetime.datetime(2023, 1, 3), test_product)
        os.remove(file_path3)

    def test_get_forecasts_by_date_range_empty(self):
        """Tests that get_forecasts_by_date_range returns an empty dict when no forecasts exist"""
        # Create a start date and end date in a period with no forecasts
        start_date = datetime.datetime(2000, 1, 1)
        end_date = datetime.datetime(2000, 1, 3)

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Call get_forecasts_by_date_range with the date range and product
        forecasts = dataframe_store.get_forecasts_by_date_range(start_date, end_date, test_product)

        # Assert that the function returns an empty dictionary
        assert forecasts == {}

    def test_get_forecast_metadata(self):
        """Tests that get_forecast_metadata retrieves metadata from a forecast"""
        # Create a test date (e.g., datetime.now())
        test_date = datetime.datetime.now()

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create and store a mock forecast dataframe with is_fallback=True
        mock_df = create_mock_forecast_data()
        file_path = dataframe_store.store_forecast(mock_df, test_date, test_product, is_fallback=True)

        # Call get_forecast_metadata with the timestamp and product
        metadata = dataframe_store.get_forecast_metadata(test_date, test_product)

        # Assert that the function returns a dictionary
        assert isinstance(metadata, dict)

        # Verify the dictionary contains expected metadata fields (storage_timestamp, storage_version, schema_version, is_fallback)
        assert 'storage_timestamp' in metadata
        assert 'storage_version' in metadata
        assert 'schema_version' in metadata
        assert 'is_fallback' in metadata

        # Verify is_fallback is True as specified when creating the forecast
        assert metadata['is_fallback'] is True

        # Clean up by deleting the test forecast
        os.remove(file_path)

    def test_get_forecast_metadata_not_found(self):
        """Tests that get_forecast_metadata raises DataFrameNotFoundError when forecast doesn't exist"""
        # Create a test date that won't have a forecast (e.g., datetime(2000, 1, 1))
        test_date = datetime.datetime(2000, 1, 1)

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Use pytest.raises to assert that DataFrameNotFoundError is raised when calling get_forecast_metadata
        with pytest.raises(DataFrameNotFoundError) as excinfo:
            dataframe_store.get_forecast_metadata(test_date, test_product)

        # Verify the error contains details about the missing forecast
        assert f"Forecast not found for {test_product} at {test_date}" in str(excinfo.value)

    def test_check_forecast_exists(self):
        """Tests that check_forecast_exists correctly identifies existing and non-existing forecasts"""
        # Create a test date (e.g., datetime.now())
        test_date = datetime.datetime.now()

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create and store a mock forecast dataframe
        mock_df = create_mock_forecast_data()
        file_path = dataframe_store.store_forecast(mock_df, test_date, test_product)

        # Call check_forecast_exists with the timestamp and product
        result = dataframe_store.check_forecast_exists(test_date, test_product)

        # Assert that the function returns True
        assert result is True

        # Call check_forecast_exists with a different timestamp
        different_date = test_date + datetime.timedelta(days=1)
        result = dataframe_store.check_forecast_exists(different_date, test_product)

        # Assert that the function returns False
        assert result is False

        # Clean up by deleting the test forecast
        os.remove(file_path)

    def test_copy_forecast(self):
        """Tests that copy_forecast creates a copy with a new timestamp"""
        # Create a source date (e.g., datetime.now())
        source_date = datetime.datetime.now()

        # Create a target date (e.g., datetime.now() + timedelta(days=1))
        target_date = source_date + datetime.timedelta(days=1)

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Create and store a mock forecast dataframe for the source date
        mock_df = create_mock_forecast_data()
        dataframe_store.store_forecast(mock_df, source_date, test_product)

        # Call copy_forecast with source_timestamp, target_timestamp, product, and mark_as_fallback=True
        file_path = dataframe_store.copy_forecast(source_date, target_date, test_product, mark_as_fallback=True)

        # Assert that the function returns a valid Path object
        assert isinstance(file_path, pathlib.Path)

        # Verify the copy exists using check_forecast_exists
        assert dataframe_store.check_forecast_exists(target_date, test_product)

        # Load the copy and verify it has the correct target timestamp
        loaded_df = dataframe_store.load_forecast(target_date, test_product)
        assert loaded_df['timestamp'].iloc[0].to_pydatetime().date() == target_date.date()

        # Verify the copy has is_fallback=True
        assert loaded_df['is_fallback'].iloc[0] is True

        # Clean up by deleting both forecasts
        os.remove(file_path)
        source_file_path = get_forecast_file_path(source_date, test_product)
        os.remove(source_file_path)

    def test_copy_forecast_source_not_found(self):
        """Tests that copy_forecast raises DataFrameNotFoundError when source doesn't exist"""
        # Create a source date that won't have a forecast (e.g., datetime(2000, 1, 1))
        source_date = datetime.datetime(2000, 1, 1)

        # Create a target date (e.g., datetime.now())
        target_date = datetime.datetime.now()

        # Define a test product (e.g., 'DALMP')
        test_product = 'DALMP'

        # Use pytest.raises to assert that DataFrameNotFoundError is raised when calling copy_forecast
        with pytest.raises(DataFrameNotFoundError) as excinfo:
            dataframe_store.copy_forecast(source_date, target_date, test_product)

        # Verify the error contains details about the missing source forecast
        assert f"Forecast not found for {test_product} at {source_date}" in str(excinfo.value)

    def test_get_storage_statistics(self):
        """Tests that get_storage_statistics returns statistics about stored forecasts"""
        # Create and store multiple mock forecast dataframes with different products and dates
        mock_df1 = create_mock_forecast_data()
        dataframe_store.store_forecast(mock_df1, datetime.datetime(2023, 1, 1), "DALMP")
        mock_df2 = create_mock_forecast_data()
        dataframe_store.store_forecast(mock_df2, datetime.datetime(2023, 1, 2), "RTLMP")
        mock_df3 = create_mock_forecast_data()
        dataframe_store.store_forecast(mock_df3, datetime.datetime(2023, 1, 3), "DALMP", is_fallback=True)

        # Call get_storage_statistics
        stats = dataframe_store.get_storage_statistics()

        # Assert that the function returns a dictionary
        assert isinstance(stats, dict)

        # Verify the dictionary contains expected statistics fields (total_forecasts, forecasts_by_product, forecasts_by_fallback, date_range)
        assert 'total_forecasts' in stats
        assert 'forecasts_by_product' in stats
        assert 'forecasts_by_fallback' in stats
        assert 'date_range' in stats

        # Clean up by deleting the test forecasts
        file_path1 = get_forecast_file_path(datetime.datetime(2023, 1, 1), "DALMP")
        os.remove(file_path1)
        file_path2 = get_forecast_file_path(datetime.datetime(2023, 1, 2), "RTLMP")
        os.remove(file_path2)
        file_path3 = get_forecast_file_path(datetime.datetime(2023, 1, 3), "DALMP")
        os.remove(file_path3)