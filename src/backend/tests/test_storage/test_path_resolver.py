import os
import pathlib
import datetime
import pytest

from ...storage.path_resolver import (
    get_base_storage_path,
    get_year_month_path,
    get_forecast_file_path,
    get_latest_file_path,
    get_index_file_path,
    create_backup_path,
    validate_product,
    resolve_relative_path,
    get_relative_storage_path
)
from ...storage.exceptions import StoragePathError
from ...config.settings import (
    STORAGE_ROOT_DIR,
    STORAGE_LATEST_DIR,
    STORAGE_INDEX_FILE,
    FORECAST_PRODUCTS
)


def test_get_base_storage_path():
    """Tests that get_base_storage_path returns the correct base storage path"""
    # Call get_base_storage_path()
    base_path = get_base_storage_path()
    
    # Assert that the returned path is a pathlib.Path object
    assert isinstance(base_path, pathlib.Path)
    
    # Assert that the returned path equals pathlib.Path(STORAGE_ROOT_DIR)
    assert base_path == pathlib.Path(STORAGE_ROOT_DIR)
    
    # Assert that the path exists (directory should be created if it doesn't exist)
    assert base_path.exists()


def test_get_year_month_path():
    """Tests that get_year_month_path returns the correct year/month directory path"""
    # Create a test date (e.g., datetime(2023, 6, 15))
    test_date = datetime.datetime(2023, 6, 15)
    
    # Call get_year_month_path(test_date)
    year_month_path = get_year_month_path(test_date)
    
    # Assert that the returned path is a pathlib.Path object
    assert isinstance(year_month_path, pathlib.Path)
    
    # Assert that the path includes the year and month in the correct format
    expected_path = pathlib.Path(STORAGE_ROOT_DIR) / "2023" / "06"
    assert year_month_path == expected_path
    
    # Assert that the path is a subdirectory of the base storage path
    assert str(year_month_path).startswith(str(pathlib.Path(STORAGE_ROOT_DIR)))
    
    # Assert that the directory exists (should be created if it doesn't exist)
    assert year_month_path.exists()
    
    # Test with a double-digit month
    test_date_double_digit = datetime.datetime(2023, 12, 15)
    year_month_path_double_digit = get_year_month_path(test_date_double_digit)
    expected_path_double_digit = pathlib.Path(STORAGE_ROOT_DIR) / "2023" / "12"
    assert year_month_path_double_digit == expected_path_double_digit


def test_get_forecast_file_path():
    """Tests that get_forecast_file_path returns the correct file path for a forecast"""
    # Create a test date (e.g., datetime(2023, 6, 15))
    test_date = datetime.datetime(2023, 6, 15)
    
    # Define a test product (e.g., 'DALMP')
    test_product = "DALMP"
    
    # Call get_forecast_file_path(test_date, test_product)
    file_path = get_forecast_file_path(test_date, test_product)
    
    # Assert that the returned path is a pathlib.Path object
    assert isinstance(file_path, pathlib.Path)
    
    # Assert that the path includes the year, month, day, and product in the correct format
    expected_path = pathlib.Path(STORAGE_ROOT_DIR) / "2023" / "06" / "15_DALMP.parquet"
    assert file_path == expected_path
    
    # Assert that the file extension is '.parquet' by default
    assert file_path.suffix == '.parquet'
    
    # Test with a different format parameter (e.g., 'csv')
    csv_path = get_forecast_file_path(test_date, test_product, format='csv')
    expected_csv_path = pathlib.Path(STORAGE_ROOT_DIR) / "2023" / "06" / "15_DALMP.csv"
    assert csv_path == expected_csv_path


def test_get_forecast_file_path_invalid_product():
    """Tests that get_forecast_file_path raises StoragePathError for invalid product"""
    # Create a test date (e.g., datetime(2023, 6, 15))
    test_date = datetime.datetime(2023, 6, 15)
    
    # Define an invalid product (e.g., 'INVALID_PRODUCT')
    invalid_product = "INVALID_PRODUCT"
    
    # Use pytest.raises to assert that StoragePathError is raised when calling get_forecast_file_path with the invalid product
    with pytest.raises(StoragePathError) as excinfo:
        get_forecast_file_path(test_date, invalid_product)
    
    # Verify the error message mentions the invalid product name
    assert invalid_product in str(excinfo.value)


def test_get_latest_file_path():
    """Tests that get_latest_file_path returns the correct path for the latest forecast file"""
    # Define a test product (e.g., 'DALMP')
    test_product = "DALMP"
    
    # Call get_latest_file_path(test_product)
    latest_path = get_latest_file_path(test_product)
    
    # Assert that the returned path is a pathlib.Path object
    assert isinstance(latest_path, pathlib.Path)
    
    # Assert that the path is in the STORAGE_LATEST_DIR directory
    assert str(latest_path.parent) == STORAGE_LATEST_DIR
    
    # Assert that the filename includes the product name
    assert test_product in latest_path.name
    
    # Assert that the file extension is '.parquet' by default
    assert latest_path.suffix == '.parquet'
    
    # Test with a different format parameter (e.g., 'csv')
    csv_path = get_latest_file_path(test_product, format='csv')
    assert csv_path.suffix == '.csv'


def test_get_latest_file_path_invalid_product():
    """Tests that get_latest_file_path raises StoragePathError for invalid product"""
    # Define an invalid product (e.g., 'INVALID_PRODUCT')
    invalid_product = "INVALID_PRODUCT"
    
    # Use pytest.raises to assert that StoragePathError is raised when calling get_latest_file_path with the invalid product
    with pytest.raises(StoragePathError) as excinfo:
        get_latest_file_path(invalid_product)
    
    # Verify the error message mentions the invalid product name
    assert invalid_product in str(excinfo.value)


def test_get_index_file_path():
    """Tests that get_index_file_path returns the correct path for the index file"""
    # Call get_index_file_path()
    index_path = get_index_file_path()
    
    # Assert that the returned path is a pathlib.Path object
    assert isinstance(index_path, pathlib.Path)
    
    # Assert that the returned path equals pathlib.Path(STORAGE_INDEX_FILE)
    assert index_path == pathlib.Path(STORAGE_INDEX_FILE)
    
    # Assert that the parent directory exists (should be created if it doesn't exist)
    assert index_path.parent.exists()


def test_create_backup_path():
    """Tests that create_backup_path generates a backup path with timestamp"""
    # Create a test file path (e.g., pathlib.Path('/path/to/test.parquet'))
    test_file_path = pathlib.Path("/path/to/test.parquet")
    
    # Call create_backup_path(test_file_path)
    backup_path = create_backup_path(test_file_path)
    
    # Assert that the returned path is a pathlib.Path object
    assert isinstance(backup_path, pathlib.Path)
    
    # Assert that the returned path has the same parent directory as the original
    assert backup_path.parent == test_file_path.parent
    
    # Assert that the returned path contains the original filename
    assert test_file_path.stem in backup_path.stem
    
    # Assert that the returned path includes a timestamp in the filename
    stem_parts = backup_path.stem.split('_')
    assert len(stem_parts) >= 3  # at least original name + date + time
    
    # The last two parts should be the date and time
    date_part = stem_parts[-2]
    time_part = stem_parts[-1]
    
    # Date should be 8 digits (YYYYMMDD)
    assert len(date_part) == 8 and date_part.isdigit()
    
    # Time should be 6 digits (HHMMSS)
    assert len(time_part) == 6 and time_part.isdigit()
    
    # Assert that the returned path has the same suffix as the original
    assert backup_path.suffix == test_file_path.suffix


def test_validate_product():
    """Tests that validate_product correctly validates product names"""
    # For each product in FORECAST_PRODUCTS, call validate_product(product)
    for product in FORECAST_PRODUCTS:
        # Assert that validate_product returns True for valid products
        assert validate_product(product) is True
    
    # Define an invalid product (e.g., 'INVALID_PRODUCT')
    invalid_product = "INVALID_PRODUCT"
    
    # Use pytest.raises to assert that StoragePathError is raised when calling validate_product with the invalid product
    with pytest.raises(StoragePathError) as excinfo:
        validate_product(invalid_product)
    
    # Verify the error message mentions the invalid product name and lists valid products
    error_msg = str(excinfo.value)
    assert invalid_product in error_msg
    for product in FORECAST_PRODUCTS:
        assert product in error_msg


def test_resolve_relative_path():
    """Tests that resolve_relative_path correctly resolves paths against the base storage path"""
    # Define a relative path (e.g., 'test/path')
    relative_path = "test/path"
    
    # Call resolve_relative_path(relative_path)
    resolved_path = resolve_relative_path(relative_path)
    
    # Assert that the returned path is a pathlib.Path object
    assert isinstance(resolved_path, pathlib.Path)
    
    # Assert that the returned path is an absolute path
    assert resolved_path.is_absolute()
    
    # Assert that the returned path starts with the base storage path
    base_path = get_base_storage_path()
    assert str(resolved_path).startswith(str(base_path))
    
    # Assert that the returned path ends with the relative path
    assert str(resolved_path).endswith(relative_path)
    
    # Test with a pathlib.Path object instead of a string
    path_obj = pathlib.Path(relative_path)
    resolved_path_2 = resolve_relative_path(path_obj)
    
    # Assert that the results are consistent regardless of input type
    assert resolved_path == resolved_path_2


def test_get_relative_storage_path():
    """Tests that get_relative_storage_path correctly calculates paths relative to the base storage path"""
    # Get the base storage path using get_base_storage_path()
    base_path = get_base_storage_path()
    
    # Create an absolute path by appending a test path to the base path
    test_path = "test/relative/path"
    absolute_path = base_path / test_path
    
    # Call get_relative_storage_path(absolute_path)
    relative_path = get_relative_storage_path(absolute_path)
    
    # Assert that the returned path is a pathlib.Path object
    assert isinstance(relative_path, pathlib.Path)
    
    # Assert that the returned path is a relative path
    assert not relative_path.is_absolute()
    
    # Assert that joining the base path with the relative path recreates the original absolute path
    assert (base_path / relative_path).resolve() == absolute_path.resolve()
    
    # Test with a path that's not a subdirectory of the base storage path
    non_storage_path = pathlib.Path("/some/other/path")
    with pytest.raises(ValueError):
        get_relative_storage_path(non_storage_path)