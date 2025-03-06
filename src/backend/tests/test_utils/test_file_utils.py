"""
Unit tests for the file_utils module, which provides file operation functions
for the Electricity Market Price Forecasting System. Tests directory management,
file path generation, dataframe serialization/deserialization, and forecast
file management functionality.
"""

import pytest  # pytest: 7.0.0+
import pandas as pd  # pandas: 2.0.0+
import numpy as np  # numpy: 1.24.0+
import os  # standard library
import pathlib  # standard library
import datetime  # standard library
import tempfile  # standard library
import shutil  # standard library

from src.backend.utils import file_utils  # Module under test
from src.backend.utils.file_utils import ensure_directory_exists, get_forecast_directory, get_forecast_file_path, save_dataframe, load_dataframe, list_forecast_files, get_latest_forecast_file, update_latest_link, clean_old_forecasts
from src.backend.config.settings import STORAGE_ROOT_DIR, STORAGE_LATEST_DIR, FORECAST_PRODUCTS
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data


def test_ensure_directory_exists():
    """Tests that ensure_directory_exists creates directories when they don't exist"""
    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a path to a non-existent subdirectory
        non_existent_path = os.path.join(tmpdir, "subdir")

        # Call ensure_directory_exists on the path
        file_utils.ensure_directory_exists(non_existent_path)

        # Verify that the directory now exists
        assert os.path.exists(non_existent_path)


def test_ensure_directory_exists_with_existing_dir():
    """Tests that ensure_directory_exists handles existing directories correctly"""
    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as tmpdir:
        # Call ensure_directory_exists on the existing directory
        returned_path = file_utils.ensure_directory_exists(tmpdir)

        # Verify that the function returns the path to the existing directory
        assert returned_path == tmpdir


def test_get_forecast_directory(monkeypatch):
    """Tests that get_forecast_directory returns the correct directory path"""
    # Create a test date
    test_date = datetime.datetime(2023, 1, 15)

    # Mock STORAGE_ROOT_DIR to a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setattr(file_utils, "STORAGE_ROOT_DIR", tmpdir)

        # Call get_forecast_directory with the test date
        directory_path = file_utils.get_forecast_directory(test_date)

        # Verify that the returned path includes year and month from the test date
        assert str(test_date.year) in str(directory_path)
        assert str(test_date.month).zfill(2) in str(directory_path)

        # Verify that the directory structure is created
        assert os.path.exists(directory_path)


def test_get_forecast_file_path(monkeypatch):
    """Tests that get_forecast_file_path generates the correct file path"""
    # Create a test date
    test_date = datetime.datetime(2023, 1, 15)

    # Mock STORAGE_ROOT_DIR to a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setattr(file_utils, "STORAGE_ROOT_DIR", tmpdir)

        # Call get_forecast_file_path with the test date, a valid product, and format
        file_path = file_utils.get_forecast_file_path(test_date, "DALMP", format="parquet")

        # Verify that the returned path has the correct structure and filename
        assert str(test_date.year) in str(file_path)
        assert str(test_date.month).zfill(2) in str(file_path)
        assert f"{test_date.day:02d}_DALMP.parquet" in str(file_path)

        # Verify that the parent directory exists
        assert os.path.exists(file_path.parent)


def test_get_forecast_file_path_invalid_product(monkeypatch):
    """Tests that get_forecast_file_path raises ValueError for invalid products"""
    # Create a test date
    test_date = datetime.datetime(2023, 1, 15)

    # Mock STORAGE_ROOT_DIR to a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setattr(file_utils, "STORAGE_ROOT_DIR", tmpdir)

        # Call get_forecast_file_path with an invalid product
        with pytest.raises(ValueError):
            file_utils.get_forecast_file_path(test_date, "INVALID_PRODUCT")


def test_save_and_load_dataframe_parquet():
    """Tests saving and loading a DataFrame in parquet format"""
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a test DataFrame with forecast data
        test_df = create_mock_forecast_data()

        # Create a file path in the temporary directory
        file_path = os.path.join(tmpdir, "test_forecast.parquet")

        # Call save_dataframe with the DataFrame, path, and 'parquet' format
        file_utils.save_dataframe(test_df, file_path, format="parquet")

        # Verify that the file exists
        assert os.path.exists(file_path)

        # Call load_dataframe with the same path and format
        loaded_df = file_utils.load_dataframe(file_path, format="parquet")

        # Verify that the loaded DataFrame matches the original
        pd.testing.assert_frame_equal(test_df, loaded_df)


def test_save_and_load_dataframe_csv():
    """Tests saving and loading a DataFrame in CSV format"""
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a test DataFrame with forecast data
        test_df = create_mock_forecast_data()

        # Create a file path in the temporary directory
        file_path = os.path.join(tmpdir, "test_forecast.csv")

        # Call save_dataframe with the DataFrame, path, and 'csv' format
        file_utils.save_dataframe(test_df, file_path, format="csv")

        # Verify that the file exists
        assert os.path.exists(file_path)

        # Call load_dataframe with the same path and format
        loaded_df = file_utils.load_dataframe(file_path, format="csv")

        # Verify that the loaded DataFrame matches the original
        pd.testing.assert_frame_equal(test_df, loaded_df)


def test_save_dataframe_invalid_format():
    """Tests that save_dataframe returns False for invalid formats"""
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a test DataFrame
        test_df = create_mock_forecast_data()

        # Create a file path in the temporary directory
        file_path = os.path.join(tmpdir, "test_forecast.invalid")

        # Call save_dataframe with an invalid format
        result = file_utils.save_dataframe(test_df, file_path, format="invalid")

        # Verify that the function returns False
        assert result is False

        # Verify that no file was created
        assert not os.path.exists(file_path)


def test_load_dataframe_nonexistent_file():
    """Tests that load_dataframe returns None for non-existent files"""
    # Create a path to a non-existent file
    file_path = "nonexistent_file.parquet"

    # Call load_dataframe with the path
    loaded_df = file_utils.load_dataframe(file_path)

    # Verify that the function returns None
    assert loaded_df is None


def test_load_dataframe_invalid_format():
    """Tests that load_dataframe returns None for invalid formats"""
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a test DataFrame
        test_df = create_mock_forecast_data()

        # Save the DataFrame in a valid format
        file_path = os.path.join(tmpdir, "test_forecast.parquet")
        file_utils.save_dataframe(test_df, file_path, format="parquet")

        # Call load_dataframe with an invalid format
        loaded_df = file_utils.load_dataframe(file_path, format="invalid")

        # Verify that the function returns None
        assert loaded_df is None


def test_list_forecast_files(monkeypatch):
    """Tests that list_forecast_files returns the correct list of files"""
    # Create a temporary directory structure with test forecast files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Mock STORAGE_ROOT_DIR to the temporary directory
        monkeypatch.setattr(file_utils, "STORAGE_ROOT_DIR", tmpdir)

        # Create test files for different dates and products
        date1 = datetime.datetime(2023, 1, 1)
        date2 = datetime.datetime(2023, 1, 2)
        date3 = datetime.datetime(2023, 2, 1)
        file_utils.save_dataframe(create_mock_forecast_data(), file_utils.get_forecast_file_path(date1, "DALMP"))
        file_utils.save_dataframe(create_mock_forecast_data(), file_utils.get_forecast_file_path(date2, "RTLMP"))
        file_utils.save_dataframe(create_mock_forecast_data(), file_utils.get_forecast_file_path(date3, "DALMP"))

        # Call list_forecast_files with a date range and product
        start_date = datetime.datetime(2023, 1, 1)
        end_date = datetime.datetime(2023, 1, 31)
        file_list = file_utils.list_forecast_files(start_date, end_date, product="DALMP")

        # Verify that the returned list contains the expected files
        assert len(file_list) == 1
        assert "01_DALMP.parquet" in str(file_list[0])


def test_get_latest_forecast_file(monkeypatch):
    """Tests that get_latest_forecast_file returns the most recent file"""
    # Create a temporary directory structure
    with tempfile.TemporaryDirectory() as tmpdir:
        # Mock STORAGE_ROOT_DIR and STORAGE_LATEST_DIR to the temporary directory
        monkeypatch.setattr(file_utils, "STORAGE_ROOT_DIR", tmpdir)
        monkeypatch.setattr(file_utils, "STORAGE_LATEST_DIR", os.path.join(tmpdir, "latest"))

        # Create test forecast files with different dates
        date1 = datetime.datetime(2023, 1, 1)
        date2 = datetime.datetime(2023, 1, 2)
        file_utils.save_dataframe(create_mock_forecast_data(), file_utils.get_forecast_file_path(date1, "DALMP"))
        file_utils.save_dataframe(create_mock_forecast_data(), file_utils.get_forecast_file_path(date2, "DALMP"))

        # Call get_latest_forecast_file for a specific product
        latest_file = file_utils.get_latest_forecast_file("DALMP")

        # Verify that the function returns the most recent file
        assert "02_DALMP.parquet" in str(latest_file)


def test_get_latest_forecast_file_with_link(monkeypatch):
    """Tests that get_latest_forecast_file uses symbolic links when available"""
    # Create a temporary directory structure
    with tempfile.TemporaryDirectory() as tmpdir:
        # Mock STORAGE_ROOT_DIR and STORAGE_LATEST_DIR to the temporary directory
        monkeypatch.setattr(file_utils, "STORAGE_ROOT_DIR", tmpdir)
        monkeypatch.setattr(file_utils, "STORAGE_LATEST_DIR", os.path.join(tmpdir, "latest"))

        # Create a test forecast file
        date1 = datetime.datetime(2023, 1, 1)
        file_path = file_utils.get_forecast_file_path(date1, "DALMP")
        file_utils.save_dataframe(create_mock_forecast_data(), file_path)

        # Create a symbolic link in the latest directory
        latest_link = os.path.join(tmpdir, "latest", "DALMP.parquet")
        os.symlink(file_path, latest_link)

        # Call get_latest_forecast_file for the product
        latest_file = file_utils.get_latest_forecast_file("DALMP")

        # Verify that the function returns the path from the symbolic link
        assert str(file_path) in str(latest_file)


def test_update_latest_link(monkeypatch):
    """Tests that update_latest_link creates or updates symbolic links correctly"""
    # Create a temporary directory structure
    with tempfile.TemporaryDirectory() as tmpdir:
        # Mock STORAGE_LATEST_DIR to the temporary directory
        monkeypatch.setattr(file_utils, "STORAGE_LATEST_DIR", os.path.join(tmpdir, "latest"))

        # Create a test forecast file
        date1 = datetime.datetime(2023, 1, 1)
        file_path = file_utils.get_forecast_file_path(date1, "DALMP")
        file_utils.save_dataframe(create_mock_forecast_data(), file_path)

        # Call update_latest_link with the file path and product
        file_utils.update_latest_link(file_path, "DALMP")

        # Verify that a symbolic link was created in the latest directory
        latest_link = os.path.join(tmpdir, "latest", "DALMP.parquet")
        assert os.path.islink(latest_link)

        # Verify that the link points to the correct file
        assert os.readlink(latest_link) == str(file_path)


def test_update_latest_link_existing(monkeypatch):
    """Tests that update_latest_link updates existing links correctly"""
    # Create a temporary directory structure
    with tempfile.TemporaryDirectory() as tmpdir:
        # Mock STORAGE_LATEST_DIR to the temporary directory
        monkeypatch.setattr(file_utils, "STORAGE_LATEST_DIR", os.path.join(tmpdir, "latest"))

        # Create two test forecast files
        date1 = datetime.datetime(2023, 1, 1)
        date2 = datetime.datetime(2023, 1, 2)
        file_path1 = file_utils.get_forecast_file_path(date1, "DALMP")
        file_path2 = file_utils.get_forecast_file_path(date2, "DALMP")
        file_utils.save_dataframe(create_mock_forecast_data(), file_path1)
        file_utils.save_dataframe(create_mock_forecast_data(), file_path2)

        # Create an initial symbolic link to the first file
        latest_link = os.path.join(tmpdir, "latest", "DALMP.parquet")
        os.symlink(file_path1, latest_link)

        # Call update_latest_link with the second file path
        file_utils.update_latest_link(file_path2, "DALMP")

        # Verify that the symbolic link now points to the second file
        assert os.readlink(latest_link) == str(file_path2)


def test_clean_old_forecasts(monkeypatch):
    """Tests that clean_old_forecasts removes files older than the retention period"""
    # Create a temporary directory structure
    with tempfile.TemporaryDirectory() as tmpdir:
        # Mock STORAGE_ROOT_DIR to the temporary directory
        monkeypatch.setattr(file_utils, "STORAGE_ROOT_DIR", tmpdir)

        # Create test forecast files with different modification times
        now = datetime.datetime.now()
        file_path1 = file_utils.get_forecast_file_path(now - datetime.timedelta(days=10), "DALMP")
        file_path2 = file_utils.get_forecast_file_path(now - datetime.timedelta(days=5), "DALMP")
        file_path3 = file_utils.get_forecast_file_path(now - datetime.timedelta(days=1), "DALMP")
        file_utils.save_dataframe(create_mock_forecast_data(), file_path1)
        file_utils.save_dataframe(create_mock_forecast_data(), file_path2)
        file_utils.save_dataframe(create_mock_forecast_data(), file_path3)

        # Call clean_old_forecasts with a retention period
        retention_days = 7
        removed_count = file_utils.clean_old_forecasts(retention_days)

        # Verify that files older than the retention period are removed
        assert not os.path.exists(file_path1)

        # Verify that newer files are preserved
        assert os.path.exists(file_path2)
        assert os.path.exists(file_path3)

        # Verify that the function returns the correct count of removed files
        assert removed_count == 1