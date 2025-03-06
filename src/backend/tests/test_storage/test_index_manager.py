# src/backend/tests/test_storage/test_index_manager.py
"""
Unit tests for the index_manager module which manages the forecast index for the Electricity Market Price Forecasting System.
Tests the creation, updating, querying, and maintenance of the forecast index that enables efficient retrieval of forecasts by date, product, and other criteria.
"""

import pytest  # pytest: 7.0.0+
import pandas as pd  # pandas: 2.0.0+
import numpy as np  # numpy: 1.24.0+
from datetime import datetime  # standard library
import os  # standard library
import pathlib  # standard library
import shutil  # standard library

# Internal imports
from src.backend.storage.index_manager import (
    initialize_index,
    load_index,
    save_index,
    add_forecast_to_index,
    remove_forecast_from_index,
    query_index_by_date,
    get_forecast_file_paths,
    update_latest_links,
    get_latest_forecast_metadata,
    clean_index,
    rebuild_index,
    get_index_statistics,
)
from src.backend.storage.path_resolver import get_index_file_path, get_latest_file_path, get_base_storage_path
from src.backend.storage.exceptions import IndexUpdateError
from src.backend.config.settings import FORECAST_PRODUCTS
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data


def test_initialize_index(temp_storage_path: pathlib.Path):
    """Tests the initialize_index function creates a new empty index"""
    # Get the index file path using get_index_file_path()
    index_file_path = get_index_file_path()

    # Ensure the index file doesn't exist initially
    assert not index_file_path.exists()

    # Call initialize_index()
    initialize_index()

    # Assert that the index file now exists
    assert index_file_path.exists()

    # Load the index and verify it's an empty DataFrame with the correct schema
    index_df = load_index()
    assert isinstance(index_df, pd.DataFrame)
    assert len(index_df) == 0
    assert "timestamp" in index_df.columns
    assert "product" in index_df.columns

    # Call initialize_index() again and verify it returns False (already exists)
    assert not initialize_index()


def test_load_index(temp_storage_path: pathlib.Path):
    """Tests the load_index function loads an existing index"""
    # Create a test index with some sample data
    test_data = {
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
        "product": ["DALMP", "RTLMP"],
        "file_path": ["/path/to/dalmp", "/path/to/rtlmp"],
        "generation_timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
        "is_fallback": [False, True]
    }
    test_index = pd.DataFrame(test_data)

    # Save the test index to disk
    save_index(test_index)

    # Call load_index()
    loaded_index = load_index()

    # Verify the loaded index matches the original test data
    pd.testing.assert_frame_equal(loaded_index, test_index)

    # Delete the index file
    index_file_path = get_index_file_path()
    os.remove(index_file_path)

    # Call load_index() and verify it creates a new empty index
    new_index = load_index()
    assert isinstance(new_index, pd.DataFrame)
    assert len(new_index) == 0


def test_save_index(temp_storage_path: pathlib.Path):
    """Tests the save_index function saves an index to disk"""
    # Create a test index with some sample data
    test_data = {
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
        "product": ["DALMP", "RTLMP"],
        "file_path": ["/path/to/dalmp", "/path/to/rtlmp"],
        "generation_timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
        "is_fallback": [False, True]
    }
    test_index = pd.DataFrame(test_data)

    # Call save_index() with the test index
    save_index(test_index)

    # Verify the index file exists
    index_file_path = get_index_file_path()
    assert index_file_path.exists()

    # Load the saved index and verify it matches the original test data
    loaded_index = load_index()
    pd.testing.assert_frame_equal(loaded_index, test_index)

    # Modify the test index
    test_index.loc[0, "product"] = "RegUp"

    # Call save_index() again
    save_index(test_index)

    # Load the saved index and verify it contains the modifications
    loaded_index = load_index()
    pd.testing.assert_frame_equal(loaded_index, test_index)


def test_add_forecast_to_index(temp_storage_path: pathlib.Path):
    """Tests adding a forecast to the index"""
    # Initialize an empty index
    initialize_index()

    # Create a test forecast file path
    file_path = temp_storage_path / "test_forecast.parquet"

    # Create test timestamp, product, and generation timestamp
    timestamp = datetime(2023, 1, 3)
    product = "DALMP"
    generation_timestamp = datetime(2023, 1, 3, 6, 0, 0)

    # Call add_forecast_to_index() with the test data
    add_forecast_to_index(file_path, timestamp, product, generation_timestamp, False)

    # Load the index and verify the forecast was added correctly
    index_df = load_index()
    assert len(index_df) == 1
    assert index_df["timestamp"][0] == timestamp
    assert index_df["product"][0] == product
    assert index_df["file_path"][0] == str(file_path)
    assert index_df["generation_timestamp"][0] == generation_timestamp
    assert index_df["is_fallback"][0] == False

    # Call add_forecast_to_index() with the same timestamp and product but different file path
    new_file_path = temp_storage_path / "test_forecast_new.parquet"
    add_forecast_to_index(new_file_path, timestamp, product, generation_timestamp, True)

    # Verify the entry was updated with the new file path
    index_df = load_index()
    assert len(index_df) == 1
    assert index_df["file_path"][0] == str(new_file_path)
    assert index_df["is_fallback"][0] == True

    # Test with an invalid product and verify it raises an exception
    with pytest.raises(ValueError):
        add_forecast_to_index(file_path, timestamp, "InvalidProduct", generation_timestamp, False)


def test_remove_forecast_from_index(temp_storage_path: pathlib.Path):
    """Tests removing a forecast from the index"""
    # Create a test index with multiple forecast entries
    test_data = {
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)],
        "product": ["DALMP", "RTLMP", "DALMP"],
        "file_path": ["/path/to/dalmp1", "/path/to/rtlmp", "/path/to/dalmp2"],
        "generation_timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)],
        "is_fallback": [False, True, False]
    }
    test_index = pd.DataFrame(test_data)
    save_index(test_index)

    # Select a forecast to remove
    timestamp = datetime(2023, 1, 2)
    product = "RTLMP"

    # Call remove_forecast_from_index() with the timestamp and product
    remove_forecast_from_index(timestamp, product)

    # Load the index and verify the forecast was removed
    index_df = load_index()
    assert len(index_df) == 2
    assert not ((index_df["timestamp"] == timestamp) & (index_df["product"] == product)).any()

    # Try to remove a non-existent forecast and verify it returns False
    assert not remove_forecast_from_index(datetime(2023, 1, 4), "DALMP")

    # Test with an invalid product and verify it raises an exception
    with pytest.raises(ValueError):
        remove_forecast_from_index(timestamp, "InvalidProduct")


def test_query_index_by_date(temp_storage_path: pathlib.Path):
    """Tests querying the index by date range"""
    # Create a test index with forecasts for multiple dates and products
    test_data = {
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3), datetime(2023, 1, 4)],
        "product": ["DALMP", "RTLMP", "DALMP", "RTLMP"],
        "file_path": ["/path/to/dalmp1", "/path/to/rtlmp1", "/path/to/dalmp2", "/path/to/rtlmp2"],
        "generation_timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3), datetime(2023, 1, 4)],
        "is_fallback": [False, True, False, True]
    }
    test_index = pd.DataFrame(test_data)
    save_index(test_index)

    # Define a date range for the query
    start_date = datetime(2023, 1, 2)
    end_date = datetime(2023, 1, 3)

    # Call query_index_by_date() with the date range
    result_df = query_index_by_date(start_date, end_date)

    # Verify the returned DataFrame contains only forecasts within the date range
    assert len(result_df) == 2
    assert all(start_date <= ts <= end_date for ts in result_df["timestamp"])

    # Call query_index_by_date() with a specific product
    product = "DALMP"
    result_df = query_index_by_date(start_date, end_date, product)

    # Verify the returned DataFrame contains only forecasts for that product
    assert len(result_df) == 1
    assert result_df["product"][0] == product

    # Test with an empty date range and verify it returns an empty DataFrame
    start_date = datetime(2023, 1, 5)
    end_date = datetime(2023, 1, 6)
    result_df = query_index_by_date(start_date, end_date)
    assert len(result_df) == 0

    # Test with an invalid product and verify it raises an exception
    with pytest.raises(ValueError):
        query_index_by_date(start_date, end_date, "InvalidProduct")


def test_get_forecast_file_paths(temp_storage_path: pathlib.Path):
    """Tests getting file paths for forecasts matching criteria"""
    # Create a test index with multiple forecast entries
    test_data = {
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)],
        "product": ["DALMP", "RTLMP", "DALMP"],
        "file_path": [str(temp_storage_path / "dalmp1.parquet"), str(temp_storage_path / "rtlmp.parquet"), str(temp_storage_path / "dalmp2.parquet")],
        "generation_timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)],
        "is_fallback": [False, True, False]
    }
    test_index = pd.DataFrame(test_data)
    save_index(test_index)

    # Query the index to get a subset of forecasts
    query_result = test_index[test_index["product"] == "DALMP"]

    # Call get_forecast_file_paths() with the query result
    file_paths = get_forecast_file_paths(query_result)

    # Verify the returned dictionary maps timestamps to correct file paths
    assert len(file_paths) == 2
    assert file_paths[datetime(2023, 1, 1)] == pathlib.Path(test_data["file_path"][0])
    assert file_paths[datetime(2023, 1, 3)] == pathlib.Path(test_data["file_path"][2])

    # Test with an empty query result and verify it returns an empty dictionary
    empty_query = test_index[test_index["product"] == "InvalidProduct"]
    file_paths = get_forecast_file_paths(empty_query)
    assert len(file_paths) == 0


def test_update_latest_links(temp_storage_path: pathlib.Path):
    """Tests updating symbolic links to the latest forecasts"""
    # Create a test index with forecasts for multiple products and timestamps
    test_data = {
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3), datetime(2023, 1, 4)],
        "product": ["DALMP", "RTLMP", "DALMP", "RTLMP"],
        "file_path": [str(temp_storage_path / "dalmp1.parquet"), str(temp_storage_path / "rtlmp1.parquet"), str(temp_storage_path / "dalmp2.parquet"), str(temp_storage_path / "rtlmp2.parquet")],
        "generation_timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3), datetime(2023, 1, 4)],
        "is_fallback": [False, True, False, True]
    }
    test_index = pd.DataFrame(test_data)
    save_index(test_index)

    # Create the actual forecast files referenced in the index
    for file_path in test_data["file_path"]:
        pathlib.Path(file_path).touch()

    # Call update_latest_links()
    update_latest_links()

    # Verify symbolic links were created for each product
    latest_dir = get_latest_file_path("DALMP").parent
    dalmp_link = latest_dir / "DALMP.parquet"
    rtlmp_link = latest_dir / "RTLMP.parquet"
    assert dalmp_link.is_symlink()
    assert rtlmp_link.is_symlink()

    # Verify each link points to the most recent forecast for that product
    assert os.readlink(dalmp_link) == os.path.relpath(test_data["file_path"][2], latest_dir)
    assert os.readlink(rtlmp_link) == os.path.relpath(test_data["file_path"][3], latest_dir)

    # Add newer forecasts to the index
    test_data_new = {
        "timestamp": [datetime(2023, 1, 5), datetime(2023, 1, 6)],
        "product": ["DALMP", "RTLMP"],
        "file_path": [str(temp_storage_path / "dalmp3.parquet"), str(temp_storage_path / "rtlmp3.parquet")],
        "generation_timestamp": [datetime(2023, 1, 5), datetime(2023, 1, 6)],
        "is_fallback": [False, True]
    }
    test_index_new = pd.DataFrame(test_data_new)
    save_index(pd.concat([load_index(), test_index_new], ignore_index=True))
    for file_path in test_data_new["file_path"]:
        pathlib.Path(file_path).touch()

    # Call update_latest_links() again
    update_latest_links()

    # Verify the links now point to the newer forecasts
    assert os.readlink(dalmp_link) == os.path.relpath(test_data_new["file_path"][0], latest_dir)
    assert os.readlink(rtlmp_link) == os.path.relpath(test_data_new["file_path"][1], latest_dir)


def test_get_latest_forecast_metadata(temp_storage_path: pathlib.Path):
    """Tests getting metadata for the latest forecasts"""
    # Create a test index with forecasts for multiple products and timestamps
    test_data = {
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3), datetime(2023, 1, 4)],
        "product": ["DALMP", "RTLMP", "DALMP", "RTLMP"],
        "file_path": ["/path/to/dalmp1", "/path/to/rtlmp1", "/path/to/dalmp2", "/path/to/rtlmp2"],
        "generation_timestamp": [datetime(2023, 1, 1, 6, 0, 0), datetime(2023, 1, 2, 6, 0, 0), datetime(2023, 1, 3, 6, 0, 0), datetime(2023, 1, 4, 6, 0, 0)],
        "is_fallback": [False, True, False, True]
    }
    test_index = pd.DataFrame(test_data)
    save_index(test_index)

    # Call get_latest_forecast_metadata()
    metadata = get_latest_forecast_metadata()

    # Verify the returned dictionary contains entries for each product
    assert len(metadata) == 2
    assert "DALMP" in metadata
    assert "RTLMP" in metadata

    # Verify each entry contains the correct metadata for the most recent forecast
    assert metadata["DALMP"]["timestamp"] == datetime(2023, 1, 3)
    assert metadata["DALMP"]["generation_timestamp"] == datetime(2023, 1, 3, 6, 0, 0)
    assert metadata["DALMP"]["is_fallback"] == False
    assert metadata["RTLMP"]["timestamp"] == datetime(2023, 1, 4)
    assert metadata["RTLMP"]["generation_timestamp"] == datetime(2023, 1, 4, 6, 0, 0)
    assert metadata["RTLMP"]["is_fallback"] == True

    # Test with an empty index and verify it returns an empty dictionary
    initialize_index()
    metadata = get_latest_forecast_metadata()
    assert len(metadata) == 0


def test_clean_index(temp_storage_path: pathlib.Path):
    """Tests cleaning the index by removing entries for non-existent files"""
    # Create a test index with multiple forecast entries
    test_data = {
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)],
        "product": ["DALMP", "RTLMP", "DALMP"],
        "file_path": [str(temp_storage_path / "dalmp1.parquet"), str(temp_storage_path / "rtlmp.parquet"), str(temp_storage_path / "dalmp2.parquet")],
        "generation_timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)],
        "is_fallback": [False, True, False]
    }
    test_index = pd.DataFrame(test_data)
    save_index(test_index)

    # Create some of the forecast files but not others
    pathlib.Path(test_data["file_path"][0]).touch()
    pathlib.Path(test_data["file_path"][2]).touch()

    # Call clean_index()
    stats = clean_index()

    # Verify entries for non-existent files were removed
    index_df = load_index()
    assert len(index_df) == 2
    assert str(temp_storage_path / "rtlmp.parquet") not in index_df["file_path"].values

    # Verify entries for existing files were kept
    assert str(temp_storage_path / "dalmp1.parquet") in index_df["file_path"].values
    assert str(temp_storage_path / "dalmp2.parquet") in index_df["file_path"].values

    # Verify the returned statistics are correct
    assert stats["total_entries"] == 3
    assert stats["removed_entries"] == 1
    assert stats["remaining_entries"] == 2


def test_rebuild_index(temp_storage_path: pathlib.Path):
    """Tests rebuilding the entire index from storage"""
    # Create a directory structure with forecast files
    year_dir = temp_storage_path / "2023"
    month_dir = year_dir / "01"
    month_dir.mkdir(parents=True)

    # Create mock forecast files with appropriate metadata
    dalmp_file = month_dir / "01_DALMP.parquet"
    rtlmp_file = month_dir / "02_RTLMP.parquet"
    dalmp_file.touch()
    rtlmp_file.touch()

    # Call rebuild_index()
    stats = rebuild_index()

    # Verify the index contains entries for all forecast files
    index_df = load_index()
    assert len(index_df) == 2
    assert str(dalmp_file) in index_df["file_path"].values
    assert str(rtlmp_file) in index_df["file_path"].values

    # Verify each entry has the correct metadata
    dalmp_entry = index_df[index_df["file_path"] == str(dalmp_file)].iloc[0]
    assert dalmp_entry["timestamp"] == datetime(2023, 1, 1)
    assert dalmp_entry["product"] == "DALMP"
    rtlmp_entry = index_df[index_df["file_path"] == str(rtlmp_file)].iloc[0]
    assert rtlmp_entry["timestamp"] == datetime(2023, 1, 2)
    assert rtlmp_entry["product"] == "RTLMP"

    # Verify the returned statistics are correct
    assert stats["files_found"] == 2
    assert stats["files_processed"] == 2
    assert stats["files_skipped"] == 0
    assert stats["index_entries"] == 2


def test_get_index_statistics(temp_storage_path: pathlib.Path):
    """Tests calculating statistics about the index"""
    # Create a test index with forecasts for multiple products, dates, and fallback status
    test_data = {
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3), datetime(2023, 1, 4)],
        "product": ["DALMP", "RTLMP", "DALMP", "RTLMP"],
        "file_path": ["/path/to/dalmp1", "/path/to/rtlmp1", "/path/to/dalmp2", "/path/to/rtlmp2"],
        "generation_timestamp": [datetime(2023, 1, 1, 6, 0, 0), datetime(2023, 1, 2, 6, 0, 0), datetime(2023, 1, 3, 6, 0, 0), datetime(2023, 1, 4, 6, 0, 0)],
        "is_fallback": [False, True, False, True]
    }
    test_index = pd.DataFrame(test_data)
    save_index(test_index)

    # Call get_index_statistics()
    stats = get_index_statistics()

    # Verify the returned dictionary contains the expected statistics
    assert stats["total_entries"] == 4
    assert stats["entries_by_product"]["DALMP"] == 2
    assert stats["entries_by_product"]["RTLMP"] == 2
    assert stats["entries_by_fallback"][False] == 2
    assert stats["entries_by_fallback"][True] == 2
    assert stats["date_range"]["min_date"] == "2023-01-01"
    assert stats["date_range"]["max_date"] == "2023-01-04"

    # Test with an empty index and verify it returns appropriate statistics
    initialize_index()
    stats = get_index_statistics()
    assert stats["total_entries"] == 0
    assert stats["entries_by_product"] == {}
    assert stats["entries_by_fallback"] == {}
    assert stats["date_range"]["min_date"] == "N/A"
    assert stats["date_range"]["max_date"] == "N/A"


def test_index_error_handling(temp_storage_path: pathlib.Path):
    """Tests error handling in index operations"""
    # Test save_index with a non-DataFrame input and verify it raises an exception
    with pytest.raises(AttributeError):
        save_index("not a dataframe")

    # Test add_forecast_to_index with an invalid product and verify it raises an exception
    with pytest.raises(ValueError):
        add_forecast_to_index(temp_storage_path / "test_forecast.parquet", datetime(2023, 1, 1), "InvalidProduct", datetime(2023, 1, 1), False)

    # Test remove_forecast_from_index with an invalid product and verify it raises an exception
    with pytest.raises(ValueError):
        remove_forecast_from_index(datetime(2023, 1, 1), "InvalidProduct")

    # Test query_index_by_date with an invalid product and verify it raises an exception
    with pytest.raises(ValueError):
        query_index_by_date(datetime(2023, 1, 1), datetime(2023, 1, 2), "InvalidProduct")

    # Make the index file read-only and test operations that write to it
    index_file_path = get_index_file_path()
    initialize_index()
    os.chmod(index_file_path, 0o444)  # Read-only
    with pytest.raises(PermissionError):
        save_index(pd.DataFrame({"timestamp": [datetime(2023, 1, 1)], "product": ["DALMP"], "file_path": ["/path/to/dalmp"], "generation_timestamp": [datetime(2023, 1, 1)], "is_fallback": [False]}))
    with pytest.raises(PermissionError):
        add_forecast_to_index(temp_storage_path / "test_forecast.parquet", datetime(2023, 1, 1), "DALMP", datetime(2023, 1, 1), False)
    with pytest.raises(PermissionError):
        remove_forecast_from_index(datetime(2023, 1, 1), "DALMP")
    os.chmod(index_file_path, 0o777)  # Restore permissions