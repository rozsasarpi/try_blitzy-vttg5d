# src/backend/tests/test_integration/test_forecast_to_storage.py
"""
Integration tests for the forecast to storage pipeline in the Electricity Market Price Forecasting System.
Verifies that forecasts are correctly stored, validated, and can be retrieved from the storage system with proper schema validation and metadata handling.
"""

import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
from datetime import datetime  # standard library
import os  # standard library
import pathlib  # standard library

from src.backend.models.forecast_models import ProbabilisticForecast  # Model class for probabilistic forecasts
from src.backend.models.forecast_models import ForecastEnsemble  # Class for ensemble of forecasts
from src.backend.storage.storage_manager import save_forecast  # Save a forecast to storage
from src.backend.storage.storage_manager import get_forecast  # Retrieve a forecast from storage
from src.backend.storage.storage_manager import get_latest_forecast  # Retrieve the latest forecast for a product
from src.backend.storage.storage_manager import check_forecast_availability  # Check if a forecast is available in storage
from src.backend.storage.storage_manager import remove_forecast  # Remove a forecast from storage
from src.backend.storage.storage_manager import initialize_storage  # Initialize the storage system
from src.backend.storage.schema_definitions import validate_forecast_schema  # Validate forecast data against schema
from src.backend.storage.schema_definitions import extract_storage_metadata  # Extract storage metadata from forecast dataframe
from src.backend.storage.exceptions import SchemaValidationError  # Exception for schema validation failures
from src.backend.storage.exceptions import DataFrameNotFoundError  # Exception for missing forecast DataFrames
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data  # Create mock forecast data for testing
from src.backend.tests.fixtures.forecast_fixtures import create_mock_probabilistic_forecast  # Create a single mock probabilistic forecast
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_ensemble  # Create a mock forecast ensemble for testing
from src.backend.tests.fixtures.forecast_fixtures import create_incomplete_forecast_data  # Create incomplete forecast data for testing validation
from src.backend.tests.fixtures.forecast_fixtures import create_invalid_forecast_data  # Create invalid forecast data for testing validation
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid forecast products


@pytest.mark.integration
def test_save_and_retrieve_forecast(temp_storage_path: pathlib.Path) -> None:
    """Tests that a forecast can be saved to storage and retrieved correctly"""
    # Initialize the storage system
    initialize_storage()

    # Create a mock forecast dataframe for a specific product and timestamp
    forecast_time = datetime(2023, 1, 1, 12, 0, 0)
    product = "DALMP"
    mock_forecast_df = create_mock_forecast_data(start_time=forecast_time, product=product, hours=1)

    # Save the forecast to storage using save_forecast
    save_forecast(mock_forecast_df, forecast_time, product)

    # Retrieve the forecast using get_forecast with the same timestamp and product
    retrieved_forecast_df = get_forecast(forecast_time, product)

    # Verify that the retrieved forecast matches the original
    pandas.testing.assert_frame_equal(retrieved_forecast_df, mock_forecast_df)

    # Check that the forecast has the expected metadata added during storage
    metadata = extract_storage_metadata(retrieved_forecast_df)
    assert "storage_timestamp" in metadata
    assert "storage_version" in metadata
    assert "schema_version" in metadata

    # Clean up by removing the test forecast from storage
    remove_forecast(forecast_time, product)


@pytest.mark.integration
def test_save_and_retrieve_latest_forecast(temp_storage_path: pathlib.Path) -> None:
    """Tests that the latest forecast for a product can be retrieved correctly"""
    # Initialize the storage system
    initialize_storage()

    # Create multiple mock forecasts with different timestamps for the same product
    product = "DALMP"
    forecast_time_1 = datetime(2023, 1, 1, 12, 0, 0)
    forecast_time_2 = datetime(2023, 1, 1, 13, 0, 0)
    mock_forecast_df_1 = create_mock_forecast_data(start_time=forecast_time_1, product=product, hours=1)
    mock_forecast_df_2 = create_mock_forecast_data(start_time=forecast_time_2, product=product, hours=1)

    # Save all forecasts to storage
    save_forecast(mock_forecast_df_1, forecast_time_1, product)
    save_forecast(mock_forecast_df_2, forecast_time_2, product)

    # Retrieve the latest forecast using get_latest_forecast
    latest_forecast_df = get_latest_forecast(product)

    # Verify that the retrieved forecast matches the most recent one saved
    pandas.testing.assert_frame_equal(latest_forecast_df, mock_forecast_df_2)

    # Clean up by removing the test forecasts from storage
    remove_forecast(forecast_time_1, product)
    remove_forecast(forecast_time_2, product)


@pytest.mark.integration
def test_forecast_availability_check(temp_storage_path: pathlib.Path) -> None:
    """Tests that forecast availability can be correctly checked"""
    # Initialize the storage system
    initialize_storage()

    # Create a mock forecast for a specific product and timestamp
    forecast_time = datetime(2023, 1, 1, 12, 0, 0)
    product = "DALMP"
    mock_forecast_df = create_mock_forecast_data(start_time=forecast_time, product=product, hours=1)

    # Check that the forecast is not available before saving
    assert not check_forecast_availability(forecast_time, product)

    # Save the forecast to storage
    save_forecast(mock_forecast_df, forecast_time, product)

    # Check that the forecast is available after saving
    assert check_forecast_availability(forecast_time, product)

    # Clean up by removing the test forecast from storage
    remove_forecast(forecast_time, product)


@pytest.mark.integration
def test_schema_validation_on_save(temp_storage_path: pathlib.Path) -> None:
    """Tests that schema validation is performed when saving a forecast"""
    # Initialize the storage system
    initialize_storage()

    # Create an invalid forecast dataframe (missing required columns)
    forecast_time = datetime(2023, 1, 1, 12, 0, 0)
    product = "DALMP"
    invalid_forecast_df = create_invalid_forecast_data(start_time=forecast_time, product=product, hours=1)

    # Attempt to save the invalid forecast and expect SchemaValidationError
    with pytest.raises(SchemaValidationError):
        save_forecast(invalid_forecast_df, forecast_time, product)

    # Verify that the invalid forecast was not saved to storage
    with pytest.raises(DataFrameNotFoundError):
        get_forecast(forecast_time, product)


@pytest.mark.integration
def test_forecast_ensemble_storage(temp_storage_path: pathlib.Path) -> None:
    """Tests that a forecast ensemble can be saved and retrieved correctly"""
    # Initialize the storage system
    initialize_storage()

    # Create a mock forecast ensemble for a product
    forecast_time = datetime(2023, 1, 1, 12, 0, 0)
    product = "DALMP"
    mock_ensemble = create_mock_forecast_ensemble(start_time=forecast_time, product=product, hours=3)

    # Convert the ensemble to a dataframe
    ensemble_df = mock_ensemble.to_dataframe()

    # Save the ensemble dataframe to storage
    save_forecast(ensemble_df, forecast_time, product)

    # Retrieve the saved dataframe
    retrieved_df = get_forecast(forecast_time, product)

    # Verify that all forecasts in the ensemble were saved and retrieved correctly
    pandas.testing.assert_frame_equal(retrieved_df, ensemble_df)

    # Clean up by removing the test forecasts from storage
    remove_forecast(forecast_time, product)


@pytest.mark.integration
def test_multiple_product_storage(temp_storage_path: pathlib.Path) -> None:
    """Tests that forecasts for multiple products can be stored and retrieved correctly"""
    # Initialize the storage system
    initialize_storage()

    # Create mock forecasts for multiple products with the same timestamp
    forecast_time = datetime(2023, 1, 1, 12, 0, 0)
    products = ["DALMP", "RTLMP", "RegUp"]
    mock_forecasts = {product: create_mock_forecast_data(start_time=forecast_time, product=product, hours=1) for product in products}

    # Save all forecasts to storage
    for product, forecast_df in mock_forecasts.items():
        save_forecast(forecast_df, forecast_time, product)

    # Retrieve each forecast by product
    retrieved_forecasts = {product: get_forecast(forecast_time, product) for product in products}

    # Verify that each retrieved forecast matches the original for that product
    for product, forecast_df in mock_forecasts.items():
        pandas.testing.assert_frame_equal(retrieved_forecasts[product], forecast_df)

    # Clean up by removing the test forecasts from storage
    for product in products:
        remove_forecast(forecast_time, product)


@pytest.mark.integration
def test_forecast_removal(temp_storage_path: pathlib.Path) -> None:
    """Tests that forecasts can be removed from storage"""
    # Initialize the storage system
    initialize_storage()

    # Create a mock forecast for a specific product and timestamp
    forecast_time = datetime(2023, 1, 1, 12, 0, 0)
    product = "DALMP"
    mock_forecast_df = create_mock_forecast_data(start_time=forecast_time, product=product, hours=1)

    # Save the forecast to storage
    save_forecast(mock_forecast_df, forecast_time, product)

    # Verify that the forecast is available in storage
    assert check_forecast_availability(forecast_time, product)

    # Remove the forecast using remove_forecast
    remove_forecast(forecast_time, product)

    # Verify that the forecast is no longer available in storage
    with pytest.raises(DataFrameNotFoundError):
        get_forecast(forecast_time, product)


@pytest.mark.integration
def test_metadata_handling(temp_storage_path: pathlib.Path) -> None:
    """Tests that storage metadata is correctly added and preserved"""
    # Initialize the storage system
    initialize_storage()

    # Create a mock forecast dataframe
    forecast_time = datetime(2023, 1, 1, 12, 0, 0)
    product = "DALMP"
    mock_forecast_df = create_mock_forecast_data(start_time=forecast_time, product=product, hours=1)

    # Save the forecast to storage
    save_forecast(mock_forecast_df, forecast_time, product)

    # Retrieve the saved forecast
    retrieved_forecast_df = get_forecast(forecast_time, product)

    # Extract storage metadata using extract_storage_metadata
    metadata = extract_storage_metadata(retrieved_forecast_df)

    # Verify that all required metadata fields are present (storage_timestamp, storage_version, schema_version)
    assert "storage_timestamp" in metadata
    assert "storage_version" in metadata
    assert "schema_version" in metadata

    # Verify that the metadata values are of the correct types
    assert isinstance(metadata["storage_timestamp"], str)
    assert isinstance(metadata["storage_version"], str)
    assert isinstance(metadata["schema_version"], str)

    # Clean up by removing the test forecast from storage
    remove_forecast(forecast_time, product)


@pytest.mark.integration
def test_fallback_flag_preservation(temp_storage_path: pathlib.Path) -> None:
    """Tests that the fallback flag is preserved when storing and retrieving forecasts"""
    # Initialize the storage system
    initialize_storage()

    # Create a mock forecast dataframe with is_fallback=True
    forecast_time = datetime(2023, 1, 1, 12, 0, 0)
    product = "DALMP"
    mock_forecast_df = create_mock_forecast_data(start_time=forecast_time, product=product, hours=1, is_fallback=True)

    # Save the forecast to storage
    save_forecast(mock_forecast_df, forecast_time, product, is_fallback=True)

    # Retrieve the saved forecast
    retrieved_forecast_df = get_forecast(forecast_time, product)

    # Verify that the is_fallback flag is still True in the retrieved forecast
    assert retrieved_forecast_df["is_fallback"].iloc[0] == True

    # Clean up by removing the test forecast from storage
    remove_forecast(forecast_time, product)


@pytest.mark.integration
@pytest.mark.slow
def test_large_forecast_storage(temp_storage_path: pathlib.Path) -> None:
    """Tests that large forecast datasets can be stored and retrieved correctly"""
    # Initialize the storage system
    initialize_storage()

    # Create a large mock forecast ensemble with the full 72-hour horizon
    forecast_time = datetime(2023, 1, 1, 12, 0, 0)
    product = "DALMP"
    mock_ensemble = create_mock_forecast_ensemble(start_time=forecast_time, product=product, hours=72)

    # Convert the ensemble to a dataframe
    ensemble_df = mock_ensemble.to_dataframe()

    # Save the large forecast to storage
    save_forecast(ensemble_df, forecast_time, product)

    # Retrieve the saved forecast
    retrieved_df = get_forecast(forecast_time, product)

    # Verify that all 72 hours of forecasts were saved and retrieved correctly
    pandas.testing.assert_frame_equal(retrieved_df, ensemble_df)

    # Clean up by removing the test forecasts from storage
    remove_forecast(forecast_time, product)