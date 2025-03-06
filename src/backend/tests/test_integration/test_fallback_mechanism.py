# src/backend/tests/test_integration/test_fallback_mechanism.py
"""Integration tests for the fallback mechanism of the Electricity Market Price Forecasting System.
Tests the end-to-end functionality of the fallback mechanism when various types of failures occur during the
forecasting process, ensuring that previous day's forecasts are correctly retrieved, adjusted, and used as fallbacks.
"""

import datetime
import pandas  # version: 2.0.0+
import unittest.mock  # standard library
import pytest  # version: 7.0.0+

from src.backend.fallback.exceptions import FallbackError, ErrorDetectionFailure, FallbackRetrievalError, TimestampAdjustmentError, NoFallbackAvailableError
from src.backend.fallback.error_detector import ErrorCategory, detect_error, should_activate_fallback
from src.backend.fallback.fallback_retriever import retrieve_fallback_forecast
from src.backend.fallback.timestamp_adjuster import adjust_timestamps
from src.backend.fallback import process_fallback
from src.backend.storage.storage_manager import save_forecast, get_forecast, check_forecast_availability
from src.backend.pipeline.forecasting_pipeline import ForecastingPipeline
from src.backend.data_ingestion.exceptions import DataIngestionError
from src.backend.feature_engineering.exceptions import FeatureEngineeringError
from src.backend.forecasting_engine.exceptions import ForecastingEngineError
from src.backend.forecast_validation.exceptions import ForecastValidationError
from src.backend.storage.exceptions import StorageError
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data
from src.backend.config.settings import FORECAST_PRODUCTS, get_previous_day_date
from src.backend.utils.date_utils import localize_to_cst, get_previous_day_date


def setup_test_forecasts(products: list = None, base_date: datetime.datetime = None, days_back: int = None) -> dict:
    """Helper function to set up test forecasts in storage for fallback testing"""
    forecast_paths = {}
    if products is None:
        products = FORECAST_PRODUCTS
    if base_date is None:
        base_date = localize_to_cst(datetime.datetime.now())
    if days_back is None:
        days_back = 3

    for day in range(1, days_back + 1):
        date = base_date - datetime.timedelta(days=day)
        for product in products:
            forecast_data = create_mock_forecast_data(product=product, start_time=date)
            save_forecast(forecast_data, date, product)
            forecast_paths[product] = str(date)
    return forecast_paths


def test_error_detection():
    """Tests the error detection functionality of the fallback mechanism"""
    # Create different types of errors
    data_error = DataIngestionError("Data ingestion failed")
    feature_error = FeatureEngineeringError("Feature engineering failed")
    model_error = ForecastingEngineError("Model execution failed", "DALMP", 1)
    validation_error = ForecastValidationError("Forecast validation failed")
    storage_error = StorageError("Storage operation failed")

    # Test each error type
    category, details = detect_error(data_error, "data_ingestion")
    assert category == ErrorCategory.DATA_SOURCE_ERROR
    assert "DataIngestionError" in details["error_type"]
    assert should_activate_fallback(category, details)

    category, details = detect_error(feature_error, "feature_engineering")
    assert category == ErrorCategory.FEATURE_ERROR
    assert "FeatureEngineeringError" in details["error_type"]
    assert should_activate_fallback(category, details)

    category, details = detect_error(model_error, "forecasting_engine")
    assert category == ErrorCategory.MODEL_ERROR
    assert "ForecastingEngineError" in details["error_type"]
    assert should_activate_fallback(category, details)

    category, details = detect_error(validation_error, "forecast_validation")
    assert category == ErrorCategory.VALIDATION_ERROR
    assert "ForecastValidationError" in details["error_type"]
    assert should_activate_fallback(category, details)

    category, details = detect_error(storage_error, "storage")
    assert category == ErrorCategory.STORAGE_ERROR
    assert "StorageError" in details["error_type"]
    assert not should_activate_fallback(category, details)  # Storage errors may not always need fallback


def test_fallback_retrieval():
    """Tests the retrieval of previous forecasts for fallback"""
    # Set up test forecasts in storage
    setup_test_forecasts()

    # Define a target date for fallback
    target_date = localize_to_cst(datetime.datetime.now())

    # Test retrieval for each product
    for product in FORECAST_PRODUCTS:
        fallback_df = retrieve_fallback_forecast(product, target_date)
        assert isinstance(fallback_df, pandas.DataFrame)
        assert 'timestamp' in fallback_df.columns
        assert 'product' in fallback_df.columns
        assert len(fallback_df) > 0

    # Test with non-existent product
    with pytest.raises(NoFallbackAvailableError):
        retrieve_fallback_forecast("InvalidProduct", target_date)

    # Test with future date where no fallback exists
    future_date = target_date + datetime.timedelta(days=10)
    with pytest.raises(NoFallbackAvailableError):
        retrieve_fallback_forecast("DALMP", future_date)


def test_timestamp_adjustment():
    """Tests the adjustment of timestamps in fallback forecasts"""
    # Create mock forecast data for a previous date
    source_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    product = "DALMP"
    forecast_data = create_mock_forecast_data(product=product, start_time=source_date)

    # Adjust timestamps
    adjusted_df = adjust_timestamps(forecast_data, product, source_date, target_date)

    # Assert that timestamps are shifted correctly
    assert adjusted_df['timestamp'].iloc[0].date() == target_date.date()
    assert len(forecast_data) == len(adjusted_df)
    assert all(adjusted_df['is_fallback'])
    assert all(adjusted_df['generation_timestamp'] <= localize_to_cst(datetime.datetime.now()))

    # Test with invalid parameters
    with pytest.raises(TimestampAdjustmentError):
        adjust_timestamps(None, product, source_date, target_date)


def test_process_fallback():
    """Tests the end-to-end fallback processing functionality"""
    # Set up test forecasts in storage
    setup_test_forecasts()

    # Create different types of errors
    data_error = DataIngestionError("Data ingestion failed")
    model_error = ForecastingEngineError("Model execution failed", "DALMP", 1)
    validation_error = ForecastValidationError("Forecast validation failed")

    # Test each error type and component combination
    target_date = localize_to_cst(datetime.datetime.now())
    for error, component, product in [
        (data_error, "data_ingestion", "DALMP"),
        (model_error, "forecasting_engine", "DALMP"),
        (validation_error, "forecast_validation", "DALMP"),
    ]:
        fallback_df = process_fallback(error, component, product, target_date)
        assert isinstance(fallback_df, pandas.DataFrame)
        assert all(fallback_df['is_fallback'])
        assert all(fallback_df['timestamp'].dt.date == target_date.date())

    # Test with error that should not trigger fallback
    storage_error = StorageError("Storage operation failed")
    with pytest.raises(StorageError):
        process_fallback(storage_error, "storage", "DALMP", target_date)


def test_pipeline_fallback_integration():
    """Tests the integration of fallback mechanism with the forecasting pipeline"""
    # Set up test forecasts in storage
    setup_test_forecasts()

    # Create a ForecastingPipeline instance
    target_date = localize_to_cst(datetime.datetime.now())
    pipeline = ForecastingPipeline(target_date, {}, "test_pipeline")

    # Mock the ingest_data method to raise DataIngestionError
    with unittest.mock.patch.object(ForecastingPipeline, 'ingest_data', side_effect=DataIngestionError("Data ingestion failed")):
        # Run pipeline and assert it returns False (indicating fallback was used)
        assert not pipeline.run()
        assert pipeline.was_fallback_used()

        # Verify that fallback forecasts were saved to storage
        for product in FORECAST_PRODUCTS:
            assert check_forecast_availability(target_date, product)
            fallback_df = get_forecast(target_date, product)
            assert all(fallback_df['is_fallback'])

    # Repeat with different pipeline stages failing
    with unittest.mock.patch.object(ForecastingPipeline, 'engineer_features', side_effect=FeatureEngineeringError("Feature engineering failed")):
        assert not pipeline.run()
        assert pipeline.was_fallback_used()

    with unittest.mock.patch.object(ForecastingPipeline, 'generate_forecasts', side_effect=ForecastingEngineError("Model execution failed", "DALMP", 1)):
        assert not pipeline.run()
        assert pipeline.was_fallback_used()

    with unittest.mock.patch.object(ForecastingPipeline, 'validate_forecasts', side_effect=ForecastValidationError("Forecast validation failed")):
        assert not pipeline.run()
        assert pipeline.was_fallback_used()


def test_fallback_cascade_prevention():
    """Tests that fallback mechanism prevents cascading fallbacks when possible"""
    # Set up test forecasts in storage, including some marked as fallbacks
    setup_test_forecasts()

    # Define a target date for fallback
    target_date = localize_to_cst(datetime.datetime.now())

    # Retrieve fallback forecast
    product = "DALMP"
    fallback_df = retrieve_fallback_forecast(product, target_date)

    # Assert that the function preferentially returns non-fallback forecasts when available
    assert not any(fallback_df['is_fallback'])

    # When only fallback forecasts are available, assert it uses the most recent one
    # (This requires more complex setup, so it's left as a potential future test)


def test_fallback_with_missing_products():
    """Tests fallback behavior when some products have no available fallbacks"""
    # Set up test forecasts in storage for only some products
    products_with_forecasts = ["DALMP", "RTLMP"]
    setup_test_forecasts(products=products_with_forecasts)

    # Create a ForecastingPipeline instance
    target_date = localize_to_cst(datetime.datetime.now())
    pipeline = ForecastingPipeline(target_date, {}, "test_pipeline")

    # Mock pipeline stages to fail
    with unittest.mock.patch.object(ForecastingPipeline, 'ingest_data', side_effect=DataIngestionError("Data ingestion failed")):
        # Call pipeline.activate_fallback() and assert it handles partial fallbacks correctly
        pipeline.run()

        # Verify that products with available fallbacks are processed
        for product in products_with_forecasts:
            assert check_forecast_availability(target_date, product)
            fallback_df = get_forecast(target_date, product)
            assert all(fallback_df['is_fallback'])

        # Verify that products without available fallbacks are reported as errors
        for product in FORECAST_PRODUCTS:
            if product not in products_with_forecasts:
                with pytest.raises(NoFallbackAvailableError):
                    retrieve_fallback_forecast(product, target_date)