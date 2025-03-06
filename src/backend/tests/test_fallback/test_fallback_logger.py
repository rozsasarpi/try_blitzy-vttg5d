"""
Unit tests for the fallback logger module that handles logging of fallback mechanism activities
in the Electricity Market Price Forecasting System. Tests verify that all logging functions
correctly record fallback events, handle errors appropriately, and provide structured context information.
"""

import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import datetime  # standard library
import logging  # standard library
import pandas  # pandas: 2.0.0+

# Internal imports
from ...fallback.fallback_logger import log_fallback_activation  # Function to log fallback mechanism activation
from ...fallback.fallback_logger import log_fallback_retrieval  # Function to log retrieval of previous forecast for fallback
from ...fallback.fallback_logger import log_timestamp_adjustment  # Function to log adjustment of timestamps for fallback forecast
from ...fallback.fallback_logger import log_fallback_completion  # Function to log successful completion of fallback process
from ...fallback.fallback_logger import log_fallback_error  # Function to log errors that occur during fallback process
from ...fallback.fallback_logger import get_fallback_metrics  # Function to collect metrics about a fallback forecast
from ...fallback.exceptions import FallbackLoggingError  # Exception for fallback logging failures
from ..fixtures.forecast_fixtures import create_mock_forecast_data  # Create mock forecast data for testing
from ...fallback.fallback_logger import component_logger  # Component logger instance for the fallback mechanism


def test_log_fallback_activation():
    """Tests that fallback activation is logged correctly"""
    # Set up test data with component name, error category, and error details
    component = "test_component"
    error_category = "test_error"
    error_details = {"detail1": "value1", "detail2": "value2"}

    # Mock the logger to capture log messages
    with unittest.mock.patch('src.backend.fallback.fallback_logger.logger') as mock_logger:
        # Call log_fallback_activation with test data
        log_fallback_activation(component, error_category, error_details)

        # Assert that logger.warning was called once
        assert mock_logger.warning.call_count == 1

        # Verify that the log message contains the component name and error category
        log_message = mock_logger.warning.call_args[0][0]
        assert component in log_message
        assert error_category in log_message

        # Check that error details are included in the log context
        assert mock_logger.warning.call_count == 1
        assert component_logger.with_context({
            'component': component,
            'error_category': error_category,
            'timestamp': unittest.mock.ANY,
            **error_details
        }).warning.call_count == 1


def test_log_fallback_activation_error_handling():
    """Tests that errors during fallback activation logging are handled correctly"""
    # Set up test data with component name, error category, and error details
    component = "test_component"
    error_category = "test_error"
    error_details = {"detail1": "value1", "detail2": "value2"}

    # Mock the logger to raise an exception when warning is called
    with unittest.mock.patch('src.backend.fallback.fallback_logger.logger.warning', side_effect=Exception("Logging failed")):
        # Call log_fallback_activation with test data and assert it raises FallbackLoggingError
        with pytest.raises(FallbackLoggingError) as exc_info:
            log_fallback_activation(component, error_category, error_details)

        # Verify that the original exception is included in the FallbackLoggingError
        assert "Logging failed" in str(exc_info.value)


def test_log_fallback_retrieval():
    """Tests that fallback retrieval is logged correctly"""
    # Set up test data with product, target date, source date, and metadata
    product = "DALMP"
    target_date = datetime.datetime(2023, 1, 2, tzinfo=datetime.timezone.utc)
    source_date = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
    metadata = {"source": "storage", "version": "1.0"}

    # Mock the logger to capture log messages
    with unittest.mock.patch('src.backend.fallback.fallback_logger.logger') as mock_logger:
        # Call log_fallback_retrieval with test data
        log_fallback_retrieval(product, target_date, source_date, metadata)

        # Assert that logger.info was called once
        assert mock_logger.info.call_count == 1

        # Verify that the log message contains the product, target date, and source date
        log_message = mock_logger.info.call_args[0][0]
        assert product in log_message
        assert target_date.strftime('%Y-%m-%d') in log_message
        assert source_date.strftime('%Y-%m-%d') in log_message

        # Check that metadata is included in the log context
        assert mock_logger.info.call_count == 1
        assert component_logger.with_context({
            'product': product,
            'target_date': target_date.strftime('%Y-%m-%d'),
            'source_date': source_date.strftime('%Y-%m-%d'),
            'days_difference': (target_date.date() - source_date.date()).days,
            'timestamp': unittest.mock.ANY,
            **metadata
        }).info.call_count == 1


def test_log_fallback_retrieval_error_handling():
    """Tests that errors during fallback retrieval logging are handled correctly"""
    # Set up test data with product, target date, source date, and metadata
    product = "DALMP"
    target_date = datetime.datetime(2023, 1, 2, tzinfo=datetime.timezone.utc)
    source_date = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
    metadata = {"source": "storage", "version": "1.0"}

    # Mock the logger to raise an exception when info is called
    with unittest.mock.patch('src.backend.fallback.fallback_logger.logger.info', side_effect=Exception("Logging failed")):
        # Call log_fallback_retrieval with test data and assert it raises FallbackLoggingError
        with pytest.raises(FallbackLoggingError) as exc_info:
            log_fallback_retrieval(product, target_date, source_date, metadata)

        # Verify that the original exception is included in the FallbackLoggingError
        assert "Logging failed" in str(exc_info.value)


def test_log_timestamp_adjustment():
    """Tests that timestamp adjustment is logged correctly"""
    # Set up test data with product, source date, target date, and forecast DataFrame
    product = "DALMP"
    source_date = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
    target_date = datetime.datetime(2023, 1, 2, tzinfo=datetime.timezone.utc)
    forecast_df = create_mock_forecast_data()

    # Mock the logger to capture log messages
    with unittest.mock.patch('src.backend.fallback.fallback_logger.logger') as mock_logger:
        # Call log_timestamp_adjustment with test data
        log_timestamp_adjustment(product, source_date, target_date, forecast_df)

        # Assert that logger.info was called once
        assert mock_logger.info.call_count == 1

        # Verify that the log message contains the product, source date, and target date
        log_message = mock_logger.info.call_args[0][0]
        assert product in log_message
        assert source_date.strftime('%Y-%m-%d') in log_message
        assert target_date.strftime('%Y-%m-%d') in log_message

        # Check that forecast data information is included in the log context
        assert mock_logger.info.call_count == 1
        assert component_logger.with_context({
            'product': product,
            'source_date': source_date.strftime('%Y-%m-%d'),
            'target_date': target_date.strftime('%Y-%m-%d'),
            'time_shift_days': (target_date.date() - source_date.date()).days,
            'timestamp': unittest.mock.ANY,
            'forecast_data_points': len(forecast_df),
            'forecast_horizon_hours': 72,
        }).info.call_count == 1


def test_log_timestamp_adjustment_error_handling():
    """Tests that errors during timestamp adjustment logging are handled correctly"""
    # Set up test data with product, source date, target date, and forecast DataFrame
    product = "DALMP"
    source_date = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
    target_date = datetime.datetime(2023, 1, 2, tzinfo=datetime.timezone.utc)
    forecast_df = create_mock_forecast_data()

    # Mock the logger to raise an exception when info is called
    with unittest.mock.patch('src.backend.fallback.fallback_logger.logger.info', side_effect=Exception("Logging failed")):
        # Call log_timestamp_adjustment with test data and assert it raises FallbackLoggingError
        with pytest.raises(FallbackLoggingError) as exc_info:
            log_timestamp_adjustment(product, source_date, target_date, forecast_df)

        # Verify that the original exception is included in the FallbackLoggingError
        assert "Logging failed" in str(exc_info.value)


def test_log_fallback_completion():
    """Tests that fallback completion is logged correctly"""
    # Set up test data with product, target date, source date, start time, and metrics
    product = "DALMP"
    target_date = datetime.datetime(2023, 1, 2, tzinfo=datetime.timezone.utc)
    source_date = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
    start_time = time.time()
    metrics = {"data_points": 72, "rmse": 2.5}

    # Mock the logger to capture log messages
    with unittest.mock.patch('src.backend.fallback.fallback_logger.logger') as mock_logger:
        # Call log_fallback_completion with test data
        log_fallback_completion(product, target_date, source_date, start_time, metrics)

        # Assert that logger.info was called once
        assert mock_logger.info.call_count == 1

        # Verify that the log message contains the product, target date, and source date
        log_message = mock_logger.info.call_args[0][0]
        assert product in log_message
        assert target_date.strftime('%Y-%m-%d') in log_message
        assert source_date.strftime('%Y-%m-%d') in log_message

        # Check that duration and metrics are included in the log context
        assert mock_logger.info.call_count == 1
        assert component_logger.with_context({
            'product': product,
            'target_date': target_date.strftime('%Y-%m-%d'),
            'source_date': source_date.strftime('%Y-%m-%d'),
            'duration_seconds': unittest.mock.ANY,
            'timestamp': unittest.mock.ANY,
            **metrics
        }).info.call_count == 1


def test_log_fallback_completion_error_handling():
    """Tests that errors during fallback completion logging are handled correctly"""
    # Set up test data with product, target date, source date, start time, and metrics
    product = "DALMP"
    target_date = datetime.datetime(2023, 1, 2, tzinfo=datetime.timezone.utc)
    source_date = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
    start_time = time.time()
    metrics = {"data_points": 72, "rmse": 2.5}

    # Mock the logger to raise an exception when info is called
    with unittest.mock.patch('src.backend.fallback.fallback_logger.logger.info', side_effect=Exception("Logging failed")):
        # Call log_fallback_completion with test data and assert it raises FallbackLoggingError
        with pytest.raises(FallbackLoggingError) as exc_info:
            log_fallback_completion(product, target_date, source_date, start_time, metrics)

        # Verify that the original exception is included in the FallbackLoggingError
        assert "Logging failed" in str(exc_info.value)


def test_log_fallback_error():
    """Tests that fallback errors are logged correctly"""
    # Set up test data with operation name, error, and context
    operation = "test_operation"
    error = Exception("Test error")
    context = {"param1": "value1", "param2": "value2"}

    # Mock the logger to capture log messages
    with unittest.mock.patch('src.backend.fallback.fallback_logger.logger') as mock_logger:
        # Call log_fallback_error with test data
        log_fallback_error(operation, error, context)

        # Assert that logger.error was called once
        assert mock_logger.error.call_count == 1

        # Verify that the log message contains the operation name and error details
        log_message = mock_logger.error.call_args[0][0]
        assert operation in log_message
        assert "Test error" in log_message

        # Check that context information is included in the log message
        assert mock_logger.error.call_count == 1
        assert component_logger.with_context({
            'operation': operation,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_details': unittest.mock.ANY,
            'timestamp': unittest.mock.ANY,
            **context
        }).error.call_count == 1


def test_log_fallback_error_handling():
    """Tests that errors during fallback error logging are handled correctly"""
    # Set up test data with operation name, error, and context
    operation = "test_operation"
    error = Exception("Test error")
    context = {"param1": "value1", "param2": "value2"}

    # Mock the logger to raise an exception when error is called
    with unittest.mock.patch('src.backend.fallback.fallback_logger.logger.error', side_effect=Exception("Logging failed")):
        # Call log_fallback_error with test data and assert it raises FallbackLoggingError
        with pytest.raises(FallbackLoggingError) as exc_info:
            log_fallback_error(operation, error, context)

        # Verify that the original exception is included in the FallbackLoggingError
        assert "Logging failed" in str(exc_info.value)


def test_get_fallback_metrics():
    """Tests that fallback metrics are collected correctly"""
    # Create mock forecast DataFrame using create_mock_forecast_data
    mock_forecast_df = create_mock_forecast_data()

    # Call get_fallback_metrics with the mock forecast data
    metrics = get_fallback_metrics(mock_forecast_df, "DALMP")

    # Verify that the returned metrics dictionary contains expected keys
    assert "product" in metrics
    assert "data_points" in metrics
    assert "horizon_hours" in metrics
    assert "is_cascading_fallback" in metrics

    # Check that data_points matches the number of rows in the DataFrame
    assert metrics["data_points"] == len(mock_forecast_df)

    # Verify that forecast_horizon matches the expected hours
    assert metrics["horizon_hours"] == 72

    # Check that is_cascading_fallback is correctly determined
    assert metrics["is_cascading_fallback"] is False


def test_get_fallback_metrics_error_handling():
    """Tests that errors during metrics collection are handled gracefully"""
    # Create a mock DataFrame that will cause an error when processed
    mock_forecast_df = pandas.DataFrame({"timestamp": [1, 2, 3], "DALMP": ["a", "b", "c"]})

    # Call get_fallback_metrics with the problematic DataFrame
    metrics = get_fallback_metrics(mock_forecast_df, "DALMP")

    # Verify that a minimal metrics dictionary is returned instead of raising an exception
    assert isinstance(metrics, dict)

    # Check that the minimal metrics dictionary contains the expected keys with default values
    assert "product" in metrics
    assert "metrics_error" in metrics