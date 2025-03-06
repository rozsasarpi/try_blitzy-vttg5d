"""
Test module for the logging utilities in the Electricity Market Price Forecasting System.

This module contains unit tests for all logging-related functions and classes
including loggers, decorators, formatters, and adapters.
"""

import pytest
import unittest.mock as mock
import time
import logging
import datetime
import json
import tempfile
import os

from src.backend.utils.logging_utils import (
    get_logger,
    log_execution_time,
    log_method_execution_time,
    format_exception,
    format_dict_for_logging,
    configure_component_logger,
    ContextAdapter,
    ComponentLogger
)
from src.backend.config.logging_config import LOG_DIR, setup_logging


def test_get_logger():
    """Test that get_logger returns a properly configured logger instance."""
    logger_name = "test_logger"
    
    # Get a logger
    logger = get_logger(logger_name)
    
    # Verify it's a proper logger instance
    assert isinstance(logger, logging.Logger)
    assert logger.name == logger_name
    
    # Get the same logger again to test caching
    logger2 = get_logger(logger_name)
    
    # Verify it's the same instance (cached)
    assert logger is logger2


@pytest.mark.parametrize('sleep_time', [0.01, 0.05, 0.1])
def test_log_execution_time(sleep_time):
    """Test that log_execution_time decorator correctly measures and logs function execution time."""
    # Create a mock logger
    mock_logger = mock.MagicMock()
    
    # Create a test function that sleeps
    @log_execution_time
    def test_function():
        time.sleep(sleep_time)
        return "test result"
    
    # Patch the get_logger function to return our mock
    with mock.patch('src.backend.utils.logging_utils.get_logger', return_value=mock_logger):
        # Call the function
        result = test_function()
        
        # Verify the function worked properly
        assert result == "test result"
        
        # Verify that logging occurred
        mock_logger.info.assert_called_once()
        
        # Verify the log message contains execution time
        log_message = mock_logger.info.call_args[0][0]
        assert "test_function executed in" in log_message
        
        # Extract the execution time from the log message
        import re
        time_pattern = r"executed in (\d+\.\d+) seconds"
        match = re.search(time_pattern, log_message)
        if match:
            logged_time = float(match.group(1))
            # Verify that the logged time is reasonably close to the sleep time
            # Allow for some overhead in the timing
            assert sleep_time <= logged_time <= sleep_time + 0.05


@pytest.mark.parametrize('sleep_time', [0.01, 0.05, 0.1])
def test_log_method_execution_time(sleep_time):
    """Test that log_method_execution_time decorator correctly measures and logs class method execution time."""
    # Create a mock logger
    mock_logger = mock.MagicMock()
    
    # Create a test class with a decorated method
    class TestClass:
        @log_method_execution_time
        def test_method(self):
            time.sleep(sleep_time)
            return "test result"
    
    # Patch the get_logger function to return our mock
    with mock.patch('src.backend.utils.logging_utils.get_logger', return_value=mock_logger):
        # Create an instance and call the method
        test_instance = TestClass()
        result = test_instance.test_method()
        
        # Verify the method worked properly
        assert result == "test result"
        
        # Verify that logging occurred
        mock_logger.info.assert_called_once()
        
        # Verify the log message contains execution time and class/method name
        log_message = mock_logger.info.call_args[0][0]
        assert "TestClass.test_method executed in" in log_message
        
        # Extract the execution time from the log message
        import re
        time_pattern = r"executed in (\d+\.\d+) seconds"
        match = re.search(time_pattern, log_message)
        if match:
            logged_time = float(match.group(1))
            # Verify that the logged time is reasonably close to the sleep time
            # Allow for some overhead in the timing
            assert sleep_time <= logged_time <= sleep_time + 0.05


def test_format_exception():
    """Test that format_exception correctly formats exception information."""
    # Test with a simple exception
    try:
        raise ValueError("This is a test error")
    except ValueError as e:
        formatted = format_exception(e)
        
        # Verify the formatted exception contains expected elements
        assert "ValueError" in formatted
        assert "This is a test error" in formatted
        assert "Traceback" in formatted
    
    # Test with a nested exception
    try:
        try:
            raise ValueError("Inner error")
        except ValueError:
            raise RuntimeError("Outer error")
    except RuntimeError as e:
        formatted = format_exception(e)
        
        # Verify the formatted exception contains expected elements
        assert "RuntimeError" in formatted
        assert "Outer error" in formatted
        assert "Traceback" in formatted


def test_format_dict_for_logging():
    """Test that format_dict_for_logging correctly formats dictionaries for logging."""
    # Test with a simple dictionary
    simple_dict = {"a": 1, "b": "test", "c": True}
    formatted = format_dict_for_logging(simple_dict)
    
    # Verify it's valid JSON and has the expected content
    parsed = json.loads(formatted)
    assert parsed["a"] == 1
    assert parsed["b"] == "test"
    assert parsed["c"] is True
    
    # Test with nested dictionaries
    nested_dict = {"a": 1, "b": {"c": 2, "d": {"e": 3}}}
    formatted = format_dict_for_logging(nested_dict)
    
    # Verify it's valid JSON and has the expected nested structure
    parsed = json.loads(formatted)
    assert parsed["a"] == 1
    assert parsed["b"]["c"] == 2
    assert parsed["b"]["d"]["e"] == 3
    
    # Test with non-serializable objects
    class TestObject:
        def __str__(self):
            return "TestObject"
    
    complex_dict = {"a": 1, "b": datetime.datetime.now(), "c": TestObject()}
    formatted = format_dict_for_logging(complex_dict)
    
    # Verify it's valid JSON and has converted non-serializable objects to strings
    parsed = json.loads(formatted)
    assert parsed["a"] == 1
    assert isinstance(parsed["b"], str)  # datetime converted to string
    assert parsed["c"] == "TestObject"  # custom object converted to string

    # Test with a dictionary containing a list
    list_dict = {"a": 1, "b": [1, 2, 3, {"c": 4}]}
    formatted = format_dict_for_logging(list_dict)
    
    # Verify it's valid JSON and has the expected list structure
    parsed = json.loads(formatted)
    assert parsed["a"] == 1
    assert parsed["b"][0] == 1
    assert parsed["b"][3]["c"] == 4


@pytest.mark.parametrize('log_level', ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
def test_configure_component_logger(log_level):
    """Test that configure_component_logger correctly configures a logger for a specific component."""
    component_name = "test_component"
    
    # Mock setup_logging to avoid changing the actual logging configuration
    with mock.patch('src.backend.utils.logging_utils.setup_logging') as mock_setup:
        # Call the function
        logger = configure_component_logger(component_name, log_level)
        
        # Verify the logger was properly configured
        assert isinstance(logger, logging.Logger)
        assert logger.name == component_name
        
        # Check that the logger has the correct log level
        expected_level = getattr(logging, log_level)
        assert logger.level == expected_level
        
        # Check that the logger has the expected handlers
        has_console_handler = any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) 
                                for h in logger.handlers)
        has_file_handler = any(isinstance(h, logging.FileHandler) for h in logger.handlers)
        
        assert has_console_handler, "Logger should have a console handler"
        assert has_file_handler, "Logger should have a file handler"
        
        # Check that propagation is disabled
        assert logger.propagate is False, "Logger should not propagate to the root logger"


def test_context_adapter_process():
    """Test that ContextAdapter.process correctly adds context to log messages."""
    # Create a mock logger
    mock_logger = mock.MagicMock()
    
    # Create a context adapter with a test context
    test_context = {"test_key": "test_value", "number": 42}
    adapter = ContextAdapter(mock_logger, test_context)
    
    # Call the process method
    msg = "Test message"
    kwargs = {}
    processed_msg, processed_kwargs = adapter.process(msg, kwargs)
    
    # Verify the message contains the original message and context
    assert msg in processed_msg
    assert "[Context:" in processed_msg
    
    # Verify the context was serialized correctly
    assert "test_key" in processed_msg
    assert "test_value" in processed_msg
    assert "42" in processed_msg


def test_context_adapter_with_context():
    """Test that ContextAdapter.with_context correctly creates a new adapter with merged context."""
    # Create a mock logger
    mock_logger = mock.MagicMock()
    
    # Create a context adapter with an initial context
    initial_context = {"initial_key": "initial_value"}
    adapter = ContextAdapter(mock_logger, initial_context)
    
    # Add additional context
    additional_context = {"additional_key": "additional_value"}
    new_adapter = adapter.with_context(additional_context)
    
    # Verify the new adapter has the merged context
    assert isinstance(new_adapter, ContextAdapter)
    assert new_adapter.context["initial_key"] == "initial_value"
    assert new_adapter.context["additional_key"] == "additional_value"
    
    # Verify the original adapter's context was not modified
    assert "additional_key" not in adapter.context


@pytest.mark.parametrize('method_name', ['debug', 'info', 'warning', 'error', 'critical', 'exception'])
def test_context_adapter_log_methods(method_name):
    """Test that ContextAdapter log methods correctly process messages and call the underlying logger."""
    # Create a mock logger
    mock_logger = mock.MagicMock()
    
    # Create a context adapter with a test context
    test_context = {"test_key": "test_value"}
    adapter = ContextAdapter(mock_logger, test_context)
    
    # Call the log method
    log_method = getattr(adapter, method_name)
    test_message = f"Test {method_name} message"
    log_method(test_message)
    
    # Verify the underlying logger's method was called
    logger_method = getattr(mock_logger, method_name)
    logger_method.assert_called_once()
    
    # Verify the message was processed to include context
    call_args = logger_method.call_args[0]
    assert len(call_args) > 0
    assert test_message in call_args[0]
    assert "Context" in call_args[0]
    assert "test_key" in call_args[0]
    assert "test_value" in call_args[0]


def test_component_logger_initialization():
    """Test that ComponentLogger initializes correctly with component name and default context."""
    component_name = "test_component"
    default_context = {"default_key": "default_value"}
    
    # Mock get_logger to return a mock logger
    with mock.patch('src.backend.utils.logging_utils.get_logger') as mock_get_logger:
        mock_logger = mock.MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Create the component logger
        component_logger = ComponentLogger(component_name, default_context)
        
        # Verify get_logger was called with the component name
        mock_get_logger.assert_called_once_with(component_name)
        
        # Verify the logger was initialized correctly
        assert component_logger.component_name == component_name
        assert component_logger.default_context == default_context
        assert component_logger.default_context.get("component") is None  # Should not override provided default_context
        
        # Test with no default context
        component_logger2 = ComponentLogger(component_name)
        assert component_logger2.default_context == {"component": component_name}


def test_component_logger_with_context():
    """Test that ComponentLogger.with_context correctly creates a new logger with merged context."""
    component_name = "test_component"
    default_context = {"default_key": "default_value"}
    
    # Mock get_logger to avoid actual logger creation
    with mock.patch('src.backend.utils.logging_utils.get_logger') as mock_get_logger:
        mock_logger = mock.MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Create the component logger
        component_logger = ComponentLogger(component_name, default_context)
        
        # Add additional context
        additional_context = {"additional_key": "additional_value"}
        new_logger = component_logger.with_context(additional_context)
        
        # Verify the new logger has the merged context
        assert isinstance(new_logger, ComponentLogger)
        assert new_logger.default_context["default_key"] == "default_value"
        assert new_logger.default_context["additional_key"] == "additional_value"
        
        # Verify the original logger's context was not modified
        assert "additional_key" not in component_logger.default_context


def test_component_logger_log_start():
    """Test that ComponentLogger.log_start correctly logs the start of an operation."""
    component_name = "test_component"
    
    # Create a mock underlying logger
    mock_logger = mock.MagicMock()
    
    # Create a mock adapter
    mock_adapter = mock.MagicMock()
    
    # Create the component logger with mocks
    with mock.patch('src.backend.utils.logging_utils.get_logger', return_value=mock_logger):
        with mock.patch('src.backend.utils.logging_utils.ContextAdapter', return_value=mock_adapter):
            component_logger = ComponentLogger(component_name)
            
            # Call log_start
            operation = "test_operation"
            details = {"param1": "value1", "param2": 42}
            component_logger.log_start(operation, details)
            
            # Verify the adapter's info method was called
            mock_adapter.info.assert_called_once()
            
            # Verify the message contains the operation name
            assert f"Started {operation}" in mock_adapter.info.call_args[0][0]


def test_component_logger_log_completion():
    """Test that ComponentLogger.log_completion correctly logs the completion of an operation."""
    component_name = "test_component"
    
    # Create a mock underlying logger
    mock_logger = mock.MagicMock()
    
    # Create a mock adapter
    mock_adapter = mock.MagicMock()
    
    # Create the component logger with mocks
    with mock.patch('src.backend.utils.logging_utils.get_logger', return_value=mock_logger):
        with mock.patch('src.backend.utils.logging_utils.ContextAdapter', return_value=mock_adapter):
            component_logger = ComponentLogger(component_name)
            
            # Call log_completion
            operation = "test_operation"
            start_time = time.time() - 1.5  # 1.5 seconds ago
            details = {"result": "success", "items_processed": 100}
            component_logger.log_completion(operation, start_time, details)
            
            # Verify the adapter's info method was called
            mock_adapter.info.assert_called_once()
            
            # Verify the message contains the operation name and duration
            message = mock_adapter.info.call_args[0][0]
            assert f"Completed {operation}" in message
            assert "seconds" in message
            
            # Verify duration is approximately correct
            import re
            time_pattern = r"in (\d+\.\d+) seconds"
            match = re.search(time_pattern, message)
            if match:
                logged_time = float(match.group(1))
                assert 1.4 <= logged_time <= 1.6  # Allow for small timing variations


def test_component_logger_log_failure():
    """Test that ComponentLogger.log_failure correctly logs the failure of an operation."""
    component_name = "test_component"
    
    # Create a mock underlying logger
    mock_logger = mock.MagicMock()
    
    # Create a mock adapter
    mock_adapter = mock.MagicMock()
    
    # Create the component logger with mocks
    with mock.patch('src.backend.utils.logging_utils.get_logger', return_value=mock_logger):
        with mock.patch('src.backend.utils.logging_utils.ContextAdapter', return_value=mock_adapter):
            component_logger = ComponentLogger(component_name)
            
            # Call log_failure
            operation = "test_operation"
            start_time = time.time() - 2.5  # 2.5 seconds ago
            error = ValueError("Test error message")
            details = {"attempt": 3, "max_retries": 5}
            component_logger.log_failure(operation, start_time, error, details)
            
            # Verify the adapter's error method was called
            mock_adapter.error.assert_called_once()
            
            # Verify the message contains the operation name, duration, and error
            message = mock_adapter.error.call_args[0][0]
            assert f"Failed {operation}" in message
            assert "seconds" in message
            assert "Test error message" in message
            
            # Verify duration is approximately correct
            import re
            time_pattern = r"after (\d+\.\d+) seconds"
            match = re.search(time_pattern, message)
            if match:
                logged_time = float(match.group(1))
                assert 2.4 <= logged_time <= 2.6  # Allow for small timing variations


@pytest.mark.parametrize('event_type', ['received', 'processed', 'validated', 'stored'])
def test_component_logger_log_data_event(event_type):
    """Test that ComponentLogger.log_data_event correctly logs data-related events."""
    component_name = "test_component"
    
    # Create a mock underlying logger
    mock_logger = mock.MagicMock()
    
    # Create a mock adapter
    mock_adapter = mock.MagicMock()
    
    # Create the component logger with mocks
    with mock.patch('src.backend.utils.logging_utils.get_logger', return_value=mock_logger):
        with mock.patch('src.backend.utils.logging_utils.ContextAdapter', return_value=mock_adapter):
            component_logger = ComponentLogger(component_name)
            
            # Test with different data types
            
            # List data
            list_data = [1, 2, 3, 4, 5]
            details = {"source": "api", "format": "json"}
            component_logger.log_data_event(event_type, list_data, details)
            
            # Verify the adapter's info method was called
            mock_adapter.info.assert_called_once()
            
            # Verify the message contains the event type
            message = mock_adapter.info.call_args[0][0]
            assert f"Data event: {event_type}" in message
            
            # Reset mock for next test
            mock_adapter.reset_mock()
            
            # Dictionary data
            dict_data = {"a": 1, "b": 2, "c": 3}
            component_logger.log_data_event(event_type, dict_data, details)
            
            # Verify call happened and contains event type
            mock_adapter.info.assert_called_once()
            assert f"Data event: {event_type}" in mock_adapter.info.call_args[0][0]
            
            # Reset mock for next test
            mock_adapter.reset_mock()
            
            # Mock pandas DataFrame
            class MockDataFrame:
                shape = (10, 5)
                
                def __init__(self):
                    pass
            
            df_data = MockDataFrame()
            component_logger.log_data_event(event_type, df_data, details)
            
            # Verify call happened and contains event type
            mock_adapter.info.assert_called_once()
            assert f"Data event: {event_type}" in mock_adapter.info.call_args[0][0]


@pytest.mark.parametrize('is_valid', [True, False])
def test_component_logger_log_validation(is_valid):
    """Test that ComponentLogger.log_validation correctly logs validation results."""
    component_name = "test_component"
    
    # Create a mock underlying logger
    mock_logger = mock.MagicMock()
    
    # Create a mock adapter
    mock_adapter = mock.MagicMock()
    
    # Create the component logger with mocks
    with mock.patch('src.backend.utils.logging_utils.get_logger', return_value=mock_logger):
        with mock.patch('src.backend.utils.logging_utils.ContextAdapter', return_value=mock_adapter):
            component_logger = ComponentLogger(component_name)
            
            # Call log_validation
            validation_type = "schema_validation"
            errors = ["Field 'x' is missing", "Field 'y' has invalid type"] if not is_valid else None
            details = {"schema": "forecast_schema", "records": 100}
            component_logger.log_validation(validation_type, is_valid, errors, details)
            
            # Verify the correct adapter method was called
            if is_valid:
                mock_adapter.info.assert_called_once()
                assert "Validation passed" in mock_adapter.info.call_args[0][0]
            else:
                mock_adapter.warning.assert_called_once()
                assert "Validation failed" in mock_adapter.warning.call_args[0][0]
                assert "2 errors" in mock_adapter.warning.call_args[0][0]


def test_integration_with_logging_config():
    """Test the integration between logging_utils and logging_config."""
    # Mock setup_logging to prevent actual setup
    with mock.patch('src.backend.config.logging_config.setup_logging') as mock_setup:
        # Test that get_logger triggers setup_logging when no handlers are configured
        with mock.patch('logging.root.handlers', []):
            logger = get_logger("test_integration")
            mock_setup.assert_called_once()
        
        # Reset mock
        mock_setup.reset_mock()
        
        # Test that configure_component_logger uses LOG_DIR from logging_config
        with mock.patch('os.path.join', return_value="/mock/path/test_component.log") as mock_join:
            configure_component_logger("test_component", "INFO")
            # Verify that os.path.join was called with LOG_DIR
            mock_join.assert_any_call(LOG_DIR, "test_component.log")