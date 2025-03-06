"""
Test module for the utility decorators in the Electricity Market Price Forecasting System.
Contains comprehensive unit tests for all decorators including timing, retry, validation,
exception handling, and performance monitoring decorators, ensuring they function correctly
in various scenarios.
"""

import pytest
import unittest.mock
import time
import logging
import warnings
import pandas as pd

# Internal imports
from ../../utils.decorators import (
    timing_decorator,
    retry,
    validate_input,
    validate_output,
    log_exceptions,
    fallback_on_exception,
    deprecated,
    memoize,
    PerformanceMonitor,
    ValidationDecorator
)
from ../../models.validation_models import ValidationResult
from ../../fallback.fallback_retriever import retrieve_fallback_forecast


class MockValidator:
    """Mock validator class for testing validation decorators."""
    
    def __init__(self, return_valid=True, errors=None):
        """Initializes the mock validator with configurable validation result.
        
        Args:
            return_valid: Whether the validator should return a valid result
            errors: Dictionary of validation errors to return if not valid
        """
        self.return_valid = return_valid
        self.errors = errors or {}
        self.call_args = []
    
    def __call__(self, *args, **kwargs):
        """Mock validation function that returns configured ValidationResult.
        
        Args:
            *args: Positional arguments to record
            **kwargs: Keyword arguments to record
            
        Returns:
            ValidationResult with configured validity and errors
        """
        # Record the call arguments for verification
        self.call_args.append((args, kwargs))
        
        return ValidationResult(
            is_valid=self.return_valid,
            errors=self.errors if not self.return_valid else None
        )


class CounterFunction:
    """Test utility class that counts the number of times it's called."""
    
    def __init__(self, return_values=None):
        """Initializes the counter function with configurable return values.
        
        Args:
            return_values: List of values to return on successive calls
        """
        self.call_count = 0
        self.call_args = []
        self.return_values = return_values or [None]
    
    def __call__(self, *args, **kwargs):
        """Callable that tracks calls and returns configured values.
        
        Args:
            *args: Positional arguments to record
            **kwargs: Keyword arguments to record
            
        Returns:
            Configured return value for this call
        """
        self.call_count += 1
        self.call_args.append((args, kwargs))
        
        # Return the appropriate value based on call count
        if self.call_count <= len(self.return_values):
            return self.return_values[self.call_count - 1]
        return self.return_values[-1]


class FailingFunction:
    """Test utility class that raises exceptions a configurable number of times."""
    
    def __init__(self, fail_count, exception, return_value=None):
        """Initializes the failing function with failure configuration.
        
        Args:
            fail_count: Number of times to fail before succeeding
            exception: Exception to raise during failures
            return_value: Value to return after failures
        """
        self.call_count = 0
        self.fail_count = fail_count
        self.exception = exception
        self.return_value = return_value
    
    def __call__(self, *args, **kwargs):
        """Callable that fails a specified number of times then succeeds.
        
        Args:
            *args: Positional arguments (ignored)
            **kwargs: Keyword arguments (ignored)
            
        Returns:
            Configured return value after failures
            
        Raises:
            The configured exception during failure phase
        """
        self.call_count += 1
        
        if self.call_count <= self.fail_count:
            raise self.exception
        
        return self.return_value


def test_timing_decorator():
    """Tests that the timing_decorator correctly measures and logs function execution time."""
    # Create a mock logger
    mock_logger = unittest.mock.Mock()
    with unittest.mock.patch('src.backend.utils.decorators.logger', mock_logger):
        # Define a test function with a known execution time
        @timing_decorator
        def test_function():
            time.sleep(0.1)  # Sleep for a predictable amount of time
            return "test_result"
        
        # Execute the decorated function
        result = test_function()
        
        # Verify that the function returns the expected result
        assert result == "test_result"
        
        # Assert that the logger was called with the execution time information
        mock_logger.info.assert_called_once()
        log_message = mock_logger.info.call_args[0][0]
        assert "test_function executed in" in log_message
        
        # Verify that the logged time is close to the expected execution time
        execution_time = float(log_message.split(" in ")[1].split(" seconds")[0])
        assert 0.05 < execution_time < 0.2


def test_retry_success_first_attempt():
    """Tests that the retry decorator returns immediately on successful execution."""
    # Define a mock function that succeeds on first attempt
    mock_func = unittest.mock.Mock(return_value="success")
    
    # Apply the retry decorator to the function
    decorated_func = retry(max_retries=3)(mock_func)
    
    # Execute the decorated function
    result = decorated_func()
    
    # Verify that the function was called exactly once
    assert mock_func.call_count == 1
    
    # Verify that the function returns the expected result
    assert result == "success"


def test_retry_success_after_failures():
    """Tests that the retry decorator retries after failures and eventually succeeds."""
    # Create a mock function that fails twice then succeeds
    failing_func = FailingFunction(2, ValueError("Test error"), "success")
    
    # Apply the retry decorator with appropriate retry settings
    decorated_func = retry(
        max_retries=3,
        initial_delay=0.01,  # Short delay for testing
        exceptions_to_retry=(ValueError,)
    )(failing_func)
    
    # Execute the decorated function
    result = decorated_func()
    
    # Verify that the function was called exactly three times
    assert failing_func.call_count == 3
    
    # Verify that the function returns the expected result
    assert result == "success"


def test_retry_max_retries_exceeded():
    """Tests that the retry decorator raises the last exception when max retries are exceeded."""
    # Create a mock function that always raises an exception
    test_exception = ValueError("Test error")
    failing_func = FailingFunction(5, test_exception)
    
    # Apply the retry decorator with a specific number of retries
    decorated_func = retry(
        max_retries=3,
        initial_delay=0.01,  # Short delay for testing
        exceptions_to_retry=(ValueError,)
    )(failing_func)
    
    # Execute the decorated function and expect an exception
    with pytest.raises(ValueError) as exc_info:
        decorated_func()
    
    # Verify that the function was called the expected number of times
    assert failing_func.call_count == 4  # Initial call + 3 retries
    
    # Verify that the raised exception is of the expected type
    assert exc_info.value.args[0] == test_exception.args[0]


def test_validate_input_success():
    """Tests that the validate_input decorator allows execution when validation passes."""
    # Create a validator function that returns a valid ValidationResult
    validator = MockValidator(return_valid=True)
    
    # Define a test function that takes parameters
    @validate_input([validator])
    def test_function(arg1, arg2=None):
        return f"{arg1}-{arg2}"
    
    # Execute the decorated function with valid inputs
    result = test_function("test", arg2="value")
    
    # Verify that the function returns the expected result
    assert result == "test-value"
    
    # Verify that the validator was called with the correct arguments
    assert validator.call_args[0][0] == ("test",)
    assert validator.call_args[0][1] == {"arg2": "value"}


def test_validate_input_failure():
    """Tests that the validate_input decorator raises ValueError when validation fails."""
    # Create a validator function that returns an invalid ValidationResult with errors
    error_dict = {"validation": ["Test validation error"]}
    validator = MockValidator(return_valid=False, errors=error_dict)
    
    # Define a test function that takes parameters
    @validate_input([validator])
    def test_function(arg1, arg2=None):
        return f"{arg1}-{arg2}"
    
    # Execute the decorated function and expect ValueError
    with pytest.raises(ValueError) as exc_info:
        test_function("test", arg2="value")
    
    # Verify that the validator was called with the correct arguments
    assert validator.call_args[0][0] == ("test",)
    assert validator.call_args[0][1] == {"arg2": "value"}
    
    # Verify that the error message contains the validation errors
    assert "validation" in str(exc_info.value)


def test_validate_output_success():
    """Tests that the validate_output decorator allows results when validation passes."""
    # Create a validator function that returns a valid ValidationResult
    validator = MockValidator(return_valid=True)
    
    # Define a test function that returns a specific result
    @validate_output([validator])
    def test_function():
        return "test_result"
    
    # Execute the decorated function
    result = test_function()
    
    # Verify that the function returns the expected result
    assert result == "test_result"
    
    # Verify that the validator was called with the function result
    assert validator.call_args[0][0] == "test_result"


def test_validate_output_failure():
    """Tests that the validate_output decorator raises ValueError when validation fails."""
    # Create a validator function that returns an invalid ValidationResult with errors
    error_dict = {"validation": ["Test validation error"]}
    validator = MockValidator(return_valid=False, errors=error_dict)
    
    # Define a test function that returns a specific result
    @validate_output([validator])
    def test_function():
        return "test_result"
    
    # Execute the decorated function and expect ValueError
    with pytest.raises(ValueError) as exc_info:
        test_function()
    
    # Verify that the validator was called with the function result
    assert validator.call_args[0][0] == "test_result"
    
    # Verify that the error message contains the validation errors
    assert "validation" in str(exc_info.value)


def test_log_exceptions():
    """Tests that the log_exceptions decorator logs exceptions before re-raising them."""
    # Create a mock logger
    mock_logger = unittest.mock.Mock()
    with unittest.mock.patch('src.backend.utils.decorators.logger', mock_logger):
        # Define a test function that raises a specific exception
        @log_exceptions
        def test_function():
            raise ValueError("Test error")
        
        # Execute the decorated function and catch the exception
        with pytest.raises(ValueError) as exc_info:
            test_function()
        
        # Verify that the exception is of the expected type
        assert exc_info.value.args[0] == "Test error"
        
        # Assert that the logger was called with the exception details
        mock_logger.error.assert_called_once()
        log_message = mock_logger.error.call_args[0][0]
        assert "Exception in test_function" in log_message
        assert "Test error" in log_message
        
        # Verify that the original exception is preserved
        assert str(exc_info.value) == "Test error"


def test_fallback_on_exception_primary_success():
    """Tests that the fallback_on_exception decorator uses primary function when it succeeds."""
    # Create a mock primary function that succeeds
    primary_func = unittest.mock.Mock(return_value="primary_result")
    
    # Create a mock fallback function
    fallback_func = unittest.mock.Mock(return_value="fallback_result")
    
    # Apply the fallback_on_exception decorator to the primary function
    decorated_func = fallback_on_exception(fallback_func)(primary_func)
    
    # Execute the decorated function
    result = decorated_func("arg1", arg2="value")
    
    # Verify that the primary function was called once
    primary_func.assert_called_once_with("arg1", arg2="value")
    
    # Verify that the fallback function was not called
    fallback_func.assert_not_called()
    
    # Verify that the function returns the primary function's result
    assert result == "primary_result"


def test_fallback_on_exception_primary_fails():
    """Tests that the fallback_on_exception decorator uses fallback when primary function fails."""
    # Create a mock primary function that raises an exception
    primary_func = unittest.mock.Mock(side_effect=ValueError("Test error"))
    
    # Create a mock fallback function that succeeds
    fallback_func = unittest.mock.Mock(return_value="fallback_result")
    
    # Apply the fallback_on_exception decorator to the primary function
    decorated_func = fallback_on_exception(
        fallback_func,
        exceptions_to_catch=(ValueError,)
    )(primary_func)
    
    # Execute the decorated function
    result = decorated_func("arg1", arg2="value")
    
    # Verify that the primary function was called once
    primary_func.assert_called_once_with("arg1", arg2="value")
    
    # Verify that the fallback function was called once with the same arguments
    fallback_func.assert_called_once_with("arg1", arg2="value")
    
    # Verify that the function returns the fallback function's result
    assert result == "fallback_result"


def test_deprecated():
    """Tests that the deprecated decorator issues a warning when the decorated function is called."""
    # Define a test function
    @deprecated("This function is deprecated; use new_function instead")
    def old_function():
        return "result"
    
    # Use pytest's warning recorder to capture warnings
    with pytest.warns(DeprecationWarning) as warning_records:
        result = old_function()
    
    # Verify that the function returns the expected result
    assert result == "result"
    
    # Verify that a DeprecationWarning was issued
    assert len(warning_records) == 1
    assert "old_function is deprecated" in str(warning_records[0].message)
    assert "use new_function instead" in str(warning_records[0].message)


def test_memoize():
    """Tests that the memoize decorator caches function results based on input arguments."""
    # Create a mock function that returns different values on each call
    mock_func = CounterFunction(["first_call", "second_call", "third_call"])
    
    # Apply the memoize decorator to the function
    memoized_func = memoize(mock_func)
    
    # Call the decorated function multiple times with the same arguments
    result1 = memoized_func("arg1", arg2="value")
    result2 = memoized_func("arg1", arg2="value")
    
    # Verify that the underlying function was called only once
    assert mock_func.call_count == 1
    
    # Verify that all calls return the same result
    assert result1 == "first_call"
    assert result2 == "first_call"
    
    # Call the function with different arguments
    result3 = memoized_func("arg2", arg2="value")
    
    # Verify that the underlying function was called again
    assert mock_func.call_count == 2
    
    # Verify that the result is different for different arguments
    assert result3 == "second_call"


def test_performance_monitor_metrics():
    """Tests that the PerformanceMonitor class correctly tracks execution metrics."""
    # Define a test function with a known execution time
    def test_function():
        time.sleep(0.1)  # Sleep for a predictable amount of time
        return "result"
    
    # Create a PerformanceMonitor instance for the function
    monitor = PerformanceMonitor(test_function)
    
    # Call the monitored function multiple times
    for _ in range(3):
        result = monitor()
        assert result == "result"
    
    # Get the performance metrics
    metrics = monitor.get_metrics()
    
    # Verify that the execution count matches the number of calls
    assert metrics["count"] == 3
    
    # Verify that the total time is approximately the expected time
    assert 0.25 < metrics["total_time"] < 0.4
    
    # Verify that min and max times are within expected ranges
    assert 0.05 < metrics["min_time"] < 0.15
    assert 0.05 < metrics["max_time"] < 0.15
    
    # Verify that the average time is calculated correctly
    assert 0.08 < metrics["avg_time"] < 0.15


def test_performance_monitor_reset():
    """Tests that the PerformanceMonitor reset_metrics method correctly resets all metrics."""
    # Define a test function
    def test_function():
        return "result"
    
    # Create a PerformanceMonitor instance for the function
    monitor = PerformanceMonitor(test_function)
    
    # Call the monitored function multiple times
    for _ in range(3):
        monitor()
    
    # Verify that metrics show the expected execution count
    metrics_before = monitor.get_metrics()
    assert metrics_before["count"] == 3
    
    # Call reset_metrics method
    monitor.reset_metrics()
    
    # Verify that execution count is reset to 0
    metrics_after = monitor.get_metrics()
    assert metrics_after["count"] == 0
    
    # Verify that total time is reset to 0
    assert metrics_after["total_time"] == 0
    
    # Verify that min time is reset to None
    assert metrics_after["min_time"] is None
    
    # Verify that max time is reset to 0
    assert metrics_after["max_time"] == 0


def test_validation_decorator():
    """Tests that the ValidationDecorator class correctly validates both inputs and outputs."""
    # Create input validator that returns valid ValidationResult
    input_validator = MockValidator(return_valid=True)
    
    # Create output validator that returns valid ValidationResult
    output_validator = MockValidator(return_valid=True)
    
    # Create a ValidationDecorator with both validators
    validator = ValidationDecorator([input_validator], [output_validator])
    
    # Define a test function
    def test_function(arg):
        return f"processed_{arg}"
    
    # Apply the ValidationDecorator to the function
    decorated_func = validator(test_function)
    
    # Execute the decorated function
    result = decorated_func("input")
    
    # Verify that the function returns the expected result
    assert result == "processed_input"
    
    # Verify that both input and output validators were called
    assert input_validator.call_args[0][0] == ("input",)
    assert output_validator.call_args[0][0] == "processed_input"
    
    # Modify input validator to return invalid ValidationResult
    input_validator.return_valid = False
    input_validator.errors = {"validation": ["Invalid input"]}
    
    # Execute the decorated function and expect ValueError
    with pytest.raises(ValueError) as exc_info:
        decorated_func("invalid")
    
    # Verify that the error message contains input validation errors
    assert "validation" in str(exc_info.value)
    
    # Reset input validator to valid and make output validator invalid
    input_validator.return_valid = True
    input_validator.errors = {}
    output_validator.return_valid = False
    output_validator.errors = {"validation": ["Invalid output"]}
    
    # Execute the decorated function and expect ValueError
    with pytest.raises(ValueError) as exc_info:
        decorated_func("input")
    
    # Verify that the error message contains output validation errors
    assert "validation" in str(exc_info.value)