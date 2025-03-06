"""
Utility module providing function decorators for the Electricity Market Price Forecasting System.
Implements various decorators for timing, retrying, validation, exception handling, and performance 
monitoring, following the functional programming approach specified in the requirements.
"""

import functools
import time
import typing
import logging
import warnings
import inspect
from datetime import datetime

# Internal imports
from .logging_utils import get_logger, format_exception
from ..models.validation_models import ValidationResult
from ..fallback.fallback_retriever import retrieve_fallback_forecast

# Initialize logger
logger = get_logger(__name__)


def timing_decorator(func):
    """
    Decorator that measures and logs the execution time of a function.
    
    Args:
        func: The function to be decorated
        
    Returns:
        Wrapped function that measures execution time
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"{func.__name__} executed in {end_time - start_time:.3f} seconds")
        return result
    return wrapper


def retry(max_retries=3, initial_delay=1.0, backoff_factor=2.0, exceptions_to_retry=(Exception,)):
    """
    Decorator that retries a function execution on failure with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        backoff_factor: Factor by which the delay increases with each retry
        exceptions_to_retry: Tuple of exception types to retry on
        
    Returns:
        Decorator function that applies retry logic
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay
            
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions_to_retry as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Maximum retries ({max_retries}) exceeded for {func.__name__}: {str(e)}")
                        raise
                    
                    logger.warning(
                        f"Retry {retries}/{max_retries} for {func.__name__} after error: {str(e)}. "
                        f"Waiting {delay:.2f} seconds."
                    )
                    time.sleep(delay)
                    delay *= backoff_factor
        
        return wrapper
    return decorator


def validate_input(validators):
    """
    Decorator that validates function inputs using provided validator functions.
    
    Args:
        validators: List of validator functions that take the function arguments
                   and return ValidationResult objects
        
    Returns:
        Decorator function that applies input validation
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            validation_results = []
            
            # Apply each validator
            for validator in validators:
                result = validator(*args, **kwargs)
                if not isinstance(result, ValidationResult):
                    raise TypeError(f"Validator {validator.__name__} did not return a ValidationResult")
                validation_results.append(result)
            
            # Check if any validation failed
            is_valid = all(result.is_valid for result in validation_results)
            if not is_valid:
                # Collect all error messages
                errors = {}
                for result in validation_results:
                    if not result.is_valid:
                        for category, messages in result.errors.items():
                            if category not in errors:
                                errors[category] = []
                            errors[category].extend(messages)
                
                error_msg = f"Input validation failed for {func.__name__}"
                logger.error(f"{error_msg}: {errors}")
                raise ValueError(error_msg, errors)
            
            # If validation passes, execute the function
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def validate_output(validators):
    """
    Decorator that validates function output using provided validator functions.
    
    Args:
        validators: List of validator functions that take the function result
                   and return ValidationResult objects
        
    Returns:
        Decorator function that applies output validation
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Execute the function first
            result = func(*args, **kwargs)
            
            validation_results = []
            
            # Apply each validator to the result
            for validator in validators:
                validation_result = validator(result)
                if not isinstance(validation_result, ValidationResult):
                    raise TypeError(f"Validator {validator.__name__} did not return a ValidationResult")
                validation_results.append(validation_result)
            
            # Check if any validation failed
            is_valid = all(result.is_valid for result in validation_results)
            if not is_valid:
                # Collect all error messages
                errors = {}
                for result in validation_results:
                    if not result.is_valid:
                        for category, messages in result.errors.items():
                            if category not in errors:
                                errors[category] = []
                            errors[category].extend(messages)
                
                error_msg = f"Output validation failed for {func.__name__}"
                logger.error(f"{error_msg}: {errors}")
                raise ValueError(error_msg, errors)
            
            # If validation passes, return the result
            return result
        
        return wrapper
    return decorator


def log_exceptions(func):
    """
    Decorator that logs exceptions raised by a function before re-raising them.
    
    Args:
        func: The function to be decorated
        
    Returns:
        Wrapped function that logs exceptions
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"Exception in {func.__name__}: {str(e)}\n"
                f"Details: {format_exception(e)}"
            )
            raise
    
    return wrapper


def fallback_on_exception(fallback_func, exceptions_to_catch=(Exception,)):
    """
    Decorator that executes a fallback function when the primary function raises an exception.
    
    Args:
        fallback_func: Function to call if the primary function fails
        exceptions_to_catch: Tuple of exception types to catch
        
    Returns:
        Decorator function that applies fallback logic
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions_to_catch as e:
                logger.warning(
                    f"Executing fallback for {func.__name__} after error: {str(e)}"
                )
                return fallback_func(*args, **kwargs)
        
        return wrapper
    return decorator


def deprecated(message="This function is deprecated and will be removed in a future version."):
    """
    Decorator that marks a function as deprecated and logs a warning when called.
    
    Args:
        message: Warning message to display
        
    Returns:
        Decorator function that marks functions as deprecated
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
                f"{func.__name__} is deprecated: {message}",
                category=DeprecationWarning,
                stacklevel=2
            )
            logger.warning(f"Deprecated function {func.__name__} was called: {message}")
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def memoize(func):
    """
    Decorator that caches function results based on input arguments.
    
    Args:
        func: The function to be decorated
        
    Returns:
        Wrapped function with caching
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Create a cache key from the arguments
        key = str(args) + str(sorted(kwargs.items()))
        
        # Check if we've cached this result
        if key not in wrapper.cache:
            wrapper.cache[key] = func(*args, **kwargs)
        
        return wrapper.cache[key]
    
    # Initialize the cache
    wrapper.cache = {}
    
    return wrapper


class PerformanceMonitor:
    """
    Class decorator that monitors and records performance metrics for a function.
    """
    
    def __init__(self, func):
        """
        Initializes the performance monitor for a function.
        
        Args:
            func: The function to be monitored
        """
        self._func = func
        functools.update_wrapper(self, func)
        
        # Initialize metrics dictionary
        self._metrics = {
            "count": 0,
            "total_time": 0,
            "min_time": None,
            "max_time": 0
        }
    
    def __call__(self, *args, **kwargs):
        """
        Executes the monitored function and records performance metrics.
        
        Args:
            *args: Positional arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            Result of the monitored function
        """
        start_time = time.time()
        result = self._func(*args, **kwargs)
        execution_time = time.time() - start_time
        
        # Update metrics
        self._metrics["count"] += 1
        self._metrics["total_time"] += execution_time
        
        if self._metrics["min_time"] is None or execution_time < self._metrics["min_time"]:
            self._metrics["min_time"] = execution_time
            
        if execution_time > self._metrics["max_time"]:
            self._metrics["max_time"] = execution_time
        
        return result
    
    def get_metrics(self):
        """
        Returns the current performance metrics.
        
        Returns:
            Dictionary of performance metrics
        """
        metrics = self._metrics.copy()
        
        # Calculate average execution time
        if metrics["count"] > 0:
            metrics["avg_time"] = metrics["total_time"] / metrics["count"]
        else:
            metrics["avg_time"] = 0
        
        return metrics
    
    def reset_metrics(self):
        """
        Resets all performance metrics to initial values.
        
        Returns:
            None: Function performs side effects only
        """
        self._metrics = {
            "count": 0,
            "total_time": 0,
            "min_time": None,
            "max_time": 0
        }


class ValidationDecorator:
    """
    Class decorator that validates both inputs and outputs of a function.
    """
    
    def __init__(self, input_validators, output_validators):
        """
        Initializes the validation decorator with input and output validators.
        
        Args:
            input_validators: List of validator functions for inputs
            output_validators: List of validator functions for outputs
        """
        self._input_validators = input_validators
        self._output_validators = output_validators
    
    def __call__(self, func):
        """
        Decorates a function with input and output validation.
        
        Args:
            func: The function to be decorated
            
        Returns:
            Decorated function with validation
        """
        self._func = func
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Validate inputs
            for validator in self._input_validators:
                result = validator(*args, **kwargs)
                if not result.is_valid:
                    error_msg = f"Input validation failed for {func.__name__}"
                    logger.error(f"{error_msg}: {result.errors}")
                    raise ValueError(error_msg, result.errors)
            
            # Execute the function
            result = func(*args, **kwargs)
            
            # Validate output
            for validator in self._output_validators:
                validation_result = validator(result)
                if not validation_result.is_valid:
                    error_msg = f"Output validation failed for {func.__name__}"
                    logger.error(f"{error_msg}: {validation_result.errors}")
                    raise ValueError(error_msg, validation_result.errors)
            
            return result
        
        return wrapper