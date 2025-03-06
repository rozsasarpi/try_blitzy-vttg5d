"""
Utility module providing logging functionality for the Electricity Market Price Forecasting System.

This module implements a structured logging approach with consistent formatting across all components,
supporting the monitoring and observability requirements. It provides utilities for contextual logging,
performance monitoring through execution time tracking, and standardized error reporting.
"""

import logging
import functools
import time
import typing
import traceback
import json

# Internal imports
from ..config.logging_config import (
    LOG_DIR,
    LOG_CONFIG_FILE,
    get_log_level,
    setup_logging
)

# Global variables
_loggers = {}  # Cache of logger instances

def get_logger(name: str) -> logging.Logger:
    """
    Gets or creates a logger with the specified name.
    
    If a logger with the given name already exists in the cache, it is returned.
    Otherwise, a new logger is created, configured, and cached for future use.
    
    Args:
        name: The name of the logger, typically the module name
        
    Returns:
        Configured logger instance
    """
    if name in _loggers:
        return _loggers[name]
    
    # Ensure logging is set up
    if not logging.root.handlers:
        setup_logging()
    
    logger = logging.getLogger(name)
    _loggers[name] = logger
    return logger

def log_execution_time(func: callable) -> callable:
    """
    Decorator that logs the execution time of a function.
    
    Args:
        func: The function to be decorated
        
    Returns:
        Wrapped function that logs execution time
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger = get_logger(func.__module__)
        
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            logger.info(f"{func.__name__} executed in {end_time - start_time:.3f} seconds")
            return result
        except Exception as e:
            end_time = time.time()
            logger.error(f"{func.__name__} failed after {end_time - start_time:.3f} seconds: {str(e)}")
            raise
    
    return wrapper

def log_method_execution_time(method: callable) -> callable:
    """
    Decorator that logs the execution time of a class method.
    
    Args:
        method: The class method to be decorated
        
    Returns:
        Wrapped method that logs execution time
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        start_time = time.time()
        logger = get_logger(self.__class__.__module__)
        
        try:
            result = method(self, *args, **kwargs)
            end_time = time.time()
            logger.info(f"{self.__class__.__name__}.{method.__name__} executed in {end_time - start_time:.3f} seconds")
            return result
        except Exception as e:
            end_time = time.time()
            logger.error(f"{self.__class__.__name__}.{method.__name__} failed after {end_time - start_time:.3f} seconds: {str(e)}")
            raise
    
    return wrapper

def format_exception(exc: Exception) -> str:
    """
    Formats an exception into a detailed string representation.
    
    Args:
        exc: The exception to format
        
    Returns:
        Formatted exception string
    """
    exc_type = type(exc).__name__
    exc_message = str(exc)
    exc_traceback = ''.join(traceback.format_tb(exc.__traceback__))
    
    return f"{exc_type}: {exc_message}\nTraceback:\n{exc_traceback}"

def format_dict_for_logging(data: dict) -> str:
    """
    Formats a dictionary for logging, handling non-serializable objects.
    
    Args:
        data: The dictionary to format
        
    Returns:
        JSON-formatted string representation of the dictionary
    """
    try:
        # Create a copy to avoid modifying the original
        serializable_data = {}
        
        # Process dictionary to handle non-serializable objects
        for key, value in data.items():
            if isinstance(value, dict):
                # Recursively process nested dictionaries
                try:
                    serializable_data[key] = json.loads(format_dict_for_logging(value))
                except:
                    serializable_data[key] = str(value)
            elif isinstance(value, (list, tuple)):
                # Process lists and tuples
                serializable_data[key] = []
                for item in value:
                    if isinstance(item, dict):
                        try:
                            serializable_data[key].append(json.loads(format_dict_for_logging(item)))
                        except:
                            serializable_data[key].append(str(item))
                    elif isinstance(item, (int, float, str, bool, type(None))):
                        serializable_data[key].append(item)
                    else:
                        # Convert non-serializable objects to string
                        serializable_data[key].append(str(item))
            elif isinstance(value, (int, float, str, bool, type(None))):
                # Pass through serializable types
                serializable_data[key] = value
            else:
                # Convert other types to string
                serializable_data[key] = str(value)
        
        return json.dumps(serializable_data)
    except Exception as e:
        # If serialization fails, return a simplified representation
        return f"{{\"data\": \"<non-serializable: {str(e)}>\" }}"

def configure_component_logger(component_name: str, log_level: str) -> logging.Logger:
    """
    Configures a logger for a specific component with appropriate handlers.
    
    Args:
        component_name: Name of the component requiring a logger
        log_level: Logging level as string (DEBUG, INFO, etc.)
        
    Returns:
        Configured component logger
    """
    logger = get_logger(component_name)
    level = get_log_level(log_level)
    logger.setLevel(level)
    
    # Add console handler if not already present
    has_console_handler = any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) 
                              for h in logger.handlers)
    if not has_console_handler:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Add file handler if not already present
    has_file_handler = any(isinstance(h, logging.FileHandler) for h in logger.handlers)
    if not has_file_handler:
        import os
        log_file = os.path.join(LOG_DIR, f"{component_name}.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Prevent propagation to avoid duplicate logs
    logger.propagate = False
    
    return logger

class ContextAdapter:
    """
    Adapter that adds contextual information to log messages.
    
    This adapter allows adding context to log messages, making it easier to 
    track related log entries and understand the state at the time of logging.
    """
    
    def __init__(self, logger: logging.Logger, context: dict = None):
        """
        Initializes the adapter with a logger and context.
        
        Args:
            logger: The logger to adapt
            context: Initial context dictionary
        """
        self.logger = logger
        self.context = context or {}
    
    def process(self, msg: str, kwargs: dict) -> tuple:
        """
        Processes the log record by adding context information.
        
        Args:
            msg: The log message
            kwargs: Additional keyword arguments for the logger
            
        Returns:
            Processed message and kwargs
        """
        if self.context:
            context_str = format_dict_for_logging(self.context)
            msg = f"{msg} [Context: {context_str}]"
        
        return msg, kwargs
    
    def with_context(self, additional_context: dict) -> 'ContextAdapter':
        """
        Creates a new adapter with additional context.
        
        Args:
            additional_context: Additional context to add
            
        Returns:
            New adapter with merged context
        """
        new_context = self.context.copy()
        new_context.update(additional_context)
        return ContextAdapter(self.logger, new_context)
    
    def debug(self, msg: str, *args, **kwargs) -> None:
        """
        Logs a message with DEBUG level.
        
        Args:
            msg: The message to log
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments
        """
        msg, kwargs = self.process(msg, kwargs)
        self.logger.debug(msg, *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs) -> None:
        """
        Logs a message with INFO level.
        
        Args:
            msg: The message to log
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments
        """
        msg, kwargs = self.process(msg, kwargs)
        self.logger.info(msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs) -> None:
        """
        Logs a message with WARNING level.
        
        Args:
            msg: The message to log
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments
        """
        msg, kwargs = self.process(msg, kwargs)
        self.logger.warning(msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs) -> None:
        """
        Logs a message with ERROR level.
        
        Args:
            msg: The message to log
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments
        """
        msg, kwargs = self.process(msg, kwargs)
        self.logger.error(msg, *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs) -> None:
        """
        Logs a message with CRITICAL level.
        
        Args:
            msg: The message to log
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments
        """
        msg, kwargs = self.process(msg, kwargs)
        self.logger.critical(msg, *args, **kwargs)
    
    def exception(self, msg: str, *args, **kwargs) -> None:
        """
        Logs an exception with ERROR level.
        
        Args:
            msg: The message to log
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments
        """
        msg, kwargs = self.process(msg, kwargs)
        self.logger.exception(msg, *args, **kwargs)

class ComponentLogger:
    """
    Logger class for specific system components with standardized formatting.
    
    This logger provides specialized methods for common logging patterns
    used by system components, such as operation start/completion,
    data events, and validation results.
    """
    
    def __init__(self, component_name: str, default_context: dict = None):
        """
        Initializes a component-specific logger.
        
        Args:
            component_name: Name of the component
            default_context: Default context for all log messages
        """
        self.component_name = component_name
        self.logger = get_logger(component_name)
        self.default_context = default_context or {'component': component_name}
        self.adapter = ContextAdapter(self.logger, self.default_context)
    
    def with_context(self, additional_context: dict) -> 'ComponentLogger':
        """
        Creates a new logger instance with additional context.
        
        Args:
            additional_context: Additional context to add
            
        Returns:
            New logger with merged context
        """
        new_logger = ComponentLogger(self.component_name, self.default_context.copy())
        new_logger.default_context.update(additional_context)
        new_logger.adapter = ContextAdapter(self.logger, new_logger.default_context)
        return new_logger
    
    def log_start(self, operation: str, details: dict = None) -> None:
        """
        Logs the start of an operation.
        
        Args:
            operation: Name of the operation
            details: Additional details about the operation
        """
        context = {
            'operation': operation,
            'status': 'started',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        }
        
        if details:
            context.update(details)
        
        self.adapter.info(f"Started {operation}")
    
    def log_completion(self, operation: str, start_time: float, details: dict = None) -> None:
        """
        Logs the successful completion of an operation.
        
        Args:
            operation: Name of the operation
            start_time: Start time of the operation (from time.time())
            details: Additional details about the operation
        """
        duration = time.time() - start_time
        
        context = {
            'operation': operation,
            'status': 'completed',
            'duration_seconds': f"{duration:.3f}",
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        }
        
        if details:
            context.update(details)
        
        self.adapter.info(f"Completed {operation} in {duration:.3f} seconds")
    
    def log_failure(self, operation: str, start_time: float, error: Exception, details: dict = None) -> None:
        """
        Logs the failure of an operation.
        
        Args:
            operation: Name of the operation
            start_time: Start time of the operation (from time.time())
            error: Exception that caused the failure
            details: Additional details about the operation
        """
        duration = time.time() - start_time
        error_details = format_exception(error)
        
        context = {
            'operation': operation,
            'status': 'failed',
            'duration_seconds': f"{duration:.3f}",
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'error': str(error),
            'error_details': error_details
        }
        
        if details:
            context.update(details)
        
        self.adapter.error(f"Failed {operation} after {duration:.3f} seconds: {str(error)}")
    
    def log_data_event(self, event_type: str, data: object, details: dict = None) -> None:
        """
        Logs a data-related event with data characteristics.
        
        Args:
            event_type: Type of data event (e.g., 'received', 'processed')
            data: The data object
            details: Additional details about the event
        """
        # Create data summary
        data_summary = {
            'data_type': type(data).__name__
        }
        
        # Add shape for pandas objects
        if hasattr(data, 'shape'):
            data_summary['shape'] = str(data.shape)
        
        # Add size information
        import sys
        try:
            data_summary['size_bytes'] = sys.getsizeof(data)
        except:
            pass
        
        context = {
            'event_type': event_type,
            'data_summary': data_summary,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        }
        
        if details:
            context.update(details)
        
        self.adapter.info(f"Data event: {event_type}")
    
    def log_validation(self, validation_type: str, is_valid: bool, errors: list = None, details: dict = None) -> None:
        """
        Logs the result of a data validation operation.
        
        Args:
            validation_type: Type of validation performed
            is_valid: Whether validation passed
            errors: List of validation errors if not valid
            details: Additional details about the validation
        """
        context = {
            'validation_type': validation_type,
            'is_valid': is_valid,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        }
        
        if not is_valid and errors:
            context['validation_errors'] = errors
        
        if details:
            context.update(details)
        
        if is_valid:
            self.adapter.info(f"Validation passed: {validation_type}")
        else:
            self.adapter.warning(f"Validation failed: {validation_type} with {len(errors or [])} errors")