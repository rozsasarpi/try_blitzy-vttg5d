"""
Middleware module that provides error handling functionality for the Electricity Market Price
Forecasting System's Dash-based visualization interface. Implements centralized error handling,
callback error wrapping, and fallback status checking to ensure a robust user experience when errors occur.
"""

import functools  # standard library
import traceback  # standard library
from typing import Callable, Dict, Any, Optional  # standard library

import dash  # version 2.9.0+
from dash import html  # version 2.9.0+

from ..config.logging_config import get_logger
from ..config.settings import DEBUG, FALLBACK_INDICATOR_ENABLED
from ..utils.error_handlers import (
    handle_callback_error,
    create_error_message,
    is_fallback_data,
    create_fallback_notice
)
from ..services.error_reporting import ErrorReportingService
from ..layouts.error_page import create_error_layout

# Initialize module logger
logger = get_logger('error_middleware')

# Initialize the error reporting service
error_reporting_service = ErrorReportingService()

# Global error registry
ERROR_REGISTRY = {}


def wrap_callback_with_error_handler(callback_func: Callable) -> Callable:
    """
    Decorator that wraps Dash callbacks with error handling functionality.
    
    Args:
        callback_func: The callback function to wrap
        
    Returns:
        Wrapped callback function with error handling
    """
    @functools.wraps(callback_func)
    def wrapper(*args, **kwargs):
        try:
            # Execute the original callback function
            return callback_func(*args, **kwargs)
        except Exception as error:
            # Get the function name for context
            func_name = getattr(callback_func, '__name__', 'unknown_callback')
            
            # Log the error
            logger.error(f"Error in callback {func_name}: {str(error)}")
            logger.debug(f"Callback args: {args}, kwargs: {kwargs}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            
            # Handle the callback error
            error_info = handle_callback_error(error, func_name)
            
            # Report to error reporting service
            error_id = error_reporting_service.report_error(
                error, 
                f"Callback {func_name}",
                include_traceback=True
            )
            
            # Return appropriate error component
            error_details = traceback.format_exc() if DEBUG else None
            return create_error_message(
                message=str(error),
                error_type="callback_error",
                details=error_details,
                show_details=DEBUG
            )
    
    return wrapper


def register_error(error: Exception, context: str) -> str:
    """
    Registers an error in the global error registry.
    
    Args:
        error: The exception that occurred
        context: Additional context about where/when the error occurred
        
    Returns:
        Error ID for reference
    """
    # Generate error ID using error_reporting_service
    error_id = error_reporting_service.report_error(error, context)
    
    # Log the error registration
    logger.info(f"Registered error [{error_id}] in context: {context}")
    
    return error_id


def get_error_component(error_id: str, with_details: bool = False) -> html.Div:
    """
    Gets an error component for display based on error ID.
    
    Args:
        error_id: The unique identifier for the error
        with_details: Whether to include detailed error information
        
    Returns:
        Error component for display
    """
    # Retrieve error details from error reporting service
    error_details = error_reporting_service.get_error(error_id)
    
    if error_details:
        # Create error component with appropriate details
        error_type = error_details.get('type', 'unknown')
        error_message = error_details.get('message', 'An unknown error occurred')
        
        details = None
        if with_details and DEBUG:
            details = error_details.get('traceback', None)
        
        return create_error_message(
            message=error_message,
            error_type=error_type,
            details=details,
            show_details=with_details and DEBUG
        )
    else:
        # Error not found, create generic error component
        return create_error_message(
            message="The error information is no longer available",
            error_type="unknown",
            show_details=False
        )


class ErrorMiddleware:
    """
    Middleware class that provides error handling for Dash applications.
    """
    
    def __init__(self):
        """
        Initializes the error middleware.
        """
        self.app = None
        self.logger = get_logger('error_middleware')
        self.error_service = ErrorReportingService()
        self.logger.info("Initialized error middleware")
    
    def apply(self, app: dash.Dash) -> dash.Dash:
        """
        Applies error handling middleware to a Dash application.
        
        Args:
            app: The Dash application to apply error handling to
            
        Returns:
            Dash application with error handling applied
        """
        self.app = app
        
        # Register error handlers with the app
        self.patch_callbacks()
        
        self.logger.info("Applied error middleware to Dash application")
        return app
    
    def handle_error(self, error: Exception, context: str) -> html.Div:
        """
        Handles errors that occur in the application.
        
        Args:
            error: The exception that occurred
            context: Additional context about where the error occurred
            
        Returns:
            Error component for display
        """
        # Log the error
        self.logger.error(f"Error in {context}: {str(error)}")
        self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        # Report to error reporting service
        error_id = self.error_service.report_error(error, context)
        
        # Create error component
        error_details = traceback.format_exc() if DEBUG else None
        return create_error_message(
            message=str(error),
            error_type="unknown",
            details=error_details,
            show_details=DEBUG
        )
    
    def check_fallback_status(self, forecast_data: Dict) -> html.Div:
        """
        Checks if forecast data is from fallback mechanism and creates notice if needed.
        
        Args:
            forecast_data: The forecast data to check
            
        Returns:
            Fallback notice component or empty div
        """
        # Check if fallback indicators are enabled
        if not FALLBACK_INDICATOR_ENABLED:
            return html.Div()
        
        # Check if forecast data is None
        if forecast_data is None:
            return html.Div()
        
        # Check if using fallback data
        if is_fallback_data(forecast_data):
            self.logger.info("Using fallback forecast data")
            return create_fallback_notice()
        
        return html.Div()
    
    def patch_callbacks(self) -> None:
        """
        Patches the app.callback method to wrap all callbacks with error handling.
        """
        # Store original callback method
        original_callback = self.app.callback
        
        # Define new callback method that wraps the original
        @functools.wraps(original_callback)
        def patched_callback(*args, **kwargs):
            # Get the original callback function and wrap it
            callback_decorator = original_callback(*args, **kwargs)
            
            # Return a wrapper that will apply error handling to the callback function
            @functools.wraps(callback_decorator)
            def wrapped_decorator(func):
                # Apply our error handler wrapper to the callback function
                wrapped_func = wrap_callback_with_error_handler(func)
                # Apply the original callback decorator to the wrapped function
                return callback_decorator(wrapped_func)
            
            return wrapped_decorator
        
        # Replace app.callback with the patched version
        self.app.callback = patched_callback
        self.logger.info("Patched app.callback method with error handling")