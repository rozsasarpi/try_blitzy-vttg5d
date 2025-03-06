"""
Initialization module for the middleware package of the Electricity Market Price Forecasting System's
Dash-based visualization interface. This file exports the middleware classes and functions that handle
authentication, error handling, and request logging for the dashboard application.
"""

# Import middleware classes and utility functions
from .auth_middleware import (
    AuthMiddleware,
    requires_auth,
    is_protected_route
)

from .error_middleware import (
    ErrorMiddleware,
    wrap_callback_with_error_handler,
    register_error,
    get_error_component
)

from .logging_middleware import (
    LoggingMiddleware,
    log_request_time,
    get_metrics_summary
)

# Export all imported middleware components for easy access
__all__ = [
    # Authentication middleware
    'AuthMiddleware',
    'requires_auth',
    'is_protected_route',
    
    # Error handling middleware
    'ErrorMiddleware',
    'wrap_callback_with_error_handler',
    'register_error',
    'get_error_component',
    
    # Logging middleware
    'LoggingMiddleware',
    'log_request_time',
    'get_metrics_summary',
    
    # Convenience function
    'apply_middleware'
]


def apply_middleware(app, verbose=False):
    """
    Applies all middleware components to a Dash application
    
    Args:
        app (dash.Dash): The Dash application to apply middleware to
        verbose (bool): Whether to enable verbose logging
        
    Returns:
        dash.Dash: The Dash application with middleware applied
    """
    # Apply error middleware first to ensure error handling works for all components
    error_middleware = ErrorMiddleware()
    error_middleware.apply(app)
    
    # Apply authentication middleware
    auth_middleware = AuthMiddleware(app)
    auth_middleware.apply()
    
    # Apply logging middleware with verbose flag
    logging_middleware = LoggingMiddleware(verbose=verbose)
    logging_middleware.apply(app)
    
    return app