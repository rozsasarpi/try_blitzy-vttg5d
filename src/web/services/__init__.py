"""
Initialization module for the services package in the Electricity Market Price Forecasting System's web visualization interface.

This module imports and exposes key service classes and functions for authentication, API interaction, and error reporting. 
It provides singleton instances of core services to ensure consistent access across the application.
"""

# Import service classes and functions
from .authentication import AuthenticationService
from .api_service import APIService, check_api_health
from .error_reporting import ErrorReportingService, ERROR_SEVERITY_LEVELS

# Create singleton instances for application-wide use
auth_service = AuthenticationService()
api_service = APIService()
error_service = ErrorReportingService()

# Export all required components
__all__ = [
    'AuthenticationService',
    'APIService',
    'ErrorReportingService',
    'auth_service',
    'api_service',
    'error_service',
    'check_api_health',
    'ERROR_SEVERITY_LEVELS',
]