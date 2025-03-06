# __init__.py
"""Initialization module for the API package of the Electricity Market Price Forecasting System.
Exposes key API components including routes, forecast API functions, health check utilities, and custom exceptions.
This module serves as the entry point for the API subsystem, making essential components available to the main application.
"""

# Version information
__version__ = "0.1.0"  # package_version: 0.1.0

# Internal imports
from .routes import api_blueprint  # Flask Blueprint for API routes
from .forecast_api import (
    get_forecast_by_date,  # Retrieve forecast for specific date and product
    get_latest_forecast,  # Retrieve latest forecast for a product
    get_forecasts_by_date_range,  # Retrieve forecasts within a date range
    get_forecast_as_model,  # Retrieve forecast as ProbabilisticForecast objects
    get_latest_forecast_as_model,  # Retrieve latest forecast as ProbabilisticForecast objects
    get_forecast_ensemble,  # Retrieve a forecast ensemble for a date range
    format_forecast_response,  # Format forecast response in requested format
    get_storage_status,  # Get information about forecast storage system
    SUPPORTED_FORMATS,  # List of supported output formats for forecasts
    ForecastAPI  # Class that encapsulates forecast API functionality
)
from .health_check import (
    SystemHealthCheck,  # System health check functionality
    get_health_status,  # Simple health status response for quick checks
    check_system_health  # Comprehensive health check of all system components
)
from .exceptions import (
    APIError,  # Base exception for API-related errors
    RequestValidationError,  # Exception for request validation failures
    ResourceNotFoundError,  # Exception for resource not found errors
    ForecastRetrievalError,  # Exception for forecast retrieval failures
    InvalidFormatError,  # Exception for invalid format requests
    AuthorizationError,  # Exception for authorization failures
    RateLimitExceededError  # Exception for rate limit exceeded errors
)
from ..utils.logging_utils import get_logger  # Get a configured logger for this module

# Initialize logger
logger = get_logger(__name__)

# Global variables
__all__ = [
    "api_blueprint",
    "get_forecast_by_date",
    "get_latest_forecast",
    "get_forecasts_by_date_range",
    "get_forecast_as_model",
    "get_latest_forecast_as_model",
    "get_forecast_ensemble",
    "format_forecast_response",
    "get_storage_status",
    "SUPPORTED_FORMATS",
    "ForecastAPI",
    "SystemHealthCheck",
    "get_health_status",
    "check_system_health",
    "APIError",
    "RequestValidationError",
    "ResourceNotFoundError",
    "ForecastRetrievalError",
    "InvalidFormatError",
    "AuthorizationError",
    "RateLimitExceededError"
]