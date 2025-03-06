"""
Custom exception classes for the API module of the Electricity Market Price Forecasting System.

Defines a hierarchy of exception types to handle various error scenarios in API operations,
including request validation, resource not found, forecast retrieval, and authorization errors.
"""

import typing
from datetime import datetime

# Internal imports
from ..utils.logging_utils import get_logger

# Create a logger for this module
logger = get_logger(__name__)

class APIError(Exception):
    """Base exception class for all API-related errors"""
    
    def __init__(self, message: str, status_code: int):
        """
        Initialize the base API error
        
        Args:
            message: Error message
            status_code: HTTP status code
        """
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        logger.error(f"API Error ({status_code}): {message}")
    
    def __str__(self) -> str:
        """
        String representation of the error
        
        Returns:
            Error message
        """
        return self.message

class RequestValidationError(APIError):
    """Exception raised when request validation fails"""
    
    def __init__(self, message: str, validation_errors: typing.Dict[str, typing.Any]):
        """
        Initialize request validation error with details
        
        Args:
            message: Error message
            validation_errors: Dictionary of validation errors
        """
        super().__init__(message, 400)
        self.validation_errors = validation_errors
        logger.warning(f"Request validation failed: {message}")
    
    def __str__(self) -> str:
        """
        String representation of the validation error
        
        Returns:
            Formatted error message with validation details
        """
        return f"{self.message} - Details: {self.validation_errors}"

class ResourceNotFoundError(APIError):
    """Exception raised when a requested resource cannot be found"""
    
    def __init__(self, message: str, resource_type: str, resource_id: typing.Any):
        """
        Initialize resource not found error with context
        
        Args:
            message: Error message
            resource_type: Type of resource that was not found
            resource_id: Identifier of the resource
        """
        super().__init__(message, 404)
        self.resource_type = resource_type
        self.resource_id = resource_id
        logger.warning(f"Resource not found: {resource_type} with ID {resource_id}")
    
    def __str__(self) -> str:
        """
        String representation of the not found error
        
        Returns:
            Formatted error message with resource details
        """
        return f"{self.message} - {self.resource_type} with ID: {self.resource_id} not found"

class ForecastRetrievalError(APIError):
    """Exception raised when forecast retrieval fails"""
    
    def __init__(self, message: str, product: str, 
                 date: typing.Optional[datetime] = None,
                 date_range: typing.Optional[typing.Tuple[datetime, datetime]] = None):
        """
        Initialize forecast retrieval error with context
        
        Args:
            message: Error message
            product: Forecast product (e.g., DALMP, RTLMP)
            date: Specific date for the forecast (optional)
            date_range: Date range for the forecast (optional)
        """
        super().__init__(message, 500)
        self.product = product
        self.date = date
        self.date_range = date_range
        
        # Build context for logging
        context = f"product={product}"
        if date:
            context += f", date={date.isoformat()}"
        if date_range:
            context += f", date_range={date_range[0].isoformat()} to {date_range[1].isoformat()}"
            
        logger.error(f"Forecast retrieval error: {message} ({context})")
    
    def __str__(self) -> str:
        """
        String representation of the forecast retrieval error
        
        Returns:
            Formatted error message with forecast details
        """
        details = f"product={self.product}"
        if self.date:
            details += f", date={self.date.isoformat()}"
        if self.date_range:
            details += f", date_range={self.date_range[0].isoformat()} to {self.date_range[1].isoformat()}"
            
        return f"{self.message} - Forecast details: {details}"

class InvalidFormatError(APIError):
    """Exception raised when an invalid format is requested"""
    
    def __init__(self, message: str, requested_format: str, supported_formats: typing.List[str]):
        """
        Initialize invalid format error with context
        
        Args:
            message: Error message
            requested_format: The format that was requested
            supported_formats: List of supported formats
        """
        super().__init__(message, 400)
        self.requested_format = requested_format
        self.supported_formats = supported_formats
        logger.warning(f"Invalid format requested: {requested_format} (Supported: {', '.join(supported_formats)})")
    
    def __str__(self) -> str:
        """
        String representation of the invalid format error
        
        Returns:
            Formatted error message with format details
        """
        return f"{self.message} - Requested format: {self.requested_format}, Supported formats: {', '.join(self.supported_formats)}"

class AuthorizationError(APIError):
    """Exception raised when authorization fails"""
    
    def __init__(self, message: str, required_permission: typing.Optional[str] = None):
        """
        Initialize authorization error with context
        
        Args:
            message: Error message
            required_permission: Permission required for the operation (optional)
        """
        super().__init__(message, 403)
        self.required_permission = required_permission
        
        if required_permission:
            logger.warning(f"Authorization error: {message} (Required permission: {required_permission})")
        else:
            logger.warning(f"Authorization error: {message}")
    
    def __str__(self) -> str:
        """
        String representation of the authorization error
        
        Returns:
            Formatted error message with permission details
        """
        if self.required_permission:
            return f"{self.message} - Required permission: {self.required_permission}"
        return self.message

class RateLimitExceededError(APIError):
    """Exception raised when rate limit is exceeded"""
    
    def __init__(self, message: str, limit: int, reset_seconds: int):
        """
        Initialize rate limit exceeded error with context
        
        Args:
            message: Error message
            limit: The rate limit that was exceeded
            reset_seconds: Seconds until the rate limit resets
        """
        super().__init__(message, 429)
        self.limit = limit
        self.reset_seconds = reset_seconds
        logger.warning(f"Rate limit exceeded: {message} (Limit: {limit}, Resets in: {reset_seconds} seconds)")
    
    def __str__(self) -> str:
        """
        String representation of the rate limit error
        
        Returns:
            Formatted error message with rate limit details
        """
        return f"{self.message} - Rate limit: {self.limit}, Resets in: {self.reset_seconds} seconds"