"""
Custom exception classes for the fallback mechanism of the Electricity Market Price Forecasting System.

This module defines a hierarchy of specialized exceptions to handle various error scenarios
that can occur during fallback operations, including error detection, fallback retrieval,
timestamp adjustment, and fallback logging.
"""

import typing
import datetime
from ..utils.logging_utils import get_logger

# Set up logger for this module
logger = get_logger(__name__)


class FallbackError(Exception):
    """Base exception class for all fallback mechanism-related errors."""
    
    def __init__(self, message: str):
        """
        Initialize the base fallback error.
        
        Args:
            message: Descriptive error message
        """
        super().__init__(message)
        self.message = message
        logger.error(message)
    
    def __str__(self) -> str:
        """
        String representation of the error.
        
        Returns:
            Error message
        """
        return self.message


class ErrorDetectionFailure(FallbackError):
    """Exception raised when the fallback mechanism fails to detect or categorize an error."""
    
    def __init__(self, message: str, original_error: Exception, component: str):
        """
        Initialize error detection failure with context.
        
        Args:
            message: Base error message
            original_error: The original exception that couldn't be properly detected
            component: The component where error detection failed
        """
        detailed_message = f"{message} in component '{component}': {str(original_error)}"
        super().__init__(detailed_message)
        self.original_error = original_error
        self.component = component
        logger.error(f"Error detection failed in component '{component}': {str(original_error)}")


class FallbackRetrievalError(FallbackError):
    """Exception raised when retrieving a previous forecast for fallback fails."""
    
    def __init__(self, message: str, product: str, target_date: datetime.datetime, 
                 original_error: typing.Optional[Exception] = None):
        """
        Initialize fallback retrieval error with context.
        
        Args:
            message: Base error message
            product: The price product for which retrieval failed
            target_date: The target date for which fallback was being retrieved
            original_error: The original exception that caused retrieval failure, if any
        """
        detailed_message = f"{message} for product '{product}' on {target_date.strftime('%Y-%m-%d')}"
        if original_error:
            detailed_message += f": {str(original_error)}"
        
        super().__init__(detailed_message)
        self.product = product
        self.target_date = target_date
        self.original_error = original_error
        
        log_details = f"Failed to retrieve fallback forecast for product '{product}' on {target_date.strftime('%Y-%m-%d')}"
        if original_error:
            log_details += f": {str(original_error)}"
        logger.error(log_details)


class TimestampAdjustmentError(FallbackError):
    """Exception raised when adjusting timestamps in a fallback forecast fails."""
    
    def __init__(self, message: str, product: str, source_date: datetime.datetime, 
                 target_date: datetime.datetime, original_error: typing.Optional[Exception] = None):
        """
        Initialize timestamp adjustment error with context.
        
        Args:
            message: Base error message
            product: The price product being adjusted
            source_date: The original date of the fallback forecast
            target_date: The target date to adjust to
            original_error: The original exception that caused adjustment failure, if any
        """
        detailed_message = (
            f"{message} for product '{product}' from {source_date.strftime('%Y-%m-%d')} "
            f"to {target_date.strftime('%Y-%m-%d')}"
        )
        if original_error:
            detailed_message += f": {str(original_error)}"
        
        super().__init__(detailed_message)
        self.product = product
        self.source_date = source_date
        self.target_date = target_date
        self.original_error = original_error
        
        log_details = (
            f"Failed to adjust timestamps for product '{product}' from {source_date.strftime('%Y-%m-%d')} "
            f"to {target_date.strftime('%Y-%m-%d')}"
        )
        if original_error:
            log_details += f": {str(original_error)}"
        logger.error(log_details)


class FallbackLoggingError(FallbackError):
    """Exception raised when logging fallback operations fails."""
    
    def __init__(self, message: str, operation: str, original_error: typing.Optional[Exception] = None):
        """
        Initialize fallback logging error with context.
        
        Args:
            message: Base error message
            operation: The fallback operation that was being logged
            original_error: The original exception that caused logging failure, if any
        """
        detailed_message = f"{message} for operation '{operation}'"
        if original_error:
            detailed_message += f": {str(original_error)}"
        
        super().__init__(detailed_message)
        self.operation = operation
        self.original_error = original_error
        
        log_details = f"Failed to log fallback operation '{operation}'"
        if original_error:
            log_details += f": {str(original_error)}"
        logger.error(log_details)


class NoFallbackAvailableError(FallbackError):
    """Exception raised when no suitable fallback forecast is available."""
    
    def __init__(self, message: str, product: str, target_date: datetime.datetime, search_days: int):
        """
        Initialize no fallback available error with context.
        
        Args:
            message: Base error message
            product: The price product for which no fallback is available
            target_date: The target date for which fallback was being sought
            search_days: The number of days searched for a suitable fallback
        """
        detailed_message = (
            f"{message} for product '{product}' on {target_date.strftime('%Y-%m-%d')} "
            f"after searching {search_days} days"
        )
        
        super().__init__(detailed_message)
        self.product = product
        self.target_date = target_date
        self.search_days = search_days
        
        logger.error(
            f"No fallback forecast available for product '{product}' on {target_date.strftime('%Y-%m-%d')} "
            f"after searching {search_days} days"
        )


class FallbackActivationError(FallbackError):
    """Exception raised when the fallback mechanism fails to activate."""
    
    def __init__(self, message: str, component: str, reason: str, 
                 original_error: typing.Optional[Exception] = None):
        """
        Initialize fallback activation error with context.
        
        Args:
            message: Base error message
            component: The component where fallback activation failed
            reason: The reason for activation failure
            original_error: The original exception that caused activation failure, if any
        """
        detailed_message = f"{message} in component '{component}' due to: {reason}"
        if original_error:
            detailed_message += f" - Original error: {str(original_error)}"
        
        super().__init__(detailed_message)
        self.component = component
        self.reason = reason
        self.original_error = original_error
        
        log_details = f"Failed to activate fallback in component '{component}' due to: {reason}"
        if original_error:
            log_details += f" - Original error: {str(original_error)}"
        logger.error(log_details)


class FallbackDataValidationError(FallbackError):
    """Exception raised when a fallback forecast fails validation checks."""
    
    def __init__(self, message: str, product: str, validation_errors: typing.List[str]):
        """
        Initialize fallback data validation error with context.
        
        Args:
            message: Base error message
            product: The price product for which validation failed
            validation_errors: List of specific validation error messages
        """
        detailed_message = f"{message} for product '{product}' with {len(validation_errors)} validation errors"
        
        super().__init__(detailed_message)
        self.product = product
        self.validation_errors = validation_errors
        
        error_details = "\n  - ".join([""] + validation_errors)
        logger.error(f"Fallback data validation failed for product '{product}' with errors:{error_details}")