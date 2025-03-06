"""
Service module that provides centralized error reporting, tracking, and monitoring for the Electricity Market Price Forecasting System's web visualization interface. Implements error collection, storage, retrieval, and optional external reporting to ensure comprehensive error management and facilitate troubleshooting.
"""

import traceback
import datetime
import uuid
import json
from typing import Dict, Any, Optional, List

import requests  # version 2.28.0+

from ..config.logging_config import get_logger
from ..config.settings import DEBUG, ENVIRONMENT
from ..utils.error_handlers import format_exception, ERROR_TYPES

# Initialize logger
logger = get_logger('error_reporting')

# In-memory error registry - dictionary mapping error ID to error details
ERROR_REGISTRY = {}

# Maximum number of errors to keep in the registry
MAX_ERROR_HISTORY = 100

# External reporting configuration
EXTERNAL_REPORTING_ENABLED = False
EXTERNAL_REPORTING_URL = None


def format_error_details(error: Exception, context: str, include_traceback: bool) -> Dict[str, Any]:
    """
    Formats error details into a structured dictionary.
    
    Args:
        error: The exception that occurred
        context: Additional context about where/when the error occurred
        include_traceback: Whether to include the full traceback
        
    Returns:
        Structured error details dictionary
    """
    # Get error type and message
    error_type = error.__class__.__name__
    error_message = str(error)
    formatted_error = f"{error_type}: {error_message}"
    
    # Create timestamp
    timestamp = datetime.datetime.now().isoformat()
    
    # Build the error details dictionary
    error_details = {
        "type": error_type,
        "message": error_message,
        "formatted_error": formatted_error,
        "context": context,
        "timestamp": timestamp,
    }
    
    # Add traceback if requested
    if include_traceback:
        error_details["traceback"] = traceback.format_exc()
    
    return error_details


def generate_error_id() -> str:
    """
    Generates a unique identifier for an error instance.
    
    Returns:
        Unique error identifier
    """
    return str(uuid.uuid4())


def report_error(error: Exception, context: str, include_traceback: bool = True) -> str:
    """
    Reports an error to the error registry and optionally to external systems.
    
    Args:
        error: The exception that occurred
        context: Additional context about where/when the error occurred
        include_traceback: Whether to include the full traceback
        
    Returns:
        Error ID for reference
    """
    # Generate unique error ID
    error_id = generate_error_id()
    
    # Format error details
    error_details = format_error_details(error, context, include_traceback)
    
    # Log the error
    logger.error(f"Error [{error_id}] in {context}: {format_exception(error)}")
    
    # Store in registry
    ERROR_REGISTRY[error_id] = error_details
    
    # Prune registry if it exceeds maximum size
    if len(ERROR_REGISTRY) > MAX_ERROR_HISTORY:
        # Find oldest error based on timestamp
        oldest_error_id = min(
            ERROR_REGISTRY.keys(),
            key=lambda k: ERROR_REGISTRY[k].get('timestamp', '')
        )
        # Remove oldest error
        ERROR_REGISTRY.pop(oldest_error_id, None)
        logger.debug(f"Removed oldest error [{oldest_error_id}] to maintain history limit")
    
    # Send to external reporting service if enabled
    if EXTERNAL_REPORTING_ENABLED and EXTERNAL_REPORTING_URL:
        send_to_external_service(error_id, error_details)
    
    return error_id


def get_error(error_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves error details from the error registry.
    
    Args:
        error_id: The unique identifier for the error
        
    Returns:
        Error details dictionary or None if not found
    """
    if error_id in ERROR_REGISTRY:
        return ERROR_REGISTRY[error_id]
    
    logger.warning(f"Attempted to retrieve non-existent error with ID: {error_id}")
    return None


def clear_error(error_id: str) -> bool:
    """
    Removes an error from the registry.
    
    Args:
        error_id: The unique identifier for the error
        
    Returns:
        True if error was cleared, False if not found
    """
    if error_id in ERROR_REGISTRY:
        ERROR_REGISTRY.pop(error_id)
        logger.info(f"Cleared error with ID: {error_id}")
        return True
    
    logger.warning(f"Attempted to clear non-existent error with ID: {error_id}")
    return False


def clear_all_errors() -> int:
    """
    Clears all errors from the registry.
    
    Returns:
        Number of errors cleared
    """
    error_count = len(ERROR_REGISTRY)
    ERROR_REGISTRY.clear()
    logger.info(f"Cleared all errors from registry ({error_count} errors)")
    return error_count


def get_error_count() -> int:
    """
    Gets the count of errors in the registry.
    
    Returns:
        Number of errors in registry
    """
    return len(ERROR_REGISTRY)


def get_error_summary() -> List[Dict[str, Any]]:
    """
    Gets a summary of all errors in the registry.
    
    Returns:
        List of error summary dictionaries
    """
    summaries = []
    
    for error_id, error_details in ERROR_REGISTRY.items():
        summary = {
            "id": error_id,
            "type": error_details.get("type", "Unknown"),
            "message": error_details.get("message", ""),
            "context": error_details.get("context", ""),
            "timestamp": error_details.get("timestamp", "")
        }
        summaries.append(summary)
    
    # Sort by timestamp, newest first
    summaries.sort(key=lambda x: x["timestamp"], reverse=True)
    
    return summaries


def send_to_external_service(error_id: str, error_details: Dict[str, Any]) -> bool:
    """
    Sends error details to an external error reporting service.
    
    Args:
        error_id: The unique identifier for the error
        error_details: The formatted error details
        
    Returns:
        True if successfully sent, False otherwise
    """
    if not EXTERNAL_REPORTING_ENABLED or not EXTERNAL_REPORTING_URL:
        return False
    
    try:
        # Prepare payload
        payload = {
            "error_id": error_id,
            "details": error_details,
            "app_environment": ENVIRONMENT,
            "reported_at": datetime.datetime.now().isoformat()
        }
        
        # Send request
        response = requests.post(
            EXTERNAL_REPORTING_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=5  # 5 second timeout
        )
        
        # Check if successful
        if 200 <= response.status_code < 300:
            logger.info(f"Successfully reported error [{error_id}] to external service")
            return True
        else:
            logger.warning(
                f"Failed to report error [{error_id}] to external service. "
                f"Status code: {response.status_code}"
            )
            return False
    
    except Exception as e:
        logger.error(f"Exception when reporting error to external service: {format_exception(e)}")
        return False


class ErrorReportingService:
    """
    Service class that provides centralized error reporting and monitoring.
    """
    
    def __init__(self, external_reporting_url: Optional[str] = None, 
                 enable_external_reporting: bool = False):
        """
        Initializes the error reporting service.
        
        Args:
            external_reporting_url: Optional URL for external error reporting
            enable_external_reporting: Whether to enable external reporting
        """
        self.error_registry = {}
        self.logger = get_logger('error_reporting')
        self.external_reporting_enabled = enable_external_reporting or EXTERNAL_REPORTING_ENABLED
        self.external_reporting_url = external_reporting_url or EXTERNAL_REPORTING_URL
        
        self.logger.info("Initialized error reporting service")
        if self.external_reporting_enabled and self.external_reporting_url:
            self.logger.info(f"External error reporting enabled: {self.external_reporting_url}")
    
    def report_error(self, error: Exception, context: str, include_traceback: bool = True) -> str:
        """
        Reports an error to the registry and optionally to external systems.
        
        Args:
            error: The exception that occurred
            context: Additional context about where/when the error occurred
            include_traceback: Whether to include the full traceback
            
        Returns:
            Error ID for reference
        """
        # Generate unique error ID
        error_id = generate_error_id()
        
        # Format error details
        error_details = format_error_details(error, context, include_traceback)
        
        # Log the error
        self.logger.error(f"Error [{error_id}] in {context}: {format_exception(error)}")
        
        # Store in registry
        self.error_registry[error_id] = error_details
        
        # Prune registry if it exceeds maximum size
        if len(self.error_registry) > MAX_ERROR_HISTORY:
            # Find oldest error based on timestamp
            oldest_error_id = min(
                self.error_registry.keys(),
                key=lambda k: self.error_registry[k].get('timestamp', '')
            )
            # Remove oldest error
            self.error_registry.pop(oldest_error_id, None)
            self.logger.debug(f"Removed oldest error [{oldest_error_id}] to maintain history limit")
        
        # Send to external reporting service if enabled
        if self.external_reporting_enabled and self.external_reporting_url:
            self.send_to_external_service(error_id, error_details)
        
        return error_id
    
    def get_error(self, error_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves error details from the registry.
        
        Args:
            error_id: The unique identifier for the error
            
        Returns:
            Error details dictionary or None if not found
        """
        if error_id in self.error_registry:
            return self.error_registry[error_id]
        
        self.logger.warning(f"Attempted to retrieve non-existent error with ID: {error_id}")
        return None
    
    def clear_error(self, error_id: str) -> bool:
        """
        Removes an error from the registry.
        
        Args:
            error_id: The unique identifier for the error
            
        Returns:
            True if error was cleared, False if not found
        """
        if error_id in self.error_registry:
            self.error_registry.pop(error_id)
            self.logger.info(f"Cleared error with ID: {error_id}")
            return True
        
        self.logger.warning(f"Attempted to clear non-existent error with ID: {error_id}")
        return False
    
    def clear_all_errors(self) -> int:
        """
        Clears all errors from the registry.
        
        Returns:
            Number of errors cleared
        """
        error_count = len(self.error_registry)
        self.error_registry.clear()
        self.logger.info(f"Cleared all errors from registry ({error_count} errors)")
        return error_count
    
    def get_error_count(self) -> int:
        """
        Gets the count of errors in the registry.
        
        Returns:
            Number of errors in registry
        """
        return len(self.error_registry)
    
    def get_error_summary(self) -> List[Dict[str, Any]]:
        """
        Gets a summary of all errors in the registry.
        
        Returns:
            List of error summary dictionaries
        """
        summaries = []
        
        for error_id, error_details in self.error_registry.items():
            summary = {
                "id": error_id,
                "type": error_details.get("type", "Unknown"),
                "message": error_details.get("message", ""),
                "context": error_details.get("context", ""),
                "timestamp": error_details.get("timestamp", "")
            }
            summaries.append(summary)
        
        # Sort by timestamp, newest first
        summaries.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return summaries
    
    def send_to_external_service(self, error_id: str, error_details: Dict[str, Any]) -> bool:
        """
        Sends error details to an external error reporting service.
        
        Args:
            error_id: The unique identifier for the error
            error_details: The formatted error details
            
        Returns:
            True if successfully sent, False otherwise
        """
        if not self.external_reporting_enabled or not self.external_reporting_url:
            return False
        
        try:
            # Prepare payload
            payload = {
                "error_id": error_id,
                "details": error_details,
                "app_environment": ENVIRONMENT,
                "reported_at": datetime.datetime.now().isoformat()
            }
            
            # Send request
            response = requests.post(
                self.external_reporting_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=5  # 5 second timeout
            )
            
            # Check if successful
            if 200 <= response.status_code < 300:
                self.logger.info(f"Successfully reported error [{error_id}] to external service")
                return True
            else:
                self.logger.warning(
                    f"Failed to report error [{error_id}] to external service. "
                    f"Status code: {response.status_code}"
                )
                return False
        
        except Exception as e:
            self.logger.error(f"Exception when reporting error to external service: {format_exception(e)}")
            return False
    
    def enable_external_reporting(self, reporting_url: Optional[str] = None) -> None:
        """
        Enables external error reporting.
        
        Args:
            reporting_url: Optional URL for external error reporting
        """
        self.external_reporting_enabled = True
        if reporting_url:
            self.external_reporting_url = reporting_url
        
        self.logger.info(
            f"External error reporting enabled: {self.external_reporting_url}"
        )
    
    def disable_external_reporting(self) -> None:
        """
        Disables external error reporting.
        """
        self.external_reporting_enabled = False
        self.logger.info("External error reporting disabled")


# Create a singleton instance for application-wide use
error_reporting_service = ErrorReportingService()