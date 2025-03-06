"""
Custom exception classes for the data ingestion module of the Electricity Market Price
Forecasting System. Provides specialized exceptions for different types of failures that
can occur during data collection, validation, and transformation processes.
"""

from typing import List, Optional
from datetime import datetime  # standard library


class DataIngestionError(Exception):
    """Base exception class for all data ingestion related errors."""
    
    def __init__(self, message: str):
        """Initialize the base data ingestion error.
        
        Args:
            message: Descriptive error message
        """
        super().__init__(message)
        self.message = message
        
    def __str__(self) -> str:
        """String representation of the error.
        
        Returns:
            Error message
        """
        return self.message


class APIConnectionError(DataIngestionError):
    """Exception raised when connection to an external API fails."""
    
    def __init__(self, api_endpoint: str, source_name: str, original_exception: Exception):
        """Initialize API connection error with details.
        
        Args:
            api_endpoint: The API endpoint that failed
            source_name: Name of the data source
            original_exception: The original exception that was raised
        """
        message = f"Failed to connect to API endpoint {api_endpoint} for source {source_name}: {str(original_exception)}"
        super().__init__(message)
        self.api_endpoint = api_endpoint
        self.source_name = source_name
        self.original_exception = original_exception


class APIResponseError(DataIngestionError):
    """Exception raised when an API response is invalid or contains an error."""
    
    def __init__(self, api_endpoint: str, status_code: int, response_data: Optional[dict] = None):
        """Initialize API response error with details.
        
        Args:
            api_endpoint: The API endpoint that returned the error
            status_code: HTTP status code
            response_data: The response data if available
        """
        message = f"API endpoint {api_endpoint} returned error status {status_code}"
        if response_data:
            message += f": {response_data}"
        super().__init__(message)
        self.api_endpoint = api_endpoint
        self.status_code = status_code
        self.response_data = response_data


class DataValidationError(DataIngestionError):
    """Exception raised when data validation fails."""
    
    def __init__(self, source_name: str, validation_errors: List[str]):
        """Initialize data validation error with details.
        
        Args:
            source_name: Name of the data source
            validation_errors: List of validation error messages
        """
        message = f"Data validation failed for source {source_name}: {', '.join(validation_errors)}"
        super().__init__(message)
        self.source_name = source_name
        self.validation_errors = validation_errors


class DataTransformationError(DataIngestionError):
    """Exception raised when data transformation fails."""
    
    def __init__(self, source_name: str, transformation_step: str, original_exception: Optional[Exception] = None):
        """Initialize data transformation error with details.
        
        Args:
            source_name: Name of the data source
            transformation_step: Name of the transformation step that failed
            original_exception: The original exception that was raised
        """
        message = f"Data transformation failed for source {source_name} during {transformation_step}"
        if original_exception:
            message += f": {str(original_exception)}"
        super().__init__(message)
        self.source_name = source_name
        self.transformation_step = transformation_step
        self.original_exception = original_exception


class MissingDataError(DataIngestionError):
    """Exception raised when required data is missing."""
    
    def __init__(self, source_name: str, missing_fields: List[str]):
        """Initialize missing data error with details.
        
        Args:
            source_name: Name of the data source
            missing_fields: List of missing field names
        """
        message = f"Required data missing from source {source_name}: {', '.join(missing_fields)}"
        super().__init__(message)
        self.source_name = source_name
        self.missing_fields = missing_fields


class DataTimeRangeError(DataIngestionError):
    """Exception raised when data for the requested time range is not available."""
    
    def __init__(self, source_name: str, requested_start: datetime, requested_end: datetime,
                 available_start: Optional[datetime] = None, available_end: Optional[datetime] = None):
        """Initialize time range error with details.
        
        Args:
            source_name: Name of the data source
            requested_start: Start of the requested time range
            requested_end: End of the requested time range
            available_start: Start of the available time range
            available_end: End of the available time range
        """
        message = f"Data for requested time range ({requested_start} to {requested_end}) not available from source {source_name}"
        if available_start and available_end:
            message += f", available range is {available_start} to {available_end}"
        super().__init__(message)
        self.source_name = source_name
        self.requested_start = requested_start
        self.requested_end = requested_end
        self.available_start = available_start
        self.available_end = available_end