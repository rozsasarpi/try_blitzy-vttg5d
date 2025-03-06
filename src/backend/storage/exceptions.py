"""
Custom exception classes for the storage module of the Electricity Market Price Forecasting System.

This module defines a hierarchy of exception types to handle various error scenarios 
in forecast storage operations, including schema validation failures, file operations,
missing data, and index management issues.
"""

import datetime  # standard library
import pathlib  # standard library
import typing  # standard library


class StorageError(Exception):
    """Base exception class for all storage-related errors."""
    
    def __init__(self, message: str):
        """Initialize the base storage error.
        
        Args:
            message: Error message
        """
        super().__init__(message)
        self.message = message
    
    def __str__(self) -> str:
        """String representation of the error.
        
        Returns:
            Error message
        """
        return self.message


class SchemaValidationError(StorageError):
    """Exception raised when a forecast dataframe fails schema validation."""
    
    def __init__(self, message: str, validation_errors: dict):
        """Initialize schema validation error with details.
        
        Args:
            message: Error message
            validation_errors: Dictionary containing validation error details
        """
        super().__init__(message)
        self.validation_errors = validation_errors
    
    def __str__(self) -> str:
        """String representation of the schema validation error.
        
        Returns:
            Formatted error message with validation details
        """
        error_details = "\n".join(f"- {key}: {value}" for key, value in self.validation_errors.items())
        return f"{self.message}\nValidation errors:\n{error_details}"


class FileOperationError(StorageError):
    """Exception raised when a file operation fails during storage operations."""
    
    def __init__(self, message: str, file_path: pathlib.Path, operation: str):
        """Initialize file operation error with context.
        
        Args:
            message: Error message
            file_path: Path of the file that caused the error
            operation: Type of operation that failed (e.g., 'read', 'write')
        """
        super().__init__(message)
        self.file_path = file_path
        self.operation = operation
    
    def __str__(self) -> str:
        """String representation of the file operation error.
        
        Returns:
            Formatted error message with file and operation details
        """
        return f"{self.message}\nOperation: {self.operation}\nFile: {self.file_path}"


class DataFrameNotFoundError(StorageError):
    """Exception raised when a requested forecast dataframe cannot be found."""
    
    def __init__(self, message: str, product: str, timestamp: datetime.datetime):
        """Initialize dataframe not found error with context.
        
        Args:
            message: Error message
            product: Name of the product being searched for
            timestamp: Timestamp of the forecast being searched for
        """
        super().__init__(message)
        self.product = product
        self.timestamp = timestamp
    
    def __str__(self) -> str:
        """String representation of the dataframe not found error.
        
        Returns:
            Formatted error message with product and timestamp details
        """
        return f"{self.message}\nProduct: {self.product}\nTimestamp: {self.timestamp.isoformat()}"


class IndexUpdateError(StorageError):
    """Exception raised when updating the forecast index fails."""
    
    def __init__(self, message: str, index_path: pathlib.Path):
        """Initialize index update error with context.
        
        Args:
            message: Error message
            index_path: Path to the index file that failed to update
        """
        super().__init__(message)
        self.index_path = index_path
    
    def __str__(self) -> str:
        """String representation of the index update error.
        
        Returns:
            Formatted error message with index path details
        """
        return f"{self.message}\nIndex path: {self.index_path}"


class StoragePathError(StorageError):
    """Exception raised when a storage path cannot be resolved or is invalid."""
    
    def __init__(self, message: str, path: typing.Union[str, pathlib.Path]):
        """Initialize storage path error with context.
        
        Args:
            message: Error message
            path: Path that caused the error
        """
        super().__init__(message)
        self.path = path
    
    def __str__(self) -> str:
        """String representation of the storage path error.
        
        Returns:
            Formatted error message with path details
        """
        return f"{self.message}\nPath: {self.path}"


class DataIntegrityError(StorageError):
    """Exception raised when a forecast dataframe fails integrity checks."""
    
    def __init__(self, message: str, file_path: pathlib.Path, integrity_issues: dict):
        """Initialize data integrity error with context.
        
        Args:
            message: Error message
            file_path: Path to the file with integrity issues
            integrity_issues: Dictionary containing integrity issue details
        """
        super().__init__(message)
        self.file_path = file_path
        self.integrity_issues = integrity_issues
    
    def __str__(self) -> str:
        """String representation of the data integrity error.
        
        Returns:
            Formatted error message with integrity issue details
        """
        issue_details = "\n".join(f"- {key}: {value}" for key, value in self.integrity_issues.items())
        return f"{self.message}\nFile: {self.file_path}\nIntegrity issues:\n{issue_details}"