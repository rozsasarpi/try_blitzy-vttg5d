"""
Custom exception classes for the feature engineering component of the
Electricity Market Price Forecasting System.

This module defines a hierarchy of exception types to handle various error 
scenarios that may occur during feature creation, normalization, selection,
and other feature engineering operations.
"""

from typing import List, Optional
from ..utils.logging_utils import get_logger

# Logger for this module
logger = get_logger(__name__)


class FeatureEngineeringError(Exception):
    """Base exception class for all feature engineering related errors."""
    
    def __init__(self, message: str, original_exception: Optional[Exception] = None):
        """
        Initialize the base feature engineering exception.
        
        Args:
            message: Error message describing the issue
            original_exception: Original exception that caused this error, if any
        """
        super().__init__(message)
        self.original_exception = original_exception
        logger.error(message)


class FeatureCreationError(FeatureEngineeringError):
    """Exception raised when there is an error creating base features."""
    
    def __init__(self, message: str, original_exception: Optional[Exception] = None):
        """
        Initialize the feature creation exception.
        
        Args:
            message: Error message describing the issue
            original_exception: Original exception that caused this error, if any
        """
        super().__init__(message, original_exception)


class FeatureNormalizationError(FeatureEngineeringError):
    """Exception raised when there is an error normalizing features."""
    
    def __init__(self, message: str, original_exception: Optional[Exception] = None):
        """
        Initialize the feature normalization exception.
        
        Args:
            message: Error message describing the issue
            original_exception: Original exception that caused this error, if any
        """
        super().__init__(message, original_exception)


class FeatureSelectionError(FeatureEngineeringError):
    """Exception raised when there is an error selecting features for a product/hour combination."""
    
    def __init__(self, message: str, product: str, hour: int, original_exception: Optional[Exception] = None):
        """
        Initialize the feature selection exception.
        
        Args:
            message: Error message describing the issue
            product: Price product identifier
            hour: Target hour
            original_exception: Original exception that caused this error, if any
        """
        detailed_message = f"Feature selection error for product '{product}', hour {hour}: {message}"
        super().__init__(detailed_message, original_exception)


class LaggedFeatureError(FeatureEngineeringError):
    """Exception raised when there is an error creating lagged features."""
    
    def __init__(self, message: str, columns: List[str], lag_periods: List[int], original_exception: Optional[Exception] = None):
        """
        Initialize the lagged feature exception.
        
        Args:
            message: Error message describing the issue
            columns: List of columns for which lagged features were being created
            lag_periods: List of lag periods that were being applied
            original_exception: Original exception that caused this error, if any
        """
        detailed_message = f"Error creating lagged features for columns {columns} with lag periods {lag_periods}: {message}"
        super().__init__(detailed_message, original_exception)


class DerivedFeatureError(FeatureEngineeringError):
    """Exception raised when there is an error creating derived features."""
    
    def __init__(self, message: str, source_features: List[str], original_exception: Optional[Exception] = None):
        """
        Initialize the derived feature exception.
        
        Args:
            message: Error message describing the issue
            source_features: List of source features used to create the derived feature
            original_exception: Original exception that caused this error, if any
        """
        detailed_message = f"Error creating derived feature from source features {source_features}: {message}"
        super().__init__(detailed_message, original_exception)


class FeatureDataTypeError(FeatureEngineeringError):
    """Exception raised when there is a data type incompatibility in features."""
    
    def __init__(self, message: str, feature_name: str, expected_type: str, actual_type: str, original_exception: Optional[Exception] = None):
        """
        Initialize the feature data type exception.
        
        Args:
            message: Error message describing the issue
            feature_name: Name of the feature with the data type issue
            expected_type: Expected data type for the feature
            actual_type: Actual data type found
            original_exception: Original exception that caused this error, if any
        """
        detailed_message = f"Data type error for feature '{feature_name}'. Expected {expected_type}, got {actual_type}: {message}"
        super().__init__(detailed_message, original_exception)


class MissingFeatureError(FeatureEngineeringError):
    """Exception raised when required features are missing."""
    
    def __init__(self, message: str, missing_features: List[str], original_exception: Optional[Exception] = None):
        """
        Initialize the missing feature exception.
        
        Args:
            message: Error message describing the issue
            missing_features: List of features that are missing
            original_exception: Original exception that caused this error, if any
        """
        detailed_message = f"Missing required features {missing_features}: {message}"
        super().__init__(detailed_message, original_exception)