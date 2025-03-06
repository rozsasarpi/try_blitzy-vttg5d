"""
Module responsible for detecting and categorizing errors in the Electricity Market Price 
Forecasting System that should trigger the fallback mechanism. Implements error detection 
strategies to identify different types of failures across various system components and 
determine appropriate fallback responses.
"""

import enum
import typing

# Internal imports
from ..utils.logging_utils import get_logger
from .exceptions import ErrorDetectionFailure
from .fallback_logger import log_fallback_activation, log_fallback_error

# Import exception types from system components
from ..data_ingestion.exceptions import DataIngestionError, APIConnectionError, MissingDataError
from ..feature_engineering.exceptions import FeatureEngineeringError
from ..forecasting_engine.exceptions import ForecastingEngineError, ModelExecutionError
from ..forecast_validation.exceptions import ForecastValidationError
from ..storage.exceptions import StorageError
from ..pipeline.exceptions import PipelineError

# Module logger
logger = get_logger(__name__)


class ErrorCategory(enum.Enum):
    """Enumeration of error categories for fallback mechanism"""
    DATA_SOURCE_ERROR = "data_source_error"
    FEATURE_ERROR = "feature_error"
    MODEL_ERROR = "model_error"
    VALIDATION_ERROR = "validation_error"
    STORAGE_ERROR = "storage_error"
    PIPELINE_ERROR = "pipeline_error"
    UNKNOWN_ERROR = "unknown_error"


def detect_error(error: Exception, component: str) -> typing.Tuple[ErrorCategory, dict]:
    """
    Main function to detect and categorize errors that should trigger the fallback mechanism.
    
    Args:
        error: The exception that occurred
        component: The component where the exception occurred
        
    Returns:
        Tuple of (ErrorCategory, dict) with error category and details
        
    Raises:
        ErrorDetectionFailure: If the error detection process itself fails
    """
    try:
        logger.info(f"Detecting error type for exception {type(error).__name__} in component {component}")
        
        # Categorize the error
        category = categorize_error(error)
        
        # Extract error details
        error_details = extract_error_details(error, category)
        
        # Log detection result
        logger.info(f"Categorized error as {category.value} with {len(error_details)} details")
        
        return category, error_details
    
    except Exception as e:
        # If error detection itself fails, log and raise a specific exception
        logger.error(f"Error detection failed: {str(e)}")
        raise ErrorDetectionFailure(
            f"Failed to detect error type", 
            original_error=error, 
            component=component
        )


def categorize_error(error: Exception) -> ErrorCategory:
    """
    Categorizes an exception into a specific error category.
    
    Args:
        error: The exception to categorize
        
    Returns:
        ErrorCategory: The categorized error type
    """
    # Data source errors
    if isinstance(error, (DataIngestionError, APIConnectionError, MissingDataError)):
        logger.debug(f"Categorized {type(error).__name__} as DATA_SOURCE_ERROR")
        return ErrorCategory.DATA_SOURCE_ERROR
    
    # Feature engineering errors
    elif isinstance(error, FeatureEngineeringError):
        logger.debug(f"Categorized {type(error).__name__} as FEATURE_ERROR")
        return ErrorCategory.FEATURE_ERROR
    
    # Model errors
    elif isinstance(error, (ForecastingEngineError, ModelExecutionError)):
        logger.debug(f"Categorized {type(error).__name__} as MODEL_ERROR")
        return ErrorCategory.MODEL_ERROR
    
    # Validation errors
    elif isinstance(error, ForecastValidationError):
        logger.debug(f"Categorized {type(error).__name__} as VALIDATION_ERROR")
        return ErrorCategory.VALIDATION_ERROR
    
    # Storage errors
    elif isinstance(error, StorageError):
        logger.debug(f"Categorized {type(error).__name__} as STORAGE_ERROR")
        return ErrorCategory.STORAGE_ERROR
    
    # Pipeline errors
    elif isinstance(error, PipelineError):
        logger.debug(f"Categorized {type(error).__name__} as PIPELINE_ERROR")
        return ErrorCategory.PIPELINE_ERROR
    
    # Default case - unknown error
    else:
        logger.debug(f"Categorized {type(error).__name__} as UNKNOWN_ERROR (no specific category match)")
        return ErrorCategory.UNKNOWN_ERROR


def extract_error_details(error: Exception, category: ErrorCategory) -> dict:
    """
    Extracts relevant details from an exception for fallback logging.
    
    Args:
        error: The exception to extract details from
        category: The categorized error type
        
    Returns:
        Dictionary of error details
    """
    # Base details that apply to all errors
    details = {
        "error_type": type(error).__name__,
        "error_message": str(error),
    }
    
    # Extract specific details based on error category and type
    if category == ErrorCategory.DATA_SOURCE_ERROR:
        if hasattr(error, "source_name"):
            details["source_name"] = getattr(error, "source_name")
        if hasattr(error, "api_endpoint"):
            details["api_endpoint"] = getattr(error, "api_endpoint")
        if hasattr(error, "missing_fields"):
            details["missing_fields"] = getattr(error, "missing_fields")
    
    elif category == ErrorCategory.FEATURE_ERROR:
        if hasattr(error, "feature_name"):
            details["feature_name"] = getattr(error, "feature_name")
        if hasattr(error, "missing_features"):
            details["missing_features"] = getattr(error, "missing_features")
        if hasattr(error, "source_features"):
            details["source_features"] = getattr(error, "source_features")
    
    elif category == ErrorCategory.MODEL_ERROR:
        if hasattr(error, "product"):
            details["product"] = getattr(error, "product")
        if hasattr(error, "hour"):
            details["hour"] = getattr(error, "hour")
        if hasattr(error, "model_id"):
            details["model_id"] = getattr(error, "model_id")
    
    elif category == ErrorCategory.VALIDATION_ERROR:
        if hasattr(error, "errors") and getattr(error, "errors"):
            details["validation_errors"] = getattr(error, "errors")
    
    elif category == ErrorCategory.STORAGE_ERROR:
        if hasattr(error, "file_path"):
            details["file_path"] = str(getattr(error, "file_path"))
        if hasattr(error, "operation"):
            details["operation"] = getattr(error, "operation")
    
    elif category == ErrorCategory.PIPELINE_ERROR:
        if hasattr(error, "pipeline_name"):
            details["pipeline_name"] = getattr(error, "pipeline_name")
        if hasattr(error, "stage_name"):
            details["stage_name"] = getattr(error, "stage_name")
        if hasattr(error, "execution_id"):
            details["execution_id"] = getattr(error, "execution_id")
    
    return details


def should_activate_fallback(category: ErrorCategory, error_details: dict) -> bool:
    """
    Determines if a given error should trigger the fallback mechanism.
    
    Args:
        category: The categorized error type
        error_details: Dictionary of error details
        
    Returns:
        True if fallback should be activated, False otherwise
    """
    # Default categories that should trigger fallback
    fallback_categories = [
        ErrorCategory.DATA_SOURCE_ERROR,
        ErrorCategory.MODEL_ERROR,
        ErrorCategory.VALIDATION_ERROR,
        ErrorCategory.FEATURE_ERROR,
    ]
    
    # Check if category is in the list of categories that should trigger fallback
    if category in fallback_categories:
        # Apply additional rules based on error details
        if category == ErrorCategory.DATA_SOURCE_ERROR:
            # Check if it's a critical data source
            if "source_name" in error_details:
                source_name = error_details["source_name"]
                # All data sources are critical for the forecasting system
                logger.info(f"Activating fallback for critical data source: {source_name}")
                return True
            return True
            
        elif category == ErrorCategory.FEATURE_ERROR:
            # All feature errors are critical since they affect model inputs
            logger.info("Activating fallback for feature engineering error")
            return True
            
        elif category == ErrorCategory.MODEL_ERROR:
            # Model errors are critical
            logger.info("Activating fallback for model execution error")
            return True
            
        elif category == ErrorCategory.VALIDATION_ERROR:
            # Validation errors indicate issues with forecast quality
            logger.info("Activating fallback for validation error")
            return True
            
        # Default to True for fallback categories
        return True
        
    elif category == ErrorCategory.STORAGE_ERROR:
        # For storage errors, only activate fallback for read operations
        # since write failures could be handled differently
        if error_details.get("operation") == "read":
            logger.info("Activating fallback for storage read error")
            return True
        logger.info("Not activating fallback for storage write error")
        return False
        
    elif category == ErrorCategory.PIPELINE_ERROR:
        # For pipeline errors, only activate fallback for critical stages
        if "stage_name" in error_details:
            stage_name = error_details["stage_name"]
            critical_stages = [
                "data_ingestion", "feature_engineering", 
                "forecasting", "validation"
            ]
            if stage_name in critical_stages:
                logger.info(f"Activating fallback for critical pipeline stage: {stage_name}")
                return True
        logger.info("Not activating fallback for non-critical pipeline error")
        return False
        
    # Default to False for unknown error categories
    logger.info(f"Not activating fallback for category: {category.value}")
    return False


def log_error_detection(
    error: Exception, 
    component: str, 
    category: ErrorCategory, 
    error_details: dict,
    activate_fallback: bool
) -> None:
    """
    Logs the error detection process and outcome.
    
    Args:
        error: The exception that occurred
        component: The component where the exception occurred
        category: The categorized error type
        error_details: Dictionary of error details
        activate_fallback: Whether fallback will be activated
        
    Returns:
        None: Function performs side effects only
    """
    try:
        if activate_fallback:
            # Log fallback activation with details
            log_fallback_activation(component, category.value, error_details)
        else:
            # Log that fallback was not activated
            logger.warning(f"Error detected but fallback not activated: {type(error).__name__} in {component}")
            
    except Exception as e:
        # If logging fails, log a basic message and continue
        log_fallback_error(
            "error_detection_logging", 
            e, 
            {"original_error": str(error), "component": component}
        )