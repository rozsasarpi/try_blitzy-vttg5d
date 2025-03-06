"""
Initialization module for the fallback mechanism of the Electricity Market Price Forecasting System.
Exports key functions and classes for detecting errors, retrieving previous forecasts, adjusting timestamps,
and logging fallback operations. This module serves as the main entry point for the fallback functionality
that provides previous day's forecasts when current forecast generation fails.
"""

import time  # standard library
import typing  # standard library
import datetime  # standard library
import pandas  # version: 2.0.0+

# Internal imports
from .exceptions import (
    FallbackError,
    ErrorDetectionFailure,
    FallbackRetrievalError,
    TimestampAdjustmentError,
    FallbackLoggingError,
    NoFallbackAvailableError,
    FallbackActivationError,
    FallbackDataValidationError
)
from .error_detector import (
    ErrorCategory,
    detect_error,
    should_activate_fallback
)
from .fallback_retriever import (
    retrieve_fallback_forecast,
    DEFAULT_MAX_SEARCH_DAYS
)
from .timestamp_adjuster import adjust_timestamps
from .fallback_logger import (
    log_fallback_activation,
    log_fallback_retrieval,
    log_timestamp_adjustment,
    log_fallback_completion,
    log_fallback_error
)
from ..utils.logging_utils import get_logger

# Global logger
logger = get_logger(__name__)


def process_fallback(
    error: Exception,
    component: str,
    product: str,
    target_date: datetime.datetime
) -> pandas.DataFrame:
    """
    Main function to process fallback when forecast generation fails

    Args:
        error: The exception that occurred
        component: The component where the exception occurred
        product: The price product for which fallback is needed
        target_date: The target date for which forecast is needed

    Returns:
        Fallback forecast dataframe

    Raises:
        FallbackActivationError: If fallback activation fails
    """
    logger.info(f"Starting fallback processing for {product} in {component} on {target_date.strftime('%Y-%m-%d')}")
    start_time = time.time()

    try:
        # Detect and categorize the error
        category, error_details = detect_error(error, component)

        # Determine if fallback should be activated
        activate_fallback = should_activate_fallback(category, error_details)

        if not activate_fallback:
            logger.info(f"Fallback not activated for {product} in {component} due to error category {category.value}")
            raise error  # Re-raise the original error

        # Log fallback activation
        log_fallback_activation(component, category.value, error_details)

        # Retrieve fallback forecast
        fallback_df = retrieve_fallback_forecast(product, target_date)

        # Log successful fallback completion
        end_time = time.time()
        duration = end_time - start_time
        log_fallback_completion(
            product,
            target_date,
            fallback_df.iloc[0]['timestamp'],  # type: ignore # Safe access due to non-empty check in retrieve_fallback_forecast
            start_time,
            {"duration": duration}
        )

        logger.info(f"Successfully processed fallback for {product} in {component} in {duration:.3f} seconds")
        return fallback_df

    except Exception as e:
        # Handle any exceptions during fallback processing
        reason = f"Error during fallback processing: {str(e)}"
        logger.error(reason)
        log_fallback_error("process_fallback", e, {"component": component, "product": product, "target_date": target_date})
        raise FallbackActivationError(
            f"Failed to process fallback for {product} in {component}",
            component,
            reason,
            original_error=e
        ) from e


__all__ = [
    "FallbackError",
    "ErrorDetectionFailure",
    "FallbackRetrievalError",
    "TimestampAdjustmentError",
    "NoFallbackAvailableError",
    "FallbackActivationError",
    "FallbackDataValidationError",
    "ErrorCategory",
    "detect_error",
    "should_activate_fallback",
    "retrieve_fallback_forecast",
    "adjust_timestamps",
    "log_fallback_activation",
    "log_fallback_retrieval",
    "log_timestamp_adjustment",
    "log_fallback_completion",
    "log_fallback_error",
    "process_fallback",
    "DEFAULT_MAX_SEARCH_DAYS"
]