"""
Module responsible for logging fallback mechanism activities in the Electricity Market Price Forecasting System.

Provides structured logging functions for different stages of the fallback process,
including activation, retrieval, timestamp adjustment, completion, and errors.
"""

import logging
import datetime
import typing
import time
import pandas  # version: 2.0.0+

# Internal imports
from ..utils.logging_utils import (
    get_logger,
    ComponentLogger,
    format_exception,
    format_dict_for_logging
)
from .exceptions import FallbackLoggingError

# Initialize module logger
logger = get_logger(__name__)

# Initialize component-specific logger with default context
component_logger = ComponentLogger('fallback', {'component': 'fallback_mechanism'})


def log_fallback_activation(component: str, error_category: str, error_details: dict) -> None:
    """
    Logs the activation of the fallback mechanism.
    
    Args:
        component: The component where the fallback is being activated
        error_category: Category of error that triggered fallback
        error_details: Dictionary with details about the error
        
    Returns:
        None: Function performs side effects only
        
    Raises:
        FallbackLoggingError: If logging fails
    """
    try:
        # Create context with component, error category, and timestamp
        context = {
            'component': component,
            'error_category': error_category,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        # Add error details to context
        context.update(error_details)
        
        # Log the fallback activation at WARNING level
        logger.warning(f"Fallback mechanism activated for component '{component}' due to {error_category} error")
        component_logger.with_context(context).warning(f"Fallback mechanism activated")
        
    except Exception as e:
        # Handle any exceptions during logging
        error_msg = f"Failed to log fallback activation: {str(e)}"
        logger.error(error_msg)
        raise FallbackLoggingError(error_msg, "activation", e)


def log_fallback_retrieval(product: str, target_date: datetime.datetime, 
                          source_date: datetime.datetime, metadata: dict) -> None:
    """
    Logs the retrieval of a previous forecast for fallback.
    
    Args:
        product: The price product for which a fallback is being retrieved
        target_date: The target date for which forecast is needed
        source_date: The date of the retrieved fallback forecast
        metadata: Additional metadata about the retrieved forecast
        
    Returns:
        None: Function performs side effects only
        
    Raises:
        FallbackLoggingError: If logging fails
    """
    try:
        # Create context with product, dates, and timestamp
        context = {
            'product': product,
            'target_date': target_date.strftime('%Y-%m-%d'),
            'source_date': source_date.strftime('%Y-%m-%d'),
            'days_difference': (target_date.date() - source_date.date()).days,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        # Add metadata about the retrieved forecast
        context.update(metadata)
        
        # Log the retrieval at INFO level
        logger.info(
            f"Retrieved fallback forecast for product '{product}' from {source_date.strftime('%Y-%m-%d')} "
            f"for target date {target_date.strftime('%Y-%m-%d')}"
        )
        component_logger.with_context(context).info(f"Fallback forecast retrieved")
        
    except Exception as e:
        # Handle any exceptions during logging
        error_msg = f"Failed to log fallback retrieval: {str(e)}"
        logger.error(error_msg)
        raise FallbackLoggingError(error_msg, "retrieval", e)


def log_timestamp_adjustment(product: str, source_date: datetime.datetime, 
                            target_date: datetime.datetime, forecast_df: pandas.DataFrame) -> None:
    """
    Logs the adjustment of timestamps in a fallback forecast.
    
    Args:
        product: The price product being adjusted
        source_date: The original date of the fallback forecast
        target_date: The target date to which timestamps are adjusted
        forecast_df: The forecast DataFrame being adjusted
        
    Returns:
        None: Function performs side effects only
        
    Raises:
        FallbackLoggingError: If logging fails
    """
    try:
        # Create context with product, dates, and timestamp
        context = {
            'product': product,
            'source_date': source_date.strftime('%Y-%m-%d'),
            'target_date': target_date.strftime('%Y-%m-%d'),
            'time_shift_days': (target_date.date() - source_date.date()).days,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        # Add information about the forecast DataFrame
        forecast_metrics = get_fallback_metrics(forecast_df, product)
        context.update({
            'forecast_data_points': forecast_metrics.get('data_points', 0),
            'forecast_horizon_hours': forecast_metrics.get('horizon_hours', 0),
        })
        
        # Log the timestamp adjustment at INFO level
        logger.info(
            f"Adjusted timestamps for product '{product}' forecast from {source_date.strftime('%Y-%m-%d')} "
            f"to {target_date.strftime('%Y-%m-%d')}"
        )
        component_logger.with_context(context).info(f"Fallback forecast timestamps adjusted")
        
    except Exception as e:
        # Handle any exceptions during logging
        error_msg = f"Failed to log timestamp adjustment: {str(e)}"
        logger.error(error_msg)
        raise FallbackLoggingError(error_msg, "timestamp_adjustment", e)


def log_fallback_completion(product: str, target_date: datetime.datetime, 
                           source_date: datetime.datetime, start_time: float, 
                           metrics: dict) -> None:
    """
    Logs the successful completion of the fallback process.
    
    Args:
        product: The price product for which fallback was completed
        target_date: The target date for which forecast was needed
        source_date: The date of the original fallback forecast
        start_time: Start time of the fallback process (from time.time())
        metrics: Performance metrics and information about the fallback
        
    Returns:
        None: Function performs side effects only
        
    Raises:
        FallbackLoggingError: If logging fails
    """
    try:
        # Calculate the duration of the fallback process
        duration = time.time() - start_time
        
        # Create context with product, dates, duration, and timestamp
        context = {
            'product': product,
            'target_date': target_date.strftime('%Y-%m-%d'),
            'source_date': source_date.strftime('%Y-%m-%d'),
            'duration_seconds': f"{duration:.3f}",
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        # Add performance metrics to context
        context.update(metrics)
        
        # Log the completion at INFO level
        logger.info(
            f"Completed fallback for product '{product}' from {source_date.strftime('%Y-%m-%d')} "
            f"to {target_date.strftime('%Y-%m-%d')} in {duration:.3f} seconds"
        )
        component_logger.with_context(context).info(f"Fallback process completed")
        
    except Exception as e:
        # Handle any exceptions during logging
        error_msg = f"Failed to log fallback completion: {str(e)}"
        logger.error(error_msg)
        raise FallbackLoggingError(error_msg, "completion", e)


def log_fallback_error(operation: str, error: Exception, context: dict) -> None:
    """
    Logs errors that occur during the fallback process.
    
    Args:
        operation: The fallback operation that encountered an error
        error: The exception that occurred
        context: Additional context about the operation
        
    Returns:
        None: Function performs side effects only
        
    Raises:
        FallbackLoggingError: If logging fails (only if different from original error)
    """
    try:
        # Format exception details
        error_details = format_exception(error)
        
        # Create error context with operation, error details, and timestamp
        error_context = {
            'operation': operation,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_details': error_details,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        # Merge with provided context
        error_context.update(context)
        
        # Log the error at ERROR level
        logger.error(f"Error during fallback {operation}: {str(error)}")
        component_logger.with_context(error_context).error(f"Fallback error in {operation}")
        
    except Exception as e:
        # Only raise new exception if it's different from the original error
        if not isinstance(e, type(error)):
            error_msg = f"Failed to log fallback error: {str(e)}"
            logger.error(error_msg)
            raise FallbackLoggingError(error_msg, "error_logging", e)


def get_fallback_metrics(forecast_df: pandas.DataFrame, product: str) -> dict:
    """
    Collects metrics about a fallback forecast for logging purposes.
    
    Args:
        forecast_df: The fallback forecast DataFrame
        product: The price product of the forecast
        
    Returns:
        Dictionary of metrics about the fallback forecast
    """
    try:
        metrics = {
            'product': product,
        }
        
        # Calculate number of data points
        if forecast_df is not None and not forecast_df.empty:
            metrics['data_points'] = len(forecast_df)
            
            # Determine forecast horizon (hours covered)
            if 'timestamp' in forecast_df.columns:
                timestamps = pandas.to_datetime(forecast_df['timestamp'])
                if len(timestamps) > 1:
                    start_time = timestamps.min()
                    end_time = timestamps.max()
                    duration = end_time - start_time
                    metrics['horizon_hours'] = round(duration.total_seconds() / 3600)
            
            # Check if this is already a fallback forecast (cascading fallback)
            if 'is_fallback' in forecast_df.columns:
                metrics['is_cascading_fallback'] = bool(forecast_df['is_fallback'].any())
            
            # Extract generation timestamp information if available
            if 'generation_timestamp' in forecast_df.columns:
                gen_timestamps = pandas.to_datetime(forecast_df['generation_timestamp'])
                if not gen_timestamps.empty:
                    metrics['original_generation_time'] = gen_timestamps.iloc[0].strftime('%Y-%m-%d %H:%M:%S')
        
        return metrics
    except Exception as e:
        # If an error occurs, return minimal metrics
        logger.warning(f"Error collecting fallback metrics: {str(e)}")
        return {'product': product, 'metrics_error': str(e)}