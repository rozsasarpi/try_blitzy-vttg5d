"""
Module responsible for adjusting timestamps in fallback forecasts when using previous day's data as a fallback.
Ensures that the forecast horizon is maintained correctly by shifting timestamps from the source date to 
the target date while preserving the time structure of the forecast.
"""

import pandas  # version: 2.0.0+
import datetime
from typing import List, Optional
import copy

# Internal imports
from .exceptions import TimestampAdjustmentError
from .fallback_logger import log_timestamp_adjustment
from ..utils.date_utils import (
    localize_to_cst,
    shift_timestamps,
    get_current_time_cst
)
from ..models.forecast_models import (
    ProbabilisticForecast,
    create_forecast_dataframe,
    forecasts_from_dataframe
)
from ..utils.logging_utils import get_logger

# Set up logger
logger = get_logger(__name__)

# Constants
TIMESTAMP_COLUMN = 'timestamp'
GENERATION_TIMESTAMP_COLUMN = 'generation_timestamp'
IS_FALLBACK_COLUMN = 'is_fallback'


def adjust_timestamps(forecast_df: pandas.DataFrame, product: str, 
                     source_date: datetime.datetime, 
                     target_date: datetime.datetime) -> pandas.DataFrame:
    """
    Main function to adjust timestamps in a fallback forecast dataframe.
    
    Args:
        forecast_df: The dataframe containing forecast data to adjust
        product: The price product (e.g., DALMP, RTLMP)
        source_date: The original date of the forecast
        target_date: The target date to adjust timestamps to
        
    Returns:
        DataFrame with adjusted timestamps and updated fallback metadata
        
    Raises:
        TimestampAdjustmentError: If adjustment fails due to invalid parameters or errors
    """
    try:
        # Validate input parameters
        if not validate_adjustment_parameters(forecast_df, product, source_date, target_date):
            raise ValueError("Invalid adjustment parameters")
        
        # Calculate time shift between source and target dates
        time_shift = calculate_time_shift(source_date, target_date)
        
        # Create a copy of the dataframe to avoid modifying the original
        adjusted_df = copy.deepcopy(forecast_df)
        
        # Shift timestamps in the dataframe
        logger.info(f"Shifting timestamps for product '{product}' by {time_shift} days")
        adjusted_df = shift_timestamps(adjusted_df, time_shift, TIMESTAMP_COLUMN)
        
        # Update fallback metadata
        adjusted_df = update_fallback_metadata(adjusted_df)
        
        # Log the timestamp adjustment
        log_timestamp_adjustment(product, source_date, target_date, adjusted_df)
        
        return adjusted_df
        
    except Exception as e:
        error_msg = f"Failed to adjust timestamps for '{product}'"
        logger.error(f"{error_msg}: {str(e)}")
        raise TimestampAdjustmentError(error_msg, product, source_date, target_date, e)


def adjust_forecast_objects(forecasts: List[ProbabilisticForecast], product: str,
                           source_date: datetime.datetime, 
                           target_date: datetime.datetime) -> List[ProbabilisticForecast]:
    """
    Adjusts timestamps in a list of forecast objects.
    
    Args:
        forecasts: List of forecast objects to adjust
        product: The price product (e.g., DALMP, RTLMP)
        source_date: The original date of the forecast
        target_date: The target date to adjust timestamps to
        
    Returns:
        List of forecast objects with adjusted timestamps
        
    Raises:
        TimestampAdjustmentError: If adjustment fails
        ValueError: If forecasts list is empty
    """
    if not forecasts:
        error_msg = f"Empty forecast list for product '{product}'"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    try:
        # Convert forecast objects to dataframe
        logger.debug(f"Converting {len(forecasts)} forecast objects to dataframe for adjustment")
        forecast_df = create_forecast_dataframe(forecasts)
        
        # Adjust timestamps in the dataframe
        adjusted_df = adjust_timestamps(forecast_df, product, source_date, target_date)
        
        # Convert adjusted dataframe back to forecast objects
        logger.debug("Converting adjusted dataframe back to forecast objects")
        adjusted_forecasts = forecasts_from_dataframe(adjusted_df)
        
        return adjusted_forecasts
        
    except Exception as e:
        error_msg = f"Failed to adjust timestamps in forecast objects for '{product}'"
        logger.error(f"{error_msg}: {str(e)}")
        raise TimestampAdjustmentError(error_msg, product, source_date, target_date, e)


def validate_adjustment_parameters(forecast_df: pandas.DataFrame, product: str,
                                  source_date: datetime.datetime,
                                  target_date: datetime.datetime) -> bool:
    """
    Validates parameters for timestamp adjustment.
    
    Args:
        forecast_df: The dataframe to adjust
        product: The price product
        source_date: The original date of the forecast
        target_date: The target date to adjust to
        
    Returns:
        True if parameters are valid, raises ValueError otherwise
    """
    # Check if forecast_df is None or empty
    if forecast_df is None or forecast_df.empty:
        logger.error("Forecast dataframe is None or empty")
        raise ValueError("Forecast dataframe cannot be None or empty")
    
    # Check if timestamp column exists
    if TIMESTAMP_COLUMN not in forecast_df.columns:
        logger.error(f"Timestamp column '{TIMESTAMP_COLUMN}' not found in dataframe")
        raise ValueError(f"Timestamp column '{TIMESTAMP_COLUMN}' must exist in dataframe")
    
    # Check if product is None or empty
    if not product:
        logger.error("Product is None or empty")
        raise ValueError("Product cannot be None or empty")
    
    # Check if source_date and target_date are None
    if source_date is None or target_date is None:
        logger.error("Source date or target date is None")
        raise ValueError("Source date and target date cannot be None")
    
    # Ensure dates are in CST timezone
    source_date = localize_to_cst(source_date)
    target_date = localize_to_cst(target_date)
    
    # Check if source_date is before target_date
    if source_date > target_date:
        logger.warning(f"Source date {source_date} is after target date {target_date}")
    
    return True


def calculate_time_shift(source_date: datetime.datetime, 
                        target_date: datetime.datetime) -> datetime.timedelta:
    """
    Calculates the time shift between source and target dates.
    
    Args:
        source_date: The original date of the forecast
        target_date: The target date to adjust to
        
    Returns:
        Timedelta representing the time shift
        
    Raises:
        ValueError: If dates are invalid
    """
    # Ensure dates are in CST timezone
    source_date = localize_to_cst(source_date)
    target_date = localize_to_cst(target_date)
    
    # Calculate the time shift
    time_shift = target_date - source_date
    
    logger.debug(f"Calculated time shift: {time_shift}")
    return time_shift


def update_fallback_metadata(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Updates metadata in the fallback forecast dataframe.
    
    Args:
        df: DataFrame to update metadata in
        
    Returns:
        DataFrame with updated metadata
    """
    # Create a copy of the input dataframe
    updated_df = df.copy()
    
    # Set is_fallback column to True for all rows
    updated_df[IS_FALLBACK_COLUMN] = True
    
    # Update generation_timestamp to current time
    current_time = get_current_time_cst()
    updated_df[GENERATION_TIMESTAMP_COLUMN] = current_time
    
    logger.debug(f"Updated fallback metadata: is_fallback=True, generation_timestamp={current_time}")
    return updated_df