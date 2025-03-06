"""
Utility module providing date and time handling functions for the Electricity Market Price Forecasting System.

This module implements timezone conversions, forecast date range generation, timestamp formatting,
and other date-related operations essential for scheduling, forecast generation, and fallback mechanisms.
"""

import datetime
import logging
from typing import List, Optional

import pandas as pd  # version: 2.0.0+
import pytz  # version: 2023.3

from ..config.settings import (
    FORECAST_HORIZON_HOURS,
    FORECAST_SCHEDULE_TIME,
    TIMEZONE,
)

# Configure logger
logger = logging.getLogger(__name__)


def get_current_time_cst() -> datetime.datetime:
    """
    Returns the current time in CST timezone.
    
    Returns:
        datetime.datetime: Current datetime in CST timezone
    """
    return datetime.datetime.now(TIMEZONE)


def localize_to_cst(dt: datetime.datetime) -> datetime.datetime:
    """
    Converts a datetime to CST timezone, handling both naive and aware datetimes.
    
    Args:
        dt (datetime.datetime): Datetime to convert to CST timezone
        
    Returns:
        datetime.datetime: Datetime in CST timezone
        
    Raises:
        TypeError: If dt is not a datetime object
    """
    if not isinstance(dt, datetime.datetime):
        raise TypeError("Input must be a datetime object")
    
    # Check if datetime is timezone-aware
    if dt.tzinfo is None:
        # Naive datetime, localize to CST
        logger.debug(f"Localizing naive datetime {dt} to CST")
        return TIMEZONE.localize(dt)
    else:
        # Timezone-aware datetime, convert to CST
        logger.debug(f"Converting aware datetime {dt} from {dt.tzinfo} to CST")
        return dt.astimezone(TIMEZONE)


def convert_to_utc(dt: datetime.datetime) -> datetime.datetime:
    """
    Converts a datetime to UTC timezone, handling both naive and aware datetimes.
    
    Args:
        dt (datetime.datetime): Datetime to convert to UTC timezone
        
    Returns:
        datetime.datetime: Datetime in UTC timezone
        
    Raises:
        TypeError: If dt is not a datetime object
    """
    if not isinstance(dt, datetime.datetime):
        raise TypeError("Input must be a datetime object")
    
    # Check if datetime is timezone-aware
    if dt.tzinfo is None:
        # Naive datetime, first localize to CST then convert to UTC
        cst_dt = TIMEZONE.localize(dt)
        logger.debug(f"Converting naive datetime {dt} to UTC (via CST)")
        return cst_dt.astimezone(pytz.UTC)
    else:
        # Timezone-aware datetime, convert to UTC
        logger.debug(f"Converting aware datetime {dt} from {dt.tzinfo} to UTC")
        return dt.astimezone(pytz.UTC)


def get_next_execution_time(reference_time: datetime.datetime) -> datetime.datetime:
    """
    Calculates the next forecast execution time based on FORECAST_SCHEDULE_TIME.
    
    Args:
        reference_time (datetime.datetime): Reference time to calculate from
        
    Returns:
        datetime.datetime: Next execution datetime in CST timezone
        
    Raises:
        TypeError: If reference_time is not a datetime object
    """
    if not isinstance(reference_time, datetime.datetime):
        raise TypeError("Reference time must be a datetime object")
    
    # Ensure reference_time is in CST timezone
    reference_time = localize_to_cst(reference_time)
    
    # Create a datetime for today at FORECAST_SCHEDULE_TIME
    next_time = datetime.datetime.combine(
        reference_time.date(),
        FORECAST_SCHEDULE_TIME,
        tzinfo=TIMEZONE
    )
    
    # If reference_time is after today's execution time, add one day
    if reference_time >= next_time:
        next_time = next_time + datetime.timedelta(days=1)
        
    logger.debug(f"Next execution time from {reference_time}: {next_time}")
    return next_time


def get_forecast_start_date(reference_time: datetime.datetime) -> datetime.datetime:
    """
    Determines the start date for the forecast period (beginning of next day).
    
    Args:
        reference_time (datetime.datetime): Reference time to calculate from
        
    Returns:
        datetime.datetime: Start datetime for forecast period in CST timezone
        
    Raises:
        TypeError: If reference_time is not a datetime object
    """
    if not isinstance(reference_time, datetime.datetime):
        raise TypeError("Reference time must be a datetime object")
    
    # Ensure reference_time is in CST timezone
    reference_time = localize_to_cst(reference_time)
    
    # Add one day to the reference_time
    next_day = reference_time + datetime.timedelta(days=1)
    
    # Set time to 00:00:00 (midnight)
    start_date = datetime.datetime.combine(
        next_day.date(),
        datetime.time(0, 0, 0),
        tzinfo=TIMEZONE
    )
    
    logger.debug(f"Forecast start date from {reference_time}: {start_date}")
    return start_date


def generate_forecast_datetimes(
    start_date: datetime.datetime, hours: Optional[int] = None
) -> List[datetime.datetime]:
    """
    Generates a list of hourly datetimes for the forecast period.
    
    Args:
        start_date (datetime.datetime): Start date for the forecast period
        hours (int, optional): Number of hours to generate. If not provided,
                               uses FORECAST_HORIZON_HOURS from settings
    
    Returns:
        List[datetime.datetime]: List of hourly datetimes for forecast period
        
    Raises:
        TypeError: If start_date is not a datetime object or hours is not an integer
        ValueError: If hours is negative
    """
    if not isinstance(start_date, datetime.datetime):
        raise TypeError("Start date must be a datetime object")
    
    # Use default forecast horizon if hours not provided
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    elif not isinstance(hours, int):
        raise TypeError("Hours must be an integer")
    elif hours < 0:
        raise ValueError("Hours must be non-negative")
    
    # Ensure start_date is in CST timezone
    start_date = localize_to_cst(start_date)
    
    # Generate hourly datetimes
    datetimes = [
        start_date + datetime.timedelta(hours=hour)
        for hour in range(hours)
    ]
    
    logger.debug(f"Generated {len(datetimes)} forecast datetimes from {start_date}")
    return datetimes


def generate_forecast_date_range(
    start_date: datetime.datetime, hours: Optional[int] = None
) -> pd.DatetimeIndex:
    """
    Generates a pandas DatetimeIndex for the forecast period with hourly frequency.
    
    Args:
        start_date (datetime.datetime): Start date for the forecast period
        hours (int, optional): Number of hours to generate. If not provided,
                               uses FORECAST_HORIZON_HOURS from settings
    
    Returns:
        pandas.DatetimeIndex: DatetimeIndex with hourly frequency for forecast period
        
    Raises:
        TypeError: If start_date is not a datetime object or hours is not an integer
        ValueError: If hours is negative
    """
    if not isinstance(start_date, datetime.datetime):
        raise TypeError("Start date must be a datetime object")
    
    # Use default forecast horizon if hours not provided
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    elif not isinstance(hours, int):
        raise TypeError("Hours must be an integer")
    elif hours < 0:
        raise ValueError("Hours must be non-negative")
    
    # Ensure start_date is in CST timezone
    start_date = localize_to_cst(start_date)
    
    # Calculate end_date (inclusive of the last hour)
    end_date = start_date + datetime.timedelta(hours=hours-1)
    
    # Generate pandas DatetimeIndex
    date_range = pd.date_range(
        start=start_date,
        end=end_date,
        freq='H',
        tz=TIMEZONE
    )
    
    logger.debug(f"Generated DatetimeIndex with {len(date_range)} entries from {start_date}")
    return date_range


def format_timestamp(
    dt: datetime.datetime, format_string: Optional[str] = None
) -> str:
    """
    Formats a datetime object as a string using the specified format.
    
    Args:
        dt (datetime.datetime): Datetime to format
        format_string (str, optional): Format string to use. If not provided,
                                       uses ISO format with timezone
    
    Returns:
        str: Formatted timestamp string
        
    Raises:
        TypeError: If dt is not a datetime object
    """
    if not isinstance(dt, datetime.datetime):
        raise TypeError("Input must be a datetime object")
    
    # Ensure dt is in CST timezone
    dt = localize_to_cst(dt)
    
    # Use ISO format if no format string provided
    if format_string is None:
        format_string = '%Y-%m-%dT%H:%M:%S%z'
    
    # Format the datetime
    formatted = dt.strftime(format_string)
    
    return formatted


def parse_timestamp(
    timestamp_str: str, format_string: Optional[str] = None
) -> datetime.datetime:
    """
    Parses a timestamp string into a datetime object in CST timezone.
    
    Args:
        timestamp_str (str): Timestamp string to parse
        format_string (str, optional): Format string to use for parsing.
                                      If not provided, uses pandas.to_datetime
    
    Returns:
        datetime.datetime: Parsed datetime in CST timezone
        
    Raises:
        TypeError: If timestamp_str is not a string
        ValueError: If the timestamp cannot be parsed
    """
    if not isinstance(timestamp_str, str):
        raise TypeError("Timestamp must be a string")
    
    try:
        # Parse with format string if provided
        if format_string is not None:
            dt = datetime.datetime.strptime(timestamp_str, format_string)
        else:
            # Use pandas to_datetime for flexible parsing
            dt = pd.to_datetime(timestamp_str)
            
            # Convert pandas Timestamp to datetime if necessary
            if isinstance(dt, pd.Timestamp):
                dt = dt.to_pydatetime()
        
        # Ensure the datetime is in CST timezone
        return localize_to_cst(dt)
    
    except ValueError as e:
        logger.error(f"Failed to parse timestamp {timestamp_str}: {e}")
        raise ValueError(f"Could not parse timestamp: {e}") from e


def shift_timestamps(
    df: pd.DataFrame, delta: datetime.timedelta, timestamp_column: str = 'timestamp'
) -> pd.DataFrame:
    """
    Shifts timestamps in a dataframe by a specified timedelta.
    
    Args:
        df (pandas.DataFrame): DataFrame containing timestamps
        delta (datetime.timedelta): Amount of time to shift
        timestamp_column (str, optional): Name of the timestamp column
                                         Defaults to 'timestamp'
    
    Returns:
        pandas.DataFrame: DataFrame with shifted timestamps
        
    Raises:
        TypeError: If df is not a pandas DataFrame or delta is not a timedelta
        ValueError: If timestamp_column does not exist in the dataframe
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")
    if not isinstance(delta, datetime.timedelta):
        raise TypeError("delta must be a datetime.timedelta object")
    
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Verify the timestamp column exists
    if timestamp_column not in df_copy.columns:
        raise ValueError(f"Column '{timestamp_column}' not found in DataFrame")
    
    # Shift the timestamps
    logger.debug(f"Shifting timestamps by {delta}")
    df_copy[timestamp_column] = df_copy[timestamp_column] + delta
    
    return df_copy


def get_previous_day_date(dt: datetime.datetime) -> datetime.datetime:
    """
    Returns a datetime for the previous day at the same time.
    
    Args:
        dt (datetime.datetime): Reference datetime
    
    Returns:
        datetime.datetime: Datetime for previous day in CST timezone
        
    Raises:
        TypeError: If dt is not a datetime object
    """
    if not isinstance(dt, datetime.datetime):
        raise TypeError("Input must be a datetime object")
    
    # Ensure dt is in CST timezone
    dt = localize_to_cst(dt)
    
    # Subtract one day
    previous_day = dt - datetime.timedelta(days=1)
    
    logger.debug(f"Previous day from {dt}: {previous_day}")
    return previous_day


def calculate_date_difference(
    date1: datetime.datetime, date2: datetime.datetime
) -> int:
    """
    Calculates the absolute difference in days between two dates.
    
    Args:
        date1 (datetime.datetime): First date
        date2 (datetime.datetime): Second date
    
    Returns:
        int: Absolute number of days between dates
        
    Raises:
        TypeError: If either input is not a datetime object
    """
    if not isinstance(date1, datetime.datetime) or not isinstance(date2, datetime.datetime):
        raise TypeError("Both inputs must be datetime objects")
    
    # Ensure both dates are in CST timezone
    date1 = localize_to_cst(date1)
    date2 = localize_to_cst(date2)
    
    # Calculate difference in days
    difference = (date1.date() - date2.date()).days
    
    logger.debug(f"Date difference between {date1.date()} and {date2.date()}: {abs(difference)} days")
    return abs(difference)