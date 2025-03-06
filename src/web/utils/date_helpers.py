"""
Utility module providing date and time handling functions for the web visualization component 
of the Electricity Market Price Forecasting System.

This module implements date formatting, range generation, and conversion functions to support 
consistent date handling across the dashboard interface.
"""

from datetime import datetime, date, time, timedelta
from typing import Union, Tuple, Optional

import pandas as pd  # version 2.0.0+
import pytz  # version 2023.3

from ..config.settings import (
    CST_TIMEZONE, 
    DEFAULT_DATE_FORMAT, 
    DEFAULT_TIME_FORMAT, 
    DEFAULT_DATETIME_FORMAT,
    MAX_FORECAST_DAYS
)

# Format string for Dash date components
DASH_DATE_FORMAT = 'Y-MM-DD'


def get_current_time_cst() -> datetime:
    """
    Returns the current time in CST timezone.
    
    Returns:
        datetime.datetime: Current datetime in CST timezone
    """
    return datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(CST_TIMEZONE)


def localize_to_cst(dt: datetime) -> datetime:
    """
    Converts a datetime to CST timezone, handling both naive and aware datetimes.
    
    Args:
        dt: A datetime object to convert to CST timezone
        
    Returns:
        datetime.datetime: Datetime in CST timezone
    """
    if dt.tzinfo is None:
        # Naive datetime - localize to CST
        return CST_TIMEZONE.localize(dt)
    else:
        # Timezone-aware datetime - convert to CST
        return dt.astimezone(CST_TIMEZONE)


def format_date(date_obj: Union[date, datetime], format_string: Optional[str] = None) -> str:
    """
    Formats a date object as a string using the specified format.
    
    Args:
        date_obj: Date or datetime object to format
        format_string: Format string to use (defaults to DEFAULT_DATE_FORMAT)
        
    Returns:
        str: Formatted date string
    """
    if format_string is None:
        format_string = DEFAULT_DATE_FORMAT
    
    # If a datetime object is provided, extract the date component
    if isinstance(date_obj, datetime):
        date_obj = date_obj.date()
    
    return date_obj.strftime(format_string)


def format_time(time_obj: Union[time, datetime], format_string: Optional[str] = None) -> str:
    """
    Formats a time object as a string using the specified format.
    
    Args:
        time_obj: Time or datetime object to format
        format_string: Format string to use (defaults to DEFAULT_TIME_FORMAT)
        
    Returns:
        str: Formatted time string
    """
    if format_string is None:
        format_string = DEFAULT_TIME_FORMAT
    
    # If a datetime object is provided, extract the time component
    if isinstance(time_obj, datetime):
        time_obj = time_obj.time()
    
    return time_obj.strftime(format_string)


def format_datetime(dt: datetime, format_string: Optional[str] = None) -> str:
    """
    Formats a datetime object as a string using the specified format.
    
    Args:
        dt: Datetime object to format
        format_string: Format string to use (defaults to DEFAULT_DATETIME_FORMAT)
        
    Returns:
        str: Formatted datetime string
    """
    if format_string is None:
        format_string = DEFAULT_DATETIME_FORMAT
    
    # Ensure datetime is in CST timezone
    dt = localize_to_cst(dt)
    
    return dt.strftime(format_string)


def parse_date(date_str: str, format_string: Optional[str] = None) -> date:
    """
    Parses a date string into a date object.
    
    Args:
        date_str: Date string to parse
        format_string: Format string to use for parsing (defaults to DEFAULT_DATE_FORMAT)
        
    Returns:
        datetime.date: Parsed date object
        
    Raises:
        ValueError: If the date string cannot be parsed with the given format
    """
    if format_string is None:
        format_string = DEFAULT_DATE_FORMAT
    
    return datetime.strptime(date_str, format_string).date()


def parse_datetime(datetime_str: str, format_string: Optional[str] = None) -> datetime:
    """
    Parses a datetime string into a datetime object in CST timezone.
    
    Args:
        datetime_str: Datetime string to parse
        format_string: Format string to use for parsing (defaults to DEFAULT_DATETIME_FORMAT)
        
    Returns:
        datetime.datetime: Parsed datetime in CST timezone
        
    Raises:
        ValueError: If the datetime string cannot be parsed with the given format
    """
    if format_string is None:
        format_string = DEFAULT_DATETIME_FORMAT
    
    dt = datetime.strptime(datetime_str, format_string)
    return localize_to_cst(dt)


def get_default_date_range() -> Tuple[date, date]:
    """
    Returns the default date range for forecast visualization (today and next MAX_FORECAST_DAYS).
    
    Returns:
        Tuple[datetime.date, datetime.date]: Tuple of (start_date, end_date)
    """
    current_time = get_current_time_cst()
    start_date = current_time.date()
    end_date = start_date + timedelta(days=MAX_FORECAST_DAYS)
    
    return start_date, end_date


def get_forecast_date_range(start_date: date, end_date: date) -> pd.DatetimeIndex:
    """
    Generates a pandas DatetimeIndex for the forecast period with hourly frequency.
    
    Args:
        start_date: Start date for the forecast range
        end_date: End date for the forecast range (inclusive)
        
    Returns:
        pandas.DatetimeIndex: DatetimeIndex with hourly frequency for forecast period
    """
    # Convert to datetime at start and end of day
    start_datetime = datetime.combine(start_date, time.min)
    end_datetime = datetime.combine(end_date, time(23, 59, 59))
    
    # Localize to CST timezone
    start_datetime = CST_TIMEZONE.localize(start_datetime)
    end_datetime = CST_TIMEZONE.localize(end_datetime)
    
    # Create hourly date range
    return pd.date_range(start_datetime, end_datetime, freq='H', tz=CST_TIMEZONE)


def date_to_dash_format(date_obj: Union[date, datetime, str]) -> str:
    """
    Converts a date object to the format expected by Dash date components.
    
    Args:
        date_obj: Date, datetime, or string representation of a date
        
    Returns:
        str: Date formatted for Dash components
    """
    # Handle string input
    if isinstance(date_obj, str):
        date_obj = parse_date(date_obj)
    
    # Handle datetime input
    if isinstance(date_obj, datetime):
        date_obj = date_obj.date()
    
    # Format for Dash
    return date_obj.strftime(DASH_DATE_FORMAT)


def dash_date_to_datetime(dash_date: str) -> datetime:
    """
    Converts a Dash date string to a datetime object.
    
    Args:
        dash_date: Date string in Dash format
        
    Returns:
        datetime.datetime: Datetime object in CST timezone
        
    Raises:
        ValueError: If the dash date string cannot be parsed
    """
    dt = datetime.strptime(dash_date, DASH_DATE_FORMAT)
    return localize_to_cst(dt)


def get_date_hour_label(dt: datetime) -> str:
    """
    Generates a formatted label for a date and hour (e.g., 'Jun 1, 15:00').
    
    Args:
        dt: Datetime object to format
        
    Returns:
        str: Formatted date-hour label
    """
    # Ensure datetime is in CST timezone
    dt = localize_to_cst(dt)
    
    # Format month and day
    month_day = dt.strftime('%b %d').replace(' 0', ' ')  # Remove leading zero from day
    
    # Format hour
    hour = dt.strftime('%H:%M')
    
    # Combine into label
    return f"{month_day}, {hour}"


def get_day_boundaries(date_obj: date) -> Tuple[datetime, datetime]:
    """
    Returns the start and end datetime for a given date.
    
    Args:
        date_obj: Date to get boundaries for
        
    Returns:
        Tuple[datetime.datetime, datetime.datetime]: Tuple of (start_datetime, end_datetime)
    """
    # Start of day (midnight)
    start_datetime = datetime.combine(date_obj, time.min)
    
    # End of day (23:59:59)
    end_datetime = datetime.combine(date_obj, time(23, 59, 59))
    
    # Localize to CST timezone
    start_datetime = CST_TIMEZONE.localize(start_datetime)
    end_datetime = CST_TIMEZONE.localize(end_datetime)
    
    return start_datetime, end_datetime


def is_same_day(dt1: datetime, dt2: datetime) -> bool:
    """
    Checks if two datetime objects represent the same day.
    
    Args:
        dt1: First datetime object
        dt2: Second datetime object
        
    Returns:
        bool: True if same day, False otherwise
    """
    # Ensure both datetimes are in CST timezone
    dt1 = localize_to_cst(dt1)
    dt2 = localize_to_cst(dt2)
    
    # Compare dates
    return dt1.date() == dt2.date()


def add_days(date_obj: Union[date, datetime], days: int) -> Union[date, datetime]:
    """
    Adds a specified number of days to a date or datetime.
    
    Args:
        date_obj: Date or datetime to add days to
        days: Number of days to add (can be negative)
        
    Returns:
        Union[datetime.date, datetime.datetime]: Date or datetime with days added
    """
    return date_obj + timedelta(days=days)