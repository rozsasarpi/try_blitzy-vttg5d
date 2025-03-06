"""
Utility module providing formatting functions for displaying electricity market price forecasts
and related data in the Dash-based visualization interface. This module contains functions for
formatting prices, dates, times, percentages, and other values consistently throughout the dashboard.
"""

import datetime
import locale
from typing import Union

import pandas as pd  # version 2.0.0+

from ..config.product_config import get_product_unit, can_be_negative
from ..config.settings import CST_TIMEZONE

# Default formatting constants
DEFAULT_DECIMAL_PLACES = 2
DEFAULT_CURRENCY_SYMBOL = '$'
DEFAULT_PERCENTAGE_DECIMAL_PLACES = 1
DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_TIME_FORMAT = '%H:%M'
DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M'
DEFAULT_HOUR_FORMAT = '%H:00'


def format_price(
    value: Union[float, int, str, None],
    product_id: str,
    decimal_places: int = None,
    include_currency: bool = True
) -> str:
    """
    Formats a price value with appropriate currency symbol and decimal places.
    
    Args:
        value: The price value to format
        product_id: The product identifier to determine formatting rules
        decimal_places: Number of decimal places (defaults to DEFAULT_DECIMAL_PLACES)
        include_currency: Whether to include the currency symbol
        
    Returns:
        Formatted price string
    """
    if value is None or value == '':
        return '-'
    
    # Convert string to float if needed
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return '-'
    
    # Check if the product can have negative prices
    if not can_be_negative(product_id) and value < 0:
        value = 0  # Set negative prices to 0 for products that can't have negative prices
    
    # Use default decimal places if not specified
    if decimal_places is None:
        decimal_places = DEFAULT_DECIMAL_PLACES
        
    # Get currency symbol and unit
    currency_symbol = DEFAULT_CURRENCY_SYMBOL
    unit = get_product_unit(product_id)
    
    # Format the value
    formatted_value = f"{value:.{decimal_places}f}"
    
    # Add currency symbol if needed
    if include_currency and '$' in unit:
        return f"{currency_symbol}{formatted_value}"
    else:
        return formatted_value


def format_percentage(
    value: Union[float, int, str, None],
    decimal_places: int = None,
    include_symbol: bool = True
) -> str:
    """
    Formats a value as a percentage with specified decimal places.
    
    Args:
        value: The value to format as percentage (0.01 = 1%)
        decimal_places: Number of decimal places (defaults to DEFAULT_PERCENTAGE_DECIMAL_PLACES)
        include_symbol: Whether to include the % symbol
        
    Returns:
        Formatted percentage string
    """
    if value is None or value == '':
        return '-'
    
    # Convert string to float if needed
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return '-'
    
    # Use default decimal places if not specified
    if decimal_places is None:
        decimal_places = DEFAULT_PERCENTAGE_DECIMAL_PLACES
    
    # Convert to percentage (multiply by 100)
    percentage_value = value * 100
    
    # Format the value
    formatted_value = f"{percentage_value:.{decimal_places}f}"
    
    # Add percentage symbol if needed
    if include_symbol:
        return f"{formatted_value}%"
    else:
        return formatted_value


def format_date(
    date: Union[str, datetime.date, datetime.datetime, pd.Timestamp, None],
    format_string: str = None
) -> str:
    """
    Formats a date object or string into a consistent date format.
    
    Args:
        date: The date to format
        format_string: The format string to use (defaults to DEFAULT_DATE_FORMAT)
        
    Returns:
        Formatted date string
    """
    if date is None:
        return '-'
    
    # Convert string to datetime if needed
    if isinstance(date, str):
        try:
            date = datetime.datetime.strptime(date, DEFAULT_DATE_FORMAT)
        except ValueError:
            try:
                date = datetime.datetime.fromisoformat(date)
            except ValueError:
                return date  # Return original if parsing fails
    
    # Convert pandas Timestamp to datetime if needed
    if isinstance(date, pd.Timestamp):
        date = date.to_pydatetime()
    
    # Use default format if not specified
    if format_string is None:
        format_string = DEFAULT_DATE_FORMAT
    
    # Format the date
    try:
        return date.strftime(format_string)
    except (AttributeError, ValueError):
        return str(date)


def format_time(
    time: Union[str, datetime.time, datetime.datetime, pd.Timestamp, None],
    format_string: str = None
) -> str:
    """
    Formats a time object or string into a consistent time format.
    
    Args:
        time: The time to format
        format_string: The format string to use (defaults to DEFAULT_TIME_FORMAT)
        
    Returns:
        Formatted time string
    """
    if time is None:
        return '-'
    
    # Convert string to datetime if needed
    if isinstance(time, str):
        try:
            time = datetime.datetime.strptime(time, DEFAULT_TIME_FORMAT)
        except ValueError:
            try:
                time = datetime.datetime.fromisoformat(time)
            except ValueError:
                return time  # Return original if parsing fails
    
    # Convert pandas Timestamp to datetime if needed
    if isinstance(time, pd.Timestamp):
        time = time.to_pydatetime()
    
    # Use default format if not specified
    if format_string is None:
        format_string = DEFAULT_TIME_FORMAT
    
    # Format the time
    try:
        if isinstance(time, datetime.datetime):
            return time.strftime(format_string)
        elif isinstance(time, datetime.time):
            return time.strftime(format_string)
        else:
            return str(time)
    except (AttributeError, ValueError):
        return str(time)


def format_datetime(
    dt: Union[str, datetime.datetime, pd.Timestamp, None],
    format_string: str = None,
    include_timezone: bool = False
) -> str:
    """
    Formats a datetime object or string into a consistent datetime format.
    
    Args:
        dt: The datetime to format
        format_string: The format string to use (defaults to DEFAULT_DATETIME_FORMAT)
        include_timezone: Whether to include timezone information
        
    Returns:
        Formatted datetime string
    """
    if dt is None:
        return '-'
    
    # Convert string to datetime if needed
    if isinstance(dt, str):
        try:
            dt = datetime.datetime.fromisoformat(dt)
        except ValueError:
            try:
                dt = datetime.datetime.strptime(dt, DEFAULT_DATETIME_FORMAT)
            except ValueError:
                return dt  # Return original if parsing fails
    
    # Convert pandas Timestamp to datetime if needed
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    
    # Use default format if not specified
    if format_string is None:
        format_string = DEFAULT_DATETIME_FORMAT
    
    # Convert to CST timezone if include_timezone is True
    if include_timezone and dt.tzinfo is not None:
        dt = dt.astimezone(CST_TIMEZONE)
    
    # Format the datetime
    try:
        formatted_dt = dt.strftime(format_string)
        if include_timezone and dt.tzinfo is not None:
            tz_abbr = dt.strftime('%Z')
            return f"{formatted_dt} {tz_abbr}"
        return formatted_dt
    except (AttributeError, ValueError):
        return str(dt)


def format_hour(
    hour: Union[int, str, None],
    format_string: str = None
) -> str:
    """
    Formats an hour value (0-23) as a time string.
    
    Args:
        hour: The hour value to format
        format_string: The format string to use (defaults to DEFAULT_HOUR_FORMAT)
        
    Returns:
        Formatted hour string
    """
    if hour is None:
        return '-'
    
    # Convert string to int if needed
    if isinstance(hour, str):
        try:
            hour = int(hour)
        except ValueError:
            return hour  # Return original if parsing fails
    
    # Validate hour range
    if not (0 <= hour <= 23):
        return str(hour)  # Return original if invalid
    
    # Use default format if not specified
    if format_string is None:
        format_string = DEFAULT_HOUR_FORMAT
    
    # Create time object for the hour
    time_obj = datetime.time(hour=hour)
    
    # Format the hour
    return time_obj.strftime(format_string)


def format_range(
    lower: Union[float, int, None],
    upper: Union[float, int, None],
    product_id: str,
    decimal_places: int = None,
    include_currency: bool = True
) -> str:
    """
    Formats a range of values with appropriate precision.
    
    Args:
        lower: The lower bound of the range
        upper: The upper bound of the range
        product_id: The product identifier to determine formatting rules
        decimal_places: Number of decimal places (defaults to DEFAULT_DECIMAL_PLACES)
        include_currency: Whether to include the currency symbol
        
    Returns:
        Formatted range string
    """
    if lower is None or upper is None:
        return '-'
    
    # Format lower and upper values
    formatted_lower = format_price(
        lower, product_id, decimal_places, include_currency
    )
    
    # Don't include currency for upper value in range
    formatted_upper = format_price(
        upper, product_id, decimal_places, False
    )
    
    return f"{formatted_lower} - {formatted_upper}"


def format_large_number(
    value: Union[float, int, str, None],
    decimal_places: int = None
) -> str:
    """
    Formats large numbers with appropriate suffixes (K, M, B).
    
    Args:
        value: The number to format
        decimal_places: Number of decimal places (defaults to DEFAULT_DECIMAL_PLACES)
        
    Returns:
        Formatted number with suffix
    """
    if value is None or value == '':
        return '-'
    
    # Convert string to float if needed
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return '-'
    
    # Use default decimal places if not specified
    if decimal_places is None:
        decimal_places = DEFAULT_DECIMAL_PLACES
    
    # Determine appropriate suffix and scale factor
    if abs(value) >= 1_000_000_000:
        suffix = 'B'
        scale = 1_000_000_000
    elif abs(value) >= 1_000_000:
        suffix = 'M'
        scale = 1_000_000
    elif abs(value) >= 1_000:
        suffix = 'K'
        scale = 1_000
    else:
        suffix = ''
        scale = 1
    
    # Scale the value and format with appropriate precision
    scaled_value = value / scale
    formatted_value = f"{scaled_value:.{decimal_places}f}"
    
    # Remove trailing zeros after decimal point
    if '.' in formatted_value:
        formatted_value = formatted_value.rstrip('0').rstrip('.')
    
    return f"{formatted_value}{suffix}"


def format_with_unit(
    value: Union[float, int, str, None],
    unit: str,
    decimal_places: int = None
) -> str:
    """
    Formats a value with its appropriate unit of measurement.
    
    Args:
        value: The value to format
        unit: The unit of measurement
        decimal_places: Number of decimal places (defaults to DEFAULT_DECIMAL_PLACES)
        
    Returns:
        Formatted value with unit
    """
    if value is None or value == '':
        return '-'
    
    # Convert string to float if needed
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return '-'
    
    # Use default decimal places if not specified
    if decimal_places is None:
        decimal_places = DEFAULT_DECIMAL_PLACES
    
    # Format the value
    formatted_value = f"{value:.{decimal_places}f}"
    
    # Remove trailing zeros after decimal point
    if '.' in formatted_value:
        formatted_value = formatted_value.rstrip('0').rstrip('.')
    
    # Combine value with unit
    return f"{formatted_value} {unit}"


def format_confidence_interval(
    lower: Union[float, int, None],
    upper: Union[float, int, None],
    product_id: str,
    decimal_places: int = None,
    include_currency: bool = True
) -> str:
    """
    Formats a confidence interval with lower and upper bounds.
    
    Args:
        lower: The lower bound of the confidence interval
        upper: The upper bound of the confidence interval
        product_id: The product identifier to determine formatting rules
        decimal_places: Number of decimal places (defaults to DEFAULT_DECIMAL_PLACES)
        include_currency: Whether to include the currency symbol
        
    Returns:
        Formatted confidence interval string
    """
    if lower is None or upper is None:
        return '-'
    
    # Format lower and upper values
    formatted_lower = format_price(
        lower, product_id, decimal_places, include_currency
    )
    
    # Don't include currency for upper value in confidence interval
    formatted_upper = format_price(
        upper, product_id, decimal_places, False
    )
    
    return f"{formatted_lower} to {formatted_upper}"


def truncate_string(text: str, max_length: int) -> str:
    """
    Truncates a string to a specified length with ellipsis.
    
    Args:
        text: The string to truncate
        max_length: Maximum length of the truncated string including ellipsis
        
    Returns:
        Truncated string
    """
    if text is None:
        return ''
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length-3] + '...'


def format_tooltip_value(
    value: Union[float, int, str, None],
    product_id: str,
    decimal_places: int = None
) -> str:
    """
    Formats a value for display in a tooltip with appropriate precision.
    
    Args:
        value: The value to format
        product_id: The product identifier to determine formatting rules
        decimal_places: Number of decimal places (defaults to one more than DEFAULT_DECIMAL_PLACES)
        
    Returns:
        Formatted tooltip value
    """
    # Use one more decimal place than default for tooltips if not specified
    if decimal_places is None:
        decimal_places = DEFAULT_DECIMAL_PLACES + 1
    
    return format_price(value, product_id, decimal_places, True)