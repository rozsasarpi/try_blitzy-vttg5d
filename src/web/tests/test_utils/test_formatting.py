"""
Unit tests for the formatting utility module that provides formatting functions
for displaying electricity market price forecasts and related data in the 
Dash-based visualization interface.
"""

import pytest
import datetime
import pandas as pd
import pytz
from pytest import mark

from ...utils.formatting import (
    format_price, format_percentage, format_date, format_time, format_datetime,
    format_hour, format_range, format_large_number, format_with_unit,
    format_confidence_interval, truncate_string, format_tooltip_value
)
from ...config.product_config import PRODUCTS, DEFAULT_PRODUCT, get_product_unit, can_be_negative
from ...config.settings import CST_TIMEZONE


def test_format_price_with_valid_values():
    """Tests format_price function with valid price values"""
    # Test with positive integer value
    assert format_price(42, DEFAULT_PRODUCT) == "$42.00"
    
    # Test with positive float value
    assert format_price(42.75, DEFAULT_PRODUCT) == "$42.75"
    
    # Test with zero value
    assert format_price(0, DEFAULT_PRODUCT) == "$0.00"
    
    # Test with negative value for product that can be negative
    for product in PRODUCTS:
        if can_be_negative(product):
            assert format_price(-10.50, product) == "$-10.50"
            break
    
    # Test with different decimal places
    assert format_price(42.75, DEFAULT_PRODUCT, decimal_places=3) == "$42.750"
    assert format_price(42.75, DEFAULT_PRODUCT, decimal_places=0) == "$43"
    
    # Test with and without currency symbol
    assert format_price(42.75, DEFAULT_PRODUCT, include_currency=False) == "42.75"


def test_format_price_with_invalid_values():
    """Tests format_price function with None, empty string, and non-numeric values"""
    # Test with None value
    assert format_price(None, DEFAULT_PRODUCT) == "-"
    
    # Test with empty string
    assert format_price("", DEFAULT_PRODUCT) == "-"
    
    # Test with non-numeric string
    assert format_price("not a number", DEFAULT_PRODUCT) == "-"


@pytest.mark.parametrize('product_id', PRODUCTS)
def test_format_price_with_different_products(product_id):
    """Tests format_price function with different product types"""
    # Test with each product
    result = format_price(42.75, product_id)
    
    # Verify currency symbol is included if the unit contains '$'
    unit = get_product_unit(product_id)
    if '$' in unit:
        assert result.startswith('$')
    
    # Verify negative values are handled correctly based on can_be_negative
    if can_be_negative(product_id):
        assert format_price(-10.50, product_id) == "$-10.50"
    else:
        assert format_price(-10.50, product_id) == "$0.00"


def test_format_percentage_with_valid_values():
    """Tests format_percentage function with valid percentage values"""
    # Test with decimal values
    assert format_percentage(0.25) == "25.0%"
    assert format_percentage(0.5) == "50.0%"
    assert format_percentage(0.75) == "75.0%"
    
    # Test with integer values
    assert format_percentage(0) == "0.0%"
    assert format_percentage(1) == "100.0%"
    assert format_percentage(2) == "200.0%"
    
    # Test with negative values
    assert format_percentage(-0.25) == "-25.0%"
    
    # Test with different decimal places
    assert format_percentage(0.25, decimal_places=2) == "25.00%"
    assert format_percentage(0.25, decimal_places=0) == "25%"
    
    # Test with and without percentage symbol
    assert format_percentage(0.25, include_symbol=False) == "25.0"


def test_format_percentage_with_invalid_values():
    """Tests format_percentage function with None, empty string, and non-numeric values"""
    # Test with None value
    assert format_percentage(None) == "-"
    
    # Test with empty string
    assert format_percentage("") == "-"
    
    # Test with non-numeric string
    assert format_percentage("not a number") == "-"


def test_format_date_with_different_formats():
    """Tests format_date function with different date formats and input types"""
    # Test with datetime.date object
    date_obj = datetime.date(2023, 6, 1)
    assert format_date(date_obj) == "2023-06-01"
    
    # Test with datetime.datetime object
    datetime_obj = datetime.datetime(2023, 6, 1, 12, 30, 45)
    assert format_date(datetime_obj) == "2023-06-01"
    
    # Test with pandas.Timestamp object
    timestamp_obj = pd.Timestamp("2023-06-01 12:30:45")
    assert format_date(timestamp_obj) == "2023-06-01"
    
    # Test with date string in different formats
    assert format_date("2023-06-01") == "2023-06-01"
    assert format_date("2023-06-01T12:30:45") == "2023-06-01"
    
    # Test with custom format strings
    assert format_date(date_obj, format_string="%m/%d/%Y") == "06/01/2023"
    assert format_date(date_obj, format_string="%B %d, %Y") == "June 01, 2023"


def test_format_date_with_invalid_values():
    """Tests format_date function with None, empty string, and invalid date strings"""
    # Test with None value
    assert format_date(None) == "-"
    
    # Test with empty string
    assert format_date("") == ""  # Returns original string if parsing fails
    
    # Test with invalid date string
    assert format_date("not a date") == "not a date"  # Returns original string if parsing fails


def test_format_time_with_different_formats():
    """Tests format_time function with different time formats and input types"""
    # Test with datetime.time object
    time_obj = datetime.time(12, 30, 45)
    assert format_time(time_obj) == "12:30"
    
    # Test with datetime.datetime object
    datetime_obj = datetime.datetime(2023, 6, 1, 12, 30, 45)
    assert format_time(datetime_obj) == "12:30"
    
    # Test with pandas.Timestamp object
    timestamp_obj = pd.Timestamp("2023-06-01 12:30:45")
    assert format_time(timestamp_obj) == "12:30"
    
    # Test with time string in different formats
    assert format_time("12:30") == "12:30"
    assert format_time("2023-06-01T12:30:45") == "12:30"
    
    # Test with custom format strings
    assert format_time(time_obj, format_string="%H:%M:%S") == "12:30:45"
    assert format_time(time_obj, format_string="%I:%M %p") == "12:30 PM"


def test_format_time_with_invalid_values():
    """Tests format_time function with None, empty string, and invalid time strings"""
    # Test with None value
    assert format_time(None) == "-"
    
    # Test with empty string
    assert format_time("") == ""  # Returns original string if parsing fails
    
    # Test with invalid time string
    assert format_time("not a time") == "not a time"  # Returns original string if parsing fails


def test_format_datetime_with_different_formats():
    """Tests format_datetime function with different datetime formats and input types"""
    # Test with datetime.datetime object
    datetime_obj = datetime.datetime(2023, 6, 1, 12, 30, 45)
    assert format_datetime(datetime_obj) == "2023-06-01 12:30"
    
    # Test with pandas.Timestamp object
    timestamp_obj = pd.Timestamp("2023-06-01 12:30:45")
    assert format_datetime(timestamp_obj) == "2023-06-01 12:30"
    
    # Test with datetime string in different formats
    assert format_datetime("2023-06-01T12:30:45") == "2023-06-01 12:30"
    
    # Test with custom format strings
    assert format_datetime(datetime_obj, format_string="%m/%d/%Y %H:%M") == "06/01/2023 12:30"
    assert format_datetime(datetime_obj, format_string="%B %d, %Y %I:%M %p") == "June 01, 2023 12:30 PM"
    
    # Test with timezone information
    tz_datetime = datetime_obj.replace(tzinfo=pytz.UTC)
    assert format_datetime(tz_datetime, include_timezone=True) == "2023-06-01 12:30 UTC"
    
    # Test with converting to CST timezone
    tz_datetime = datetime_obj.replace(tzinfo=pytz.UTC)
    formatted = format_datetime(tz_datetime, include_timezone=True)
    assert "UTC" in formatted  # The timezone should be included


def test_format_datetime_with_invalid_values():
    """Tests format_datetime function with None, empty string, and invalid datetime strings"""
    # Test with None value
    assert format_datetime(None) == "-"
    
    # Test with empty string
    assert format_datetime("") == ""  # Returns original string if parsing fails
    
    # Test with invalid datetime string
    assert format_datetime("not a datetime") == "not a datetime"  # Returns original string if parsing fails


@pytest.mark.parametrize('hour', range(24))
def test_format_hour_with_valid_values(hour):
    """Tests format_hour function with valid hour values"""
    # Test with hour as integer
    result = format_hour(hour)
    expected = f"{hour:02d}:00"
    assert result == expected
    
    # Test with hour as string
    result = format_hour(str(hour))
    assert result == expected
    
    # Test with custom format string
    result = format_hour(hour, format_string="%H")
    assert result == f"{hour:02d}"
    
    # Test with 12-hour format
    result = format_hour(hour, format_string="%I %p")
    if hour == 0:
        assert result == "12 AM"
    elif hour < 12:
        assert result == f"{hour:02d} AM"
    elif hour == 12:
        assert result == "12 PM"
    else:
        assert result == f"{hour-12:02d} PM"


def test_format_hour_with_invalid_values():
    """Tests format_hour function with None, empty string, and out-of-range values"""
    # Test with None value
    assert format_hour(None) == "-"
    
    # Test with empty string
    assert format_hour("") == ""  # Returns original if parsing fails
    
    # Test with negative hour
    assert format_hour(-1) == "-1"  # Returns original for invalid hour
    
    # Test with hour > 23
    assert format_hour(24) == "24"  # Returns original for invalid hour
    
    # Test with non-numeric string
    assert format_hour("not an hour") == "not an hour"  # Returns original if parsing fails


def test_format_range_with_valid_values():
    """Tests format_range function with valid range values"""
    # Test with positive lower and upper bounds
    assert format_range(10.5, 20.75, DEFAULT_PRODUCT) == "$10.50 - 20.75"
    
    # Test with zero lower bound
    assert format_range(0, 20.75, DEFAULT_PRODUCT) == "$0.00 - 20.75"
    
    # Test with negative lower bound for product that can be negative
    for product in PRODUCTS:
        if can_be_negative(product):
            assert format_range(-10.5, 20.75, product) == "$-10.50 - 20.75"
            break
    
    # Test with different decimal places
    assert format_range(10.5, 20.75, DEFAULT_PRODUCT, decimal_places=3) == "$10.500 - 20.750"
    assert format_range(10.5, 20.75, DEFAULT_PRODUCT, decimal_places=0) == "$11 - 21"
    
    # Test with and without currency symbol
    assert format_range(10.5, 20.75, DEFAULT_PRODUCT, include_currency=False) == "10.50 - 20.75"


def test_format_range_with_invalid_values():
    """Tests format_range function with None, empty string, and invalid range values"""
    # Test with None for lower bound
    assert format_range(None, 20.75, DEFAULT_PRODUCT) == "-"
    
    # Test with None for upper bound
    assert format_range(10.5, None, DEFAULT_PRODUCT) == "-"
    
    # Test with None for both bounds
    assert format_range(None, None, DEFAULT_PRODUCT) == "-"
    
    # Test with upper bound less than lower bound
    # This is a valid case - function doesn't enforce order
    assert format_range(20.75, 10.5, DEFAULT_PRODUCT) == "$20.75 - 10.50"


def test_format_large_number_with_valid_values():
    """Tests format_large_number function with valid numeric values"""
    # Test with values < 1000 (no suffix)
    assert format_large_number(42) == "42"
    assert format_large_number(999.99) == "999.99"
    
    # Test with values 1000-999999 (K suffix)
    assert format_large_number(1000) == "1K"
    assert format_large_number(1500) == "1.5K"
    assert format_large_number(999999) == "999.99K"
    
    # Test with values 1000000-999999999 (M suffix)
    assert format_large_number(1000000) == "1M"
    assert format_large_number(1500000) == "1.5M"
    assert format_large_number(999999999) == "999.99M"
    
    # Test with values >= 1000000000 (B suffix)
    assert format_large_number(1000000000) == "1B"
    assert format_large_number(1500000000) == "1.5B"
    
    # Test with negative values
    assert format_large_number(-1500) == "-1.5K"
    
    # Test with different decimal places
    assert format_large_number(1500, decimal_places=3) == "1.5K"  # Still removes trailing zeros
    assert format_large_number(1500, decimal_places=0) == "2K"


def test_format_large_number_with_invalid_values():
    """Tests format_large_number function with None, empty string, and non-numeric values"""
    # Test with None value
    assert format_large_number(None) == "-"
    
    # Test with empty string
    assert format_large_number("") == "-"
    
    # Test with non-numeric string
    assert format_large_number("not a number") == "-"


def test_format_with_unit_with_valid_values():
    """Tests format_with_unit function with valid values and units"""
    # Test with positive values and different units
    assert format_with_unit(42.75, "MW") == "42.75 MW"
    assert format_with_unit(100, "kWh") == "100 kWh"
    
    # Test with zero value
    assert format_with_unit(0, "MW") == "0 MW"
    
    # Test with negative values
    assert format_with_unit(-42.75, "MW") == "-42.75 MW"
    
    # Test with different decimal places
    assert format_with_unit(42.75, "MW", decimal_places=3) == "42.75 MW"  # Removes trailing zeros
    assert format_with_unit(42.75, "MW", decimal_places=0) == "43 MW"


def test_format_with_unit_with_invalid_values():
    """Tests format_with_unit function with None, empty string, and non-numeric values"""
    # Test with None value
    assert format_with_unit(None, "MW") == "-"
    
    # Test with empty string
    assert format_with_unit("", "MW") == "-"
    
    # Test with non-numeric string
    assert format_with_unit("not a number", "MW") == "-"


def test_format_confidence_interval_with_valid_values():
    """Tests format_confidence_interval function with valid confidence interval bounds"""
    # Test with positive lower and upper bounds
    assert format_confidence_interval(10.5, 20.75, DEFAULT_PRODUCT) == "$10.50 to 20.75"
    
    # Test with zero lower bound
    assert format_confidence_interval(0, 20.75, DEFAULT_PRODUCT) == "$0.00 to 20.75"
    
    # Test with negative lower bound for product that can be negative
    for product in PRODUCTS:
        if can_be_negative(product):
            assert format_confidence_interval(-10.5, 20.75, product) == "$-10.50 to 20.75"
            break
    
    # Test with different decimal places
    assert format_confidence_interval(10.5, 20.75, DEFAULT_PRODUCT, decimal_places=3) == "$10.500 to 20.750"
    assert format_confidence_interval(10.5, 20.75, DEFAULT_PRODUCT, decimal_places=0) == "$11 to 21"
    
    # Test with and without currency symbol
    assert format_confidence_interval(10.5, 20.75, DEFAULT_PRODUCT, include_currency=False) == "10.50 to 20.75"


def test_format_confidence_interval_with_invalid_values():
    """Tests format_confidence_interval function with None and invalid confidence interval bounds"""
    # Test with None for lower bound
    assert format_confidence_interval(None, 20.75, DEFAULT_PRODUCT) == "-"
    
    # Test with None for upper bound
    assert format_confidence_interval(10.5, None, DEFAULT_PRODUCT) == "-"
    
    # Test with None for both bounds
    assert format_confidence_interval(None, None, DEFAULT_PRODUCT) == "-"


def test_truncate_string_with_valid_values():
    """Tests truncate_string function with valid strings and max lengths"""
    # Test with string shorter than max length
    assert truncate_string("test", 10) == "test"
    
    # Test with string equal to max length
    assert truncate_string("test", 4) == "test"
    
    # Test with string longer than max length
    assert truncate_string("test string", 7) == "test..."
    
    # Test with very short max length
    assert truncate_string("test", 1) == "..."
    assert truncate_string("test", 3) == "..."
    assert truncate_string("test", 4) == "test"


def test_truncate_string_with_invalid_values():
    """Tests truncate_string function with None, empty string, and invalid max length"""
    # Test with None string
    assert truncate_string(None, 10) == ""
    
    # Test with empty string
    assert truncate_string("", 10) == ""
    
    # Test with negative max length
    assert truncate_string("test", -1) == "test"  # Negative max length behaves like unlimited
    
    # Test with zero max length
    assert truncate_string("test", 0) == "..."


def test_format_tooltip_value_with_valid_values():
    """Tests format_tooltip_value function with valid values"""
    # Test with positive values
    assert format_tooltip_value(42, DEFAULT_PRODUCT) == "$42.000"  # Note extra decimal place
    
    # Test with zero value
    assert format_tooltip_value(0, DEFAULT_PRODUCT) == "$0.000"
    
    # Test with negative value for product that can be negative
    for product in PRODUCTS:
        if can_be_negative(product):
            assert format_tooltip_value(-10.5, product) == "$-10.500"
            break
    
    # Test with different decimal places
    assert format_tooltip_value(42, DEFAULT_PRODUCT, decimal_places=4) == "$42.0000"
    assert format_tooltip_value(42, DEFAULT_PRODUCT, decimal_places=1) == "$42.0"


def test_format_tooltip_value_with_invalid_values():
    """Tests format_tooltip_value function with None, empty string, and non-numeric values"""
    # Test with None value
    assert format_tooltip_value(None, DEFAULT_PRODUCT) == "-"
    
    # Test with empty string
    assert format_tooltip_value("", DEFAULT_PRODUCT) == "-"
    
    # Test with non-numeric string
    assert format_tooltip_value("not a number", DEFAULT_PRODUCT) == "-"