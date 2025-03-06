"""
Unit tests for the date_helpers utility module which provides date and time handling functions
for the web visualization component of the Electricity Market Price Forecasting System.
"""

import pytest
from datetime import datetime, date, time, timedelta
import pytz  # version 2023.3
from freezegun import freeze_time  # version 1.2.0+
import pandas as pd  # version 2.0.0+

from ...utils.date_helpers import (
    get_current_time_cst,
    localize_to_cst,
    format_date,
    format_time,
    format_datetime,
    parse_date,
    parse_datetime,
    get_default_date_range,
    get_forecast_date_range,
    date_to_dash_format,
    dash_date_to_datetime,
    get_date_hour_label,
    get_day_boundaries,
    is_same_day,
    add_days,
)
from ...config.settings import (
    CST_TIMEZONE, 
    DEFAULT_DATE_FORMAT, 
    DEFAULT_TIME_FORMAT, 
    DEFAULT_DATETIME_FORMAT,
    MAX_FORECAST_DAYS
)

# Constants for testing
DASH_DATE_FORMAT = 'Y-MM-DD'
TEST_DATE_STR = '2023-06-01'
TEST_DATETIME_STR = '2023-06-01 15:30:45'


@freeze_time('2023-06-01 12:00:00')
def test_get_current_time_cst():
    """Tests that get_current_time_cst returns the current time in CST timezone."""
    current_time = get_current_time_cst()
    
    # Verify the time is timezone-aware and in CST
    assert current_time.tzinfo is not None
    assert current_time.tzinfo.zone == CST_TIMEZONE.zone
    
    # Check that the time components match expectations
    # Note: The actual hour will depend on CST offset from UTC
    # For CST (UTC-6), 12:00 UTC would be 06:00 CST
    cst_offset = datetime.now(CST_TIMEZONE).utcoffset().total_seconds() / 3600
    expected_hour = (12 + cst_offset) % 24
    
    assert current_time.year == 2023
    assert current_time.month == 6
    assert current_time.day == 1
    assert current_time.hour == int(expected_hour)


def test_localize_to_cst_naive():
    """Tests that localize_to_cst correctly converts a naive datetime to CST timezone."""
    naive_dt = datetime(2023, 6, 1, 12, 30, 45)
    cst_dt = localize_to_cst(naive_dt)
    
    # Verify the datetime is now timezone-aware and in CST
    assert cst_dt.tzinfo is not None
    assert cst_dt.tzinfo.zone == CST_TIMEZONE.zone
    
    # Verify the datetime components remain unchanged
    assert cst_dt.year == 2023
    assert cst_dt.month == 6
    assert cst_dt.day == 1
    assert cst_dt.hour == 12
    assert cst_dt.minute == 30
    assert cst_dt.second == 45


def test_localize_to_cst_aware():
    """Tests that localize_to_cst correctly converts an aware datetime from another timezone to CST."""
    # Create a UTC datetime
    utc_dt = datetime(2023, 6, 1, 12, 30, 45, tzinfo=pytz.UTC)
    cst_dt = localize_to_cst(utc_dt)
    
    # Verify the datetime is timezone-aware and in CST
    assert cst_dt.tzinfo is not None
    assert cst_dt.tzinfo.zone == CST_TIMEZONE.zone
    
    # Verify the datetime represents the same moment in time
    # UTC to CST conversion (UTC-6): 12:30 UTC -> 06:30 CST
    utc_to_cst_offset = CST_TIMEZONE.utcoffset(utc_dt).total_seconds() / 3600
    expected_hour = (utc_dt.hour + utc_to_cst_offset) % 24
    
    assert cst_dt.year == 2023
    assert cst_dt.month == 6
    assert cst_dt.day == 1
    assert cst_dt.hour == int(expected_hour)
    assert cst_dt.minute == 30
    assert cst_dt.second == 45


def test_format_date():
    """Tests that format_date correctly formats a date object as a string."""
    test_date = date(2023, 6, 1)
    
    # Test with default format
    result = format_date(test_date)
    expected = test_date.strftime(DEFAULT_DATE_FORMAT)
    assert result == expected
    
    # Test with custom format
    custom_format = '%d/%m/%Y'
    result = format_date(test_date, custom_format)
    expected = test_date.strftime(custom_format)
    assert result == expected


def test_format_date_with_datetime():
    """Tests that format_date correctly handles datetime objects by extracting the date component."""
    test_datetime = datetime(2023, 6, 1, 15, 30, 45)
    
    # Test with default format
    result = format_date(test_datetime)
    expected = test_datetime.date().strftime(DEFAULT_DATE_FORMAT)
    assert result == expected


def test_format_time():
    """Tests that format_time correctly formats a time object as a string."""
    test_time = time(15, 30, 45)
    
    # Test with default format
    result = format_time(test_time)
    expected = test_time.strftime(DEFAULT_TIME_FORMAT)
    assert result == expected
    
    # Test with custom format
    custom_format = '%I:%M %p'
    result = format_time(test_time, custom_format)
    expected = test_time.strftime(custom_format)
    assert result == expected


def test_format_time_with_datetime():
    """Tests that format_time correctly handles datetime objects by extracting the time component."""
    test_datetime = datetime(2023, 6, 1, 15, 30, 45)
    
    # Test with default format
    result = format_time(test_datetime)
    expected = test_datetime.time().strftime(DEFAULT_TIME_FORMAT)
    assert result == expected


def test_format_datetime():
    """Tests that format_datetime correctly formats a datetime object as a string."""
    test_datetime = datetime(2023, 6, 1, 15, 30, 45)
    
    # Test with default format
    result = format_datetime(test_datetime)
    # Ensure datetime is in CST timezone for formatting
    expected_dt = localize_to_cst(test_datetime)
    expected = expected_dt.strftime(DEFAULT_DATETIME_FORMAT)
    assert result == expected
    
    # Test with custom format
    custom_format = '%d/%m/%Y %I:%M %p'
    result = format_datetime(test_datetime, custom_format)
    expected = expected_dt.strftime(custom_format)
    assert result == expected


def test_parse_date():
    """Tests that parse_date correctly parses a date string into a date object."""
    # Test with default format
    date_str = '2023-06-01'  # Using DEFAULT_DATE_FORMAT
    result = parse_date(date_str)
    expected = date(2023, 6, 1)
    assert result == expected
    
    # Test with custom format
    date_str = '01/06/2023'
    custom_format = '%d/%m/%Y'
    result = parse_date(date_str, custom_format)
    expected = date(2023, 6, 1)
    assert result == expected


def test_parse_datetime():
    """Tests that parse_datetime correctly parses a datetime string into a datetime object in CST timezone."""
    # Test with default format
    datetime_str = '2023-06-01 15:30:45'  # Using DEFAULT_DATETIME_FORMAT
    result = parse_datetime(datetime_str)
    
    # Verify the datetime is timezone-aware and in CST
    assert result.tzinfo is not None
    assert result.tzinfo.zone == CST_TIMEZONE.zone
    
    # Verify the datetime components
    assert result.year == 2023
    assert result.month == 6
    assert result.day == 1
    assert result.hour == 15
    assert result.minute == 30
    assert result.second == 45
    
    # Test with custom format
    datetime_str = '01/06/2023 03:30 PM'
    custom_format = '%d/%m/%Y %I:%M %p'
    result = parse_datetime(datetime_str, custom_format)
    
    # Verify the datetime is timezone-aware and in CST
    assert result.tzinfo is not None
    assert result.tzinfo.zone == CST_TIMEZONE.zone
    
    # Verify the datetime components
    assert result.year == 2023
    assert result.month == 6
    assert result.day == 1
    assert result.hour == 15
    assert result.minute == 30
    assert result.second == 0  # No seconds in the format


@freeze_time('2023-06-01 12:00:00')
def test_get_default_date_range():
    """Tests that get_default_date_range returns the expected date range based on current date and MAX_FORECAST_DAYS."""
    start_date, end_date = get_default_date_range()
    
    # Verify start date is current date
    assert start_date == date(2023, 6, 1)
    
    # Verify end date is current date + MAX_FORECAST_DAYS
    assert end_date == date(2023, 6, 1) + timedelta(days=MAX_FORECAST_DAYS)


def test_get_forecast_date_range():
    """Tests that get_forecast_date_range generates a pandas DatetimeIndex with hourly frequency for the specified date range."""
    start_date = date(2023, 6, 1)
    end_date = date(2023, 6, 3)
    
    date_range = get_forecast_date_range(start_date, end_date)
    
    # Verify the result is a pandas DatetimeIndex
    assert isinstance(date_range, pd.DatetimeIndex)
    
    # Verify the timezone
    assert date_range.tz == CST_TIMEZONE
    
    # Verify the frequency is hourly
    assert date_range.freq == pd.tseries.frequencies.to_offset('H')
    
    # Verify the first timestamp is at midnight on start_date
    assert date_range[0].date() == start_date
    assert date_range[0].hour == 0
    assert date_range[0].minute == 0
    
    # Verify the last timestamp is at 23:00 on end_date
    assert date_range[-1].date() == end_date
    assert date_range[-1].hour == 23
    assert date_range[-1].minute == 0
    
    # Verify the number of timestamps (3 days * 24 hours = 72 timestamps)
    assert len(date_range) == 72


def test_date_to_dash_format():
    """Tests that date_to_dash_format correctly converts a date object to the format expected by Dash date components."""
    # Test with date object
    test_date = date(2023, 6, 1)
    result = date_to_dash_format(test_date)
    expected = '2023-06-01'  # DASH_DATE_FORMAT: 'Y-MM-DD'
    assert result == expected
    
    # Test with datetime object
    test_datetime = datetime(2023, 6, 1, 15, 30, 45)
    result = date_to_dash_format(test_datetime)
    expected = '2023-06-01'
    assert result == expected
    
    # Test with date string
    date_str = '2023-06-01'
    result = date_to_dash_format(date_str)
    expected = '2023-06-01'
    assert result == expected


def test_dash_date_to_datetime():
    """Tests that dash_date_to_datetime correctly converts a Dash date string to a datetime object in CST timezone."""
    dash_date = '2023-06-01'
    result = dash_date_to_datetime(dash_date)
    
    # Verify the datetime is timezone-aware and in CST
    assert result.tzinfo is not None
    assert result.tzinfo.zone == CST_TIMEZONE.zone
    
    # Verify the datetime components
    assert result.year == 2023
    assert result.month == 6
    assert result.day == 1
    assert result.hour == 0  # Midnight
    assert result.minute == 0
    assert result.second == 0


def test_get_date_hour_label():
    """Tests that get_date_hour_label generates a formatted label for a date and hour."""
    test_datetime = datetime(2023, 6, 1, 15, 30, 45)
    result = get_date_hour_label(test_datetime)
    
    # Expected format: "Jun 1, 15:30"
    expected = "Jun 1, 15:30"
    assert result == expected


def test_get_day_boundaries():
    """Tests that get_day_boundaries returns the start and end datetime for a given date."""
    test_date = date(2023, 6, 1)
    start_dt, end_dt = get_day_boundaries(test_date)
    
    # Verify both datetimes are timezone-aware and in CST
    assert start_dt.tzinfo is not None
    assert end_dt.tzinfo is not None
    assert start_dt.tzinfo.zone == CST_TIMEZONE.zone
    assert end_dt.tzinfo.zone == CST_TIMEZONE.zone
    
    # Verify start datetime is at midnight
    assert start_dt.year == 2023
    assert start_dt.month == 6
    assert start_dt.day == 1
    assert start_dt.hour == 0
    assert start_dt.minute == 0
    assert start_dt.second == 0
    
    # Verify end datetime is at end of day
    assert end_dt.year == 2023
    assert end_dt.month == 6
    assert end_dt.day == 1
    assert end_dt.hour == 23
    assert end_dt.minute == 59
    assert end_dt.second == 59


def test_is_same_day():
    """Tests that is_same_day correctly identifies when two datetimes represent the same day."""
    # Test two datetimes on the same day
    dt1 = datetime(2023, 6, 1, 10, 30, 0)
    dt2 = datetime(2023, 6, 1, 15, 45, 0)
    assert is_same_day(dt1, dt2) is True
    
    # Test two datetimes on different days
    dt3 = datetime(2023, 6, 2, 10, 30, 0)
    assert is_same_day(dt1, dt3) is False
    
    # Test datetimes in different timezones
    # Late night UTC might be previous day in CST
    utc_dt = datetime(2023, 6, 2, 3, 0, 0, tzinfo=pytz.UTC)  # 3 AM UTC
    cst_dt = localize_to_cst(datetime(2023, 6, 1, 22, 0, 0))  # 10 PM CST
    
    # These might represent the same moment in time but different calendar days
    # When both are converted to CST, they should be on the same day
    utc_as_cst = utc_dt.astimezone(CST_TIMEZONE)
    expected_same_day = utc_as_cst.date() == cst_dt.date()
    
    assert is_same_day(utc_dt, cst_dt) == expected_same_day


def test_add_days():
    """Tests that add_days correctly adds a specified number of days to a date or datetime."""
    # Test with date object
    test_date = date(2023, 6, 1)
    result = add_days(test_date, 5)
    expected = date(2023, 6, 6)
    assert result == expected
    
    # Test with datetime object
    test_datetime = datetime(2023, 6, 1, 15, 30, 45)
    result = add_days(test_datetime, 5)
    expected = datetime(2023, 6, 6, 15, 30, 45)
    assert result == expected
    
    # Test with negative days (subtraction)
    result = add_days(test_date, -5)
    expected = date(2023, 5, 27)
    assert result == expected


def test_edge_cases():
    """Tests edge cases for date helper functions."""
    # Test parse_date with invalid date string
    with pytest.raises(ValueError):
        parse_date("not a date")
    
    # Test parse_datetime with invalid datetime string
    with pytest.raises(ValueError):
        parse_datetime("not a datetime")
    
    # Test format_date with None
    with pytest.raises(TypeError):
        format_date(None)
    
    # Test localize_to_cst with None
    with pytest.raises(TypeError):
        localize_to_cst(None)