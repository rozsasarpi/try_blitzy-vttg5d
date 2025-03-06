"""
Unit tests for the date_utils module, which provides date and time handling functions for the 
Electricity Market Price Forecasting System. Tests timezone conversions, forecast date range 
generation, timestamp formatting, and other date-related operations.
"""

import datetime

import pandas as pd
import pytest
import pytz
from freezegun import freeze_time  # version: 1.2.0+

from ../../utils.date_utils import (
    calculate_date_difference,
    convert_to_utc,
    format_timestamp,
    generate_forecast_date_range,
    generate_forecast_datetimes,
    get_current_time_cst,
    get_forecast_start_date,
    get_next_execution_time,
    get_previous_day_date,
    localize_to_cst,
    parse_timestamp,
    shift_timestamps,
)
from ../../config.settings import (
    FORECAST_HORIZON_HOURS,
    FORECAST_SCHEDULE_TIME,
    TIMEZONE,
)


def test_get_current_time_cst():
    """Tests that get_current_time_cst returns a timezone-aware datetime in CST."""
    current_time = get_current_time_cst()
    
    # Check that the datetime is timezone-aware
    assert current_time.tzinfo is not None
    
    # Check that the timezone is CST or CDT (depending on daylight saving)
    assert 'CST' in str(current_time.tzinfo) or 'CDT' in str(current_time.tzinfo)


def test_localize_to_cst_naive():
    """Tests that localize_to_cst correctly converts naive datetime to CST."""
    # Create a naive datetime
    naive_dt = datetime.datetime(2023, 6, 1, 12, 0, 0)
    
    # Convert to CST
    cst_dt = localize_to_cst(naive_dt)
    
    # Check that the datetime is now timezone-aware
    assert cst_dt.tzinfo is not None
    
    # Check that the timezone is CST or CDT
    assert 'CST' in str(cst_dt.tzinfo) or 'CDT' in str(cst_dt.tzinfo)
    
    # Check that the time values are preserved
    assert cst_dt.hour == naive_dt.hour
    assert cst_dt.minute == naive_dt.minute
    assert cst_dt.second == naive_dt.second


def test_localize_to_cst_aware():
    """Tests that localize_to_cst correctly converts timezone-aware datetime to CST."""
    # Create a timezone-aware datetime in UTC
    utc_dt = datetime.datetime(2023, 6, 1, 12, 0, 0, tzinfo=pytz.UTC)
    
    # Convert to CST
    cst_dt = localize_to_cst(utc_dt)
    
    # Check that the datetime is timezone-aware
    assert cst_dt.tzinfo is not None
    
    # Check that the timezone is CST or CDT
    assert 'CST' in str(cst_dt.tzinfo) or 'CDT' in str(cst_dt.tzinfo)
    
    # Check that the hour is adjusted correctly for timezone difference
    # UTC to CST is typically -5 or -6 hours depending on daylight saving
    assert cst_dt.hour in [utc_dt.hour - 5, utc_dt.hour - 6]


def test_convert_to_utc_naive():
    """Tests that convert_to_utc correctly converts naive datetime to UTC."""
    # Create a naive datetime
    naive_dt = datetime.datetime(2023, 6, 1, 12, 0, 0)
    
    # Convert to UTC
    utc_dt = convert_to_utc(naive_dt)
    
    # Check that the datetime is now timezone-aware
    assert utc_dt.tzinfo is not None
    
    # Check that the timezone is UTC
    assert utc_dt.tzinfo == pytz.UTC
    
    # Check that the hour is adjusted correctly for timezone difference
    # CST to UTC is typically +5 or +6 hours depending on daylight saving
    # First get CST version to compare
    cst_dt = localize_to_cst(naive_dt)
    assert utc_dt.hour in [cst_dt.hour + 5, cst_dt.hour + 6]


def test_convert_to_utc_aware():
    """Tests that convert_to_utc correctly converts timezone-aware datetime to UTC."""
    # Create a timezone-aware datetime in CST
    cst_dt = datetime.datetime(2023, 6, 1, 12, 0, 0, tzinfo=TIMEZONE)
    
    # Convert to UTC
    utc_dt = convert_to_utc(cst_dt)
    
    # Check that the datetime is timezone-aware
    assert utc_dt.tzinfo is not None
    
    # Check that the timezone is UTC
    assert utc_dt.tzinfo == pytz.UTC
    
    # Check that the hour is adjusted correctly for timezone difference
    # CST to UTC is typically +5 or +6 hours depending on daylight saving
    assert utc_dt.hour in [cst_dt.hour + 5, cst_dt.hour + 6]


@freeze_time('2023-06-01 06:00:00', tz_offset=-6)
def test_get_next_execution_time_before():
    """Tests that get_next_execution_time returns correct time when reference time is before execution time."""
    # Get current time (frozen at 6 AM CST)
    current_time = get_current_time_cst()
    
    # Get next execution time
    next_time = get_next_execution_time(current_time)
    
    # Check that next execution time is on the same day (today)
    assert next_time.date() == current_time.date()
    
    # Check that the hour is set to FORECAST_SCHEDULE_TIME (7 AM)
    assert next_time.hour == FORECAST_SCHEDULE_TIME.hour
    assert next_time.minute == FORECAST_SCHEDULE_TIME.minute
    assert next_time.second == FORECAST_SCHEDULE_TIME.second


@freeze_time('2023-06-01 08:00:00', tz_offset=-6)
def test_get_next_execution_time_after():
    """Tests that get_next_execution_time returns next day when reference time is after execution time."""
    # Get current time (frozen at 8 AM CST)
    current_time = get_current_time_cst()
    
    # Get next execution time
    next_time = get_next_execution_time(current_time)
    
    # Check that next execution time is on the next day
    assert next_time.date() == (current_time + datetime.timedelta(days=1)).date()
    
    # Check that the hour is set to FORECAST_SCHEDULE_TIME (7 AM)
    assert next_time.hour == FORECAST_SCHEDULE_TIME.hour
    assert next_time.minute == FORECAST_SCHEDULE_TIME.minute
    assert next_time.second == FORECAST_SCHEDULE_TIME.second


@freeze_time('2023-06-01 07:00:00', tz_offset=-6)
def test_get_forecast_start_date():
    """Tests that get_forecast_start_date returns midnight of the next day."""
    # Get current time (frozen at 7 AM CST on June 1)
    current_time = get_current_time_cst()
    
    # Get forecast start date
    start_date = get_forecast_start_date(current_time)
    
    # Check that the date is the next day (June 2)
    expected_date = (current_time + datetime.timedelta(days=1)).date()
    assert start_date.date() == expected_date
    
    # Check that the time is midnight (00:00:00)
    assert start_date.hour == 0
    assert start_date.minute == 0
    assert start_date.second == 0
    
    # Check that the datetime is timezone-aware
    assert start_date.tzinfo is not None
    
    # Check that the timezone is CST or CDT
    assert 'CST' in str(start_date.tzinfo) or 'CDT' in str(start_date.tzinfo)


def test_generate_forecast_datetimes():
    """Tests that generate_forecast_datetimes returns correct list of hourly datetimes."""
    # Create a start date in CST
    start_date = datetime.datetime(2023, 6, 1, 0, 0, 0, tzinfo=TIMEZONE)
    
    # Generate forecast datetimes for 24 hours
    datetimes = generate_forecast_datetimes(start_date, hours=24)
    
    # Check that we get 24 datetimes
    assert len(datetimes) == 24
    
    # Check that each datetime is timezone-aware and hours increment correctly
    for i, dt in enumerate(datetimes):
        assert dt.tzinfo is not None
        assert dt.hour == (start_date.hour + i) % 24
        # If we cross day boundary, check the date
        if i >= 24 - start_date.hour:
            assert dt.date() == (start_date + datetime.timedelta(days=1)).date()
        else:
            assert dt.date() == start_date.date()
    
    # Check that default hours parameter uses FORECAST_HORIZON_HOURS
    default_datetimes = generate_forecast_datetimes(start_date)
    assert len(default_datetimes) == FORECAST_HORIZON_HOURS


def test_generate_forecast_date_range():
    """Tests that generate_forecast_date_range returns correct pandas DatetimeIndex."""
    # Create a start date in CST
    start_date = datetime.datetime(2023, 6, 1, 0, 0, 0, tzinfo=TIMEZONE)
    
    # Generate forecast date range for 24 hours
    date_range = generate_forecast_date_range(start_date, hours=24)
    
    # Check that we get a pandas DatetimeIndex
    assert isinstance(date_range, pd.DatetimeIndex)
    
    # Check that it has 24 elements
    assert len(date_range) == 24
    
    # Check that the frequency is hourly
    assert date_range.freq == 'H'
    
    # Check that all timestamps have the correct timezone
    for ts in date_range:
        assert ts.tzinfo is not None
        assert 'CST' in str(ts.tzinfo) or 'CDT' in str(ts.tzinfo)
    
    # Check that default hours parameter uses FORECAST_HORIZON_HOURS
    default_date_range = generate_forecast_date_range(start_date)
    assert len(default_date_range) == FORECAST_HORIZON_HOURS


def test_format_timestamp():
    """Tests that format_timestamp correctly formats datetime as string."""
    # Create a datetime in CST
    dt = datetime.datetime(2023, 6, 1, 12, 30, 45, tzinfo=TIMEZONE)
    
    # Format with a custom format string
    custom_format = '%Y-%m-%d %H:%M:%S %Z'
    formatted = format_timestamp(dt, custom_format)
    
    # Check that the formatted string matches the expected format
    # The %Z might be CST or CDT depending on daylight saving
    assert formatted.startswith('2023-06-01 12:30:45')
    
    # Test default format (ISO format)
    default_formatted = format_timestamp(dt)
    assert default_formatted.startswith('2023-06-01T12:30:45')


def test_parse_timestamp():
    """Tests that parse_timestamp correctly parses string to datetime."""
    # Create a timestamp string
    timestamp_str = '2023-06-01 12:30:45'
    
    # Parse with a custom format string
    format_string = '%Y-%m-%d %H:%M:%S'
    dt = parse_timestamp(timestamp_str, format_string)
    
    # Check that the datetime values match the expected
    assert dt.year == 2023
    assert dt.month == 6
    assert dt.day == 1
    assert dt.hour == 12
    assert dt.minute == 30
    assert dt.second == 45
    
    # Check that the datetime is timezone-aware
    assert dt.tzinfo is not None
    
    # Check that the timezone is CST or CDT
    assert 'CST' in str(dt.tzinfo) or 'CDT' in str(dt.tzinfo)
    
    # Test parsing with default format (using pandas.to_datetime)
    iso_timestamp = '2023-06-01T12:30:45'
    dt_default = parse_timestamp(iso_timestamp)
    
    assert dt_default.year == 2023
    assert dt_default.month == 6
    assert dt_default.day == 1
    assert dt_default.hour == 12
    assert dt_default.minute == 30
    assert dt_default.second == 45
    assert dt_default.tzinfo is not None


def test_shift_timestamps():
    """Tests that shift_timestamps correctly shifts timestamps in a dataframe."""
    # Create a DataFrame with a timestamp column
    df = pd.DataFrame({
        'timestamp': [
            datetime.datetime(2023, 6, 1, 12, 0, 0, tzinfo=TIMEZONE),
            datetime.datetime(2023, 6, 1, 13, 0, 0, tzinfo=TIMEZONE),
            datetime.datetime(2023, 6, 1, 14, 0, 0, tzinfo=TIMEZONE)
        ],
        'value': [1, 2, 3]
    })
    
    # Create a timedelta for shifting (1 day)
    delta = datetime.timedelta(days=1)
    
    # Shift timestamps
    shifted_df = shift_timestamps(df, delta, 'timestamp')
    
    # Check that the returned dataframe is a copy (not the original)
    assert shifted_df is not df
    
    # Check that each timestamp is shifted by 1 day
    for i, row in shifted_df.iterrows():
        original_ts = df.loc[i, 'timestamp']
        shifted_ts = row['timestamp']
        assert shifted_ts == original_ts + delta
    
    # Test with default column name
    df_default = pd.DataFrame({
        'timestamp': [
            datetime.datetime(2023, 6, 1, 12, 0, 0, tzinfo=TIMEZONE)
        ]
    })
    shifted_default = shift_timestamps(df_default, delta)
    assert shifted_default.loc[0, 'timestamp'] == df_default.loc[0, 'timestamp'] + delta


def test_get_previous_day_date():
    """Tests that get_previous_day_date returns correct date for previous day."""
    # Create a datetime in CST
    dt = datetime.datetime(2023, 6, 1, 12, 30, 45, tzinfo=TIMEZONE)
    
    # Get previous day
    prev_day = get_previous_day_date(dt)
    
    # Check that it's exactly 1 day earlier
    assert prev_day == dt - datetime.timedelta(days=1)
    
    # Check that the time values are preserved
    assert prev_day.hour == dt.hour
    assert prev_day.minute == dt.minute
    assert prev_day.second == dt.second
    
    # Check that the datetime is timezone-aware
    assert prev_day.tzinfo is not None
    
    # Check that the timezone is CST or CDT
    assert 'CST' in str(prev_day.tzinfo) or 'CDT' in str(prev_day.tzinfo)


def test_calculate_date_difference():
    """Tests that calculate_date_difference returns correct number of days between dates."""
    # Create two datetimes with a known difference
    date1 = datetime.datetime(2023, 6, 1, 12, 0, 0, tzinfo=TIMEZONE)
    date2 = datetime.datetime(2023, 6, 10, 18, 0, 0, tzinfo=TIMEZONE)
    
    # Calculate difference
    diff = calculate_date_difference(date1, date2)
    
    # Check that the difference is 9 days
    assert diff == 9
    
    # Test with dates in reverse order (should still return positive value)
    diff_reverse = calculate_date_difference(date2, date1)
    assert diff_reverse == 9
    
    # Test with datetimes in different timezones
    date_utc = datetime.datetime(2023, 6, 1, 18, 0, 0, tzinfo=pytz.UTC)
    date_cst = datetime.datetime(2023, 6, 1, 12, 0, 0, tzinfo=TIMEZONE)
    
    # These should represent the same day despite different times/timezones
    assert calculate_date_difference(date_utc, date_cst) == 0