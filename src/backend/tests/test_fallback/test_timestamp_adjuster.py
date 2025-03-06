"""
Unit tests for the timestamp_adjuster module which is responsible for adjusting timestamps
in fallback forecasts when using previous day's data. These tests ensure that the timestamp
adjustment functionality works correctly for both dataframes and forecast objects.
"""

import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import datetime
import numpy  # numpy: 1.24.0+

# Internal imports
from src.backend.fallback.timestamp_adjuster import adjust_timestamps  # Main function to adjust timestamps in fallback forecasts
from src.backend.fallback.timestamp_adjuster import adjust_forecast_objects  # Function to adjust timestamps in forecast objects
from src.backend.fallback.timestamp_adjuster import TIMESTAMP_COLUMN  # Constant defining the timestamp column name
from src.backend.fallback.timestamp_adjuster import GENERATION_TIMESTAMP_COLUMN  # Constant defining the generation timestamp column name
from src.backend.fallback.timestamp_adjuster import IS_FALLBACK_COLUMN  # Constant defining the is_fallback column name
from src.backend.fallback.exceptions import TimestampAdjustmentError  # Exception for timestamp adjustment failures
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data  # Create mock forecast data for testing
from src.backend.tests.fixtures.forecast_fixtures import create_mock_probabilistic_forecasts  # Create mock probabilistic forecasts for testing
from src.backend.utils.date_utils import localize_to_cst  # Convert datetime to CST timezone
from src.backend.models.forecast_models import ProbabilisticForecast  # Forecast model class for working with probabilistic forecasts


def test_adjust_timestamps_basic():
    """Tests basic functionality of adjust_timestamps with valid inputs"""
    # Create a mock forecast dataframe using create_mock_forecast_data
    mock_df = create_mock_forecast_data()
    # Define source_date and target_date with a 1-day difference
    source_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    product = "DALMP"
    # Call adjust_timestamps with the mock dataframe, product, source_date, and target_date
    result_df = adjust_timestamps(mock_df, product, source_date, target_date)
    # Verify that timestamps in the result are shifted by 1 day
    assert all(result_df[TIMESTAMP_COLUMN].dt.date == target_date.date())
    # Verify that is_fallback column is set to True
    assert all(result_df[IS_FALLBACK_COLUMN])
    # Verify that generation_timestamp is updated
    assert all(result_df[GENERATION_TIMESTAMP_COLUMN] > source_date)


def test_adjust_timestamps_multiple_days():
    """Tests adjust_timestamps with a multi-day time shift"""
    # Create a mock forecast dataframe using create_mock_forecast_data
    mock_df = create_mock_forecast_data()
    # Define source_date and target_date with a 3-day difference
    source_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    target_date = localize_to_cst(datetime.datetime(2023, 1, 4))
    product = "DALMP"
    # Call adjust_timestamps with the mock dataframe, product, source_date, and target_date
    result_df = adjust_timestamps(mock_df, product, source_date, target_date)
    # Verify that timestamps in the result are shifted by 3 days
    assert all(result_df[TIMESTAMP_COLUMN].dt.date == target_date.date())
    # Verify that is_fallback column is set to True
    assert all(result_df[IS_FALLBACK_COLUMN])
    # Verify that generation_timestamp is updated
    assert all(result_df[GENERATION_TIMESTAMP_COLUMN] > source_date)


def test_adjust_timestamps_same_day():
    """Tests adjust_timestamps when source and target dates are the same day"""
    # Create a mock forecast dataframe using create_mock_forecast_data
    mock_df = create_mock_forecast_data()
    # Define source_date and target_date as the same day but different hours
    source_date = localize_to_cst(datetime.datetime(2023, 1, 1, 6))
    target_date = localize_to_cst(datetime.datetime(2023, 1, 1, 12))
    product = "DALMP"
    # Call adjust_timestamps with the mock dataframe, product, source_date, and target_date
    result_df = adjust_timestamps(mock_df, product, source_date, target_date)
    # Verify that timestamps in the result are shifted by the correct number of hours
    time_diff = target_date - source_date
    assert all(result_df[TIMESTAMP_COLUMN] == mock_df[TIMESTAMP_COLUMN] + time_diff)
    # Verify that is_fallback column is set to True
    assert all(result_df[IS_FALLBACK_COLUMN])
    # Verify that generation_timestamp is updated
    assert all(result_df[GENERATION_TIMESTAMP_COLUMN] > source_date)


def test_adjust_timestamps_empty_dataframe():
    """Tests adjust_timestamps with an empty dataframe"""
    # Create an empty pandas DataFrame with the required columns
    empty_df = pandas.DataFrame(columns=['timestamp', 'product', 'point_forecast', 'sample_001', 'generation_timestamp', 'is_fallback'])
    # Define source_date and target_date
    source_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    product = "DALMP"
    # Call adjust_timestamps with the empty dataframe, product, source_date, and target_date
    result_df = adjust_timestamps(empty_df, product, source_date, target_date)
    # Verify that an empty dataframe is returned
    assert isinstance(result_df, pandas.DataFrame)
    assert result_df.empty
    # Verify that the returned dataframe has the same columns as the input
    assert list(result_df.columns) == list(empty_df.columns)


@pytest.mark.parametrize(
    "input_df,product,source_date,target_date,expected_error",
    [
        (None, "DALMP", localize_to_cst(datetime.datetime(2023, 1, 1)), localize_to_cst(datetime.datetime(2023, 1, 2)), "Forecast dataframe cannot be None or empty"),
        (pandas.DataFrame(), "DALMP", localize_to_cst(datetime.datetime(2023, 1, 1)), localize_to_cst(datetime.datetime(2023, 1, 2)), "Forecast dataframe cannot be None or empty"),
        (create_mock_forecast_data().drop(columns=['timestamp']), "DALMP", localize_to_cst(datetime.datetime(2023, 1, 1)), localize_to_cst(datetime.datetime(2023, 1, 2)), "Timestamp column 'timestamp' must exist in dataframe"),
        (create_mock_forecast_data(), None, localize_to_cst(datetime.datetime(2023, 1, 1)), localize_to_cst(datetime.datetime(2023, 1, 2)), "Product cannot be None or empty"),
        (create_mock_forecast_data(), "DALMP", None, localize_to_cst(datetime.datetime(2023, 1, 2)), "Source date or target date is None"),
        (create_mock_forecast_data(), "DALMP", localize_to_cst(datetime.datetime(2023, 1, 1)), None, "Source date or target date is None"),
        (create_mock_forecast_data(), "DALMP", localize_to_cst(datetime.datetime(2023, 1, 2)), localize_to_cst(datetime.datetime(2023, 1, 1)), None),
    ],
)
def test_adjust_timestamps_invalid_inputs(input_df, product, source_date, target_date, expected_error):
    """Tests adjust_timestamps with invalid inputs"""
    # Define test cases with various invalid inputs (None dataframe, missing columns, None product, None dates, target_date before source_date)
    # For each test case, verify that TimestampAdjustmentError is raised with the expected error message
    with pytest.raises(TimestampAdjustmentError) as exc_info:
        adjust_timestamps(input_df, product, source_date, target_date)
    if expected_error:
        assert str(exc_info.value) == expected_error


def test_adjust_forecast_objects_basic():
    """Tests basic functionality of adjust_forecast_objects with valid inputs"""
    # Create a list of mock forecast objects using create_mock_probabilistic_forecasts
    mock_forecasts = create_mock_probabilistic_forecasts()
    # Define source_date and target_date with a 1-day difference
    source_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    product = "DALMP"
    # Call adjust_forecast_objects with the mock forecasts, product, source_date, and target_date
    adjusted_forecasts = adjust_forecast_objects(mock_forecasts, product, source_date, target_date)
    # Verify that timestamps in the result are shifted by 1 day
    assert all(forecast.timestamp.date() == target_date.date() for forecast in adjusted_forecasts)
    # Verify that is_fallback flag is set to True for all forecasts
    assert all(forecast.is_fallback for forecast in adjusted_forecasts)
    # Verify that generation_timestamp is updated for all forecasts
    assert all(forecast.generation_timestamp > source_date for forecast in adjusted_forecasts)


def test_adjust_forecast_objects_empty_list():
    """Tests adjust_forecast_objects with an empty list"""
    # Create an empty list of forecasts
    empty_forecasts = []
    # Define source_date and target_date
    source_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    product = "DALMP"
    # Verify that TimestampAdjustmentError is raised when calling adjust_forecast_objects with the empty list
    with pytest.raises(ValueError) as exc_info:
        adjust_forecast_objects(empty_forecasts, product, source_date, target_date)
    assert str(exc_info.value) == "Empty forecast list for product 'DALMP'"


@pytest.mark.parametrize(
    "forecasts,product,source_date,target_date,expected_error",
    [
        (None, "DALMP", localize_to_cst(datetime.datetime(2023, 1, 1)), localize_to_cst(datetime.datetime(2023, 1, 2)), "Empty forecast list for product 'DALMP'"),
        ([ProbabilisticForecast(timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)), product="DALMP", point_forecast=1.0, samples=[1.0]*100, generation_timestamp=localize_to_cst(datetime.datetime(2023, 1, 1))),
          ProbabilisticForecast(timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)), product="RTLMP", point_forecast=1.0, samples=[1.0]*100, generation_timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)))],
         "DALMP", localize_to_cst(datetime.datetime(2023, 1, 1)), localize_to_cst(datetime.datetime(2023, 1, 2)), None),
        ([ProbabilisticForecast(timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)), product="DALMP", point_forecast=1.0, samples=[1.0]*100, generation_timestamp=localize_to_cst(datetime.datetime(2023, 1, 1))),
          ProbabilisticForecast(timestamp=localize_to_cst(datetime.datetime(2023, 1, 2)), product="DALMP", point_forecast=1.0, samples=[1.0]*100, generation_timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)))],
         "DALMP", localize_to_cst(datetime.datetime(2023, 1, 1)), localize_to_cst(datetime.datetime(2023, 1, 2)), None),
        ([ProbabilisticForecast(timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)), product="DALMP", point_forecast=1.0, samples=[1.0]*100, generation_timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)))],
         None, localize_to_cst(datetime.datetime(2023, 1, 1)), localize_to_cst(datetime.datetime(2023, 1, 2)), None),
        ([ProbabilisticForecast(timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)), product="DALMP", point_forecast=1.0, samples=[1.0]*100, generation_timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)))],
         "DALMP", None, localize_to_cst(datetime.datetime(2023, 1, 2)), None),
         ([ProbabilisticForecast(timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)), product="DALMP", point_forecast=1.0, samples=[1.0]*100, generation_timestamp=localize_to_cst(datetime.datetime(2023, 1, 1)))],
         "DALMP", localize_to_cst(datetime.datetime(2023, 1, 1)), None, None),
    ],
)
def test_adjust_forecast_objects_invalid_inputs(forecasts, product, source_date, target_date, expected_error):
    """Tests adjust_forecast_objects with invalid inputs"""
    # Define test cases with various invalid inputs (None forecasts, None product, None dates, target_date before source_date)
    # For each test case, verify that TimestampAdjustmentError is raised with the expected error message
    if expected_error:
        with pytest.raises(TimestampAdjustmentError) as exc_info:
            adjust_forecast_objects(forecasts, product, source_date, target_date)
        assert str(exc_info.value) == expected_error
    else:
        with pytest.raises(Exception) as exc_info:
            adjust_forecast_objects(forecasts, product, source_date, target_date)


def test_timestamp_column_constant():
    """Tests that the TIMESTAMP_COLUMN constant has the expected value"""
    # Verify that TIMESTAMP_COLUMN equals 'timestamp'
    assert TIMESTAMP_COLUMN == 'timestamp'


def test_generation_timestamp_column_constant():
    """Tests that the GENERATION_TIMESTAMP_COLUMN constant has the expected value"""
    # Verify that GENERATION_TIMESTAMP_COLUMN equals 'generation_timestamp'
    assert GENERATION_TIMESTAMP_COLUMN == 'generation_timestamp'


def test_is_fallback_column_constant():
    """Tests that the IS_FALLBACK_COLUMN constant has the expected value"""
    # Verify that IS_FALLBACK_COLUMN equals 'is_fallback'
    assert IS_FALLBACK_COLUMN == 'is_fallback'