# 3rd party imports
import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
from datetime import datetime  # standard library

# Internal imports
from src.backend.forecast_validation.completeness_validator import CompletenessValidator, validate_forecast_completeness
from src.backend.forecast_validation.validation_result import ValidationCategory
from src.backend.forecast_validation.exceptions import CompletenessValidationError
from src.backend.config.settings import FORECAST_PRODUCTS, FORECAST_HORIZON_HOURS
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data, create_incomplete_forecast_data


def test_completeness_validator_init():
    """Tests the initialization of the CompletenessValidator class"""
    # Create a CompletenessValidator with default parameters
    validator = CompletenessValidator()

    # Verify that _required_products is set to FORECAST_PRODUCTS
    assert validator._required_products == FORECAST_PRODUCTS

    # Verify that _forecast_horizon_hours is set to FORECAST_HORIZON_HOURS
    assert validator._forecast_horizon_hours == FORECAST_HORIZON_HOURS

    # Create a CompletenessValidator with custom parameters
    custom_products = ["DALMP", "RTLMP"]
    custom_horizon = 48
    validator = CompletenessValidator(required_products=custom_products, forecast_horizon_hours=custom_horizon)

    # Verify that custom parameters are correctly set
    assert validator._required_products == custom_products
    assert validator._forecast_horizon_hours == custom_horizon


def test_get_expected_timestamps():
    """Tests the generation of expected timestamps for the forecast horizon"""
    # Create a CompletenessValidator instance
    validator = CompletenessValidator()

    # Define a start date for testing
    start_date = datetime(2023, 1, 1, 0, 0, 0)

    # Call get_expected_timestamps with the start date
    timestamps = validator.get_expected_timestamps(start_date)

    # Verify that the correct number of timestamps is generated (FORECAST_HORIZON_HOURS)
    assert len(timestamps) == FORECAST_HORIZON_HOURS

    # Verify that timestamps are hourly and in sequence
    for i in range(1, len(timestamps)):
        assert timestamps[i] == timestamps[i - 1] + pandas.Timedelta(hours=1)

    # Verify that the first timestamp matches the start date
    assert timestamps[0] == start_date

    # Verify that the last timestamp is start date + (FORECAST_HORIZON_HOURS - 1) hours
    assert timestamps[-1] == start_date + pandas.Timedelta(hours=FORECAST_HORIZON_HOURS - 1)


def test_get_expected_combinations():
    """Tests the generation of expected product/timestamp combinations"""
    # Create a CompletenessValidator instance
    validator = CompletenessValidator()

    # Define a list of test timestamps
    timestamps = [
        datetime(2023, 1, 1, 0, 0, 0),
        datetime(2023, 1, 1, 1, 0, 0),
        datetime(2023, 1, 1, 2, 0, 0),
    ]

    # Call get_expected_combinations with the timestamps
    combinations = validator.get_expected_combinations(timestamps)

    # Verify that the correct number of combinations is generated (len(products) * len(timestamps))
    assert len(combinations) == len(FORECAST_PRODUCTS) * len(timestamps)

    # Verify that all expected product/timestamp combinations are present
    for product in FORECAST_PRODUCTS:
        for timestamp in timestamps:
            assert (product, timestamp) in combinations


def test_get_actual_combinations():
    """Tests the extraction of actual product/timestamp combinations from a forecast dataframe"""
    # Create a CompletenessValidator instance
    validator = CompletenessValidator()

    # Create mock forecast data with known products and timestamps
    data = [
        {"timestamp": datetime(2023, 1, 1, 0, 0, 0), "product": "DALMP"},
        {"timestamp": datetime(2023, 1, 1, 1, 0, 0), "product": "DALMP"},
        {"timestamp": datetime(2023, 1, 1, 0, 0, 0), "product": "RTLMP"},
    ]
    forecast_df = pandas.DataFrame(data)

    # Call get_actual_combinations with the forecast dataframe
    combinations = validator.get_actual_combinations(forecast_df)

    # Verify that the correct combinations are extracted
    assert ("DALMP", datetime(2023, 1, 1, 0, 0, 0)) in combinations
    assert ("DALMP", datetime(2023, 1, 1, 1, 0, 0)) in combinations
    assert ("RTLMP", datetime(2023, 1, 1, 0, 0, 0)) in combinations

    # Verify that the count matches the expected number of rows in the dataframe
    assert len(combinations) == len(forecast_df)


def test_validate_complete_forecast():
    """Tests validation of a complete forecast with all required products and timestamps"""
    # Create a CompletenessValidator instance
    validator = CompletenessValidator()

    # Create complete mock forecast data
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    forecast_df = create_mock_forecast_data(start_time=start_date)

    # Define a start date for the forecast
    start_date = datetime(2023, 1, 1, 0, 0, 0)

    # Call validate with the forecast dataframe and start date
    result = validator.validate(forecast_df, start_date)

    # Verify that the validation result is valid (is_valid=True)
    assert result.is_valid is True

    # Verify that the validation category is COMPLETENESS
    assert result.category == ValidationCategory.COMPLETENESS

    # Verify that there are no errors in the validation result
    assert not result.errors


def test_validate_missing_hours():
    """Tests validation of a forecast with missing hours"""
    # Create a CompletenessValidator instance
    validator = CompletenessValidator()

    # Create mock forecast data with specific hours removed
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    forecast_df = create_mock_forecast_data(start_time=start_date)
    hours_to_remove = [1, 5, 10]
    incomplete_df = create_incomplete_forecast_data(forecast_df, hours_to_remove)

    # Define a start date for the forecast
    start_date = datetime(2023, 1, 1, 0, 0, 0)

    # Call validate with the incomplete forecast dataframe and start date
    result = validator.validate(incomplete_df, start_date)

    # Verify that the validation result is invalid (is_valid=False)
    assert result.is_valid is False

    # Verify that the validation category is COMPLETENESS
    assert result.category == ValidationCategory.COMPLETENESS

    # Verify that the errors dictionary contains entries for missing hours
    assert "missing_timestamps" in result.errors

    # Verify that the error messages correctly identify the missing hours
    expected_missing_hours = [start_date + pandas.Timedelta(hours=h) for h in hours_to_remove]
    for missing_hour in expected_missing_hours:
        assert any(missing_hour.strftime("%Y-%m-%d %H:00") in msg for msg in result.errors["missing_timestamps"])


def test_validate_missing_products():
    """Tests validation of a forecast with missing products"""
    # Create a CompletenessValidator instance
    validator = CompletenessValidator()

    # Create mock forecast data for a subset of products
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    products_to_include = ["DALMP", "RTLMP"]
    forecast_df = create_mock_forecast_data(start_time=start_date)
    forecast_df = forecast_df[forecast_df["product"].isin(products_to_include)]

    # Define a start date for the forecast
    start_date = datetime(2023, 1, 1, 0, 0, 0)

    # Call validate with the incomplete forecast dataframe and start date
    result = validator.validate(forecast_df, start_date)

    # Verify that the validation result is invalid (is_valid=False)
    assert result.is_valid is False

    # Verify that the validation category is COMPLETENESS
    assert result.category == ValidationCategory.COMPLETENESS

    # Verify that the errors dictionary contains entries for missing products
    assert "missing_products" in result.errors

    # Verify that the error messages correctly identify the missing products
    expected_missing_products = [p for p in FORECAST_PRODUCTS if p not in products_to_include]
    assert any(missing_product in msg for missing_product in expected_missing_products for msg in result.errors["missing_products"])


def test_validate_empty_forecast():
    """Tests validation of an empty forecast dataframe"""
    # Create a CompletenessValidator instance
    validator = CompletenessValidator()

    # Create an empty pandas DataFrame
    empty_df = pandas.DataFrame()

    # Define a start date for the forecast
    start_date = datetime(2023, 1, 1, 0, 0, 0)

    # Call validate with the empty dataframe and start date
    result = validator.validate(empty_df, start_date)

    # Verify that the validation result is invalid (is_valid=False)
    assert result.is_valid is False

    # Verify that the validation category is COMPLETENESS
    assert result.category == ValidationCategory.COMPLETENESS

    # Verify that the errors dictionary contains an appropriate error message
    assert "general" in result.errors
    assert "Forecast dataframe is empty or None" in result.errors["general"][0]


def test_validate_none_forecast():
    """Tests validation when None is passed instead of a forecast dataframe"""
    # Create a CompletenessValidator instance
    validator = CompletenessValidator()

    # Define a start date for the forecast
    start_date = datetime(2023, 1, 1, 0, 0, 0)

    # Call validate with None and the start date
    result = validator.validate(None, start_date)

    # Verify that the validation result is invalid (is_valid=False)
    assert result.is_valid is False

    # Verify that the validation category is COMPLETENESS
    assert result.category == ValidationCategory.COMPLETENESS

    # Verify that the errors dictionary contains an appropriate error message
    assert "general" in result.errors
    assert "Forecast dataframe is empty or None" in result.errors["general"][0]


def test_validate_forecast_completeness_function():
    """Tests the standalone validate_forecast_completeness function"""
    # Create complete mock forecast data
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    forecast_df = create_mock_forecast_data(start_time=start_date)

    # Define a start date for the forecast
    start_date = datetime(2023, 1, 1, 0, 0, 0)

    # Call validate_forecast_completeness with the forecast dataframe and start date
    result = validate_forecast_completeness(forecast_df, start_date)

    # Verify that the validation result is valid for complete data
    assert result.is_valid is True

    # Create incomplete mock forecast data
    incomplete_df = create_incomplete_forecast_data(forecast_df)

    # Call validate_forecast_completeness with the incomplete data and start date
    result = validate_forecast_completeness(incomplete_df, start_date)

    # Verify that the validation result is invalid for incomplete data
    assert result.is_valid is False

    # Verify that the error messages correctly identify the missing data
    assert "missing_timestamps" in result.errors or "missing_products" in result.errors or "partial_missing" in result.errors


def test_validate_with_custom_products():
    """Tests validation with a custom list of required products"""
    # Define a custom list of products (subset of FORECAST_PRODUCTS)
    custom_products = ["DALMP", "RTLMP"]

    # Create a CompletenessValidator with the custom products list
    validator = CompletenessValidator(required_products=custom_products)

    # Create mock forecast data for only those products
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    forecast_df = create_mock_forecast_data(start_time=start_date)
    forecast_df = forecast_df[forecast_df["product"].isin(custom_products)]

    # Define a start date for the forecast
    start_date = datetime(2023, 1, 1, 0, 0, 0)

    # Call validate with the forecast dataframe and start date
    result = validator.validate(forecast_df, start_date)

    # Verify that the validation result is valid (is_valid=True)
    assert result.is_valid is True

    # Create mock forecast data missing one of the custom products
    forecast_df = forecast_df[forecast_df["product"] != "DALMP"]

    # Call validate with the incomplete dataframe and start date
    result = validator.validate(forecast_df, start_date)

    # Verify that the validation result is invalid (is_valid=False)
    assert result.is_valid is False

    # Verify that the error messages correctly identify the missing product
    assert "missing_products" in result.errors
    assert "DALMP" in result.errors["missing_products"][0]


def test_validate_with_custom_horizon():
    """Tests validation with a custom forecast horizon"""
    # Define a custom forecast horizon (shorter than FORECAST_HORIZON_HOURS)
    custom_horizon = 24

    # Create a CompletenessValidator with the custom horizon
    validator = CompletenessValidator(forecast_horizon_hours=custom_horizon)

    # Create mock forecast data with the standard horizon
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    forecast_df = create_mock_forecast_data(start_time=start_date)

    # Define a start date for the forecast
    start_date = datetime(2023, 1, 1, 0, 0, 0)

    # Call validate with the forecast dataframe and start date
    result = validator.validate(forecast_df, start_date)

    # Verify that the validation result is valid (is_valid=True)
    assert result.is_valid is True

    # Create mock forecast data with fewer hours than the custom horizon
    forecast_df = forecast_df[forecast_df["timestamp"] < start_date + pandas.Timedelta(hours=custom_horizon)]

    # Call validate with the incomplete dataframe and start date
    result = validator.validate(forecast_df, start_date)

    # Verify that the validation result is invalid (is_valid=False)
    assert result.is_valid is True