"""
Unit tests for the plausibility validator component of the Electricity Market Price Forecasting System.
This module tests the validation of forecast plausibility, ensuring that forecast values are within
acceptable ranges and don't contain extreme outliers.
"""

import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
from datetime import datetime  # standard library

# Internal imports
from src.backend.forecast_validation.plausibility_validator import PlausibilityValidator, validate_forecast_plausibility, PRODUCT_CONSTRAINTS
from src.backend.forecast_validation.validation_result import ValidationCategory, ValidationResult
from src.backend.forecast_validation.exceptions import PlausibilityValidationError
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data, create_invalid_forecast_data


def test_plausibility_validator_init():
    """Tests the initialization of the PlausibilityValidator class"""
    # Create a PlausibilityValidator with default parameters
    validator = PlausibilityValidator()
    assert validator._product_constraints == PRODUCT_CONSTRAINTS

    # Create a PlausibilityValidator with custom product constraints
    custom_constraints = {"DALMP": {"min_value": -1000, "max_value": 3000, "outlier_threshold": 12}}
    validator = PlausibilityValidator(product_constraints=custom_constraints)
    assert validator._product_constraints == custom_constraints

    # Create a PlausibilityValidator with custom outlier threshold
    validator = PlausibilityValidator(default_outlier_threshold=8.0)
    assert validator._default_outlier_threshold == 8.0


@pytest.mark.parametrize('value,min_value,max_value,expected', [
    (10, 0, 100, True),
    (-5, 0, 100, False),
    (150, 0, 100, False),
    (0, 0, 100, True),
    (100, 0, 100, True),
    (None, 0, 100, False),
    (float('nan'), 0, 100, False)
])
def test_is_value_in_range(value, min_value, max_value, expected):
    """Tests the is_value_in_range method of PlausibilityValidator"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Call is_value_in_range with the test parameters
    result = validator.is_value_in_range(value, min_value, max_value)

    # Assert that the result matches the expected value
    assert result == expected


def test_validate_value_ranges_valid_data():
    """Tests validate_value_ranges with valid forecast data"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Create mock forecast data with values within acceptable ranges
    valid_data = create_mock_forecast_data()

    # Call validate_value_ranges with the mock data
    errors = validator.validate_value_ranges(valid_data)

    # Assert that no errors are returned (empty dictionary)
    assert not errors


def test_validate_value_ranges_invalid_data():
    """Tests validate_value_ranges with invalid forecast data"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Create invalid forecast data with values outside acceptable ranges
    invalid_data = create_invalid_forecast_data(invalid_columns={'point_forecast': -1000})

    # Call validate_value_ranges with the invalid data
    errors = validator.validate_value_ranges(invalid_data)

    # Assert that errors are returned for the appropriate products
    assert 'DALMP' in errors

    # Verify that error messages correctly identify the out-of-range values
    assert "Point forecast at" in errors['DALMP'][0]
    assert "has value -1000.0 outside allowed range" in errors['DALMP'][0]


def test_detect_outliers_no_outliers():
    """Tests detect_outliers with data containing no outliers"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Create mock forecast data with consistent values (no outliers)
    valid_data = create_mock_forecast_data()

    # Call detect_outliers with the mock data
    errors = validator.detect_outliers(valid_data)

    # Assert that no errors are returned (empty dictionary)
    assert not errors


def test_detect_outliers_with_outliers():
    """Tests detect_outliers with data containing outliers"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Create mock forecast data with some extreme outlier values
    outlier_data = create_mock_forecast_data()
    outlier_data.at[5, 'point_forecast'] = 10000  # Add an outlier

    # Call detect_outliers with the mock data
    errors = validator.detect_outliers(outlier_data)

    # Assert that errors are returned for the products with outliers
    assert 'DALMP' in errors

    # Verify that error messages correctly identify the outlier values
    assert "Outlier detected at" in errors['DALMP'][0]
    assert "value 10000.0 is" in errors['DALMP'][0]


def test_validate_valid_forecast():
    """Tests the validate method with valid forecast data"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Create mock forecast data with valid values
    valid_data = create_mock_forecast_data()

    # Call validate with the mock data
    result = validator.validate(valid_data)

    # Assert that the validation result is valid (is_valid=True)
    assert result.is_valid

    # Assert that there are no errors in the validation result
    assert not result.errors


def test_validate_invalid_forecast_range():
    """Tests the validate method with forecast data containing range violations"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Create invalid forecast data with values outside acceptable ranges
    invalid_data = create_invalid_forecast_data(invalid_columns={'point_forecast': -1000})

    # Call validate with the invalid data
    result = validator.validate(invalid_data)

    # Assert that the validation result is invalid (is_valid=False)
    assert not result.is_valid

    # Assert that the errors contain range validation failures
    assert 'DALMP_range' in result.errors

    # Verify that the validation category is PLAUSIBILITY
    assert result.category == ValidationCategory.PLAUSIBILITY


def test_validate_invalid_forecast_outliers():
    """Tests the validate method with forecast data containing outliers"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Create invalid forecast data with outlier values
    outlier_data = create_mock_forecast_data()
    outlier_data.at[5, 'point_forecast'] = 10000  # Add an outlier

    # Call validate with the invalid data
    result = validator.validate(outlier_data)

    # Assert that the validation result is invalid (is_valid=False)
    assert not result.is_valid

    # Assert that the errors contain outlier detection failures
    assert 'DALMP_outliers' in result.errors

    # Verify that the validation category is PLAUSIBILITY
    assert result.category == ValidationCategory.PLAUSIBILITY


def test_validate_empty_forecast():
    """Tests the validate method with empty forecast data"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Create an empty DataFrame
    empty_df = pandas.DataFrame()

    # Call validate with the empty DataFrame
    result = validator.validate(empty_df)

    # Assert that the validation result is invalid (is_valid=False)
    assert not result.is_valid

    # Assert that the errors indicate empty forecast data
    assert 'general' in result.errors
    assert "Empty or None forecast dataframe provided" in result.errors['general'][0]

    # Verify that the validation category is PLAUSIBILITY
    assert result.category == ValidationCategory.PLAUSIBILITY


def test_validate_forecast_plausibility_function():
    """Tests the standalone validate_forecast_plausibility function"""
    # Create mock forecast data with valid values
    valid_data = create_mock_forecast_data()

    # Call validate_forecast_plausibility with the mock data
    result = validate_forecast_plausibility(valid_data)

    # Assert that the validation result is valid (is_valid=True)
    assert result.is_valid

    # Create invalid forecast data
    invalid_data = create_invalid_forecast_data()

    # Call validate_forecast_plausibility with the invalid data
    result = validate_forecast_plausibility(invalid_data)

    # Assert that the validation result is invalid (is_valid=False)
    assert not result.is_valid

    # Verify that the validation category is PLAUSIBILITY
    assert result.category == ValidationCategory.PLAUSIBILITY


def test_get_product_constraint():
    """Tests the get_product_constraint method of PlausibilityValidator"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Call get_product_constraint for each product in PRODUCT_CONSTRAINTS
    for product in PRODUCT_CONSTRAINTS:
        constraint = validator.get_product_constraint(product)
        # Verify that the correct constraints are returned for each product
        assert constraint == PRODUCT_CONSTRAINTS[product]

    # Call get_product_constraint for a non-existent product
    constraint = validator.get_product_constraint("NonExistentProduct")
    # Verify that default constraints are returned with the default outlier threshold
    assert constraint == {
        "min_value": float("-inf"),
        "max_value": float("inf"),
        "outlier_threshold": 5.0
    }


def test_format_error_messages():
    """Tests the format_error_messages method of PlausibilityValidator"""
    # Create a PlausibilityValidator instance
    validator = PlausibilityValidator()

    # Create sample range errors and outlier errors dictionaries
    range_errors = {
        "DALMP": ["DALMP range error 1", "DALMP range error 2"],
        "RTLMP": ["RTLMP range error 1"]
    }
    outlier_errors = {
        "DALMP": ["DALMP outlier error 1"],
        "RegUp": ["RegUp outlier error 1", "RegUp outlier error 2"]
    }

    # Call format_error_messages with the sample errors
    formatted_errors = validator.format_error_messages(range_errors, outlier_errors)

    # Verify that the formatted error messages contain the expected information
    assert "DALMP_range" in formatted_errors
    assert "DALMP range error 1" in formatted_errors["DALMP_range"]
    assert "RTLMP_range" in formatted_errors
    assert "RTLMP range error 1" in formatted_errors["RTLMP_range"]
    assert "DALMP_outliers" in formatted_errors
    assert "DALMP outlier error 1" in formatted_errors["DALMP_outliers"]
    assert "RegUp_outliers" in formatted_errors
    assert "RegUp outlier error 1" in formatted_errors["RegUp_outliers"]

    # Verify that range errors and outlier errors are properly formatted
    assert len(formatted_errors) == 4