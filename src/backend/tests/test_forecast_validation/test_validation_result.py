import pytest
from typing import Dict, List

from ../../forecast_validation.validation_result import (
    ValidationCategory,
    ValidationResult,
    create_success_result,
    create_error_result,
    combine_validation_results,
    get_validation_error
)
from ../../forecast_validation.exceptions import (
    ForecastValidationError,
    CompletenessValidationError,
    PlausibilityValidationError,
    ConsistencyValidationError,
    SchemaValidationError
)


def test_validation_category_enum():
    """Tests that ValidationCategory enum has the expected values."""
    assert ValidationCategory.COMPLETENESS == ValidationCategory.COMPLETENESS
    assert ValidationCategory.PLAUSIBILITY == ValidationCategory.PLAUSIBILITY
    assert ValidationCategory.CONSISTENCY == ValidationCategory.CONSISTENCY
    assert ValidationCategory.SCHEMA == ValidationCategory.SCHEMA
    assert ValidationCategory.GENERIC == ValidationCategory.GENERIC
    assert len(ValidationCategory) == 5


def test_validation_result_init():
    """Tests the initialization of ValidationResult with different parameters."""
    # Test initialization with is_valid=True and no errors
    result = ValidationResult(is_valid=True, category=ValidationCategory.COMPLETENESS)
    assert result.is_valid is True
    assert result.category == ValidationCategory.COMPLETENESS
    assert result.errors == {}
    
    # Test initialization with is_valid=False and errors
    errors = {"missing_data": ["Data point X is missing", "Data point Y is missing"]}
    result = ValidationResult(is_valid=False, category=ValidationCategory.PLAUSIBILITY, _errors=errors)
    assert result.is_valid is False
    assert result.category == ValidationCategory.PLAUSIBILITY
    assert result.errors == errors


def test_validation_result_add_error():
    """Tests adding errors to a ValidationResult."""
    result = ValidationResult(is_valid=True, category=ValidationCategory.COMPLETENESS)
    
    # Add an error
    result.add_error("missing_data", "Data is missing")
    assert result.is_valid is False
    assert "missing_data" in result.errors
    assert "Data is missing" in result.errors["missing_data"]
    
    # Add another error to the same category
    result.add_error("missing_data", "Another data point is missing")
    assert len(result.errors["missing_data"]) == 2
    assert "Another data point is missing" in result.errors["missing_data"]
    
    # Add an error to a different category
    result.add_error("invalid_format", "Data has incorrect format")
    assert "invalid_format" in result.errors
    assert "Data has incorrect format" in result.errors["invalid_format"]


def test_validation_result_merge_errors():
    """Tests merging errors from another ValidationResult."""
    # Create a result with some errors
    result1 = ValidationResult(is_valid=False, category=ValidationCategory.COMPLETENESS)
    result1.add_error("missing_data", "Data is missing")
    
    # Create another result with different errors
    result2 = ValidationResult(is_valid=False, category=ValidationCategory.PLAUSIBILITY)
    result2.add_error("out_of_range", "Value is out of range")
    
    # Merge errors from result2 into result1
    result1.merge_errors(result2)
    
    # Check that result1 now contains all errors
    assert "missing_data" in result1.errors
    assert "out_of_range" in result1.errors
    assert "Data is missing" in result1.errors["missing_data"]
    assert "Value is out of range" in result1.errors["out_of_range"]
    
    # Create a valid result
    valid_result = ValidationResult(is_valid=True, category=ValidationCategory.CONSISTENCY)
    
    # Merge with an invalid result
    valid_result.merge_errors(result1)
    
    # The valid result should now be invalid
    assert valid_result.is_valid is False
    assert "missing_data" in valid_result.errors
    assert "out_of_range" in valid_result.errors


def test_validation_result_has_errors():
    """Tests the has_errors method of ValidationResult."""
    # Create a result with no errors
    result = ValidationResult(is_valid=True, category=ValidationCategory.COMPLETENESS)
    assert result.has_errors() is False
    
    # Add an error
    result.add_error("missing_data", "Data is missing")
    assert result.has_errors() is True
    
    # Create a result directly with errors
    errors = {"missing_data": ["Data is missing"]}
    result = ValidationResult(is_valid=False, category=ValidationCategory.COMPLETENESS, _errors=errors)
    assert result.has_errors() is True


def test_validation_result_get_error_count():
    """Tests the get_error_count method of ValidationResult."""
    # Create a result with no errors
    result = ValidationResult(is_valid=True, category=ValidationCategory.COMPLETENESS)
    assert result.get_error_count() == 0
    
    # Add an error
    result.add_error("missing_data", "Data is missing")
    assert result.get_error_count() == 1
    
    # Add another error to the same category
    result.add_error("missing_data", "Another data point is missing")
    assert result.get_error_count() == 2
    
    # Add an error to a different category
    result.add_error("invalid_format", "Data has incorrect format")
    assert result.get_error_count() == 3


def test_validation_result_format_errors():
    """Tests the format_errors method of ValidationResult."""
    # Create a result with no errors
    result = ValidationResult(is_valid=True, category=ValidationCategory.COMPLETENESS)
    assert result.format_errors() == "No validation errors"
    
    # Add errors to multiple categories
    result.add_error("missing_data", "Data point X is missing")
    result.add_error("missing_data", "Data point Y is missing")
    result.add_error("invalid_format", "Data has incorrect format")
    
    # Check the formatted errors
    formatted = result.format_errors()
    assert "missing_data:" in formatted
    assert "invalid_format:" in formatted
    assert "  - Data point X is missing" in formatted
    assert "  - Data point Y is missing" in formatted
    assert "  - Data has incorrect format" in formatted


def test_create_success_result():
    """Tests the create_success_result function."""
    # Test with COMPLETENESS category
    result = create_success_result(ValidationCategory.COMPLETENESS)
    assert isinstance(result, ValidationResult)
    assert result.is_valid is True
    assert result.category == ValidationCategory.COMPLETENESS
    assert result.errors == {}
    
    # Test with PLAUSIBILITY category
    result = create_success_result(ValidationCategory.PLAUSIBILITY)
    assert result.is_valid is True
    assert result.category == ValidationCategory.PLAUSIBILITY
    assert result.errors == {}


def test_create_error_result():
    """Tests the create_error_result function."""
    # Create an errors dictionary
    errors = {
        "missing_data": ["Data point X is missing", "Data point Y is missing"],
        "invalid_format": ["Data has incorrect format"]
    }
    
    # Test with SCHEMA category
    result = create_error_result(ValidationCategory.SCHEMA, errors)
    assert isinstance(result, ValidationResult)
    assert result.is_valid is False
    assert result.category == ValidationCategory.SCHEMA
    assert result.errors == errors


def test_combine_validation_results_all_valid():
    """Tests combining multiple valid validation results."""
    # Create multiple valid results
    result1 = create_success_result(ValidationCategory.COMPLETENESS)
    result2 = create_success_result(ValidationCategory.PLAUSIBILITY)
    result3 = create_success_result(ValidationCategory.CONSISTENCY)
    
    # Combine the results
    combined = combine_validation_results([result1, result2, result3])
    
    # Check the combined result
    assert combined.is_valid is True
    assert combined.category == ValidationCategory.GENERIC
    assert combined.has_errors() is False


def test_combine_validation_results_some_invalid():
    """Tests combining a mix of valid and invalid validation results."""
    # Create some valid results
    valid1 = create_success_result(ValidationCategory.COMPLETENESS)
    valid2 = create_success_result(ValidationCategory.PLAUSIBILITY)
    
    # Create some invalid results
    invalid1 = ValidationResult(is_valid=False, category=ValidationCategory.CONSISTENCY)
    invalid1.add_error("inconsistent_data", "Products have inconsistent values")
    
    invalid2 = ValidationResult(is_valid=False, category=ValidationCategory.SCHEMA)
    invalid2.add_error("schema_error", "Missing required column")
    
    # Combine all results
    combined = combine_validation_results([valid1, valid2, invalid1, invalid2])
    
    # Check the combined result
    assert combined.is_valid is False
    assert combined.category == ValidationCategory.GENERIC
    assert "inconsistent_data" in combined.errors
    assert "schema_error" in combined.errors
    assert combined.get_error_count() == 2


def test_combine_validation_results_empty_list():
    """Tests combining an empty list of validation results."""
    combined = combine_validation_results([])
    assert isinstance(combined, ValidationResult)
    assert combined.is_valid is True
    assert combined.category == ValidationCategory.GENERIC
    assert combined.errors == {}


def test_get_validation_error_valid_result():
    """Tests get_validation_error with a valid result."""
    # Create a valid result
    result = create_success_result(ValidationCategory.COMPLETENESS)
    
    # Get validation error
    error = get_validation_error(result, "Validation failed")
    
    # Should return None for a valid result
    assert error is None


def test_get_validation_error_completeness():
    """Tests get_validation_error with a completeness validation failure."""
    # Create an invalid result with COMPLETENESS category
    result = ValidationResult(is_valid=False, category=ValidationCategory.COMPLETENESS)
    result.add_error("missing_data", "Data is missing")
    
    # Get validation error
    error = get_validation_error(result, "Completeness validation failed")
    
    # Check the error type and message
    assert isinstance(error, CompletenessValidationError)
    assert str(error) == "Completeness validation failed"
    assert error.errors == result.errors


def test_get_validation_error_plausibility():
    """Tests get_validation_error with a plausibility validation failure."""
    # Create an invalid result with PLAUSIBILITY category
    result = ValidationResult(is_valid=False, category=ValidationCategory.PLAUSIBILITY)
    result.add_error("out_of_range", "Value is out of range")
    
    # Get validation error
    error = get_validation_error(result, "Plausibility validation failed")
    
    # Check the error type and message
    assert isinstance(error, PlausibilityValidationError)
    assert str(error) == "Plausibility validation failed"
    assert error.errors == result.errors


def test_get_validation_error_consistency():
    """Tests get_validation_error with a consistency validation failure."""
    # Create an invalid result with CONSISTENCY category
    result = ValidationResult(is_valid=False, category=ValidationCategory.CONSISTENCY)
    result.add_error("inconsistent_data", "Products have inconsistent values")
    
    # Get validation error
    error = get_validation_error(result, "Consistency validation failed")
    
    # Check the error type and message
    assert isinstance(error, ConsistencyValidationError)
    assert str(error) == "Consistency validation failed"
    assert error.errors == result.errors


def test_get_validation_error_schema():
    """Tests get_validation_error with a schema validation failure."""
    # Create an invalid result with SCHEMA category
    result = ValidationResult(is_valid=False, category=ValidationCategory.SCHEMA)
    result.add_error("schema_error", "Missing required column")
    
    # Get validation error
    error = get_validation_error(result, "Schema validation failed")
    
    # Check the error type and message
    assert isinstance(error, SchemaValidationError)
    assert str(error) == "Schema validation failed"
    assert error.errors == result.errors


def test_get_validation_error_generic():
    """Tests get_validation_error with a generic validation failure."""
    # Create an invalid result with GENERIC category
    result = ValidationResult(is_valid=False, category=ValidationCategory.GENERIC)
    result.add_error("generic_error", "Something went wrong")
    
    # Get validation error
    error = get_validation_error(result, "Generic validation failed")
    
    # Check the error type and message
    assert isinstance(error, ForecastValidationError)
    assert str(error) == "Generic validation failed"
    assert error.errors == result.errors