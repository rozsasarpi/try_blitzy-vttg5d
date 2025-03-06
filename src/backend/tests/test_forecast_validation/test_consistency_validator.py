# src/backend/tests/test_forecast_validation/test_consistency_validator.py

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

# Internal imports
from ...forecast_validation.consistency_validator import ConsistencyValidator, validate_forecast_consistency, PRODUCT_RELATIONSHIPS, TEMPORAL_SMOOTHNESS_THRESHOLD
from ...forecast_validation.validation_result import ValidationCategory
from ...forecast_validation.exceptions import ConsistencyValidationError
from ..fixtures.forecast_fixtures import create_mock_forecast_data, create_inconsistent_forecast_data

# version: 7.0.0+
# version: 2.0.0+
# version: 1.24.0+
# standard library

def test_consistency_validator_init():
    """Tests the initialization of the ConsistencyValidator class"""
    # Initialize ConsistencyValidator with default parameters
    validator = ConsistencyValidator()
    assert validator._product_relationships == PRODUCT_RELATIONSHIPS
    assert validator._temporal_smoothness_threshold == TEMPORAL_SMOOTHNESS_THRESHOLD

    # Initialize ConsistencyValidator with custom parameters
    custom_relationships = {"DALMP": {"related_products": ["RTLMP"], "relationship": "greater_than"}}
    custom_threshold = 0.2
    validator = ConsistencyValidator(product_relationships=custom_relationships, temporal_smoothness_threshold=custom_threshold)
    assert validator._product_relationships == custom_relationships
    assert validator._temporal_smoothness_threshold == custom_threshold

@pytest.mark.parametrize('value1,value2,relationship,expected', [
    (10, 5, 'greater_than', True),
    (5, 10, 'greater_than', False),
    (5, 10, 'less_than', True),
    (10, 5, 'less_than', False),
    (5, 5, 'equal_to', True),
    (5, 6, 'equal_to', False),
    (None, 5, 'greater_than', False),
    (5, None, 'greater_than', False),
    (float('nan'), 5, 'greater_than', False)
])
def test_check_relationship(value1, value2, relationship, expected):
    """Tests the check_relationship method with different relationship types"""
    # Initialize ConsistencyValidator
    validator = ConsistencyValidator()

    # Call check_relationship with the test parameters
    result = validator.check_relationship(value1, value2, relationship)

    # Assert that the result matches the expected value
    assert result == expected

def test_validate_product_relationships_valid():
    """Tests that validate_product_relationships returns no errors for valid data"""
    # Create mock forecast data with valid product relationships
    data = create_mock_forecast_data()

    # Initialize ConsistencyValidator
    validator = ConsistencyValidator()

    # Call validate_product_relationships with the mock data
    result = validator.validate_product_relationships(data)

    # Assert that the result is an empty dictionary (no errors)
    assert result == {}

def test_validate_product_relationships_invalid():
    """Tests that validate_product_relationships detects invalid relationships"""
    # Create inconsistent forecast data with invalid product relationships
    data = create_inconsistent_forecast_data()

    # Initialize ConsistencyValidator
    validator = ConsistencyValidator()

    # Call validate_product_relationships with the inconsistent data
    result = validator.validate_product_relationships(data)

    # Assert that the result contains error entries for the invalid relationships
    assert 'DALMP_RegUp_relationship' in result
    assert 'DALMP_RegDown_relationship' in result

    # Verify that error messages correctly identify the problematic products and values
    assert "DALMP" in result['DALMP_RegUp_relationship'][0]
    assert "RegUp" in result['DALMP_RegUp_relationship'][0]

def test_validate_temporal_smoothness_valid():
    """Tests that validate_temporal_smoothness returns no errors for smooth data"""
    # Create mock forecast data with smooth temporal patterns
    data = create_mock_forecast_data()

    # Initialize ConsistencyValidator
    validator = ConsistencyValidator()

    # Call validate_temporal_smoothness with the mock data
    result = validator.validate_temporal_smoothness(data)

    # Assert that the result is an empty dictionary (no errors)
    assert result == {}

def test_validate_temporal_smoothness_invalid():
    """Tests that validate_temporal_smoothness detects abrupt changes"""
    # Create inconsistent forecast data with abrupt temporal changes
    data = create_inconsistent_forecast_data()

    # Initialize ConsistencyValidator
    validator = ConsistencyValidator()

    # Call validate_temporal_smoothness with the inconsistent data
    result = validator.validate_temporal_smoothness(data)

    # Assert that the result contains error entries for the products with abrupt changes
    assert 'DALMP_smoothness' in result

    # Verify that error messages correctly identify the problematic hours and values
    assert "excessive change" in result['DALMP_smoothness'][0]
    assert "between" in result['DALMP_smoothness'][0]

def test_validate_empty_dataframe():
    """Tests that validate returns an error result for empty dataframe"""
    # Create an empty pandas DataFrame
    empty_df = pd.DataFrame()

    # Initialize ConsistencyValidator
    validator = ConsistencyValidator()

    # Call validate with the empty DataFrame
    result = validator.validate(empty_df)

    # Assert that the result is_valid is False
    assert result.is_valid is False

    # Assert that the result category is ValidationCategory.CONSISTENCY
    assert result.category == ValidationCategory.CONSISTENCY

    # Assert that the result contains an error about empty data
    assert "No forecast data provided" in result.errors["forecast_data"][0]

def test_validate_valid_data():
    """Tests that validate returns a success result for valid data"""
    # Create mock forecast data with valid relationships and smooth patterns
    valid_data = create_mock_forecast_data()

    # Initialize ConsistencyValidator
    validator = ConsistencyValidator()

    # Call validate with the valid data
    result = validator.validate(valid_data)

    # Assert that the result is_valid is True
    assert result.is_valid is True

    # Assert that the result category is ValidationCategory.CONSISTENCY
    assert result.category == ValidationCategory.CONSISTENCY

    # Assert that the result has no errors
    assert not result.errors

def test_validate_invalid_relationships():
    """Tests that validate detects invalid product relationships"""
    # Create inconsistent forecast data with invalid product relationships
    inconsistent_data = create_inconsistent_forecast_data()

    # Initialize ConsistencyValidator
    validator = ConsistencyValidator()

    # Call validate with the inconsistent data
    result = validator.validate(inconsistent_data)

    # Assert that the result is_valid is False
    assert result.is_valid is False

    # Assert that the result category is ValidationCategory.CONSISTENCY
    assert result.category == ValidationCategory.CONSISTENCY

    # Assert that the result contains errors about invalid relationships
    assert "product_relationships" in result.errors

    # Verify that error messages correctly identify the problematic products
    assert "DALMP" in result.errors["product_relationships"][0]
    assert "RegUp" in result.errors["product_relationships"][0]

def test_validate_invalid_smoothness():
    """Tests that validate detects temporal smoothness violations"""
    # Create inconsistent forecast data with abrupt temporal changes
    inconsistent_data = create_inconsistent_forecast_data()

    # Initialize ConsistencyValidator
    validator = ConsistencyValidator()

    # Call validate with the inconsistent data
    result = validator.validate(inconsistent_data)

    # Assert that the result is_valid is False
    assert result.is_valid is False

    # Assert that the result category is ValidationCategory.CONSISTENCY
    assert result.category == ValidationCategory.CONSISTENCY

    # Assert that the result contains errors about temporal smoothness
    assert "temporal_smoothness" in result.errors

    # Verify that error messages correctly identify the problematic hours
    assert "excessive change" in result.errors["temporal_smoothness"][0]

def test_validate_forecast_consistency_function():
    """Tests the standalone validate_forecast_consistency function"""
    # Create mock forecast data
    mock_data = create_mock_forecast_data()

    # Call validate_forecast_consistency with the mock data
    result = validate_forecast_consistency(mock_data)

    # Assert that the result has the correct structure and validation category
    assert result.is_valid is True
    assert result.category == ValidationCategory.CONSISTENCY

    # Create inconsistent forecast data
    inconsistent_data = create_inconsistent_forecast_data()

    # Call validate_forecast_consistency with the inconsistent data
    result = validate_forecast_consistency(inconsistent_data)

    # Assert that the result is_valid is False and contains appropriate errors
    assert result.is_valid is False
    assert result.category == ValidationCategory.CONSISTENCY
    assert result.errors

def test_custom_product_relationships():
    """Tests the validator with custom product relationship definitions"""
    # Define custom product relationships
    custom_relationships = {
        "DALMP": {
            "related_products": ["RTLMP"],
            "relationship": "greater_than"
        }
    }

    # Create mock forecast data
    mock_data = create_mock_forecast_data()

    # Initialize ConsistencyValidator with custom relationships
    validator = ConsistencyValidator(product_relationships=custom_relationships)

    # Call validate with the mock data
    result = validator.validate(mock_data)

    # Assert that validation uses the custom relationships correctly
    assert result.is_valid is True

def test_custom_smoothness_threshold():
    """Tests the validator with a custom temporal smoothness threshold"""
    # Define a custom smoothness threshold
    custom_threshold = 0.1

    # Create mock forecast data with moderate changes
    mock_data = create_mock_forecast_data()

    # Initialize ConsistencyValidator with default threshold
    default_validator = ConsistencyValidator()

    # Call validate with the mock data
    default_result = default_validator.validate(mock_data)

    # Assert that validation passes with default threshold
    assert default_result.is_valid is True

    # Initialize ConsistencyValidator with stricter custom threshold
    custom_validator = ConsistencyValidator(temporal_smoothness_threshold=custom_threshold)

    # Call validate with the same data
    custom_result = custom_validator.validate(mock_data)

    # Assert that validation fails with stricter threshold
    assert custom_result.is_valid is False