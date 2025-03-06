# Third-party imports
import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import pandera  # pandera: 0.16.0+
from pytest_mock import MockerFixture  # mock: 3.10.0+

# Internal imports
from ...forecast_validation.schema_validator import (
    validate_forecast_schema,
    check_schema_compatibility,
    get_schema_requirements,
    SchemaValidator
)
from ...forecast_validation.validation_result import ValidationCategory
from ...forecast_validation.exceptions import SchemaValidationError
from ...config.schema_config import FORECAST_OUTPUT_SCHEMA, SCHEMA_VERSION
from ..fixtures.forecast_fixtures import create_mock_forecast_data, create_invalid_forecast_data


def test_validate_forecast_schema_valid_data():
    """Tests that validate_forecast_schema correctly validates a valid forecast dataframe"""
    # Create a valid mock forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Call validate_forecast_schema with the mock dataframe
    result = validate_forecast_schema(valid_df)

    # Assert that the result is_valid is True
    assert result.is_valid is True

    # Assert that the result category is ValidationCategory.SCHEMA
    assert result.category == ValidationCategory.SCHEMA

    # Assert that the result has no errors
    assert not result.has_errors()


def test_validate_forecast_schema_invalid_data():
    """Tests that validate_forecast_schema correctly identifies invalid forecast data"""
    # Create an invalid mock forecast dataframe using create_invalid_forecast_data
    invalid_df = create_invalid_forecast_data()

    # Call validate_forecast_schema with the invalid dataframe
    result = validate_forecast_schema(invalid_df)

    # Assert that the result is_valid is False
    assert result.is_valid is False

    # Assert that the result category is ValidationCategory.SCHEMA
    assert result.category == ValidationCategory.SCHEMA

    # Assert that the result has errors
    assert result.has_errors()

    # Assert that the errors contain expected validation failure messages
    assert "point_forecast" in result.errors
    assert "is_fallback" in result.errors


def test_validate_forecast_schema_missing_columns():
    """Tests that validate_forecast_schema correctly identifies missing required columns"""
    # Create a valid mock forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Remove a required column from the dataframe
    required_column = "point_forecast"
    modified_df = valid_df.drop(columns=[required_column])

    # Call validate_forecast_schema with the modified dataframe
    result = validate_forecast_schema(modified_df)

    # Assert that the result is_valid is False
    assert result.is_valid is False

    # Assert that the result category is ValidationCategory.SCHEMA
    assert result.category == ValidationCategory.SCHEMA

    # Assert that the errors mention the missing column
    assert "schema" in result.errors
    assert any(required_column in error for error in result.errors["schema"])


def test_check_schema_compatibility_same_version():
    """Tests that check_schema_compatibility returns success for same schema version"""
    # Create a valid mock forecast dataframe
    valid_df = create_mock_forecast_data()

    # Call check_schema_compatibility with the dataframe and current SCHEMA_VERSION
    result = check_schema_compatibility(valid_df, SCHEMA_VERSION)

    # Assert that the result is_valid is True
    assert result.is_valid is True

    # Assert that the result has no errors
    assert not result.has_errors()


def test_check_schema_compatibility_different_version():
    """Tests that check_schema_compatibility handles different schema versions"""
    # Create a valid mock forecast dataframe
    valid_df = create_mock_forecast_data()

    # Call check_schema_compatibility with the dataframe and a different schema version
    different_version = "9.9.9"
    result = check_schema_compatibility(valid_df, different_version)

    # Assert that the result is_valid is False
    assert result.is_valid is False

    # Assert that the errors mention schema version incompatibility
    assert "schema_compatibility" in result.errors
    assert any(different_version in error for error in result.errors["schema_compatibility"])


def test_get_schema_requirements():
    """Tests that get_schema_requirements returns the correct schema information"""
    # Call get_schema_requirements
    requirements = get_schema_requirements()

    # Assert that the returned dictionary contains expected keys
    assert "version" in requirements
    assert "columns" in requirements
    assert "validation_rules" in requirements

    # Assert that the schema version matches SCHEMA_VERSION
    assert requirements["version"] == SCHEMA_VERSION

    # Assert that required columns are listed
    assert "timestamp" in requirements["columns"]
    assert "product" in requirements["columns"]

    # Assert that validation rules are included
    assert "timestamp" in requirements["validation_rules"]


def test_schema_validator_class_initialization():
    """Tests that SchemaValidator class initializes correctly"""
    # Create a SchemaValidator instance with default parameters
    validator = SchemaValidator()

    # Assert that the instance has the correct schema
    assert validator._schema == FORECAST_OUTPUT_SCHEMA

    # Assert that the instance has the correct schema version
    assert validator._schema_version == SCHEMA_VERSION

    # Create a SchemaValidator with custom schema and version
    custom_schema = pandera.DataFrameSchema({"custom_col": pandera.Column(str)})
    custom_version = "2.0.0"
    validator = SchemaValidator(schema=custom_schema, schema_version=custom_version)

    # Assert that the custom parameters are used correctly
    assert validator._schema == custom_schema
    assert validator._schema_version == custom_version


def test_schema_validator_validate_method():
    """Tests that SchemaValidator.validate correctly validates forecast data"""
    # Create a SchemaValidator instance
    validator = SchemaValidator()

    # Create a valid mock forecast dataframe
    valid_df = create_mock_forecast_data()

    # Call validator.validate with the dataframe
    result = validator.validate(valid_df)

    # Assert that the result is_valid is True
    assert result.is_valid is True

    # Create an invalid mock forecast dataframe
    invalid_df = create_invalid_forecast_data()

    # Call validator.validate with the invalid dataframe
    result = validator.validate(invalid_df)

    # Assert that the result is_valid is False
    assert result.is_valid is False

    # Assert that the errors contain expected validation messages
    assert "point_forecast" in result.errors
    assert "is_fallback" in result.errors


def test_schema_validator_check_compatibility_method():
    """Tests that SchemaValidator.check_compatibility correctly checks schema compatibility"""
    # Create a SchemaValidator instance
    validator = SchemaValidator()

    # Create a valid mock forecast dataframe
    valid_df = create_mock_forecast_data()

    # Call validator.check_compatibility with matching schema version
    result = validator.check_compatibility(valid_df, SCHEMA_VERSION)

    # Assert that the result is_valid is True
    assert result.is_valid is True

    # Call validator.check_compatibility with different schema version
    different_version = "9.9.9"
    result = validator.check_compatibility(valid_df, different_version)

    # Assert that the result is_valid is False
    assert result.is_valid is False

    # Assert that the errors mention version incompatibility
    assert "schema_compatibility" in result.errors
    assert any(different_version in error for error in result.errors["schema_compatibility"])


def test_schema_validator_get_requirements_method():
    """Tests that SchemaValidator.get_requirements returns correct schema information"""
    # Create a SchemaValidator instance
    validator = SchemaValidator()

    # Call validator.get_requirements
    requirements = validator.get_requirements()

    # Assert that the returned dictionary contains expected keys
    assert "version" in requirements
    assert "columns" in requirements
    assert "validation_rules" in requirements

    # Assert that the schema version is correct
    assert requirements["version"] == validator._schema_version

    # Assert that required columns are listed
    assert "timestamp" in requirements["columns"]
    assert "product" in requirements["columns"]

    # Assert that validation rules are included
    assert "timestamp" in requirements["validation_rules"]


def test_validate_forecast_schema_with_mocked_validation(mocker: MockerFixture):
    """Tests validate_forecast_schema with mocked validation function"""
    # Mock the validate_dataframe function to return a controlled result
    mocked_validate_dataframe = mocker.patch(
        "src.backend.forecast_validation.schema_validator.validate_dataframe",
        return_value=(True, {})
    )

    # Create a mock forecast dataframe
    mock_df = create_mock_forecast_data()

    # Call validate_forecast_schema with the dataframe
    result = validate_forecast_schema(mock_df)

    # Assert that validate_dataframe was called with correct parameters
    mocked_validate_dataframe.assert_called_once_with(mock_df, FORECAST_OUTPUT_SCHEMA)

    # Assert that the result matches the expected mocked result
    assert result.is_valid is True
    assert result.category == ValidationCategory.SCHEMA
    assert not result.has_errors()


def test_schema_validator_with_exception_handling(mocker: MockerFixture):
    """Tests that SchemaValidator properly handles exceptions during validation"""
    # Mock validate_dataframe to raise a pandera.errors.SchemaError
    mocked_validate_dataframe = mocker.patch(
        "src.backend.forecast_validation.schema_validator.validate_dataframe",
        side_effect=pandera.errors.SchemaError("Mocked schema error", data=None, schema=None)
    )

    # Create a SchemaValidator instance
    validator = SchemaValidator()

    # Create a mock forecast dataframe
    mock_df = create_mock_forecast_data()

    # Call validator.validate with the dataframe
    result = validator.validate(mock_df)

    # Assert that the result is_valid is False
    assert result.is_valid is False

    # Assert that the errors contain information from the exception
    assert "schema" in result.errors
    assert any("Mocked schema error" in error for error in result.errors["schema"])