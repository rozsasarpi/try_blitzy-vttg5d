# src/backend/tests/test_utils/test_validation_utils.py
"""
Unit tests for the validation utility functions in the Electricity Market Price Forecasting System.
This module tests the validation functions that support schema validation, data quality checks, and error formatting throughout the forecasting pipeline.
"""
import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
import pandera  # pandera: 0.16.0+

from src.backend.utils.validation_utils import validate_dataframe  # Function to validate dataframes against schemas
from src.backend.utils.validation_utils import format_validation_errors  # Function to format validation errors
from src.backend.utils.validation_utils import check_required_columns  # Function to check if dataframe has required columns
from src.backend.utils.validation_utils import check_value_ranges  # Function to check if values are within expected ranges
from src.backend.utils.validation_utils import check_completeness  # Function to check if dataframe contains complete data
from src.backend.utils.validation_utils import detect_outliers  # Function to detect outliers in dataframe columns
from src.backend.utils.validation_utils import validate_consistency  # Function to validate consistency between related columns
from src.backend.utils.validation_utils import get_schema_column_info  # Function to extract column information from schema
from src.backend.utils.validation_utils import compare_schemas  # Function to compare two schemas
from src.backend.utils.validation_utils import ValidationCategory  # Enumeration of validation categories
from src.backend.utils.validation_utils import ValidationOutcome  # Class representing validation results
from src.backend.utils.validation_utils import create_success_outcome  # Function to create a successful validation outcome
from src.backend.utils.validation_utils import create_error_outcome  # Function to create a validation outcome with errors
from src.backend.utils.validation_utils import combine_validation_outcomes  # Function to combine multiple validation outcomes
from src.backend.utils.validation_utils import DataFrameValidator  # Class for validating dataframes against multiple criteria
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data  # Create mock forecast data for testing
from src.backend.tests.fixtures.forecast_fixtures import create_invalid_forecast_data  # Create invalid forecast data for testing
from src.backend.tests.fixtures.forecast_fixtures import create_incomplete_forecast_data  # Create incomplete forecast data for testing


def test_format_validation_errors_empty():
    """Test that format_validation_errors returns 'No errors' for empty error dictionary"""
    # Create an empty errors dictionary
    errors = {}

    # Call format_validation_errors with the empty dictionary
    result = format_validation_errors(errors)

    # Assert that the result is 'No errors'
    assert result == "No errors"


def test_format_validation_errors_with_errors():
    """Test that format_validation_errors correctly formats error dictionary"""
    # Create an errors dictionary with multiple categories and messages
    errors = {
        "category1": ["message1", "message2"],
        "category2": ["message3"]
    }

    # Call format_validation_errors with the errors dictionary
    result = format_validation_errors(errors)

    # Assert that the result contains all error categories and messages
    assert "--- CATEGORY1 ---" in result
    assert "  • message1" in result
    assert "  • message2" in result
    assert "--- CATEGORY2 ---" in result
    assert "  • message3" in result

    # Assert that the formatting includes headers and bullet points
    assert result.startswith("---")
    assert "  •" in result


def test_validate_dataframe_valid():
    """Test that validate_dataframe returns success for valid dataframe"""
    # Create a valid pandas DataFrame
    data = {"col1": [1, 2], "col2": ["a", "b"]}
    df = pandas.DataFrame(data)

    # Create a pandera schema that matches the DataFrame
    schema = pandera.DataFrameSchema({"col1": pandera.Column(int), "col2": pandera.Column(str)})

    # Call validate_dataframe with the DataFrame and schema
    is_valid, errors = validate_dataframe(df, schema)

    # Assert that the result is (True, {})
    assert is_valid is True
    assert errors == {}


def test_validate_dataframe_invalid():
    """Test that validate_dataframe returns errors for invalid dataframe"""
    # Create an invalid pandas DataFrame
    data = {"col1": [1, 2], "col2": [1, 2]}
    df = pandas.DataFrame(data)

    # Create a pandera schema that the DataFrame doesn't match
    schema = pandera.DataFrameSchema({"col1": pandera.Column(int), "col2": pandera.Column(str)})

    # Call validate_dataframe with the DataFrame and schema
    is_valid, errors = validate_dataframe(df, schema)

    # Assert that the result is (False, errors) where errors is not empty
    assert is_valid is False
    assert errors != {}


def test_check_required_columns_all_present():
    """Test that check_required_columns returns success when all columns are present"""
    # Create a pandas DataFrame with specific columns
    data = {"col1": [1, 2], "col2": ["a", "b"], "col3": [True, False]}
    df = pandas.DataFrame(data)

    # Call check_required_columns with a subset of those columns
    required_columns = ["col1", "col2"]
    result = check_required_columns(df, required_columns)

    # Assert that the result is a ValidationOutcome with is_valid=True
    assert result.is_valid is True

    # Assert that the category is ValidationCategory.SCHEMA
    assert result.category == ValidationCategory.SCHEMA


def test_check_required_columns_missing():
    """Test that check_required_columns returns errors when columns are missing"""
    # Create a pandas DataFrame with specific columns
    data = {"col1": [1, 2], "col2": ["a", "b"]}
    df = pandas.DataFrame(data)

    # Call check_required_columns with columns that include some not in the DataFrame
    required_columns = ["col1", "col2", "col3"]
    result = check_required_columns(df, required_columns)

    # Assert that the result is a ValidationOutcome with is_valid=False
    assert result.is_valid is False

    # Assert that the errors dictionary contains the missing columns
    assert "missing_columns" in result.errors


def test_check_value_ranges_within_range():
    """Test that check_value_ranges returns success when values are within range"""
    # Create a pandas DataFrame with numeric columns
    data = {"price": [10, 20, 30], "quantity": [1, 2, 3]}
    df = pandas.DataFrame(data)

    # Define range specifications that the values satisfy
    range_specs = {"price": {"min": 0, "max": 100}, "quantity": {"min": 0, "max": 10}}

    # Call check_value_ranges with the DataFrame and range specs
    result = check_value_ranges(df, range_specs)

    # Assert that the result is a ValidationOutcome with is_valid=True
    assert result.is_valid is True

    # Assert that the category is ValidationCategory.RANGE
    assert result.category == ValidationCategory.RANGE


def test_check_value_ranges_outside_range():
    """Test that check_value_ranges returns errors when values are outside range"""
    # Create a pandas DataFrame with numeric columns
    data = {"price": [-10, 20, 110], "quantity": [1, 2, 11]}
    df = pandas.DataFrame(data)

    # Define range specifications that some values don't satisfy
    range_specs = {"price": {"min": 0, "max": 100}, "quantity": {"min": 0, "max": 10}}

    # Call check_value_ranges with the DataFrame and range specs
    result = check_value_ranges(df, range_specs)

    # Assert that the result is a ValidationOutcome with is_valid=False
    assert result.is_valid is False

    # Assert that the errors dictionary contains the out-of-range values
    assert "range_violations" in result.errors


def test_check_completeness_complete():
    """Test that check_completeness returns success for complete data"""
    # Create a pandas DataFrame with complete time series data
    data = {"timestamp": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "d", "e"], "product": ["X"] * 5}
    df = pandas.DataFrame(data)

    # Define the expected values for the timestamp column
    expected_values = [1, 2, 3, 4, 5]

    # Call check_completeness with the DataFrame, timestamp column, expected values, and groupby column
    result = check_completeness(df, "timestamp", expected_values, "product")

    # Assert that the result is a ValidationOutcome with is_valid=True
    assert result.is_valid is True

    # Assert that the category is ValidationCategory.COMPLETENESS
    assert result.category == ValidationCategory.COMPLETENESS


def test_check_completeness_incomplete():
    """Test that check_completeness returns errors for incomplete data"""
    # Create a pandas DataFrame with incomplete time series data
    data = {"timestamp": [1, 2, 4, 5], "value": ["a", "b", "d", "e"], "product": ["X"] * 4}
    df = pandas.DataFrame(data)

    # Define the expected values for the timestamp column
    expected_values = [1, 2, 3, 4, 5]

    # Call check_completeness with the DataFrame, timestamp column, expected values, and groupby column
    result = check_completeness(df, "timestamp", expected_values, "product")

    # Assert that the result is a ValidationOutcome with is_valid=False
    assert result.is_valid is False

    # Assert that the errors dictionary contains the missing timestamps
    assert "missing_values" in result.errors


def test_detect_outliers_no_outliers():
    """Test that detect_outliers returns success when no outliers are present"""
    # Create a pandas DataFrame with normally distributed data
    data = {"col1": numpy.random.normal(0, 1, 100)}
    df = pandas.DataFrame(data)

    # Call detect_outliers with the DataFrame, columns to check, and a threshold
    result = detect_outliers(df, ["col1"], threshold=3)

    # Assert that the result is a ValidationOutcome with is_valid=True
    assert result.is_valid is True

    # Assert that the category is ValidationCategory.OUTLIER
    assert result.category == ValidationCategory.OUTLIER


def test_detect_outliers_with_outliers():
    """Test that detect_outliers returns errors when outliers are present"""
    # Create a pandas DataFrame with data containing outliers
    data = {"col1": numpy.concatenate([numpy.random.normal(0, 1, 95), numpy.array([5, 6, 7, -5, -6])])}
    df = pandas.DataFrame(data)

    # Call detect_outliers with the DataFrame, columns to check, and a threshold
    result = detect_outliers(df, ["col1"], threshold=3)

    # Assert that the result is a ValidationOutcome with is_valid=False
    assert result.is_valid is False

    # Assert that the errors dictionary contains the outlier information
    assert "outliers" in result.errors


def test_validate_consistency_consistent():
    """Test that validate_consistency returns success for consistent data"""
    # Create a pandas DataFrame with consistent relationships between columns
    data = {"col1": [1, 2, 3], "col2": [2, 4, 6]}
    df = pandas.DataFrame(data)

    # Define consistency rules that the data satisfies
    consistency_rules = [{"name": "col2_greater_than_col1", "function": lambda df: (df["col2"] >= df["col1"]).all()}]

    # Call validate_consistency with the DataFrame and rules
    result = validate_consistency(df, consistency_rules)

    # Assert that the result is a ValidationOutcome with is_valid=True
    assert result.is_valid is True

    # Assert that the category is ValidationCategory.CONSISTENCY
    assert result.category == ValidationCategory.CONSISTENCY


def test_validate_consistency_inconsistent():
    """Test that validate_consistency returns errors for inconsistent data"""
    # Create a pandas DataFrame with inconsistent relationships between columns
    data = {"col1": [1, 2, 3], "col2": [2, 1, 6]}
    df = pandas.DataFrame(data)

    # Define consistency rules that the data violates
    consistency_rules = [{"name": "col2_greater_than_col1", "function": lambda df: (df["col2"] >= df["col1"]).all()}]

    # Call validate_consistency with the DataFrame and rules
    result = validate_consistency(df, consistency_rules)

    # Assert that the result is a ValidationOutcome with is_valid=False
    assert result.is_valid is False

    # Assert that the errors dictionary contains the consistency violations
    assert "consistency_violations" in result.errors


def test_get_schema_column_info():
    """Test that get_schema_column_info correctly extracts column information"""
    # Create a pandera schema with various column types and constraints
    schema = pandera.DataFrameSchema({
        "col1": pandera.Column(int, nullable=False, checks=[pandera.Check.greater_than(0)]),
        "col2": pandera.Column(str, nullable=True),
        "col3": pandera.Column(float, checks=[pandera.Check.isin([1.0, 2.0, 3.0])])
    })

    # Call get_schema_column_info with the schema
    column_info = get_schema_column_info(schema)

    # Assert that the result contains all columns from the schema
    assert "col1" in column_info
    assert "col2" in column_info
    assert "col3" in column_info

    # Assert that the result includes correct data types and constraints for each column
    assert column_info["col1"]["dtype"] == "DataType(int64)"
    assert column_info["col1"]["nullable"] is False
    assert len(column_info["col1"]["checks"]) == 1
    assert column_info["col2"]["dtype"] == "DataType(object)"
    assert column_info["col2"]["nullable"] is True
    assert column_info["col3"]["dtype"] == "DataType(float64)"
    assert len(column_info["col3"]["checks"]) == 1


def test_compare_schemas_identical():
    """Test that compare_schemas returns empty differences for identical schemas"""
    # Create two identical pandera schemas
    schema1 = pandera.DataFrameSchema({"col1": pandera.Column(int)})
    schema2 = pandera.DataFrameSchema({"col1": pandera.Column(int)})

    # Call compare_schemas with the two schemas
    differences = compare_schemas(schema1, schema2)

    # Assert that the result indicates no differences
    assert differences["only_in_schema1"] == []
    assert differences["only_in_schema2"] == []
    assert differences["different_definitions"] == {}


def test_compare_schemas_different():
    """Test that compare_schemas correctly identifies differences between schemas"""
    # Create two different pandera schemas
    schema1 = pandera.DataFrameSchema({"col1": pandera.Column(int), "col2": pandera.Column(str)})
    schema2 = pandera.DataFrameSchema({"col1": pandera.Column(float), "col3": pandera.Column(bool)})

    # Call compare_schemas with the two schemas
    differences = compare_schemas(schema1, schema2)

    # Assert that the result correctly identifies added, removed, and modified columns
    assert "col2" in differences["only_in_schema1"]
    assert "col3" in differences["only_in_schema2"]
    assert "col1" in differences["different_definitions"]


def test_create_success_outcome():
    """Test that create_success_outcome creates a valid success outcome"""
    # Call create_success_outcome with a validation category
    category = ValidationCategory.SCHEMA
    outcome = create_success_outcome(category)

    # Assert that the result is a ValidationOutcome with is_valid=True
    assert outcome.is_valid is True

    # Assert that the category matches the provided category
    assert outcome.category == category

    # Assert that the errors dictionary is empty
    assert outcome.errors == {}


def test_create_error_outcome():
    """Test that create_error_outcome creates a valid error outcome"""
    # Create an errors dictionary
    errors = {"col1": ["error1", "error2"]}

    # Call create_error_outcome with a validation category and the errors
    category = ValidationCategory.RANGE
    outcome = create_error_outcome(errors, category)

    # Assert that the result is a ValidationOutcome with is_valid=False
    assert outcome.is_valid is False

    # Assert that the category matches the provided category
    assert outcome.category == category

    # Assert that the errors dictionary matches the provided errors
    assert outcome.errors == errors


def test_combine_validation_outcomes_all_valid():
    """Test that combine_validation_outcomes returns valid outcome when all inputs are valid"""
    # Create multiple valid ValidationOutcome objects
    outcome1 = ValidationOutcome(is_valid=True, category=ValidationCategory.SCHEMA)
    outcome2 = ValidationOutcome(is_valid=True, category=ValidationCategory.RANGE)
    outcome3 = ValidationOutcome(is_valid=True, category=ValidationCategory.COMPLETENESS)

    # Call combine_validation_outcomes with these outcomes
    outcomes = [outcome1, outcome2, outcome3]
    combined_outcome = combine_validation_outcomes(outcomes)

    # Assert that the result is a ValidationOutcome with is_valid=True
    assert combined_outcome.is_valid is True

    # Assert that the errors dictionary is empty
    assert combined_outcome.errors == {}


def test_combine_validation_outcomes_some_invalid():
    """Test that combine_validation_outcomes returns invalid outcome when some inputs are invalid"""
    # Create a mix of valid and invalid ValidationOutcome objects
    outcome1 = ValidationOutcome(is_valid=True, category=ValidationCategory.SCHEMA)
    outcome2 = ValidationOutcome(is_valid=False, errors={"col1": ["error1"]}, category=ValidationCategory.RANGE)
    outcome3 = ValidationOutcome(is_valid=False, errors={"col2": ["error2"]}, category=ValidationCategory.COMPLETENESS)

    # Call combine_validation_outcomes with these outcomes
    outcomes = [outcome1, outcome2, outcome3]
    combined_outcome = combine_validation_outcomes(outcomes)

    # Assert that the result is a ValidationOutcome with is_valid=False
    assert combined_outcome.is_valid is False

    # Assert that the errors dictionary contains all errors from the invalid outcomes
    assert "col1" in combined_outcome.errors
    assert "col2" in combined_outcome.errors


def test_combine_validation_outcomes_empty():
    """Test that combine_validation_outcomes handles empty input list"""
    # Call combine_validation_outcomes with an empty list
    outcomes = []
    combined_outcome = combine_validation_outcomes(outcomes)

    # Assert that the result is a ValidationOutcome with is_valid=True
    assert combined_outcome.is_valid is True

    # Assert that the errors dictionary is empty
    assert combined_outcome.errors == {}


def test_dataframe_validator_add_rule():
    """Test that DataFrameValidator.add_rule correctly adds validation rules"""
    # Create a DataFrameValidator instance
    validator = DataFrameValidator()

    # Define a simple validation rule function
    def is_positive(df, column):
        return (df[column] > 0).all()

    # Call add_rule with a name, the function, and parameters
    validator.add_rule("positive_price", is_positive, {"column": "price"})

    # Assert that the rule was added to the validator's _validation_rules
    assert "positive_price" in validator._validation_rules
    assert validator._validation_rules["positive_price"]["function"] == is_positive
    assert validator._validation_rules["positive_price"]["params"] == {"column": "price"}


def test_dataframe_validator_validate():
    """Test that DataFrameValidator.validate correctly applies all rules"""
    # Create a DataFrameValidator instance
    validator = DataFrameValidator()

    # Add multiple validation rules, some that pass and some that fail
    def is_positive(df, column):
        return (df[column] > 0).all()

    def is_less_than_100(df, column):
        return (df[column] < 100).all()

    validator.add_rule("positive_price", is_positive, {"column": "price"})
    validator.add_rule("less_than_100", is_less_than_100, {"column": "price"})

    # Create a test DataFrame
    data = {"price": [10, 20, 110]}
    df = pandas.DataFrame(data)

    # Call validate with the DataFrame
    result = validator.validate(df)

    # Assert that the result correctly reflects the validation outcomes
    assert result.is_valid is False
    assert "rule_execution_error" not in result.errors
    assert "positive_price" not in result.errors
    assert "less_than_100" in result.errors


def test_dataframe_validator_validate_with_schema():
    """Test that DataFrameValidator.validate_with_schema validates against schema and rules"""
    # Create a DataFrameValidator instance
    validator = DataFrameValidator()

    # Add validation rules
    def is_positive(df, column):
        return (df[column] > 0).all()

    validator.add_rule("positive_price", is_positive, {"column": "price"})

    # Create a test DataFrame
    data = {"price": [10, 20, 30], "product": ["A", "B", "C"]}
    df = pandas.DataFrame(data)

    # Create a pandera schema
    schema = pandera.DataFrameSchema({"price": pandera.Column(int), "product": pandera.Column(str)})

    # Call validate_with_schema with the DataFrame and schema
    result = validator.validate_with_schema(df, schema)

    # Assert that the result correctly reflects both schema validation and rule validation
    assert result.is_valid is True
    assert "schema" not in result.errors
    assert "positive_price" not in result.errors