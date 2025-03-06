"""
Utility functions for data validation throughout the Electricity Market Price Forecasting System.

This module provides reusable validation functions that support schema validation, data quality checks,
and error formatting for various components of the forecasting pipeline.
"""

import logging
import pandas as pd
import pandera as pa
from typing import Dict, List, Tuple, Callable, Optional, Union, Any, Set
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
import numpy as np

from ..forecast_validation.exceptions import ForecastValidationError, SchemaValidationError

# Set up logger
logger = logging.getLogger(__name__)


class ValidationCategory(Enum):
    """Enumeration of validation categories"""
    SCHEMA = "schema"
    RANGE = "range"
    COMPLETENESS = "completeness"
    OUTLIER = "outlier"
    CONSISTENCY = "consistency"


@dataclass
class ValidationOutcome:
    """Class representing the result of a validation operation"""
    is_valid: bool
    errors: Dict[str, List[str]] = field(default_factory=dict)
    category: ValidationCategory = field(default=ValidationCategory.SCHEMA)
    validation_time: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        """Ensure errors is a dictionary if None was provided"""
        if self.errors is None:
            self.errors = {}

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the validation outcome to a dictionary
        
        Returns:
            Dictionary representation of the validation outcome
        """
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "category": self.category.value,
            "validation_time": self.validation_time.isoformat()
        }

    def format_errors(self) -> str:
        """
        Formats the validation errors into a human-readable string
        
        Returns:
            Formatted error message
        """
        if self.is_valid:
            return "Validation successful"
        return format_validation_errors(self.errors)

    def log_result(self) -> None:
        """
        Logs the validation outcome with appropriate level
        """
        if self.is_valid:
            logger.info(f"Validation successful [category={self.category.value}, time={self.validation_time.isoformat()}]")
        else:
            logger.error(
                f"Validation failed [category={self.category.value}, time={self.validation_time.isoformat()}]\n"
                f"{self.format_errors()}"
            )

    def add_error(self, category: str, message: str) -> None:
        """
        Adds an error to the validation outcome
        
        Args:
            category: The category of the error
            message: The error message
        """
        self.is_valid = False
        if category not in self.errors:
            self.errors[category] = []
        self.errors[category].append(message)

    def merge(self, other: 'ValidationOutcome') -> 'ValidationOutcome':
        """
        Merges another validation outcome into this one
        
        Args:
            other: Another validation outcome to merge
        
        Returns:
            Self, for method chaining
        """
        self.is_valid = self.is_valid and other.is_valid
        
        # Merge errors
        for category, messages in other.errors.items():
            if category not in self.errors:
                self.errors[category] = []
            self.errors[category].extend(messages)
        
        # Keep the most recent validation time
        if other.validation_time > self.validation_time:
            self.validation_time = other.validation_time
        
        return self


def format_validation_errors(errors: Dict[str, List[str]]) -> str:
    """
    Formats validation errors into a human-readable string
    
    Args:
        errors: Dictionary of errors by category
    
    Returns:
        Formatted error message
    """
    if not errors:
        return "No errors"
    
    formatted_messages = []
    for category, messages in errors.items():
        formatted_category = f"--- {category.upper()} ---"
        formatted_messages.append(formatted_category)
        for message in messages:
            formatted_messages.append(f"  • {message}")
    
    return "\n".join(formatted_messages)


def validate_dataframe(df: pd.DataFrame, schema: pa.DataFrameSchema) -> Tuple[bool, Dict[str, List[str]]]:
    """
    Validates a pandas DataFrame against a pandera schema
    
    Args:
        df: DataFrame to validate
        schema: Pandera schema to validate against
    
    Returns:
        Tuple of (bool, dict) indicating validation success and errors
    """
    logger.info(f"Validating dataframe against schema: {schema.name if hasattr(schema, 'name') else 'unnamed'}")
    
    try:
        # Validate the dataframe against the schema
        schema.validate(df)
        return True, {}
    except pa.errors.SchemaError as e:
        # Extract and format error information
        error_dict = {"schema": []}
        
        if hasattr(e, "failure_cases") and e.failure_cases is not None:
            for _, row in e.failure_cases.iterrows():
                column = row.get("column", "unknown")
                check = row.get("check", "unknown")
                failure_case = row.get("failure_case", "unknown")
                error_dict["schema"].append(
                    f"Column '{column}' failed check '{check}' with value '{failure_case}'"
                )
        else:
            # Fallback for cases where failure_cases is not available
            error_dict["schema"].append(str(e))
        
        logger.error(f"Schema validation failed: {format_validation_errors(error_dict)}")
        return False, error_dict


def check_required_columns(df: pd.DataFrame, required_columns: List[str]) -> ValidationOutcome:
    """
    Checks if a dataframe contains all required columns
    
    Args:
        df: DataFrame to check
        required_columns: List of required column names
    
    Returns:
        ValidationOutcome indicating if all required columns are present
    """
    df_columns = set(df.columns)
    missing_columns = [col for col in required_columns if col not in df_columns]
    
    if not missing_columns:
        return create_success_outcome(ValidationCategory.SCHEMA)
    
    errors = {
        "missing_columns": [
            f"Missing required column(s): {', '.join(missing_columns)}"
        ]
    }
    return create_error_outcome(errors, ValidationCategory.SCHEMA)


def check_value_ranges(df: pd.DataFrame, range_specs: Dict[str, Dict[str, float]]) -> ValidationOutcome:
    """
    Validates that values in specified columns fall within expected ranges
    
    Args:
        df: DataFrame to check
        range_specs: Dict mapping column names to range specs (min, max)
            Example: {"price": {"min": 0.0, "max": 1000.0}}
    
    Returns:
        ValidationOutcome indicating if values are within ranges
    """
    errors = {}
    
    for column, specs in range_specs.items():
        # Skip if column doesn't exist
        if column not in df.columns:
            if "missing_columns" not in errors:
                errors["missing_columns"] = []
            errors["missing_columns"].append(f"Column '{column}' not found in dataframe")
            continue
        
        # Check minimum value if specified
        if "min" in specs and (df[column] < specs["min"]).any():
            if "range_violations" not in errors:
                errors["range_violations"] = []
            
            violations = df[df[column] < specs["min"]]
            violation_count = len(violations)
            min_value = specs["min"]
            
            errors["range_violations"].append(
                f"Column '{column}' has {violation_count} values below minimum ({min_value})"
            )
        
        # Check maximum value if specified
        if "max" in specs and (df[column] > specs["max"]).any():
            if "range_violations" not in errors:
                errors["range_violations"] = []
            
            violations = df[df[column] > specs["max"]]
            violation_count = len(violations)
            max_value = specs["max"]
            
            errors["range_violations"].append(
                f"Column '{column}' has {violation_count} values above maximum ({max_value})"
            )
    
    if not errors:
        return create_success_outcome(ValidationCategory.RANGE)
    
    return create_error_outcome(errors, ValidationCategory.RANGE)


def check_completeness(
    df: pd.DataFrame, 
    timestamp_column: str, 
    expected_values: List[Any],
    groupby_column: str
) -> ValidationOutcome:
    """
    Checks if a dataframe contains complete data for a time range
    
    Args:
        df: DataFrame to check
        timestamp_column: Column containing timestamps
        expected_values: List of expected values in timestamp_column
        groupby_column: Column to group by (e.g., 'product')
    
    Returns:
        ValidationOutcome indicating completeness
    """
    # Convert expected_values to a set for faster lookups
    expected_set = set(expected_values)
    
    # Group by the specified column
    groups = df.groupby(groupby_column)
    
    errors = {}
    
    for group_name, group_df in groups:
        # Get the set of timestamp values for this group
        actual_values = set(group_df[timestamp_column])
        
        # Find missing values for this group
        missing_values = expected_set - actual_values
        
        if missing_values:
            if "missing_values" not in errors:
                errors["missing_values"] = []
            
            # Limit the number of reported missing values to avoid very long error messages
            missing_samples = list(missing_values)[:5]
            remaining_count = len(missing_values) - len(missing_samples)
            
            if remaining_count > 0:
                missing_str = f"{', '.join(str(v) for v in missing_samples)} and {remaining_count} more"
            else:
                missing_str = f"{', '.join(str(v) for v in missing_samples)}"
            
            errors["missing_values"].append(
                f"Group '{group_name}' is missing {len(missing_values)} expected values in '{timestamp_column}': {missing_str}"
            )
    
    if not errors:
        return create_success_outcome(ValidationCategory.COMPLETENESS)
    
    return create_error_outcome(errors, ValidationCategory.COMPLETENESS)


def detect_outliers(df: pd.DataFrame, columns: List[str], threshold: float = 3.0) -> ValidationOutcome:
    """
    Detects outliers in numerical columns using statistical methods
    
    Args:
        df: DataFrame to check
        columns: List of numerical columns to check for outliers
        threshold: Z-score threshold for outlier detection (default 3.0)
    
    Returns:
        ValidationOutcome with outlier information
    """
    errors = {}
    
    for column in columns:
        # Skip if column doesn't exist
        if column not in df.columns:
            if "missing_columns" not in errors:
                errors["missing_columns"] = []
            errors["missing_columns"].append(f"Column '{column}' not found in dataframe")
            continue
        
        # Skip if column is not numeric
        if not pd.api.types.is_numeric_dtype(df[column]):
            if "non_numeric_columns" not in errors:
                errors["non_numeric_columns"] = []
            errors["non_numeric_columns"].append(f"Column '{column}' is not numeric")
            continue
        
        # Calculate z-scores
        mean = df[column].mean()
        std = df[column].std()
        
        # Handle case where std is 0 to avoid division by zero
        if std == 0:
            continue
        
        z_scores = (df[column] - mean) / std
        
        # Identify outliers
        outliers = df[abs(z_scores) > threshold]
        
        if not outliers.empty:
            if "outliers" not in errors:
                errors["outliers"] = []
            
            outlier_count = len(outliers)
            
            # Get a sample of outlier values to include in the error message
            outlier_samples = outliers[column].sample(min(3, outlier_count)).tolist()
            sample_str = ", ".join(str(round(val, 2)) for val in outlier_samples)
            
            errors["outliers"].append(
                f"Column '{column}' has {outlier_count} outliers (z-score > {threshold}). "
                f"Sample values: {sample_str}"
            )
    
    if not errors:
        return create_success_outcome(ValidationCategory.OUTLIER)
    
    return create_error_outcome(errors, ValidationCategory.OUTLIER)


def validate_consistency(
    df: pd.DataFrame, 
    consistency_rules: List[Dict[str, Any]]
) -> ValidationOutcome:
    """
    Validates consistency between related columns in a dataframe
    
    Args:
        df: DataFrame to check
        consistency_rules: List of rule dictionaries with keys:
            - 'function': Callable that takes a dataframe and returns violations
            - 'name': Human-readable rule name
    
    Returns:
        ValidationOutcome indicating consistency
    """
    errors = {}
    
    for rule in consistency_rules:
        rule_function = rule.get('function')
        rule_name = rule.get('name', 'Unnamed rule')
        
        if not callable(rule_function):
            logger.warning(f"Skipping invalid consistency rule '{rule_name}': function is not callable")
            continue
        
        # Apply the rule function to get violations
        violations = rule_function(df)
        
        if violations:
            if "consistency_violations" not in errors:
                errors["consistency_violations"] = []
            
            # Handle different return types from rule functions
            if isinstance(violations, bool) and violations:
                errors["consistency_violations"].append(f"Rule '{rule_name}' failed")
            elif isinstance(violations, str):
                errors["consistency_violations"].append(f"Rule '{rule_name}' failed: {violations}")
            elif isinstance(violations, list):
                errors["consistency_violations"].append(
                    f"Rule '{rule_name}' failed with {len(violations)} violations"
                )
                # Add specific violation details if available
                if violations and isinstance(violations[0], str):
                    for violation in violations[:3]:  # Limit to first 3 to avoid long messages
                        errors["consistency_violations"].append(f"  - {violation}")
                    if len(violations) > 3:
                        errors["consistency_violations"].append(f"  - ... and {len(violations) - 3} more")
    
    if not errors:
        return create_success_outcome(ValidationCategory.CONSISTENCY)
    
    return create_error_outcome(errors, ValidationCategory.CONSISTENCY)


def get_schema_column_info(schema: pa.DataFrameSchema) -> Dict[str, Dict[str, Any]]:
    """
    Extracts column information from a pandera schema
    
    Args:
        schema: Pandera schema to extract information from
    
    Returns:
        Dictionary with column names, types, and constraints
    """
    column_info = {}
    
    for col_name, col_schema in schema.columns.items():
        col_info = {
            "dtype": str(col_schema.dtype),
            "nullable": col_schema.nullable
        }
        
        # Extract checks
        checks = []
        for check in col_schema.checks:
            check_info = {"name": check.__class__.__name__}
            
            # Extract check parameters
            if hasattr(check, "min_value"):
                check_info["min_value"] = check.min_value
            if hasattr(check, "max_value"):
                check_info["max_value"] = check.max_value
            if hasattr(check, "allowed_values"):
                check_info["allowed_values"] = check.allowed_values
            
            checks.append(check_info)
        
        col_info["checks"] = checks
        column_info[col_name] = col_info
    
    return column_info


def compare_schemas(
    schema1: pa.DataFrameSchema, 
    schema2: pa.DataFrameSchema
) -> Dict[str, Any]:
    """
    Compares two pandera schemas and identifies differences
    
    Args:
        schema1: First pandera schema
        schema2: Second pandera schema
    
    Returns:
        Dictionary describing differences between schemas
    """
    schema1_info = get_schema_column_info(schema1)
    schema2_info = get_schema_column_info(schema2)
    
    schema1_cols = set(schema1_info.keys())
    schema2_cols = set(schema2_info.keys())
    
    # Find columns present in one schema but not the other
    only_in_schema1 = schema1_cols - schema2_cols
    only_in_schema2 = schema2_cols - schema1_cols
    
    # Find columns present in both but with different definitions
    common_cols = schema1_cols.intersection(schema2_cols)
    different_cols = {}
    
    for col in common_cols:
        col_diffs = {}
        
        # Compare dtype
        if schema1_info[col]["dtype"] != schema2_info[col]["dtype"]:
            col_diffs["dtype"] = {
                "schema1": schema1_info[col]["dtype"],
                "schema2": schema2_info[col]["dtype"]
            }
        
        # Compare nullable
        if schema1_info[col]["nullable"] != schema2_info[col]["nullable"]:
            col_diffs["nullable"] = {
                "schema1": schema1_info[col]["nullable"],
                "schema2": schema2_info[col]["nullable"]
            }
        
        # Compare checks (simplified comparison)
        schema1_checks = {check["name"]: check for check in schema1_info[col]["checks"]}
        schema2_checks = {check["name"]: check for check in schema2_info[col]["checks"]}
        
        schema1_check_names = set(schema1_checks.keys())
        schema2_check_names = set(schema2_checks.keys())
        
        if schema1_check_names != schema2_check_names:
            col_diffs["checks"] = {
                "only_in_schema1": list(schema1_check_names - schema2_check_names),
                "only_in_schema2": list(schema2_check_names - schema1_check_names)
            }
        
        # If there are differences, add to the different_cols dictionary
        if col_diffs:
            different_cols[col] = col_diffs
    
    differences = {
        "only_in_schema1": list(only_in_schema1),
        "only_in_schema2": list(only_in_schema2),
        "different_definitions": different_cols
    }
    
    return differences


def create_success_outcome(category: ValidationCategory) -> ValidationOutcome:
    """
    Creates a successful validation outcome
    
    Args:
        category: Validation category
    
    Returns:
        Successful validation outcome
    """
    return ValidationOutcome(
        is_valid=True,
        errors={},
        category=category,
        validation_time=datetime.now()
    )


def create_error_outcome(
    errors: Dict[str, List[str]], 
    category: ValidationCategory
) -> ValidationOutcome:
    """
    Creates a validation outcome with errors
    
    Args:
        errors: Dictionary of validation errors
        category: Validation category
    
    Returns:
        Validation outcome with errors
    """
    return ValidationOutcome(
        is_valid=False,
        errors=errors,
        category=category,
        validation_time=datetime.now()
    )


def combine_validation_outcomes(outcomes: List[ValidationOutcome]) -> ValidationOutcome:
    """
    Combines multiple validation outcomes into a single outcome
    
    Args:
        outcomes: List of validation outcomes to combine
    
    Returns:
        Combined validation outcome
    """
    if not outcomes:
        return ValidationOutcome(is_valid=True, errors={})
    
    # Start with the first outcome
    combined = ValidationOutcome(
        is_valid=all(outcome.is_valid for outcome in outcomes),
        errors={},
        validation_time=max(outcome.validation_time for outcome in outcomes)
    )
    
    # Merge all errors
    for outcome in outcomes:
        for category, messages in outcome.errors.items():
            if category not in combined.errors:
                combined.errors[category] = []
            combined.errors[category].extend(messages)
    
    return combined


class DataFrameValidator:
    """Class for validating pandas DataFrames against various criteria"""
    
    def __init__(self, validation_rules: Dict[str, Dict[str, Any]] = None):
        """
        Initializes a DataFrameValidator with optional validation rules
        
        Args:
            validation_rules: Dictionary of validation rules
        """
        self._validation_rules = validation_rules or {}
        self._logger = logging.getLogger(__name__ + '.DataFrameValidator')
    
    def add_rule(
        self, 
        rule_name: str, 
        rule_function: Callable,
        rule_params: Dict[str, Any] = None
    ) -> 'DataFrameValidator':
        """
        Adds a validation rule to the validator
        
        Args:
            rule_name: Name of the rule
            rule_function: Function that implements the rule
            rule_params: Parameters to pass to the rule function
        
        Returns:
            Self for method chaining
        """
        self._validation_rules[rule_name] = {
            'function': rule_function,
            'params': rule_params or {}
        }
        return self
    
    def validate(self, df: pd.DataFrame) -> ValidationOutcome:
        """
        Validates a dataframe against all registered rules
        
        Args:
            df: DataFrame to validate
        
        Returns:
            Combined validation result
        """
        validation_results = []
        
        for rule_name, rule_info in self._validation_rules.items():
            self._logger.debug(f"Applying validation rule: {rule_name}")
            rule_function = rule_info['function']
            rule_params = rule_info['params']
            
            try:
                result = rule_function(df, **rule_params)
                validation_results.append(result)
            except Exception as e:
                self._logger.error(f"Error applying rule {rule_name}: {str(e)}")
                errors = {"rule_execution_error": [f"Rule '{rule_name}' failed with exception: {str(e)}"]}
                validation_results.append(create_error_outcome(errors, ValidationCategory.SCHEMA))
        
        combined_result = combine_validation_outcomes(validation_results)
        combined_result.log_result()
        
        return combined_result
    
    def validate_with_schema(
        self, 
        df: pd.DataFrame, 
        schema: pa.DataFrameSchema
    ) -> ValidationOutcome:
        """
        Validates a dataframe against a pandera schema and additional rules
        
        Args:
            df: DataFrame to validate
            schema: Pandera schema to validate against
        
        Returns:
            Combined validation outcome
        """
        # First validate against the schema
        schema_valid, schema_errors = validate_dataframe(df, schema)
        
        if not schema_valid:
            # If schema validation fails, return immediately
            return create_error_outcome(schema_errors, ValidationCategory.SCHEMA)
        
        # Continue with other validation rules
        return self.validate(df)