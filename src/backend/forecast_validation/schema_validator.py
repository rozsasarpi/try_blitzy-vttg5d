"""
Implements schema validation for forecast dataframes using pandera.

This module provides functions and classes to validate forecast data against predefined schemas,
check schema compatibility, and extract schema requirements. It serves as a critical component
in ensuring data quality and consistency throughout the forecasting system.
"""

import pandas as pd  # version 2.0.0
import pandera as pa  # version 0.16.0
from typing import Dict, List, Any

# Internal imports
from ..config.schema_config import FORECAST_OUTPUT_SCHEMA, SCHEMA_VERSION
from .exceptions import SchemaValidationError
from .validation_result import (
    ValidationCategory,
    ValidationResult,
    create_success_result,
    create_error_result
)
from ..utils.validation_utils import validate_dataframe, format_validation_errors
from ..utils.logging_utils import get_logger

# Set up logger
logger = get_logger(__name__)


def validate_forecast_schema(forecast_df: pd.DataFrame) -> ValidationResult:
    """
    Validates a forecast dataframe against the predefined schema.
    
    Args:
        forecast_df: Forecast dataframe to validate
        
    Returns:
        ValidationResult: Validation result indicating success or failure with error details
    """
    logger.info(f"Validating forecast dataframe with shape {forecast_df.shape} against schema")
    
    # Validate the dataframe against the schema
    is_valid, validation_errors = validate_dataframe(forecast_df, FORECAST_OUTPUT_SCHEMA)
    
    if is_valid:
        logger.info("Forecast schema validation successful")
        return create_success_result(ValidationCategory.SCHEMA)
    else:
        logger.error(f"Forecast schema validation failed: {format_validation_errors(validation_errors)}")
        return create_error_result(ValidationCategory.SCHEMA, validation_errors)


def check_schema_compatibility(forecast_df: pd.DataFrame, schema_version: str) -> ValidationResult:
    """
    Checks if a forecast dataframe is compatible with the current schema version.
    
    Args:
        forecast_df: Forecast dataframe to check compatibility for
        schema_version: Schema version used to generate the forecast
        
    Returns:
        ValidationResult: Validation result indicating compatibility
    """
    logger.info(f"Checking schema compatibility: dataframe version={schema_version}, current version={SCHEMA_VERSION}")
    
    # Check if the versions match exactly
    if schema_version == SCHEMA_VERSION:
        logger.info("Schema versions match exactly")
        return create_success_result(ValidationCategory.SCHEMA)
    
    # Check backward compatibility using semantic versioning principles
    try:
        current_major, current_minor, current_patch = map(int, SCHEMA_VERSION.split('.'))
        df_major, df_minor, df_patch = map(int, schema_version.split('.'))
        
        # Only consider compatible if major versions match
        if current_major == df_major:
            # For minor version differences, we consider it compatible
            warning_message = (
                f"Schema version difference detected: {schema_version} vs {SCHEMA_VERSION}. "
                f"Backward compatibility assumed due to same major version."
            )
            logger.warning(warning_message)
            
            # Still return a successful result for compatible versions
            return create_success_result(ValidationCategory.SCHEMA)
        else:
            # Major version mismatch - consider incompatible
            error_message = f"Schema version incompatible: {schema_version} vs {SCHEMA_VERSION}. Major version mismatch."
            logger.error(error_message)
            return create_error_result(
                ValidationCategory.SCHEMA, 
                {"schema_compatibility": [error_message]}
            )
    except ValueError:
        # Handle invalid version strings
        error_message = f"Invalid schema version format: {schema_version} or {SCHEMA_VERSION}. Expected format: MAJOR.MINOR.PATCH"
        logger.error(error_message)
        return create_error_result(
            ValidationCategory.SCHEMA,
            {"schema_compatibility": [error_message]}
        )


def get_schema_requirements() -> Dict[str, Any]:
    """
    Returns the requirements and constraints of the forecast schema.
    
    Returns:
        Dict: Dictionary of schema requirements and constraints
    """
    requirements = {
        "version": SCHEMA_VERSION,
        "columns": {},
        "validation_rules": {}
    }
    
    # Extract column information
    for col_name, col_schema in FORECAST_OUTPUT_SCHEMA.columns.items():
        requirements["columns"][col_name] = {
            "dtype": str(col_schema.dtype),
            "nullable": col_schema.nullable
        }
        
        # Extract validation rules for this column
        column_rules = []
        for check in col_schema.checks:
            rule = {"type": check.__class__.__name__}
            
            # Add specific check parameters based on check type
            if hasattr(check, "min_value"):
                rule["min_value"] = check.min_value
            if hasattr(check, "max_value"):
                rule["max_value"] = check.max_value
            if hasattr(check, "regex"):
                rule["regex"] = check.regex
            if hasattr(check, "isin"):
                rule["allowed_values"] = check.isin
                
            column_rules.append(rule)
        
        if column_rules:
            requirements["validation_rules"][col_name] = column_rules
    
    # Add schema properties
    requirements["properties"] = {
        "strict": FORECAST_OUTPUT_SCHEMA.strict,
        "coerce": FORECAST_OUTPUT_SCHEMA.coerce
    }
    
    logger.debug(f"Generated schema requirements with {len(requirements['columns'])} columns")
    return requirements


class SchemaValidator:
    """
    Class for validating forecast dataframes against schema definitions.
    """
    
    def __init__(self, schema: pa.DataFrameSchema = None, schema_version: str = None):
        """
        Initializes a SchemaValidator with the forecast output schema.
        
        Args:
            schema: Optional custom schema to use instead of FORECAST_OUTPUT_SCHEMA
            schema_version: Optional schema version to use instead of SCHEMA_VERSION
        """
        self._schema = schema if schema is not None else FORECAST_OUTPUT_SCHEMA
        self._schema_version = schema_version if schema_version is not None else SCHEMA_VERSION
        logger.info(f"Initialized SchemaValidator with schema version {self._schema_version}")
    
    def validate(self, forecast_df: pd.DataFrame) -> ValidationResult:
        """
        Validates a forecast dataframe against the schema.
        
        Args:
            forecast_df: Forecast dataframe to validate
            
        Returns:
            ValidationResult: Validation result indicating success or failure
        """
        logger.info(f"Validating forecast dataframe with shape {forecast_df.shape}")
        
        # Validate the dataframe against the schema
        is_valid, validation_errors = validate_dataframe(forecast_df, self._schema)
        
        if is_valid:
            logger.info("Forecast schema validation successful")
            return create_success_result(ValidationCategory.SCHEMA)
        else:
            logger.error(f"Forecast schema validation failed: {format_validation_errors(validation_errors)}")
            return create_error_result(ValidationCategory.SCHEMA, validation_errors)
    
    def check_compatibility(self, forecast_df: pd.DataFrame, df_schema_version: str) -> ValidationResult:
        """
        Checks if a dataframe is compatible with the schema version.
        
        Args:
            forecast_df: Forecast dataframe to check compatibility for
            df_schema_version: Schema version used to generate the forecast
            
        Returns:
            ValidationResult: Validation result indicating compatibility
        """
        logger.info(f"Checking schema compatibility: dataframe version={df_schema_version}, validator version={self._schema_version}")
        
        # Check if the versions match exactly
        if df_schema_version == self._schema_version:
            logger.info("Schema versions match exactly")
            return create_success_result(ValidationCategory.SCHEMA)
        
        # Check backward compatibility using semantic versioning principles
        try:
            current_major, current_minor, current_patch = map(int, self._schema_version.split('.'))
            df_major, df_minor, df_patch = map(int, df_schema_version.split('.'))
            
            # Only consider compatible if major versions match
            if current_major == df_major:
                # For minor version differences, we consider it compatible
                warning_message = (
                    f"Schema version difference detected: {df_schema_version} vs {self._schema_version}. "
                    f"Backward compatibility assumed due to same major version."
                )
                logger.warning(warning_message)
                
                # Still return a successful result for compatible versions
                return create_success_result(ValidationCategory.SCHEMA)
            else:
                # Major version mismatch - consider incompatible
                error_message = f"Schema version incompatible: {df_schema_version} vs {self._schema_version}. Major version mismatch."
                logger.error(error_message)
                return create_error_result(
                    ValidationCategory.SCHEMA, 
                    {"schema_compatibility": [error_message]}
                )
        except ValueError:
            # Handle invalid version strings
            error_message = f"Invalid schema version format: {df_schema_version} or {self._schema_version}. Expected format: MAJOR.MINOR.PATCH"
            logger.error(error_message)
            return create_error_result(
                ValidationCategory.SCHEMA,
                {"schema_compatibility": [error_message]}
            )
    
    def get_requirements(self) -> Dict[str, Any]:
        """
        Returns the requirements of the schema.
        
        Returns:
            Dict: Dictionary of schema requirements
        """
        requirements = {
            "version": self._schema_version,
            "columns": {},
            "validation_rules": {}
        }
        
        # Extract column information
        for col_name, col_schema in self._schema.columns.items():
            requirements["columns"][col_name] = {
                "dtype": str(col_schema.dtype),
                "nullable": col_schema.nullable
            }
            
            # Extract validation rules for this column
            column_rules = []
            for check in col_schema.checks:
                rule = {"type": check.__class__.__name__}
                
                # Add specific check parameters based on check type
                if hasattr(check, "min_value"):
                    rule["min_value"] = check.min_value
                if hasattr(check, "max_value"):
                    rule["max_value"] = check.max_value
                if hasattr(check, "regex"):
                    rule["regex"] = check.regex
                if hasattr(check, "isin"):
                    rule["allowed_values"] = check.isin
                    
                column_rules.append(rule)
            
            if column_rules:
                requirements["validation_rules"][col_name] = column_rules
        
        # Add schema properties
        requirements["properties"] = {
            "strict": self._schema.strict,
            "coerce": self._schema.coerce
        }
        
        logger.debug(f"Generated schema requirements with {len(requirements['columns'])} columns")
        return requirements