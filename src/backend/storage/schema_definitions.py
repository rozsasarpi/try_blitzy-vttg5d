"""
Defines and implements schema validation functions for forecast dataframes in the storage system.
This module provides utilities to validate, enhance, and verify the integrity of forecast data
before storage and after retrieval, ensuring data quality and consistency throughout the system.
"""

import pandas as pd  # version 2.0.0
import pandera as pa  # version 0.16.0
from datetime import datetime  # standard library
import logging  # standard library
from typing import Dict, List, Tuple, Optional, Any, Union  # standard library

from ..config.schema_config import FORECAST_OUTPUT_SCHEMA, SCHEMA_VERSION
from .exceptions import SchemaValidationError, DataIntegrityError
from ..utils.validation_utils import validate_dataframe, format_validation_errors

# Set up module logger
logger = logging.getLogger(__name__)

# Define storage metadata fields and their expected types
STORAGE_METADATA_FIELDS = {
    "storage_timestamp": "datetime64[ns]",
    "storage_version": "str",
    "schema_version": "str"
}


def validate_forecast_schema(df: pd.DataFrame) -> Tuple[bool, Dict[str, List[str]]]:
    """
    Validates a forecast dataframe against the output schema.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Tuple of (bool, dict) indicating success and any validation errors
    """
    logger.info("Starting schema validation for forecast dataframe")
    
    # Use validate_dataframe from validation_utils to check against FORECAST_OUTPUT_SCHEMA
    is_valid, validation_errors = validate_dataframe(df, FORECAST_OUTPUT_SCHEMA)
    
    if is_valid:
        logger.info("Schema validation successful")
        return True, {}
    else:
        logger.error(f"Schema validation failed: {format_validation_errors(validation_errors)}")
        return False, validation_errors


def add_storage_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds storage metadata to a forecast dataframe.
    
    Args:
        df: DataFrame to enhance with metadata
        
    Returns:
        DataFrame with added storage metadata
    """
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Add storage timestamp (when the forecast was stored)
    df_copy["storage_timestamp"] = pd.Timestamp.now()
    
    # Add storage version information
    df_copy["storage_version"] = "1.0.0"  # This should ideally come from a version config
    
    # Add schema version
    df_copy["schema_version"] = SCHEMA_VERSION
    
    return df_copy


def check_storage_integrity(df: pd.DataFrame) -> Tuple[bool, Dict[str, List[str]]]:
    """
    Checks the integrity of a stored forecast dataframe.
    
    Args:
        df: DataFrame to check for integrity
        
    Returns:
        Tuple of (bool, dict) indicating integrity status and any issues
    """
    issues = {}
    
    # Check that all required metadata fields are present
    missing_fields = []
    for field in STORAGE_METADATA_FIELDS:
        if field not in df.columns:
            missing_fields.append(field)
    
    if missing_fields:
        issues["missing_metadata"] = [f"Missing metadata field: {field}" for field in missing_fields]
    
    # Check that timestamps are in the correct format
    if "timestamp" in df.columns and not pd.api.types.is_datetime64_dtype(df["timestamp"]):
        if "data_type_issues" not in issues:
            issues["data_type_issues"] = []
        issues["data_type_issues"].append("'timestamp' column is not datetime64[ns] type")
    
    if "generation_timestamp" in df.columns and not pd.api.types.is_datetime64_dtype(df["generation_timestamp"]):
        if "data_type_issues" not in issues:
            issues["data_type_issues"] = []
        issues["data_type_issues"].append("'generation_timestamp' column is not datetime64[ns] type")
    
    if "storage_timestamp" in df.columns and not pd.api.types.is_datetime64_dtype(df["storage_timestamp"]):
        if "data_type_issues" not in issues:
            issues["data_type_issues"] = []
        issues["data_type_issues"].append("'storage_timestamp' column is not datetime64[ns] type")
    
    # Check for data consistency between point_forecast and samples
    if "point_forecast" in df.columns and any(col.startswith("sample_") for col in df.columns):
        sample_cols = [col for col in df.columns if col.startswith("sample_")]
        
        if sample_cols:
            # Check if point_forecast is within the range of samples for each row
            for idx, row in df.iterrows():
                point_forecast = row["point_forecast"]
                samples = [row[col] for col in sample_cols]
                min_sample = min(samples)
                max_sample = max(samples)
                
                # If point forecast is significantly outside the range of samples, flag it
                if point_forecast < min_sample * 0.9 or point_forecast > max_sample * 1.1:
                    if "data_consistency_issues" not in issues:
                        issues["data_consistency_issues"] = []
                    issues["data_consistency_issues"].append(
                        f"Row {idx}: point_forecast ({point_forecast}) outside sample range ({min_sample:.2f}, {max_sample:.2f})"
                    )
                    
                    # Limit the number of reported issues to avoid very long error messages
                    if len(issues.get("data_consistency_issues", [])) >= 5:
                        issues["data_consistency_issues"].append("... and more issues (showing only first 5)")
                        break
    
    # Return True if no issues, otherwise False with the issues dict
    if not issues:
        return True, {}
    else:
        return False, issues


def extract_storage_metadata(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Extracts storage metadata from a forecast dataframe.
    
    Args:
        df: DataFrame to extract metadata from
        
    Returns:
        Dictionary of storage metadata
    """
    metadata = {}
    
    # Extract standard metadata fields if they exist
    for field in STORAGE_METADATA_FIELDS:
        if field in df.columns:
            # Convert timestamp to ISO format string if it's a timestamp
            if field == "storage_timestamp" and pd.api.types.is_datetime64_dtype(df[field]):
                # Take the first value since metadata should be the same for all rows
                timestamp_value = df[field].iloc[0]
                metadata[field] = timestamp_value.isoformat() if hasattr(timestamp_value, "isoformat") else str(timestamp_value)
            else:
                # For other fields, just take the first value
                metadata[field] = df[field].iloc[0]
    
    return metadata


def verify_schema_compatibility(df: pd.DataFrame) -> bool:
    """
    Verifies that a dataframe is compatible with the current schema version.
    
    Args:
        df: DataFrame to check for compatibility
        
    Returns:
        True if compatible, False otherwise
    """
    # Check if schema_version metadata exists
    if "schema_version" not in df.columns:
        logger.warning("No schema_version found in dataframe, assuming incompatible")
        return False
    
    # Get the schema version from the dataframe
    df_schema_version = df["schema_version"].iloc[0]
    
    # If versions match exactly, they're compatible
    if df_schema_version == SCHEMA_VERSION:
        return True
    
    # For different versions, check backward compatibility
    # This is a simplified version; in practice, you'd have more complex logic here
    # based on specific version changes
    
    # Parse version components for comparison
    df_version_parts = df_schema_version.split(".")
    current_version_parts = SCHEMA_VERSION.split(".")
    
    # If major versions match, assume backward compatibility
    if len(df_version_parts) >= 1 and len(current_version_parts) >= 1:
        if df_version_parts[0] == current_version_parts[0]:
            logger.info(f"Schema version {df_schema_version} is backward compatible with current version {SCHEMA_VERSION}")
            return True
    
    logger.warning(f"Schema version {df_schema_version} may not be compatible with current version {SCHEMA_VERSION}")
    return False


def upgrade_schema_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Upgrades a dataframe to the current schema version if needed.
    
    Args:
        df: DataFrame that might need schema upgrade
        
    Returns:
        DataFrame with updated schema if needed
    """
    # Check if an upgrade is needed
    if verify_schema_compatibility(df):
        # If already compatible, no upgrade needed
        if df["schema_version"].iloc[0] == SCHEMA_VERSION:
            return df
        else:
            # Compatible but different version, just update the version
            df_copy = df.copy()
            df_copy["schema_version"] = SCHEMA_VERSION
            logger.info(f"Updated schema version from {df['schema_version'].iloc[0]} to {SCHEMA_VERSION}")
            return df_copy
    
    # If not compatible, need to apply transformations based on the version
    df_copy = df.copy()
    old_version = df["schema_version"].iloc[0] if "schema_version" in df.columns else "unknown"
    
    logger.info(f"Upgrading schema from version {old_version} to {SCHEMA_VERSION}")
    
    # Apply version-specific transformations
    # This is a placeholder; in a real implementation, you'd have specific
    # transformation logic for each version upgrade path
    
    # Example of a transformation (adding a missing column)
    if "new_required_column" in FORECAST_OUTPUT_SCHEMA.columns and "new_required_column" not in df_copy.columns:
        df_copy["new_required_column"] = None  # Default value
    
    # Update the schema version
    df_copy["schema_version"] = SCHEMA_VERSION
    
    return df_copy


def get_schema_info() -> Dict[str, Any]:
    """
    Returns information about the storage schema for documentation.
    
    Returns:
        Dictionary with schema information
    """
    schema_info = {
        "columns": {},
        "metadata_fields": {},
        "version": SCHEMA_VERSION
    }
    
    # Extract column information from the schema
    for col_name, col_schema in FORECAST_OUTPUT_SCHEMA.columns.items():
        schema_info["columns"][col_name] = {
            "dtype": str(col_schema.dtype),
            "nullable": col_schema.nullable,
        }
        
        # Add check information if available
        if hasattr(col_schema, "checks") and col_schema.checks:
            checks = []
            for check in col_schema.checks:
                check_info = {"name": type(check).__name__}
                
                # Extract common properties of checks
                for attr in ["min_value", "max_value", "isin", "regex"]:
                    if hasattr(check, attr):
                        check_info[attr] = getattr(check, attr)
                
                checks.append(check_info)
            
            schema_info["columns"][col_name]["checks"] = checks
    
    # Add metadata field information
    for field_name, field_type in STORAGE_METADATA_FIELDS.items():
        schema_info["metadata_fields"][field_name] = {
            "dtype": field_type
        }
    
    return schema_info