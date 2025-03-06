# src/backend/tests/test_storage/test_schema_definitions.py
"""
Unit tests for the schema_definitions module which handles forecast dataframe schema validation,
metadata management, and schema compatibility. This test file verifies that forecast data
properly adheres to the defined pandera schema and that storage metadata is correctly handled.
"""

import pytest  # package_version: 7.0.0+
import pandas as pd  # package_version: 2.0.0+
import numpy as np  # package_version: 1.24.0+
from datetime import datetime  # standard library

from src.backend.storage.schema_definitions import (
    validate_forecast_schema,
    add_storage_metadata,
    check_storage_integrity,
    extract_storage_metadata,
    verify_schema_compatibility,
    upgrade_schema_if_needed,
    get_schema_info,
    STORAGE_METADATA_FIELDS,
    SchemaValidationError,
    DataIntegrityError
)
from src.backend.config.schema_config import FORECAST_OUTPUT_SCHEMA, SCHEMA_VERSION
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data, create_invalid_forecast_data


def test_validate_forecast_schema_valid_data():
    """
    Tests that validate_forecast_schema correctly validates a valid forecast dataframe
    """
    # Create a valid forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Call validate_forecast_schema with the valid dataframe
    is_valid, errors = validate_forecast_schema(valid_df)

    # Assert that the validation result is True
    assert is_valid is True

    # Assert that the error dictionary is empty
    assert not errors


def test_validate_forecast_schema_invalid_data():
    """
    Tests that validate_forecast_schema correctly identifies invalid forecast data
    """
    # Create an invalid forecast dataframe using create_invalid_forecast_data
    invalid_df = create_invalid_forecast_data()

    # Call validate_forecast_schema with the invalid dataframe
    is_valid, errors = validate_forecast_schema(invalid_df)

    # Assert that the validation result is False
    assert is_valid is False

    # Assert that the error dictionary contains expected validation errors
    assert errors


def test_add_storage_metadata():
    """
    Tests that add_storage_metadata correctly adds metadata to a forecast dataframe
    """
    # Create a valid forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Call add_storage_metadata with the dataframe
    df_with_metadata = add_storage_metadata(valid_df)

    # Assert that all required metadata fields are present in the result
    for field in STORAGE_METADATA_FIELDS:
        assert field in df_with_metadata.columns

    # Assert that storage_timestamp is a datetime64[ns] type
    assert pd.api.types.is_datetime64_dtype(df_with_metadata["storage_timestamp"])

    # Assert that storage_version is a string
    assert pd.api.types.is_string_dtype(df_with_metadata["storage_version"])

    # Assert that schema_version matches the expected SCHEMA_VERSION
    assert df_with_metadata["schema_version"].iloc[0] == SCHEMA_VERSION


def test_check_storage_integrity_valid_data():
    """
    Tests that check_storage_integrity correctly validates a dataframe with proper metadata
    """
    # Create a valid forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Add storage metadata using add_storage_metadata
    df_with_metadata = add_storage_metadata(valid_df)

    # Call check_storage_integrity with the dataframe
    is_valid, issues = check_storage_integrity(df_with_metadata)

    # Assert that the integrity check result is True
    assert is_valid is True

    # Assert that the issues dictionary is empty
    assert not issues


def test_check_storage_integrity_missing_metadata():
    """
    Tests that check_storage_integrity correctly identifies missing metadata
    """
    # Create a valid forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Call check_storage_integrity with the dataframe (without adding metadata)
    is_valid, issues = check_storage_integrity(valid_df)

    # Assert that the integrity check result is False
    assert is_valid is False

    # Assert that the issues dictionary contains missing metadata field errors
    assert "missing_metadata" in issues


def test_extract_storage_metadata():
    """
    Tests that extract_storage_metadata correctly extracts metadata from a dataframe
    """
    # Create a valid forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Add storage metadata using add_storage_metadata
    df_with_metadata = add_storage_metadata(valid_df)

    # Call extract_storage_metadata with the dataframe
    metadata = extract_storage_metadata(df_with_metadata)

    # Assert that the returned metadata dictionary contains all expected fields
    for field in STORAGE_METADATA_FIELDS:
        assert field in metadata

    # Assert that the values match those in the dataframe
    assert metadata["storage_version"] == df_with_metadata["storage_version"].iloc[0]
    assert metadata["schema_version"] == df_with_metadata["schema_version"].iloc[0]


def test_verify_schema_compatibility_same_version():
    """
    Tests that verify_schema_compatibility returns True for same schema version
    """
    # Create a valid forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Add storage metadata using add_storage_metadata
    df_with_metadata = add_storage_metadata(valid_df)

    # Call verify_schema_compatibility with the dataframe
    is_compatible = verify_schema_compatibility(df_with_metadata)

    # Assert that the result is True (compatible)
    assert is_compatible is True


def test_verify_schema_compatibility_different_version():
    """
    Tests that verify_schema_compatibility handles different schema versions
    """
    # Create a valid forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Add storage metadata using add_storage_metadata
    df_with_metadata = add_storage_metadata(valid_df)

    # Modify the schema_version to an older version
    df_with_metadata["schema_version"] = "0.9.0"

    # Call verify_schema_compatibility with the dataframe
    is_compatible = verify_schema_compatibility(df_with_metadata)

    # Assert the expected compatibility result based on version difference
    assert is_compatible is True


def test_upgrade_schema_if_needed_no_upgrade():
    """
    Tests that upgrade_schema_if_needed returns original dataframe when no upgrade needed
    """
    # Create a valid forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Add storage metadata using add_storage_metadata
    df_with_metadata = add_storage_metadata(valid_df)

    # Call upgrade_schema_if_needed with the dataframe
    upgraded_df = upgrade_schema_if_needed(df_with_metadata)

    # Assert that the returned dataframe is identical to the input
    pd.testing.assert_frame_equal(df_with_metadata, upgraded_df)


def test_upgrade_schema_if_needed_with_upgrade():
    """
    Tests that upgrade_schema_if_needed correctly upgrades an older schema version
    """
    # Create a valid forecast dataframe using create_mock_forecast_data
    valid_df = create_mock_forecast_data()

    # Add storage metadata using add_storage_metadata
    df_with_metadata = add_storage_metadata(valid_df)

    # Modify the schema_version to an older version
    df_with_metadata["schema_version"] = "0.9.0"

    # Call upgrade_schema_if_needed with the dataframe
    upgraded_df = upgrade_schema_if_needed(df_with_metadata)

    # Assert that the schema_version in the result matches the current SCHEMA_VERSION
    assert upgraded_df["schema_version"].iloc[0] == SCHEMA_VERSION

    # Assert that any necessary transformations were applied
    assert "new_required_column" in upgraded_df.columns


def test_get_schema_info():
    """
    Tests that get_schema_info returns correct schema information
    """
    # Call get_schema_info to retrieve schema information
    schema_info = get_schema_info()

    # Assert that the returned dictionary contains expected keys
    assert "columns" in schema_info
    assert "metadata_fields" in schema_info
    assert "version" in schema_info

    # Assert that column information is present and accurate
    assert "timestamp" in schema_info["columns"]
    assert schema_info["columns"]["timestamp"]["dtype"] == "datetime64[ns]"

    # Assert that metadata field information is present and accurate
    assert "storage_timestamp" in schema_info["metadata_fields"]
    assert schema_info["metadata_fields"]["storage_timestamp"]["dtype"] == "datetime64[ns]"

    # Assert that version information matches SCHEMA_VERSION
    assert schema_info["version"] == SCHEMA_VERSION