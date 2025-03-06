"""
Defines pandera schema configurations for validating data structures throughout the 
Electricity Market Price Forecasting System. This module provides centralized schema 
definitions for input data sources, forecast outputs, and storage formats, ensuring 
data consistency and quality across the system.
"""

import pandera as pa  # version 0.16.0
import pandas as pd  # version 2.0.0
import numpy as np  # version 1.24.0
from datetime import datetime, timedelta

from .settings import (
    FORECAST_PRODUCTS,
    PROBABILISTIC_SAMPLE_COUNT,
    FORECAST_HORIZON_HOURS
)
from ..models.data_models import create_sample_columns

# Version of the schema definitions
SCHEMA_VERSION = "1.0.0"

# Generate sample column names for schema definition
SAMPLE_COLUMN_NAMES = create_sample_columns(PROBABILISTIC_SAMPLE_COUNT)


def create_base_schema():
    """
    Creates a base pandera schema with common validation rules.
    
    Returns:
        pandera.DataFrameSchema: Base schema with common validation rules
    """
    schema = pa.DataFrameSchema(
        {
            "timestamp": pa.Column(
                pa.Timestamp,
                nullable=False,
                checks=[
                    pa.Check.not_null(),
                    # Ensure timestamps are within a reasonable range (past and future)
                    pa.Check(
                        lambda x: x > pd.Timestamp.now() - pd.Timedelta(days=365),
                        element_wise=True,
                        error="Timestamp too old"
                    ),
                    pa.Check(
                        lambda x: x < pd.Timestamp.now() + pd.Timedelta(days=365),
                        element_wise=True,
                        error="Timestamp too far in future"
                    )
                ]
            )
        },
        strict=False,  # Allow additional columns not defined in schema
        coerce=True   # Attempt to coerce data types when possible
    )
    
    return schema


def create_forecast_schema():
    """
    Creates a schema for forecast data with probabilistic samples.
    
    Returns:
        pandera.DataFrameSchema: Schema for forecast data validation
    """
    # Start with base schema
    base_schema = create_base_schema()
    
    # Add forecast-specific columns
    schema_dict = {
        "product": pa.Column(
            pa.String,
            nullable=False,
            checks=[
                pa.Check.isin(FORECAST_PRODUCTS),
                pa.Check.not_null()
            ]
        ),
        "point_forecast": pa.Column(
            pa.Float,
            nullable=False,
            checks=[
                pa.Check.not_null()
            ]
        ),
        "generation_timestamp": pa.Column(
            pa.Timestamp,
            nullable=False,
            checks=[
                pa.Check.not_null(),
                # Ensure generation timestamp is not in the future
                pa.Check(
                    lambda x: x <= pd.Timestamp.now(), 
                    element_wise=True,
                    error="Generation timestamp cannot be in the future"
                )
            ]
        ),
        "is_fallback": pa.Column(
            pa.Boolean,
            nullable=False,
            checks=[
                pa.Check.not_null()
            ]
        )
    }
    
    # Add sample columns
    for sample_col in SAMPLE_COLUMN_NAMES:
        schema_dict[sample_col] = pa.Column(
            pa.Float,
            nullable=False,
            checks=[
                pa.Check.not_null()
            ]
        )
    
    # Update base schema with forecast columns
    for col_name, col_schema in schema_dict.items():
        base_schema.columns[col_name] = col_schema
    
    return base_schema


def create_load_forecast_schema():
    """
    Creates a schema for load forecast data.
    
    Returns:
        pandera.DataFrameSchema: Schema for load forecast validation
    """
    # Start with base schema
    base_schema = create_base_schema()
    
    # Add load forecast specific columns
    schema_dict = {
        "load_mw": pa.Column(
            pa.Float,
            nullable=False,
            checks=[
                pa.Check.not_null(),
                pa.Check.greater_than(0)  # Load must be positive
            ]
        ),
        "region": pa.Column(
            pa.String,
            nullable=False,
            checks=[
                pa.Check.not_null(),
                pa.Check(lambda x: len(x) > 0, element_wise=True)  # Non-empty string
            ]
        )
    }
    
    # Update base schema with load forecast columns
    for col_name, col_schema in schema_dict.items():
        base_schema.columns[col_name] = col_schema
    
    return base_schema


def create_historical_price_schema():
    """
    Creates a schema for historical price data.
    
    Returns:
        pandera.DataFrameSchema: Schema for historical price validation
    """
    # Start with base schema
    base_schema = create_base_schema()
    
    # Add historical price specific columns
    schema_dict = {
        "product": pa.Column(
            pa.String,
            nullable=False,
            checks=[
                pa.Check.isin(FORECAST_PRODUCTS),
                pa.Check.not_null()
            ]
        ),
        "price": pa.Column(
            pa.Float,
            nullable=False,
            checks=[
                pa.Check.not_null()
                # Note: No lower bound check since prices can be negative in some markets
            ]
        ),
        "node": pa.Column(
            pa.String,
            nullable=False,
            checks=[
                pa.Check.not_null(),
                pa.Check(lambda x: len(x) > 0, element_wise=True)  # Non-empty string
            ]
        )
    }
    
    # Update base schema with historical price columns
    for col_name, col_schema in schema_dict.items():
        base_schema.columns[col_name] = col_schema
    
    return base_schema


def create_generation_forecast_schema():
    """
    Creates a schema for generation forecast data.
    
    Returns:
        pandera.DataFrameSchema: Schema for generation forecast validation
    """
    # Start with base schema
    base_schema = create_base_schema()
    
    # Add generation forecast specific columns
    schema_dict = {
        "fuel_type": pa.Column(
            pa.String,
            nullable=False,
            checks=[
                pa.Check.not_null(),
                pa.Check(lambda x: len(x) > 0, element_wise=True)  # Non-empty string
            ]
        ),
        "generation_mw": pa.Column(
            pa.Float,
            nullable=False,
            checks=[
                pa.Check.not_null(),
                pa.Check.greater_than_or_equal_to(0)  # Generation must be non-negative
            ]
        ),
        "region": pa.Column(
            pa.String,
            nullable=False,
            checks=[
                pa.Check.not_null(),
                pa.Check(lambda x: len(x) > 0, element_wise=True)  # Non-empty string
            ]
        )
    }
    
    # Update base schema with generation forecast columns
    for col_name, col_schema in schema_dict.items():
        base_schema.columns[col_name] = col_schema
    
    return base_schema


def get_schema_info(schema):
    """
    Returns information about a schema for documentation purposes.
    
    Args:
        schema (pandera.DataFrameSchema): Schema to get information for
        
    Returns:
        dict: Dictionary with schema information
    """
    info = {
        "columns": {},
        "checks": {},
        "coerce": schema.coerce,
        "strict": schema.strict,
        "version": SCHEMA_VERSION
    }
    
    # Extract column information
    for col_name, col_schema in schema.columns.items():
        info["columns"][col_name] = {
            "dtype": str(col_schema.dtype),
            "nullable": col_schema.nullable
        }
        
        # Extract check information
        checks = []
        for check in col_schema.checks:
            checks.append(str(check))
        
        info["checks"][col_name] = checks
    
    return info


# Create base schema with common validation rules
FORECAST_BASE_SCHEMA = create_base_schema()

# Create forecast schemas
FORECAST_INPUT_SCHEMA = create_historical_price_schema()  # For historical price inputs
FORECAST_OUTPUT_SCHEMA = create_forecast_schema()  # For forecast outputs

# Create schemas for external data sources
LOAD_FORECAST_SCHEMA = create_load_forecast_schema()
HISTORICAL_PRICE_SCHEMA = create_historical_price_schema()
GENERATION_FORECAST_SCHEMA = create_generation_forecast_schema()