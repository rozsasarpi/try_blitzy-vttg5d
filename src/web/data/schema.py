"""
Schema validation and transformation functions for forecast data in the web visualization interface.

This module provides utilities to validate, transform, and prepare forecast dataframes
for visualization, ensuring data quality and consistency throughout the dashboard components.
"""

import pandas as pd  # version 2.0.0
import pandera as pa  # version 0.16.0
import numpy as np  # version 1.24.0
import logging  # standard library
from typing import Dict, List, Tuple, Optional, Any, Union  # standard library

from ..config.product_config import PRODUCTS, get_product_unit, can_be_negative
from ..config.dashboard_config import DISTRIBUTION_CONFIG
from ...backend.models.data_models import PriceForecast, SAMPLE_COLUMN_PREFIX

# Configure logger
logger = logging.getLogger(__name__)

# Define the schema for web visualization dataframes
WEB_VISUALIZATION_SCHEMA = pa.DataFrameSchema({
    "timestamp": pa.Column(pd.Timestamp),
    "product": pa.Column(str, checks=pa.Check.isin(PRODUCTS)),
    "point_forecast": pa.Column(float),
    "lower_bound": pa.Column(float),
    "upper_bound": pa.Column(float),
    "is_fallback": pa.Column(bool)
})

# Default percentiles for uncertainty bands
DEFAULT_PERCENTILES = [10, 90]

def validate_forecast_dataframe(df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
    """
    Validates a forecast dataframe against the web visualization schema.
    
    Args:
        df: The forecast dataframe to validate
        
    Returns:
        Tuple of (bool, dict) indicating success and any validation errors
    """
    logger.info("Validating forecast dataframe against web visualization schema")
    
    # Check for required basic columns
    required_columns = ["timestamp", "product", "point_forecast"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        error_details = {"missing_columns": missing_columns}
        logger.error(f"Forecast dataframe missing required columns: {missing_columns}")
        return False, error_details
    
    # Check if this is a backend format dataframe (has sample columns)
    sample_columns = get_sample_columns(df)
    
    if not sample_columns and "lower_bound" not in df.columns and "upper_bound" not in df.columns:
        error_details = {"error": "Dataframe must have either sample columns or lower/upper bounds"}
        logger.error("Forecast dataframe has neither sample columns nor uncertainty bounds")
        return False, error_details
    
    # If already in visualization format (has lower_bound and upper_bound), validate against schema
    if "lower_bound" in df.columns and "upper_bound" in df.columns:
        try:
            WEB_VISUALIZATION_SCHEMA.validate(df)
            logger.info("Forecast dataframe successfully validated against web visualization schema")
            return True, {}
        except pa.errors.SchemaError as e:
            logger.error(f"Schema validation failed: {str(e)}")
            # Format validation errors in a more readable format
            formatted_errors = {}
            for error in e.collected_errors:
                formatted_errors[error.column] = str(error)
            return False, formatted_errors
    
    # If it has sample columns but not lower/upper bounds, it needs transformation
    logger.info("Dataframe has sample columns but not visualization format. Needs transformation.")
    return True, {"needs_transformation": True}

def prepare_dataframe_for_visualization(df: pd.DataFrame, percentiles: List[int] = None) -> pd.DataFrame:
    """
    Transforms a backend forecast dataframe into the format needed for visualization.
    
    Args:
        df: The forecast dataframe to transform
        percentiles: List of percentiles to use for lower and upper bounds. Default: [10, 90]
        
    Returns:
        Transformed dataframe ready for visualization
    """
    # Validate input dataframe has required columns
    required_columns = ["timestamp", "product", "point_forecast"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Dataframe missing required columns: {missing_columns}")
    
    # Use default percentiles if none provided
    if percentiles is None:
        percentiles = DEFAULT_PERCENTILES
    
    if len(percentiles) != 2:
        raise ValueError("Percentiles must be a list of exactly 2 values (lower, upper)")
    
    # Create a copy to avoid modifying the original
    viz_df = df.copy()
    
    # Sample columns exist - extract bounds from samples
    sample_columns = get_sample_columns(df)
    if sample_columns:
        logger.info(f"Extracting percentiles {percentiles} from sample columns")
        
        # Get lower and upper bounds from samples
        sample_values = viz_df[sample_columns].values
        
        # Calculate percentiles along the sample axis (axis=1)
        lower_percentile = np.percentile(sample_values, percentiles[0], axis=1)
        upper_percentile = np.percentile(sample_values, percentiles[1], axis=1)
        
        # Add bounds to dataframe
        viz_df["lower_bound"] = lower_percentile
        viz_df["upper_bound"] = upper_percentile
    
    # Keep only columns needed for visualization
    keep_columns = ["timestamp", "product", "point_forecast", "lower_bound", "upper_bound", "is_fallback"]
    
    # Ensure is_fallback column exists
    if "is_fallback" not in viz_df.columns:
        viz_df["is_fallback"] = False
    
    # Filter columns and ensure they are in the right order
    viz_df = viz_df[keep_columns]
    
    # Sort by timestamp and product for consistent display
    viz_df = viz_df.sort_values(["timestamp", "product"])
    
    return viz_df

def extract_samples_from_dataframe(df: pd.DataFrame, percentiles: List[int] = None) -> pd.DataFrame:
    """
    Extracts probabilistic samples from a forecast dataframe.
    
    Args:
        df: The forecast dataframe containing sample columns
        percentiles: List of percentiles to extract, default: [10, 90]
        
    Returns:
        Dataframe with extracted percentile values
    """
    # Validate input dataframe has required columns
    required_columns = ["timestamp", "product", "point_forecast"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Dataframe missing required columns: {missing_columns}")
    
    # Use default percentiles if none provided
    if percentiles is None:
        percentiles = DEFAULT_PERCENTILES
    
    # Create a copy to avoid modifying the original
    result_df = df.copy()
    
    # Get sample columns
    sample_columns = get_sample_columns(df)
    if not sample_columns:
        logger.warning("No sample columns found in dataframe")
        return result_df
    
    logger.info(f"Extracting percentiles {percentiles} from {len(sample_columns)} sample columns")
    
    # Calculate percentiles for each row
    for percentile in percentiles:
        column_name = f"percentile_{percentile}"
        sample_values = result_df[sample_columns].values
        result_df[column_name] = np.percentile(sample_values, percentile, axis=1)
    
    return result_df

def get_sample_columns(df: pd.DataFrame) -> List[str]:
    """
    Identifies sample columns in a forecast dataframe.
    
    Args:
        df: The forecast dataframe
        
    Returns:
        List of sample column names
    """
    return [col for col in df.columns if col.startswith(SAMPLE_COLUMN_PREFIX)]

def convert_to_price_forecast_models(df: pd.DataFrame) -> List[PriceForecast]:
    """
    Converts a forecast dataframe to a list of PriceForecast model instances.
    
    Args:
        df: The forecast dataframe with sample columns
        
    Returns:
        List of PriceForecast model instances
    """
    # Validate input dataframe has required columns
    required_columns = ["timestamp", "product", "point_forecast", "generation_timestamp"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Dataframe missing required columns: {missing_columns}")
    
    # Get sample columns
    sample_columns = get_sample_columns(df)
    if not sample_columns:
        raise ValueError("No sample columns found in dataframe")
    
    logger.info(f"Converting dataframe with {len(df)} rows to PriceForecast models")
    
    # Convert each row to a PriceForecast model
    forecast_models = []
    for _, row in df.iterrows():
        forecast_model = PriceForecast.from_dataframe_row(row)
        forecast_models.append(forecast_model)
    
    return forecast_models

def add_unit_information(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds unit information to a visualization dataframe.
    
    Args:
        df: The visualization dataframe
        
    Returns:
        Dataframe with unit information added
    """
    # Create a copy to avoid modifying the original
    result_df = df.copy()
    
    # Add unit column based on product
    result_df["unit"] = result_df["product"].apply(get_product_unit)
    
    return result_df

def validate_price_ranges(df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
    """
    Validates that price ranges in the dataframe are appropriate for each product.
    
    Args:
        df: The visualization dataframe
        
    Returns:
        Tuple of (bool, dict) indicating validation status and any issues
    """
    logger.info("Validating price ranges for forecast dataframe")
    
    validation_issues = {}
    
    # Check for each product
    for product in df["product"].unique():
        product_df = df[df["product"] == product]
        
        # Check if product can have negative prices
        if not can_be_negative(product):
            # Check if any values are negative
            neg_point = product_df[product_df["point_forecast"] < 0]
            neg_lower = product_df[product_df["lower_bound"] < 0]
            neg_upper = product_df[product_df["upper_bound"] < 0]
            
            if not neg_point.empty or not neg_lower.empty or not neg_upper.empty:
                validation_issues[f"{product}_negative_prices"] = {
                    "product": product,
                    "error": f"{product} cannot have negative prices",
                    "point_forecast_count": len(neg_point),
                    "lower_bound_count": len(neg_lower),
                    "upper_bound_count": len(neg_upper)
                }
        
        # Check if lower_bound <= point_forecast <= upper_bound
        invalid_bounds = product_df[
            (product_df["lower_bound"] > product_df["point_forecast"]) | 
            (product_df["point_forecast"] > product_df["upper_bound"])
        ]
        
        if not invalid_bounds.empty:
            validation_issues[f"{product}_invalid_bounds"] = {
                "product": product,
                "error": f"{product} has invalid bounds (lower > point or point > upper)",
                "count": len(invalid_bounds)
            }
    
    if validation_issues:
        logger.error(f"Validation found {len(validation_issues)} issues with price ranges")
        return False, validation_issues
    
    logger.info("Price range validation passed")
    return True, {}

def get_schema_info() -> Dict[str, Any]:
    """
    Returns information about the web visualization schema.
    
    Returns:
        Dictionary with schema information
    """
    # Extract column information from schema
    columns_info = {}
    for column_name, column in WEB_VISUALIZATION_SCHEMA.columns.items():
        column_info = {
            "type": str(column.dtype),
            "nullable": column.nullable
        }
        
        # Add checks information if available
        if column.checks:
            checks = []
            for check in column.checks:
                checks.append(str(check))
            column_info["checks"] = checks
        
        columns_info[column_name] = column_info
    
    schema_info = {
        "columns": columns_info,
        "transformations": {
            "from_backend_format": "Use prepare_dataframe_for_visualization() to transform backend format to visualization format",
            "required_columns": ["timestamp", "product", "point_forecast", "lower_bound", "upper_bound", "is_fallback"]
        },
        "percentiles": {
            "default": DEFAULT_PERCENTILES,
            "configuration": "See DISTRIBUTION_CONFIG for visualization-specific percentile settings"
        }
    }
    
    return schema_info