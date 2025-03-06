"""
Implements validation for forecast plausibility in the Electricity Market Price Forecasting System.
This module ensures that forecast values are physically plausible, checking for valid price ranges,
extreme outliers, and other physical constraints specific to each product type.
"""

import pandas as pd  # version: 2.0.0
import numpy as np  # version: 1.24.0
from typing import Dict, List, Any, Optional, Union

# Internal imports
from .exceptions import PlausibilityValidationError
from .validation_result import ValidationCategory, ValidationResult, create_success_result, create_error_result
from ..config.settings import FORECAST_PRODUCTS
from ..models.forecast_models import ProbabilisticForecast
from ..utils.logging_utils import get_logger

# Set up logger
logger = get_logger(__name__)

# Define default constraints for each product type
PRODUCT_CONSTRAINTS = {
    "DALMP": {
        "min_value": -500.0,  # LMP products can have negative prices in some scenarios
        "max_value": 2000.0,   # Upper cap on reasonable price
        "outlier_threshold": 10.0  # Standard deviations for outlier detection
    },
    "RTLMP": {
        "min_value": -500.0,
        "max_value": 2000.0,
        "outlier_threshold": 10.0
    },
    # Ancillary services cannot have negative prices
    "RegUp": {
        "min_value": 0.0,
        "max_value": 1000.0,
        "outlier_threshold": 5.0
    },
    "RegDown": {
        "min_value": 0.0,
        "max_value": 1000.0,
        "outlier_threshold": 5.0
    },
    "RRS": {  # Responsive Reserve Service
        "min_value": 0.0,
        "max_value": 500.0,
        "outlier_threshold": 5.0
    },
    "NSRS": {  # Non-Spinning Reserve Service
        "min_value": 0.0,
        "max_value": 500.0,
        "outlier_threshold": 5.0
    }
}


def validate_forecast_plausibility(forecast_df: pd.DataFrame) -> ValidationResult:
    """
    Validates that forecast values are physically plausible.
    
    This function checks that values are within acceptable ranges for each product
    and detects statistical outliers that may indicate unrealistic forecasts.
    
    Args:
        forecast_df: DataFrame containing forecast data to validate
        
    Returns:
        ValidationResult indicating plausibility status
    """
    logger.info("Starting forecast plausibility validation")
    
    # Check if forecast_df is empty or None
    if forecast_df is None or forecast_df.empty:
        logger.warning("Empty or None forecast dataframe provided for validation")
        return create_error_result(
            ValidationCategory.PLAUSIBILITY,
            {"general": ["Empty or None forecast dataframe provided"]}
        )
    
    # Initialize error dictionary
    errors: Dict[str, List[str]] = {}
    
    # Validate value ranges for each product
    range_errors = validate_value_ranges(forecast_df, PRODUCT_CONSTRAINTS)
    if range_errors:
        errors.update(range_errors)
    
    # Detect outliers for each product
    outlier_errors = detect_outliers(forecast_df, PRODUCT_CONSTRAINTS)
    if outlier_errors:
        errors.update(outlier_errors)
    
    # Return success if no errors, otherwise return error result
    if not errors:
        logger.info("Forecast plausibility validation passed")
        return create_success_result(ValidationCategory.PLAUSIBILITY)
    else:
        logger.warning(f"Forecast plausibility validation failed with {len(errors)} error categories")
        return create_error_result(ValidationCategory.PLAUSIBILITY, errors)


def validate_value_ranges(forecast_df: pd.DataFrame, product_constraints: Dict[str, Dict[str, float]]) -> Dict[str, List[str]]:
    """
    Validates that forecast values are within acceptable ranges for each product.
    
    Args:
        forecast_df: DataFrame containing forecast data
        product_constraints: Dictionary of constraints for each product
        
    Returns:
        Dictionary of range validation errors by product
    """
    errors: Dict[str, List[str]] = {}
    
    # Group by product
    grouped = forecast_df.groupby('product')
    
    for product, group in grouped:
        # Skip if product is not in constraints (should be caught by other validators)
        if product not in product_constraints:
            continue
        
        constraints = product_constraints[product]
        min_value = constraints.get("min_value", float("-inf"))
        max_value = constraints.get("max_value", float("inf"))
        
        # Check point_forecast values
        if 'point_forecast' in group.columns:
            invalid_points = group[~group['point_forecast'].apply(
                lambda x: is_value_in_range(x, min_value, max_value)
            )]
            
            if not invalid_points.empty:
                if product not in errors:
                    errors[product] = []
                
                for _, row in invalid_points.iterrows():
                    timestamp = row['timestamp']
                    value = row['point_forecast']
                    errors[product].append(
                        f"Point forecast at {timestamp} has value {value} outside allowed range [{min_value}, {max_value}]"
                    )
        
        # Check sample values (if present)
        sample_columns = [col for col in group.columns if col.startswith('sample_')]
        for sample_col in sample_columns:
            invalid_samples = group[~group[sample_col].apply(
                lambda x: is_value_in_range(x, min_value, max_value)
            )]
            
            if not invalid_samples.empty:
                if product not in errors:
                    errors[product] = []
                
                for _, row in invalid_samples.iterrows():
                    timestamp = row['timestamp']
                    value = row[sample_col]
                    errors[product].append(
                        f"Sample {sample_col} at {timestamp} has value {value} outside allowed range [{min_value}, {max_value}]"
                    )
    
    return errors


def detect_outliers(forecast_df: pd.DataFrame, product_constraints: Dict[str, Dict[str, float]]) -> Dict[str, List[str]]:
    """
    Detects outlier values in forecast data based on statistical properties.
    
    Args:
        forecast_df: DataFrame containing forecast data
        product_constraints: Dictionary of constraints for each product
        
    Returns:
        Dictionary of outlier detection errors by product
    """
    errors: Dict[str, List[str]] = {}
    
    # Group by product
    grouped = forecast_df.groupby('product')
    
    for product, group in grouped:
        # Skip if product is not in constraints
        if product not in product_constraints:
            continue
            
        constraints = product_constraints[product]
        outlier_threshold = constraints.get("outlier_threshold", 5.0)
        
        # Calculate mean and standard deviation of point_forecast
        if 'point_forecast' in group.columns and len(group) > 1:
            mean = group['point_forecast'].mean()
            std = group['point_forecast'].std()
            
            # If std is near zero, skip outlier detection to avoid division issues
            if std < 1e-8:
                continue
                
            # Identify outliers
            outliers = group[abs(group['point_forecast'] - mean) > outlier_threshold * std]
            
            if not outliers.empty:
                if product not in errors:
                    errors[product] = []
                    
                for _, row in outliers.iterrows():
                    timestamp = row['timestamp']
                    value = row['point_forecast']
                    z_score = (value - mean) / std
                    errors[product].append(
                        f"Outlier detected at {timestamp}: value {value} is {abs(z_score):.2f} standard deviations from mean"
                    )
    
    return errors


def is_value_in_range(value: float, min_value: float, max_value: float) -> bool:
    """
    Checks if a value is within the specified range.
    
    Args:
        value: Value to check
        min_value: Minimum allowed value
        max_value: Maximum allowed value
        
    Returns:
        True if value is within range, False otherwise
    """
    # Check for None or NaN
    if value is None or pd.isna(value):
        return False
        
    # Check range
    return min_value <= value <= max_value


class PlausibilityValidator:
    """
    Class for validating the plausibility of forecast data.
    
    This validator ensures that forecast values are physically plausible,
    checking for valid price ranges, extreme outliers, and other
    physical constraints specific to each product type.
    """
    
    def __init__(self, product_constraints: Optional[Dict[str, Dict[str, float]]] = None, default_outlier_threshold: float = 5.0):
        """
        Initializes the plausibility validator with configuration.
        
        Args:
            product_constraints: Dictionary of constraints for each product
            default_outlier_threshold: Default threshold for outlier detection
        """
        self._product_constraints = product_constraints or PRODUCT_CONSTRAINTS
        self._default_outlier_threshold = default_outlier_threshold
        logger.info(f"Initialized PlausibilityValidator with {len(self._product_constraints)} product constraints")
    
    def validate(self, forecast_df: pd.DataFrame) -> ValidationResult:
        """
        Validates the plausibility of a forecast dataframe.
        
        This method checks that values are within acceptable ranges for each product
        and detects statistical outliers that may indicate unrealistic forecasts.
        
        Args:
            forecast_df: DataFrame containing forecast data to validate
            
        Returns:
            ValidationResult indicating plausibility status
        """
        logger.info(f"Starting forecast plausibility validation with validator instance")
        
        # Check if forecast_df is empty or None
        if forecast_df is None or forecast_df.empty:
            logger.warning("Empty or None forecast dataframe provided for validation")
            return create_error_result(
                ValidationCategory.PLAUSIBILITY,
                {"general": ["Empty or None forecast dataframe provided"]}
            )
        
        # Validate value ranges for each product
        range_errors = self.validate_value_ranges(forecast_df)
        
        # Detect outliers for each product
        outlier_errors = self.detect_outliers(forecast_df)
        
        # Format error messages
        if range_errors or outlier_errors:
            errors = self.format_error_messages(range_errors, outlier_errors)
            logger.warning(f"Forecast plausibility validation failed with {len(errors)} error categories")
            return create_error_result(ValidationCategory.PLAUSIBILITY, errors)
        else:
            logger.info("Forecast plausibility validation passed")
            return create_success_result(ValidationCategory.PLAUSIBILITY)
    
    def validate_value_ranges(self, forecast_df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Validates that forecast values are within acceptable ranges for each product.
        
        Args:
            forecast_df: DataFrame containing forecast data
            
        Returns:
            Dictionary of range validation errors by product
        """
        errors: Dict[str, List[str]] = {}
        
        # Group by product
        grouped = forecast_df.groupby('product')
        
        for product, group in grouped:
            # Get constraints for this product
            constraints = self.get_product_constraint(product)
            min_value = constraints.get("min_value", float("-inf"))
            max_value = constraints.get("max_value", float("inf"))
            
            # Check point_forecast values
            if 'point_forecast' in group.columns:
                invalid_points = group[~group['point_forecast'].apply(
                    lambda x: self.is_value_in_range(x, min_value, max_value)
                )]
                
                if not invalid_points.empty:
                    if product not in errors:
                        errors[product] = []
                    
                    for _, row in invalid_points.iterrows():
                        timestamp = row['timestamp']
                        value = row['point_forecast']
                        errors[product].append(
                            f"Point forecast at {timestamp} has value {value} outside allowed range [{min_value}, {max_value}]"
                        )
            
            # Check sample values (if present)
            sample_columns = [col for col in group.columns if col.startswith('sample_')]
            for sample_col in sample_columns:
                invalid_samples = group[~group[sample_col].apply(
                    lambda x: self.is_value_in_range(x, min_value, max_value)
                )]
                
                if not invalid_samples.empty:
                    if product not in errors:
                        errors[product] = []
                    
                    for _, row in invalid_samples.iterrows():
                        timestamp = row['timestamp']
                        value = row[sample_col]
                        errors[product].append(
                            f"Sample {sample_col} at {timestamp} has value {value} outside allowed range [{min_value}, {max_value}]"
                        )
        
        return errors
    
    def detect_outliers(self, forecast_df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Detects outlier values in forecast data based on statistical properties.
        
        Args:
            forecast_df: DataFrame containing forecast data
            
        Returns:
            Dictionary of outlier detection errors by product
        """
        errors: Dict[str, List[str]] = {}
        
        # Group by product
        grouped = forecast_df.groupby('product')
        
        for product, group in grouped:
            # Get outlier threshold for this product
            constraints = self.get_product_constraint(product)
            outlier_threshold = constraints.get("outlier_threshold", self._default_outlier_threshold)
            
            # Calculate mean and standard deviation of point_forecast
            if 'point_forecast' in group.columns and len(group) > 1:
                mean = group['point_forecast'].mean()
                std = group['point_forecast'].std()
                
                # If std is near zero, skip outlier detection to avoid division issues
                if std < 1e-8:
                    continue
                    
                # Identify outliers
                outliers = group[abs(group['point_forecast'] - mean) > outlier_threshold * std]
                
                if not outliers.empty:
                    if product not in errors:
                        errors[product] = []
                        
                    for _, row in outliers.iterrows():
                        timestamp = row['timestamp']
                        value = row['point_forecast']
                        z_score = (value - mean) / std
                        errors[product].append(
                            f"Outlier detected at {timestamp}: value {value} is {abs(z_score):.2f} standard deviations from mean"
                        )
        
        return errors
        
    def is_value_in_range(self, value: float, min_value: float, max_value: float) -> bool:
        """
        Checks if a value is within the specified range.
        
        Args:
            value: Value to check
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            
        Returns:
            True if value is within range, False otherwise
        """
        # Check for None or NaN
        if value is None or pd.isna(value):
            return False
            
        # Check range
        return min_value <= value <= max_value
    
    def get_product_constraint(self, product: str) -> Dict[str, float]:
        """
        Gets constraints for a specific product.
        
        Args:
            product: Product to get constraints for
            
        Returns:
            Constraints dictionary for the product
        """
        # Return product constraints if they exist, otherwise return default
        if product in self._product_constraints:
            return self._product_constraints[product]
        else:
            logger.warning(f"No constraints defined for product {product}, using defaults")
            return {
                "min_value": float("-inf"),
                "max_value": float("inf"),
                "outlier_threshold": self._default_outlier_threshold
            }
    
    def format_error_messages(self, range_errors: Dict[str, List[str]], outlier_errors: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """
        Formats error messages for plausibility validation.
        
        Args:
            range_errors: Dictionary of range validation errors by product
            outlier_errors: Dictionary of outlier detection errors by product
            
        Returns:
            Dictionary of formatted error messages
        """
        error_messages: Dict[str, List[str]] = {}
        
        # Format range validation errors
        for product, messages in range_errors.items():
            category = f"{product}_range"
            error_messages[category] = messages
        
        # Format outlier detection errors
        for product, messages in outlier_errors.items():
            category = f"{product}_outliers"
            error_messages[category] = messages
        
        return error_messages