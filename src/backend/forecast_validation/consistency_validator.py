"""
Implements validation for forecast consistency in the Electricity Market Price Forecasting System.
This module ensures that forecasts maintain expected relationships between related products and
follow logical patterns, detecting anomalies that could impact trading decisions.
"""

import pandas as pd  # version: 2.0.0
import numpy as np  # version: 1.24.0
from typing import Dict, List, Any

# Internal imports
from .exceptions import ConsistencyValidationError
from .validation_result import (
    ValidationCategory,
    ValidationResult,
    create_success_result,
    create_error_result
)
from ..config.settings import FORECAST_PRODUCTS
from ..models.forecast_models import ProbabilisticForecast
from ..utils.logging_utils import get_logger

# Initialize logger
logger = get_logger(__name__)

# Default product relationships to validate
PRODUCT_RELATIONSHIPS = {
    "DALMP": {
        "related_products": ["RegUp", "RegDown", "RRS", "NSRS"],
        "relationship": "greater_than"
    },
    "RTLMP": {
        "related_products": ["RegUp", "RegDown", "RRS", "NSRS"],
        "relationship": "greater_than"
    }
}

# Default threshold for temporal smoothness validation (30% change)
TEMPORAL_SMOOTHNESS_THRESHOLD = 0.3


def validate_forecast_consistency(forecast_df: pd.DataFrame) -> ValidationResult:
    """
    Validates that forecast values maintain expected relationships between products and temporal patterns.
    
    Args:
        forecast_df: DataFrame containing forecast data with product, timestamp, and point_forecast columns
        
    Returns:
        ValidationResult indicating consistency status
    """
    logger.info("Starting forecast consistency validation")
    
    # Check if forecast_df is empty or None
    if forecast_df is None or forecast_df.empty:
        logger.warning("Empty or None forecast_df provided for consistency validation")
        return create_error_result(
            ValidationCategory.CONSISTENCY,
            {"forecast_data": ["No forecast data provided for consistency validation"]}
        )
    
    # Initialize error dictionary
    errors = {}
    
    # Validate product relationships
    relationship_errors = validate_product_relationships(forecast_df, PRODUCT_RELATIONSHIPS)
    
    # Validate temporal smoothness
    smoothness_errors = validate_temporal_smoothness(forecast_df, TEMPORAL_SMOOTHNESS_THRESHOLD)
    
    # Combine errors
    errors.update(relationship_errors)
    errors.update(smoothness_errors)
    
    # Return validation result
    if not errors:
        logger.info("Forecast consistency validation passed")
        return create_success_result(ValidationCategory.CONSISTENCY)
    else:
        logger.warning(f"Forecast consistency validation failed with {len(errors)} issues")
        return create_error_result(ValidationCategory.CONSISTENCY, errors)


def validate_product_relationships(
    forecast_df: pd.DataFrame,
    product_relationships: Dict[str, Dict[str, Any]]
) -> Dict[str, List[str]]:
    """
    Validates that relationships between related products are maintained.
    
    Args:
        forecast_df: DataFrame containing forecast data
        product_relationships: Dictionary defining relationships between products
        
    Returns:
        Dictionary of relationship validation errors by product pair
    """
    errors = {}
    
    # Group by timestamp to compare products at the same time
    grouped = forecast_df.groupby('timestamp')
    
    for timestamp, group in grouped:
        # Create a dictionary of products to their point forecasts for this timestamp
        product_forecasts = dict(zip(group['product'], group['point_forecast']))
        
        # Check relationships for each product with defined relationships
        for product, relationship_config in product_relationships.items():
            # Skip if this product is not in the current timestamp group
            if product not in product_forecasts:
                continue
            
            product_value = product_forecasts[product]
            related_products = relationship_config['related_products']
            relationship_type = relationship_config['relationship']
            
            # Check each related product
            for related_product in related_products:
                # Skip if this related product is not in the current timestamp group
                if related_product not in product_forecasts:
                    continue
                
                related_value = product_forecasts[related_product]
                
                # Check if relationship is maintained
                is_valid = check_relationship(product_value, related_value, relationship_type)
                
                if not is_valid:
                    error_key = f"{product}_{related_product}_relationship"
                    if error_key not in errors:
                        errors[error_key] = []
                    
                    errors[error_key].append(
                        f"At timestamp {timestamp}, {product} ({product_value}) should be "
                        f"{relationship_type} {related_product} ({related_value})"
                    )
    
    return errors


def validate_temporal_smoothness(
    forecast_df: pd.DataFrame,
    smoothness_threshold: float
) -> Dict[str, List[str]]:
    """
    Validates that forecast values change smoothly over time.
    
    Args:
        forecast_df: DataFrame containing forecast data
        smoothness_threshold: Maximum allowed percentage change between consecutive hours
        
    Returns:
        Dictionary of temporal smoothness errors by product
    """
    errors = {}
    
    # Group by product to check temporal consistency
    grouped = forecast_df.groupby('product')
    
    for product, group in grouped:
        # Sort by timestamp
        sorted_group = group.sort_values('timestamp')
        
        # Get point forecast values
        values = sorted_group['point_forecast'].values
        timestamps = sorted_group['timestamp'].values
        
        # Calculate percentage changes
        for i in range(1, len(values)):
            if values[i-1] == 0:
                # Avoid division by zero
                continue
                
            percent_change = abs(values[i] - values[i-1]) / abs(values[i-1])
            
            # If change exceeds threshold, record error
            if percent_change > smoothness_threshold:
                error_key = f"{product}_smoothness"
                if error_key not in errors:
                    errors[error_key] = []
                
                errors[error_key].append(
                    f"For product {product}, excessive change of {percent_change:.2f} (threshold: {smoothness_threshold}) "
                    f"between {timestamps[i-1]} ({values[i-1]:.2f}) and {timestamps[i]} ({values[i]:.2f})"
                )
    
    return errors


def check_relationship(value1: float, value2: float, relationship: str) -> bool:
    """
    Checks if a relationship between two values is maintained.
    
    Args:
        value1: First value
        value2: Second value
        relationship: Type of relationship ('greater_than', 'less_than', or 'equal_to')
        
    Returns:
        True if relationship is maintained, False otherwise
    """
    # Handle None or NaN values
    if value1 is None or value2 is None or np.isnan(value1) or np.isnan(value2):
        return False
    
    if relationship == "greater_than":
        return value1 > value2
    elif relationship == "less_than":
        return value1 < value2
    elif relationship == "equal_to":
        return value1 == value2
    else:
        logger.warning(f"Unknown relationship type: {relationship}")
        return False


class ConsistencyValidator:
    """
    Class for validating the consistency of forecast data.
    
    This validator ensures that forecasts maintain expected relationships between products
    and follow logical patterns, detecting anomalies that could impact trading decisions.
    """
    
    def __init__(self, product_relationships=None, temporal_smoothness_threshold=None):
        """
        Initializes the consistency validator with configuration.
        
        Args:
            product_relationships: Dictionary defining relationships between products,
                defaults to global PRODUCT_RELATIONSHIPS
            temporal_smoothness_threshold: Maximum allowed percentage change between consecutive hours,
                defaults to global TEMPORAL_SMOOTHNESS_THRESHOLD
        """
        self._product_relationships = product_relationships or PRODUCT_RELATIONSHIPS
        self._temporal_smoothness_threshold = temporal_smoothness_threshold or TEMPORAL_SMOOTHNESS_THRESHOLD
        logger.info(f"Initialized ConsistencyValidator with {len(self._product_relationships)} product relationships "
                   f"and smoothness threshold {self._temporal_smoothness_threshold}")
    
    def validate(self, forecast_df: pd.DataFrame) -> ValidationResult:
        """
        Validates the consistency of a forecast dataframe.
        
        Args:
            forecast_df: DataFrame containing forecast data
            
        Returns:
            ValidationResult indicating consistency status
        """
        logger.info(f"Starting consistency validation with {type(self).__name__}")
        
        # Check if forecast_df is empty or None
        if forecast_df is None or forecast_df.empty:
            logger.warning("Empty or None forecast_df provided for consistency validation")
            return create_error_result(
                ValidationCategory.CONSISTENCY,
                {"forecast_data": ["No forecast data provided for consistency validation"]}
            )
        
        # Initialize error dictionary
        errors = {}
        
        # Validate product relationships
        relationship_errors = self.validate_product_relationships(forecast_df)
        
        # Validate temporal smoothness
        smoothness_errors = self.validate_temporal_smoothness(forecast_df)
        
        # Format error messages
        formatted_errors = self.format_error_messages(relationship_errors, smoothness_errors)
        
        # Return validation result
        if not formatted_errors:
            logger.info("Forecast consistency validation passed")
            return create_success_result(ValidationCategory.CONSISTENCY)
        else:
            logger.warning(f"Forecast consistency validation failed with {len(formatted_errors)} issues")
            return create_error_result(ValidationCategory.CONSISTENCY, formatted_errors)
    
    def validate_product_relationships(self, forecast_df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Validates that relationships between related products are maintained.
        
        Args:
            forecast_df: DataFrame containing forecast data
            
        Returns:
            Dictionary of relationship validation errors by product pair
        """
        errors = {}
        
        # Group by timestamp to compare products at the same time
        grouped = forecast_df.groupby('timestamp')
        
        for timestamp, group in grouped:
            # Create a dictionary of products to their point forecasts for this timestamp
            product_forecasts = dict(zip(group['product'], group['point_forecast']))
            
            # Check relationships for each product with defined relationships
            for product, relationship_config in self._product_relationships.items():
                # Skip if this product is not in the current timestamp group
                if product not in product_forecasts:
                    continue
                
                product_value = product_forecasts[product]
                related_products = relationship_config['related_products']
                relationship_type = relationship_config['relationship']
                
                # Check each related product
                for related_product in related_products:
                    # Skip if this related product is not in the current timestamp group
                    if related_product not in product_forecasts:
                        continue
                    
                    related_value = product_forecasts[related_product]
                    
                    # Check if relationship is maintained
                    is_valid = self.check_relationship(product_value, related_value, relationship_type)
                    
                    if not is_valid:
                        error_key = f"{product}_{related_product}_relationship"
                        if error_key not in errors:
                            errors[error_key] = []
                        
                        errors[error_key].append(
                            f"At timestamp {timestamp}, {product} ({product_value}) should be "
                            f"{relationship_type} {related_product} ({related_value})"
                        )
        
        return errors
    
    def validate_temporal_smoothness(self, forecast_df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Validates that forecast values change smoothly over time.
        
        Args:
            forecast_df: DataFrame containing forecast data
            
        Returns:
            Dictionary of temporal smoothness errors by product
        """
        errors = {}
        
        # Group by product to check temporal consistency
        grouped = forecast_df.groupby('product')
        
        for product, group in grouped:
            # Sort by timestamp
            sorted_group = group.sort_values('timestamp')
            
            # Get point forecast values
            values = sorted_group['point_forecast'].values
            timestamps = sorted_group['timestamp'].values
            
            # Calculate percentage changes
            for i in range(1, len(values)):
                if values[i-1] == 0:
                    # Avoid division by zero
                    continue
                    
                percent_change = abs(values[i] - values[i-1]) / abs(values[i-1])
                
                # If change exceeds threshold, record error
                if percent_change > self._temporal_smoothness_threshold:
                    error_key = f"{product}_smoothness"
                    if error_key not in errors:
                        errors[error_key] = []
                    
                    errors[error_key].append(
                        f"For product {product}, excessive change of {percent_change:.2f} "
                        f"(threshold: {self._temporal_smoothness_threshold}) "
                        f"between {timestamps[i-1]} ({values[i-1]:.2f}) and {timestamps[i]} ({values[i]:.2f})"
                    )
        
        return errors
    
    def check_relationship(self, value1: float, value2: float, relationship: str) -> bool:
        """
        Checks if a relationship between two values is maintained.
        
        Args:
            value1: First value
            value2: Second value
            relationship: Type of relationship ('greater_than', 'less_than', or 'equal_to')
            
        Returns:
            True if relationship is maintained, False otherwise
        """
        # Handle None or NaN values
        if value1 is None or value2 is None or np.isnan(value1) or np.isnan(value2):
            return False
        
        if relationship == "greater_than":
            return value1 > value2
        elif relationship == "less_than":
            return value1 < value2
        elif relationship == "equal_to":
            return value1 == value2
        else:
            logger.warning(f"Unknown relationship type: {relationship}")
            return False
    
    def format_error_messages(self, relationship_errors: Dict[str, List[str]], 
                             smoothness_errors: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """
        Formats error messages for consistency validation.
        
        Args:
            relationship_errors: Dictionary of relationship validation errors
            smoothness_errors: Dictionary of temporal smoothness errors
            
        Returns:
            Dictionary of formatted error messages
        """
        formatted_errors = {}
        
        # Format relationship errors
        if relationship_errors:
            formatted_errors["product_relationships"] = []
            for product_pair, messages in relationship_errors.items():
                formatted_errors["product_relationships"].extend(messages)
        
        # Format smoothness errors
        if smoothness_errors:
            formatted_errors["temporal_smoothness"] = []
            for product_key, messages in smoothness_errors.items():
                formatted_errors["temporal_smoothness"].extend(messages)
        
        return formatted_errors