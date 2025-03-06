"""
Implements validation for forecast completeness in the Electricity Market Price Forecasting System.

This module ensures that forecasts contain all required products and hours for the specified forecast horizon,
detecting missing data points that would impact downstream systems.
"""

import pandas as pd
import datetime
from typing import Dict, List, Set, Tuple, Optional

# Internal imports
from .exceptions import CompletenessValidationError
from .validation_result import (
    ValidationCategory,
    ValidationResult,
    create_success_result, 
    create_error_result
)
from ..config.settings import FORECAST_PRODUCTS, FORECAST_HORIZON_HOURS
from ..utils.logging_utils import get_logger

# Setup logger
logger = get_logger(__name__)

def validate_forecast_completeness(forecast_df: pd.DataFrame, start_date: datetime.datetime) -> ValidationResult:
    """
    Validates that a forecast dataframe contains all required products and hours.
    
    Args:
        forecast_df: DataFrame containing forecast data
        start_date: Start date/time for the forecast horizon
        
    Returns:
        ValidationResult indicating completeness status
    """
    logger.info(f"Starting completeness validation for forecast starting at {start_date}")
    
    # Check if forecast_df is empty or None
    if forecast_df is None or forecast_df.empty:
        return create_error_result(
            ValidationCategory.COMPLETENESS,
            {"general": ["Forecast dataframe is empty or None"]}
        )
    
    # Get missing product/timestamp combinations
    missing_combinations = get_missing_combinations(forecast_df, start_date)
    
    # If no missing combinations, validation is successful
    if not missing_combinations:
        logger.info("Completeness validation successful: all required products and hours present")
        return create_success_result(ValidationCategory.COMPLETENESS)
    
    # Categorize missing combinations
    categorized_missing = categorize_missing_combinations(missing_combinations)
    
    # Create error messages
    error_messages = {}
    
    if categorized_missing["missing_products"]:
        products_str = ", ".join(categorized_missing["missing_products"])
        error_messages["missing_products"] = [f"Missing products: {products_str}"]
    
    if categorized_missing["missing_timestamps"]:
        timestamps_str = ", ".join([ts.strftime("%Y-%m-%d %H:00") for ts in categorized_missing["missing_timestamps"]])
        error_messages["missing_timestamps"] = [f"Missing timestamps: {timestamps_str}"]
    
    if categorized_missing["partial_missing"]:
        partial_str = []
        for product, timestamps in categorized_missing["partial_missing"].items():
            ts_str = ", ".join([ts.strftime("%Y-%m-%d %H:00") for ts in timestamps])
            partial_str.append(f"Product {product} missing at: {ts_str}")
        
        error_messages["partial_missing"] = partial_str
    
    logger.warning(f"Completeness validation failed: found {len(missing_combinations)} missing combinations")
    
    return create_error_result(ValidationCategory.COMPLETENESS, error_messages)

def get_missing_combinations(forecast_df: pd.DataFrame, start_date: datetime.datetime) -> Set[Tuple[str, datetime.datetime]]:
    """
    Identifies missing product/timestamp combinations in a forecast dataframe.
    
    Args:
        forecast_df: DataFrame containing forecast data
        start_date: Start date/time for the forecast horizon
        
    Returns:
        Set of missing (product, timestamp) tuples
    """
    # Create list of expected timestamps
    expected_timestamps = []
    for hour in range(FORECAST_HORIZON_HOURS):
        expected_timestamps.append(start_date + datetime.timedelta(hours=hour))
    
    # Create set of all expected product/timestamp combinations
    expected_combinations = set()
    for product in FORECAST_PRODUCTS:
        for timestamp in expected_timestamps:
            expected_combinations.add((product, timestamp))
    
    # Get actual combinations from the dataframe
    actual_combinations = set()
    for _, row in forecast_df.iterrows():
        actual_combinations.add((row['product'], row['timestamp']))
    
    # Return the difference (missing combinations)
    return expected_combinations - actual_combinations

def categorize_missing_combinations(missing_combinations: Set[Tuple[str, datetime.datetime]]) -> Dict:
    """
    Categorizes missing combinations by product and timestamp.
    
    Args:
        missing_combinations: Set of missing (product, timestamp) tuples
        
    Returns:
        Dictionary with missing products and timestamps
    """
    missing_products = set()
    missing_timestamps = set()
    partial_missing = {}
    
    # Extract all products and timestamps that appear in the missing combinations
    all_products = set([product for product, _ in missing_combinations])
    all_timestamps = set([timestamp for _, timestamp in missing_combinations])
    
    # Check if any products are completely missing
    for product in all_products:
        product_timestamps = [timestamp for p, timestamp in missing_combinations if p == product]
        if len(product_timestamps) == FORECAST_HORIZON_HOURS:
            missing_products.add(product)
        else:
            partial_missing[product] = product_timestamps
    
    # Check if any timestamps are completely missing
    for timestamp in all_timestamps:
        timestamp_products = [product for product, ts in missing_combinations if ts == timestamp]
        if len(timestamp_products) == len(FORECAST_PRODUCTS):
            missing_timestamps.add(timestamp)
    
    return {
        "missing_products": missing_products,
        "missing_timestamps": missing_timestamps,
        "partial_missing": partial_missing
    }


class CompletenessValidator:
    """
    Class for validating the completeness of forecast data.
    """
    
    def __init__(self, required_products: List[str] = None, forecast_horizon_hours: int = None):
        """
        Initializes the completeness validator with configuration.
        
        Args:
            required_products: List of required products (defaults to FORECAST_PRODUCTS)
            forecast_horizon_hours: Forecast horizon in hours (defaults to FORECAST_HORIZON_HOURS)
        """
        self._required_products = required_products or FORECAST_PRODUCTS
        self._forecast_horizon_hours = forecast_horizon_hours or FORECAST_HORIZON_HOURS
        logger.info(f"Initialized CompletenessValidator with {len(self._required_products)} products and "
                    f"{self._forecast_horizon_hours} hour horizon")
    
    def validate(self, forecast_df: pd.DataFrame, start_date: datetime.datetime) -> ValidationResult:
        """
        Validates the completeness of a forecast dataframe.
        
        Args:
            forecast_df: DataFrame containing forecast data
            start_date: Start date/time for the forecast horizon
            
        Returns:
            ValidationResult indicating completeness status
        """
        logger.info(f"Starting completeness validation with validator instance for forecast starting at {start_date}")
        
        # Check if forecast_df is empty or None
        if forecast_df is None or forecast_df.empty:
            return create_error_result(
                ValidationCategory.COMPLETENESS,
                {"general": ["Forecast dataframe is empty or None"]}
            )
        
        # Generate expected timestamps
        expected_timestamps = self.get_expected_timestamps(start_date)
        
        # Create set of expected combinations
        expected_combinations = self.get_expected_combinations(expected_timestamps)
        
        # Get actual combinations from the dataframe
        actual_combinations = self.get_actual_combinations(forecast_df)
        
        # Find missing combinations
        missing_combinations = expected_combinations - actual_combinations
        
        # If no missing combinations, validation is successful
        if not missing_combinations:
            logger.info("Completeness validation successful: all required products and hours present")
            return create_success_result(ValidationCategory.COMPLETENESS)
        
        # Categorize missing combinations
        categorized_missing = self._categorize_missing(missing_combinations)
        
        # Create error messages
        error_messages = self.format_error_messages(categorized_missing)
        
        logger.warning(f"Completeness validation failed: found {len(missing_combinations)} missing combinations")
        
        return create_error_result(ValidationCategory.COMPLETENESS, error_messages)
    
    def get_expected_timestamps(self, start_date: datetime.datetime) -> List[datetime.datetime]:
        """
        Generates the expected timestamps for the forecast horizon.
        
        Args:
            start_date: Start date/time for the forecast horizon
            
        Returns:
            List of expected timestamps
        """
        return [start_date + datetime.timedelta(hours=hour) for hour in range(self._forecast_horizon_hours)]
    
    def get_expected_combinations(self, timestamps: List[datetime.datetime]) -> Set[Tuple[str, datetime.datetime]]:
        """
        Generates all expected product/timestamp combinations.
        
        Args:
            timestamps: List of timestamps
            
        Returns:
            Set of expected (product, timestamp) tuples
        """
        combinations = set()
        for product in self._required_products:
            for timestamp in timestamps:
                combinations.add((product, timestamp))
        return combinations
    
    def get_actual_combinations(self, forecast_df: pd.DataFrame) -> Set[Tuple[str, datetime.datetime]]:
        """
        Extracts actual product/timestamp combinations from forecast dataframe.
        
        Args:
            forecast_df: DataFrame containing forecast data
            
        Returns:
            Set of actual (product, timestamp) tuples
        """
        return set(zip(forecast_df['product'], forecast_df['timestamp']))
    
    def _categorize_missing(self, missing_combinations: Set[Tuple[str, datetime.datetime]]) -> Dict:
        """
        Categorizes missing combinations by product and timestamp.
        
        Args:
            missing_combinations: Set of missing (product, timestamp) tuples
            
        Returns:
            Dictionary with missing products and timestamps
        """
        missing_products = set()
        missing_timestamps = set()
        partial_missing = {}
        
        # Extract all products and timestamps that appear in the missing combinations
        all_products = set([product for product, _ in missing_combinations])
        all_timestamps = set([timestamp for _, timestamp in missing_combinations])
        
        # Check if any products are completely missing
        for product in all_products:
            product_timestamps = [timestamp for p, timestamp in missing_combinations if p == product]
            if len(product_timestamps) == self._forecast_horizon_hours:
                missing_products.add(product)
            else:
                partial_missing[product] = product_timestamps
        
        # Check if any timestamps are completely missing
        for timestamp in all_timestamps:
            timestamp_products = [product for product, ts in missing_combinations if ts == timestamp]
            if len(timestamp_products) == len(self._required_products):
                missing_timestamps.add(timestamp)
        
        return {
            "missing_products": missing_products,
            "missing_timestamps": missing_timestamps,
            "partial_missing": partial_missing
        }
    
    def format_error_messages(self, categorized_missing: Dict) -> Dict[str, List[str]]:
        """
        Formats error messages for missing combinations.
        
        Args:
            categorized_missing: Dictionary with categorized missing combinations
            
        Returns:
            Dictionary of formatted error messages
        """
        error_messages = {}
        
        if categorized_missing["missing_products"]:
            products_str = ", ".join(categorized_missing["missing_products"])
            error_messages["missing_products"] = [f"Missing products: {products_str}"]
        
        if categorized_missing["missing_timestamps"]:
            timestamps_str = ", ".join([ts.strftime("%Y-%m-%d %H:00") for ts in categorized_missing["missing_timestamps"]])
            error_messages["missing_timestamps"] = [f"Missing timestamps: {timestamps_str}"]
        
        if categorized_missing["partial_missing"]:
            partial_str = []
            for product, timestamps in categorized_missing["partial_missing"].items():
                ts_str = ", ".join([ts.strftime("%Y-%m-%d %H:00") for ts in timestamps])
                partial_str.append(f"Product {product} missing at: {ts_str}")
            
            error_messages["partial_missing"] = partial_str
        
        return error_messages