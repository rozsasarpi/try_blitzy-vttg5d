"""
Module responsible for selecting relevant features for specific product/hour combinations 
in the Electricity Market Price Forecasting System.

Implements a feature selection strategy tailored to each product/hour combination, 
recognizing that different factors influence electricity prices at different times 
of day and for different market products.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional

# Internal imports
from .exceptions import FeatureSelectionError, MissingFeatureError
from ..utils.logging_utils import get_logger, log_execution_time
from ..config.settings import FORECAST_PRODUCTS

# Get logger for this module
logger = get_logger(__name__)

# Define base features that are always included
BASE_FEATURES = ["timestamp", "load_mw", "hour_of_day", "day_of_week", "month", "is_weekend"]

# Define product-specific features
PRODUCT_SPECIFIC_FEATURES = {
    "DALMP": ["day_ahead_demand_forecast", "wind_forecast", "solar_forecast", "thermal_availability"],
    "RTLMP": ["real_time_demand", "wind_generation", "solar_generation", "thermal_generation"],
    "RegUp": ["regulation_requirement", "available_regulation_capacity", "thermal_ramp_rate"],
    "RegDown": ["regulation_requirement", "available_regulation_capacity", "thermal_ramp_rate"],
    "RRS": ["responsive_reserve_requirement", "available_responsive_capacity"],
    "NSRS": ["non_spinning_reserve_requirement", "available_non_spinning_capacity"]
}

# Define hour-specific features
HOUR_SPECIFIC_FEATURES = {
    "peak_hours": ["peak_load_ratio", "peak_price_ratio"],
    "off_peak_hours": ["off_peak_load_ratio", "off_peak_price_ratio"],
    "solar_hours": ["solar_generation", "solar_forecast", "cloud_cover"],
    "evening_ramp": ["ramp_rate", "load_change_rate"]
}

# Cache for feature selection results
FEATURE_SELECTION_CACHE = {}


def get_hour_category(hour: int) -> List[str]:
    """
    Determines the category of an hour for feature selection purposes.
    
    Args:
        hour: The hour to categorize (0-23)
        
    Returns:
        List of hour categories this hour belongs to
    """
    categories = []
    
    # Peak hours (typical business hours)
    if 7 <= hour <= 22:
        categories.append("peak_hours")
    
    # Off-peak hours (overnight)
    if hour <= 6 or hour == 23:
        categories.append("off_peak_hours")
    
    # Solar generation hours
    if 8 <= hour <= 17:
        categories.append("solar_hours")
    
    # Evening ramp hours (when load typically increases rapidly)
    if 16 <= hour <= 20:
        categories.append("evening_ramp")
    
    return categories


def get_feature_list_for_product_hour(product: str, hour: int, available_features: List[str]) -> List[str]:
    """
    Gets the list of feature names appropriate for a product/hour combination.
    
    Args:
        product: The price product identifier
        hour: The target hour (0-23)
        available_features: List of features available in the dataset
        
    Returns:
        List of feature names appropriate for the product/hour
    """
    # Start with base features
    feature_list = BASE_FEATURES.copy()
    
    # Add product-specific features
    if product in PRODUCT_SPECIFIC_FEATURES:
        feature_list.extend(PRODUCT_SPECIFIC_FEATURES[product])
    
    # Add hour-specific features
    hour_categories = get_hour_category(hour)
    for category in hour_categories:
        if category in HOUR_SPECIFIC_FEATURES:
            feature_list.extend(HOUR_SPECIFIC_FEATURES[category])
    
    # Filter to include only features that exist in available_features
    filtered_list = [feature for feature in feature_list if feature in available_features]
    
    return filtered_list


def validate_features_exist(df: pd.DataFrame, required_features: List[str]) -> bool:
    """
    Validates that required features exist in the DataFrame.
    
    Args:
        df: The DataFrame to validate
        required_features: List of features that should exist
        
    Returns:
        True if all required features exist
        
    Raises:
        MissingFeatureError: If any required features are missing
    """
    columns = df.columns.tolist()
    missing_features = [feature for feature in required_features if feature not in columns]
    
    if missing_features:
        error_message = f"Missing required features: {missing_features}"
        logger.error(error_message)
        raise MissingFeatureError(error_message, missing_features)
    
    return True


def get_cache_key(product: str, hour: int) -> str:
    """
    Generates a cache key for a product/hour combination.
    
    Args:
        product: The price product identifier
        hour: The target hour (0-23)
        
    Returns:
        Cache key string
    """
    return f"{product}_{hour}"


class FeatureSelector:
    """
    Class responsible for selecting appropriate features for specific product/hour combinations.
    """
    
    def __init__(self):
        """
        Initializes the FeatureSelector.
        """
        self._feature_cache = {}
        self._feature_lists = {}
        self.logger = logger
    
    def select_features(self, features_df: pd.DataFrame, product: str, hour: int) -> pd.DataFrame:
        """
        Selects appropriate features for a product/hour combination.
        
        Args:
            features_df: DataFrame containing all potential features
            product: The price product identifier
            hour: The target hour (0-23)
            
        Returns:
            DataFrame with selected features
            
        Raises:
            FeatureSelectionError: If feature selection fails
        """
        try:
            # Validate input parameters
            if product not in FORECAST_PRODUCTS:
                raise ValueError(f"Invalid product: {product}")
            
            if not 0 <= hour <= 23:
                raise ValueError(f"Invalid hour: {hour}, must be between 0 and 23")
            
            # Check if we have already performed this selection
            cache_key = get_cache_key(product, hour)
            if cache_key in self._feature_cache:
                self.logger.debug(f"Using cached feature selection for product={product}, hour={hour}")
                return self._feature_cache[cache_key]
            
            # Get list of available features
            available_features = features_df.columns.tolist()
            
            # Get appropriate feature list for this product/hour
            feature_list = self.get_feature_list(product, hour, available_features)
            
            # Validate that required features exist
            validate_features_exist(features_df, feature_list)
            
            # Select the subset of features for this product/hour
            selected_features = features_df[feature_list].copy()
            
            # Store in cache for future use
            self._feature_cache[cache_key] = selected_features
            
            self.logger.info(f"Selected {len(feature_list)} features for product={product}, hour={hour}")
            return selected_features
            
        except Exception as e:
            error_message = f"Feature selection failed for product={product}, hour={hour}: {str(e)}"
            self.logger.error(error_message)
            raise FeatureSelectionError(error_message, product, hour, e)
    
    def get_feature_list(self, product: str, hour: int, available_features: List[str]) -> List[str]:
        """
        Gets the list of feature names for a product/hour combination.
        
        Args:
            product: The price product identifier
            hour: The target hour (0-23)
            available_features: List of features available in the dataset
            
        Returns:
            List of feature names
        """
        # Check if we have already calculated this feature list
        cache_key = get_cache_key(product, hour)
        if cache_key in self._feature_lists:
            return self._feature_lists[cache_key]
        
        # Generate the feature list
        feature_list = get_feature_list_for_product_hour(product, hour, available_features)
        
        # Cache for future use
        self._feature_lists[cache_key] = feature_list
        
        return feature_list
    
    def clear_cache(self) -> None:
        """
        Clears the feature selection cache.
        """
        self._feature_cache = {}
        self._feature_lists = {}
        self.logger.info("Feature selection cache cleared")
    
    def update_feature_dataframe(self, features_df: pd.DataFrame, new_features: Dict[str, np.ndarray]) -> pd.DataFrame:
        """
        Updates a feature DataFrame with additional features.
        
        Args:
            features_df: The original feature DataFrame
            new_features: Dictionary of new features to add (name -> values)
            
        Returns:
            Updated DataFrame with new features
        """
        df_copy = features_df.copy()
        
        for feature_name, feature_values in new_features.items():
            df_copy[feature_name] = feature_values
        
        # Clear cache since the feature set has changed
        self.clear_cache()
        
        return df_copy
    
    def add_interaction_features(self, features_df: pd.DataFrame, feature_pairs: List[Tuple[str, str]]) -> pd.DataFrame:
        """
        Adds interaction features (products of feature pairs).
        
        Args:
            features_df: The original feature DataFrame
            feature_pairs: List of feature name pairs to create interactions for
            
        Returns:
            DataFrame with added interaction features
        """
        df_copy = features_df.copy()
        
        for feature1, feature2 in feature_pairs:
            if feature1 in df_copy.columns and feature2 in df_copy.columns:
                interaction_name = f"{feature1}_x_{feature2}"
                df_copy[interaction_name] = df_copy[feature1] * df_copy[feature2]
                self.logger.debug(f"Created interaction feature: {interaction_name}")
            else:
                missing = []
                if feature1 not in df_copy.columns:
                    missing.append(feature1)
                if feature2 not in df_copy.columns:
                    missing.append(feature2)
                self.logger.warning(f"Cannot create interaction feature - missing columns: {missing}")
        
        # Clear cache since feature set has changed
        self.clear_cache()
        
        return df_copy


@log_execution_time
def select_features_by_product_hour(features_df: pd.DataFrame, product: str, hour: int) -> pd.DataFrame:
    """
    Selects appropriate features for a specific product/hour combination.
    
    This function creates a FeatureSelector instance and uses it to select
    the appropriate features for the given product and hour.
    
    Args:
        features_df: DataFrame containing all potential features
        product: The price product identifier
        hour: The target hour (0-23)
        
    Returns:
        DataFrame with selected features
        
    Raises:
        ValueError: If product or hour is invalid
        MissingFeatureError: If required features are missing
        FeatureSelectionError: If feature selection fails
    """
    # Validate product
    if product not in FORECAST_PRODUCTS:
        raise ValueError(f"Invalid product: {product}")
    
    # Validate hour
    if not 0 <= hour <= 23:
        raise ValueError(f"Invalid hour: {hour}, must be between 0 and 23")
    
    # Check if features_df contains all required base features
    validate_features_exist(features_df, BASE_FEATURES)
    
    # Create feature selector and select features
    selector = FeatureSelector()
    return selector.select_features(features_df, product, hour)