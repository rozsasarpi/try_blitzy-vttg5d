"""
Implements product and hour-specific feature creation for the Electricity Market Price Forecasting System.
This module is responsible for generating tailored feature vectors for each product/hour combination,
recognizing that different factors influence electricity prices at different times of day and for different market products.
It integrates base features, derived features, lagged features, and applies appropriate feature selection and normalization.
"""

import pandas  # pandas 2.0.0+
import numpy  # numpy 1.24.0+
from typing import Optional
from typing import List
from typing import Dict
import typing
from datetime import datetime

# Internal imports
from ..utils.logging_utils import get_logger
from ..utils.logging_utils import log_execution_time
from .base_features import BaseFeatureCreator  # Create base features from raw input data
from .derived_features import DerivedFeatureCreator  # Create derived features from base features
from .lagged_features import LaggedFeatureGenerator  # Generate lagged features from time series data
from .lagged_features import DEFAULT_LAG_PERIODS  # Default lag periods to use for feature generation
from .feature_selector import FeatureSelector  # Select relevant features for specific product/hour combinations
from .feature_normalizer import FeatureNormalizer  # Normalize features for model input
from .exceptions import FeatureEngineeringError  # Base exception for feature engineering errors
from .exceptions import FeatureSelectionError  # Exception for feature selection failures
from ..config.settings import FORECAST_PRODUCTS  # List of valid price products for validation

# Initialize logger
logger = get_logger(__name__)

# Global cache for product/hour features
PRODUCT_HOUR_FEATURE_CACHE: Dict = {}

# Define interaction feature pairs
INTERACTION_FEATURE_PAIRS: List[Tuple[str, str]] = [
    ('load_mw', 'hour'),
    ('load_mw', 'is_weekend'),
    ('renewable_ratio', 'hour'),
    ('price_volatility', 'hour')
]


@log_execution_time
def create_product_hour_features(
    base_features_df: pandas.DataFrame,
    product: str,
    hour: int,
    normalize: bool,
    normalizer_id: str
) -> pandas.DataFrame:
    """
    Creates feature vectors for a specific product/hour combination.

    Args:
        base_features_df (pandas.DataFrame): DataFrame containing base features
        product (str): The price product identifier
        hour (int): The target hour (0-23)
        normalize (bool): Whether to apply feature normalization
        normalizer_id (str): Identifier for the normalizer

    Returns:
        pandas.DataFrame: Feature vector for the specified product/hour combination
    """
    try:
        # Validate that product is in FORECAST_PRODUCTS
        if product not in FORECAST_PRODUCTS:
            raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")

        # Validate that hour is between 0 and 23
        if not 0 <= hour <= 23:
            raise ValueError(f"Invalid hour: {hour}. Must be between 0 and 23")

        # Create a ProductHourFeatureCreator instance
        feature_creator = ProductHourFeatureCreator()

        # Call create_features method with the specified product and hour
        feature_vector = feature_creator.create_features(product, hour)

        # If normalize is True, apply feature normalization
        if normalize:
            normalizer = FeatureNormalizer(method='standard', normalizer_id=normalizer_id)
            feature_vector = normalizer.transform(feature_vector)

        # Return the feature vector for the product/hour combination
        return feature_vector

    except Exception as e:
        # Handle exceptions by raising appropriate error with context
        error_message = f"Failed to create features for product={product}, hour={hour}: {str(e)}"
        logger.error(error_message)
        raise FeatureEngineeringError(error_message, e)


def get_cache_key(product: str, hour: int) -> str:
    """
    Generates a cache key for a product/hour combination.

    Args:
        product (str): The price product identifier
        hour (int): The target hour (0-23)

    Returns:
        str: Cache key string
    """
    # Format product and hour into a string key: '{product}_{hour}'
    key = f"{product}_{hour}"

    # Return the formatted key
    return key


def clear_product_hour_cache() -> None:
    """
    Clears the product/hour feature cache.
    """
    # Reset the PRODUCT_HOUR_FEATURE_CACHE dictionary to empty
    global PRODUCT_HOUR_FEATURE_CACHE
    PRODUCT_HOUR_FEATURE_CACHE = {}

    # Log that cache has been cleared
    logger.info("Product/hour feature cache cleared")


class ProductHourFeatureCreator:
    """
    Class responsible for creating and managing product/hour-specific features.
    """

    def __init__(
        self,
        base_features_df: Optional[pandas.DataFrame] = None,
        base_feature_creator: Optional[BaseFeatureCreator] = None
    ):
        """
        Initializes the ProductHourFeatureCreator with optional base features.

        Args:
            base_features_df (typing.Optional[pandas.DataFrame]): DataFrame containing base features
            base_feature_creator (typing.Optional[BaseFeatureCreator]): BaseFeatureCreator instance
        """
        # Initialize base_feature_creator if provided or create a new instance
        self._base_feature_creator = base_feature_creator or BaseFeatureCreator()

        # If base_features_df is provided, use it to initialize the base_feature_creator
        if base_features_df is not None:
            self._base_feature_creator._feature_df = base_features_df

        # Initialize derived_feature_creator with base features
        self._derived_feature_creator = DerivedFeatureCreator(self._base_feature_creator.get_feature_dataframe())

        # Initialize feature_selector
        self._feature_selector = FeatureSelector()

        # Initialize empty feature cache dictionary
        self._feature_cache: Dict = {}

        # Initialize combined_features_df as None
        self._combined_features_df: Optional[pandas.DataFrame] = None

    def create_features(self, product: str, hour: int) -> pandas.DataFrame:
        """
        Creates features for a specific product/hour combination.

        Args:
            product (str): The price product identifier
            hour (int): The target hour (0-23)

        Returns:
            pandas.DataFrame: Feature vector for the product/hour combination
        """
        try:
            # Check if features for this product/hour are already in cache
            cache_key = get_cache_key(product, hour)
            if cache_key in self._feature_cache:
                logger.debug(f"Using cached features for product={product}, hour={hour}")
                return self._feature_cache[cache_key]

            # Get combined base and derived features
            if self._combined_features_df is None:
                self._combined_features_df = self._derived_feature_creator.get_combined_features()
            combined_features_df = self._combined_features_df

            # Add lagged features appropriate for this product/hour
            lagged_features_df = self.add_lagged_features(combined_features_df, product, hour)

            # Select relevant features for this product/hour using feature_selector
            available_features = lagged_features_df.columns.tolist()
            selected_features_df = self._feature_selector.select_features(lagged_features_df, product, hour)

            # Add interaction features specific to this product/hour
            interaction_features_df = self.create_interaction_features(selected_features_df, product, hour)

            # Store result in cache for future use
            self._feature_cache[cache_key] = interaction_features_df

            # Return the feature vector
            logger.info(f"Created features for product={product}, hour={hour}")
            return interaction_features_df

        except Exception as e:
            # Handle exceptions by raising FeatureSelectionError with context
            error_message = f"Failed to create features for product={product}, hour={hour}: {str(e)}"
            logger.error(error_message)
            raise FeatureSelectionError(error_message, product, hour, e)

    def get_feature_dataframe(self, product: str, hour: int) -> pandas.DataFrame:
        """
        Returns the feature DataFrame for a specific product/hour combination.

        Args:
            product (str): The price product identifier
            hour (int): The target hour (0-23)

        Returns:
            pandas.DataFrame: Feature DataFrame for the product/hour combination
        """
        try:
            # Call create_features to ensure features are generated
            return self.create_features(product, hour)

        except Exception as e:
            # Handle exceptions by raising appropriate error with context
            error_message = f"Failed to get feature dataframe for product={product}, hour={hour}: {str(e)}"
            logger.error(error_message)
            raise FeatureSelectionError(error_message, product, hour, e)

    def create_all_product_hour_features(self) -> Dict[str, pandas.DataFrame]:
        """
        Creates features for all product/hour combinations.

        Returns:
            Dict: Dictionary mapping product/hour keys to feature DataFrames
        """
        try:
            # Initialize empty result dictionary
            result: Dict[str, pandas.DataFrame] = {}

            # For each product in FORECAST_PRODUCTS:
            for product in FORECAST_PRODUCTS:
                # For each hour from 0 to 23:
                for hour in range(24):
                    # Create features for this product/hour combination
                    features_df = self.create_features(product, hour)

                    # Store in result dictionary with appropriate key
                    cache_key = get_cache_key(product, hour)
                    result[cache_key] = features_df

            # Return the complete dictionary of features
            return result

        except Exception as e:
            # Handle exceptions by raising appropriate error with context
            error_message = f"Failed to create all product/hour features: {str(e)}"
            logger.error(error_message)
            raise FeatureEngineeringError(error_message, e)

    def update_base_features(self, new_base_features: pandas.DataFrame) -> None:
        """
        Updates the base features used for feature creation.

        Args:
            new_base_features (pandas.DataFrame): DataFrame containing new base features
        """
        # Update the base_feature_creator with new base features
        self._base_feature_creator._feature_df = new_base_features

        # Update the derived_feature_creator with new base features
        self._derived_feature_creator = DerivedFeatureCreator(new_base_features)

        # Clear the feature cache since base data has changed
        self.clear_cache()

        # Set combined_features_df to None to force recalculation
        self._combined_features_df = None

        # Log the update of base features
        logger.info("Base features updated")

    def clear_cache(self) -> None:
        """
        Clears the feature cache.
        """
        # Reset the feature cache dictionary to empty
        self._feature_cache = {}

        # Log that cache has been cleared
        logger.info("Feature cache cleared")

    def get_feature_list(self, product: str, hour: int) -> List[str]:
        """
        Gets the list of features for a product/hour combination.

        Args:
            product (str): The price product identifier
            hour (int): The target hour (0-23)

        Returns:
            List[str]: List of feature names
        """
        try:
            # Get the feature DataFrame for the product/hour combination
            feature_df = self.get_feature_dataframe(product, hour)

            # Return the list of column names from the DataFrame
            return feature_df.columns.tolist()

        except Exception as e:
            # Handle exceptions by raising appropriate error with context
            error_message = f"Failed to get feature list for product={product}, hour={hour}: {str(e)}"
            logger.error(error_message)
            raise FeatureSelectionError(error_message, product, hour, e)

    def create_interaction_features(self, features_df: pandas.DataFrame, product: str, hour: int) -> pandas.DataFrame:
        """
        Creates interaction features for a specific product/hour.

        Args:
            features_df (pandas.DataFrame): DataFrame to add interaction features to
            product (str): The price product identifier
            hour (int): The target hour (0-23)

        Returns:
            pandas.DataFrame: DataFrame with added interaction features
        """
        try:
            # Determine appropriate feature pairs for this product/hour
            # For now, use the global INTERACTION_FEATURE_PAIRS
            feature_pairs = INTERACTION_FEATURE_PAIRS

            # Use feature_selector to add interaction features
            interaction_features_df = self._feature_selector.add_interaction_features(features_df, feature_pairs)

            # Return the DataFrame with added interaction features
            return interaction_features_df

        except Exception as e:
            # Handle exceptions by raising appropriate error with context
            error_message = f"Failed to create interaction features for product={product}, hour={hour}: {str(e)}"
            logger.error(error_message)
            raise FeatureEngineeringError(error_message, e)

    def add_lagged_features(self, features_df: pandas.DataFrame, product: str, hour: int) -> pandas.DataFrame:
        """
        Adds lagged features appropriate for a product/hour combination.

        Args:
            features_df (pandas.DataFrame): DataFrame to add lagged features to
            product (str): The price product identifier
            hour (int): The target hour (0-23)

        Returns:
            pandas.DataFrame: DataFrame with added lagged features
        """
        try:
            # Determine appropriate columns for lagging based on product/hour
            # For now, lag all columns
            columns_to_lag = features_df.columns.tolist()
            if 'timestamp' in columns_to_lag:
                columns_to_lag.remove('timestamp')

            # Create a LaggedFeatureGenerator instance
            lagged_feature_generator = LaggedFeatureGenerator(features_df, timestamp_column='timestamp', lag_periods=DEFAULT_LAG_PERIODS)

            # Add the selected feature columns to the generator
            lagged_feature_generator.add_feature_columns(columns_to_lag)

            # Generate all lagged features
            lagged_features_df = lagged_feature_generator.generate_all_lagged_features()

            # Return the DataFrame with added lagged features
            return lagged_features_df

        except Exception as e:
            # Handle exceptions by raising appropriate error with context
            error_message = f"Failed to add lagged features for product={product}, hour={hour}: {str(e)}"
            logger.error(error_message)
            raise FeatureEngineeringError(error_message, e)