"""
Initialization module for the feature engineering component of the Electricity Market Price Forecasting System.
This module exports the key classes and functions from the feature engineering submodules, providing a unified interface for creating, transforming, and selecting features for the forecasting models. It follows a functional programming approach with clear separation of concerns.
"""

# Package version information
__version__ = "0.1.0"

# Standard library imports
import typing
from typing import Optional
from typing import List
from typing import Dict
from datetime import datetime

# Internal imports
from ..utils.logging_utils import get_logger  # Get a configured logger for this module
from .base_features import BaseFeatureCreator  # Create base features from raw input data
from .base_features import create_base_features  # Function to create base features from input data
from .derived_features import DerivedFeatureCreator  # Create derived features from base features
from .derived_features import create_derived_features  # Function to create derived features from base features
from .lagged_features import LaggedFeatureGenerator  # Generate lagged features from time series data
from .lagged_features import generate_lagged_features  # Function to create lagged features for any columns
from .lagged_features import DEFAULT_LAG_PERIODS  # Default lag periods to use for feature generation
from .feature_normalizer import FeatureNormalizer  # Normalize features for model input
from .feature_normalizer import normalize_features  # Function to normalize features using specified method
from .feature_normalizer import NORMALIZATION_METHODS  # Dictionary mapping method names to scaler classes
from .feature_selector import FeatureSelector  # Select relevant features for specific product/hour combinations
from .feature_selector import select_features_by_product_hour  # Function to select features for a product/hour combination
from .product_hour_features import ProductHourFeatureCreator  # Create and manage product/hour-specific features
from .product_hour_features import create_product_hour_features  # Function to create features for a specific product/hour combination
from .exceptions import FeatureEngineeringError  # Base exception for all feature engineering-related errors
from .exceptions import FeatureCreationError  # Exception for feature creation failures
from .exceptions import FeatureNormalizationError  # Exception for feature normalization failures
from .exceptions import FeatureSelectionError  # Exception for feature selection failures
from .exceptions import LaggedFeatureError  # Exception for lagged feature creation failures
from .exceptions import DerivedFeatureError  # Exception for derived feature creation failures
from .exceptions import MissingFeatureError  # Exception for missing required features
import pandas as pd


# Initialize logger
logger = get_logger(__name__)

__all__ = [
    "BaseFeatureCreator",
    "create_base_features",
    "DerivedFeatureCreator",
    "create_derived_features",
    "LaggedFeatureGenerator",
    "generate_lagged_features",
    "DEFAULT_LAG_PERIODS",
    "FeatureNormalizer",
    "normalize_features",
    "NORMALIZATION_METHODS",
    "FeatureSelector",
    "select_features_by_product_hour",
    "ProductHourFeatureCreator",
    "create_product_hour_features",
    "create_feature_pipeline",
    "FeatureEngineeringError",
    "FeatureCreationError",
    "FeatureNormalizationError",
    "FeatureSelectionError",
    "LaggedFeatureError",
    "DerivedFeatureError",
    "MissingFeatureError",
]


def create_feature_pipeline(
    input_data: typing.Optional[pd.DataFrame] = None,
    start_date: typing.Optional[datetime] = None,
    end_date: typing.Optional[datetime] = None,
    product: str = "DALMP",
    hour: int = 0,
    normalize: bool = True,
) -> pd.DataFrame:
    """
    Creates a complete feature engineering pipeline for a given product and hour

    Args:
        input_data (typing.Optional[pandas.DataFrame]): Input data for feature creation.
        start_date (typing.Optional[datetime.datetime]): Start date for the data range.
        end_date (typing.Optional[datetime.datetime]): End date for the data range.
        product (str): The price product identifier.
        hour (int): The target hour (0-23).
        normalize (bool): Whether to apply feature normalization.

    Returns:
        pandas.DataFrame: Feature vector ready for model input
    """
    try:
        # Create base features using create_base_features
        base_features_df = create_base_features(
            input_data=input_data, start_date=start_date, end_date=end_date
        )

        # Create derived features using create_derived_features
        derived_features_df = create_derived_features(base_features_df)

        # Create product/hour specific features using create_product_hour_features
        feature_vector = select_features_by_product_hour(
            derived_features_df, product, hour
        )

        # If normalize is True, apply feature normalization
        if normalize:
            normalizer = FeatureNormalizer(method="standard")
            feature_vector = normalizer.fit_transform(feature_vector)

        # Return the final feature vector
        return feature_vector

    except Exception as e:
        # Handle exceptions by logging and re-raising with appropriate context
        error_message = f"Failed to create feature pipeline for product={product}, hour={hour}: {str(e)}"
        logger.error(error_message)
        raise FeatureEngineeringError(error_message, e)