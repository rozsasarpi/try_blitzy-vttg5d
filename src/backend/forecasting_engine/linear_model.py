# src/backend/forecasting_engine/linear_model.py
"""Implements linear model functionality for the Electricity Market Price Forecasting System.
This module provides functions and classes for creating, training, and executing linear models
to generate point forecasts for electricity market prices. It follows a functional programming
approach with caching capabilities for improved performance.
"""

import pandas as pd  # package_version: 2.0.0+
import numpy as np  # package_version: 1.24.0+
from sklearn.linear_model import LinearRegression  # package_version: 1.2.0+
from typing import Dict, List, Optional  # package_version: standard library
from datetime import datetime  # package_version: standard library

# Internal imports
from .exceptions import ModelExecutionError, InvalidFeatureError  # Module: src/backend/forecasting_engine/exceptions.py
from ..utils.logging_utils import get_logger  # Module: src/backend/utils/logging_utils.py
from ..utils.decorators import timing_decorator, log_exceptions, memoize  # Module: src/backend/utils/decorators.py
from ..utils.metrics_utils import calculate_rmse, calculate_r2  # Module: src/backend/utils/metrics_utils.py
from ..config.settings import FORECAST_PRODUCTS  # Module: src/backend/config/settings.py

# Initialize logger
logger = get_logger(__name__)

# Default model parameters
DEFAULT_MODEL_PARAMS = {"fit_intercept": True, "copy_X": True, "n_jobs": None}


@log_exceptions
def create_linear_model(model_params: Optional[Dict] = None) -> LinearRegression:
    """Creates a new linear regression model with specified parameters

    Args:
        model_params (Optional[Dict]): Dictionary of model parameters. If None, uses DEFAULT_MODEL_PARAMS.

    Returns:
        LinearRegression: Initialized linear model
    """
    # Use default parameters if none are provided
    params = model_params if model_params else DEFAULT_MODEL_PARAMS

    # Create a new LinearRegression instance with the provided parameters
    model = LinearRegression(**params)
    return model


def validate_features(features: pd.DataFrame, required_columns: Optional[List[str]] = None,
                      product: Optional[str] = None, hour: Optional[int] = None) -> bool:
    """Validates feature inputs for a linear model

    Args:
        features (pd.DataFrame): Feature DataFrame
        required_columns (Optional[List[str]]): List of required column names. Defaults to None.
        product (Optional[str]): The product being forecasted. Defaults to None.
        hour (Optional[int]): The hour being forecasted. Defaults to None.

    Returns:
        bool: True if features are valid, raises exception otherwise
    """
    # Check if features is a pandas DataFrame
    if not isinstance(features, pd.DataFrame):
        raise InvalidFeatureError("Features must be a pandas DataFrame", product, hour, [])

    # If required_columns is provided, check if all required columns are present
    if required_columns:
        missing_columns = [col for col in required_columns if col not in features.columns]
        if missing_columns:
            raise InvalidFeatureError(
                f"Missing required columns: {missing_columns}", product, hour, missing_columns
            )

    # Check for missing values in features
    if features.isnull().any().any():
        missing_value_columns = features.columns[features.isnull().any()].tolist()
        raise InvalidFeatureError(
            f"Missing values found in columns: {missing_value_columns}", product, hour, missing_value_columns
        )

    # Return True if all validations pass
    return True


@timing_decorator
@log_exceptions
def train_linear_model(model: Optional[LinearRegression], features: pd.DataFrame, target: pd.Series,
                       product: str, hour: int) -> LinearRegression:
    """Trains a linear model with historical data

    Args:
        model (Optional[LinearRegression]): Linear model instance. If None, a new model is created.
        features (pd.DataFrame): Feature DataFrame
        target (pd.Series): Target Series
        product (str): The product being forecasted
        hour (int): The hour being forecasted

    Returns:
        LinearRegression: Trained linear model
    """
    # Validate features using validate_features function
    validate_features(features, product=product, hour=hour)

    # If model is None, create a new model using create_linear_model
    if model is None:
        model = create_linear_model()

    # Fit the model with features and target
    model.fit(features, target)

    # Calculate training metrics (RMSE, R²)
    predictions = model.predict(features)
    rmse = calculate_rmse(target.tolist(), predictions.tolist())
    r2 = calculate_r2(target.tolist(), predictions.tolist())

    # Log training results with metrics
    logger.info(f"Trained model for {product} hour {hour}: RMSE={rmse:.3f}, R²={r2:.3f}")

    # Return the trained model
    return model


@timing_decorator
@log_exceptions
def execute_linear_model(model: LinearRegression, features: pd.DataFrame, product: str, hour: int) -> float:
    """Executes a trained model to generate a point forecast

    Args:
        model (LinearRegression): Trained linear model
        features (pd.DataFrame): Feature DataFrame
        product (str): The product being forecasted
        hour (int): The hour being forecasted

    Returns:
        float: Point forecast value
    """
    # Validate features using validate_features function
    validate_features(features, product=product, hour=hour)

    # Check if model is trained (has coef_ attribute)
    if not hasattr(model, "coef_"):
        raise ModelExecutionError("Model has not been trained", product, hour, str(model))

    # Generate prediction using model.predict
    prediction = model.predict(features)

    # Extract the scalar prediction value
    forecast_value = prediction[0]

    # Log the prediction details
    logger.info(f"Generated forecast for {product} hour {hour}: {forecast_value:.3f}")

    # Return the point forecast value
    return forecast_value


def get_model_coefficients(model: LinearRegression, feature_names: Optional[List[str]] = None) -> Dict:
    """Extracts model coefficients and feature importance

    Args:
        model (LinearRegression): Trained linear model
        feature_names (Optional[List[str]]): List of feature names. Defaults to None.

    Returns:
        Dict: Dictionary of coefficients, intercept, and feature importance
    """
    # Check if model is trained (has coef_ attribute)
    if not hasattr(model, "coef_"):
        raise ModelExecutionError("Model has not been trained", "unknown", 0, str(model))

    # Extract coefficients from model
    coefficients = model.coef_
    intercept = model.intercept_

    # If feature_names is provided, create a mapping of feature names to coefficients
    if feature_names:
        feature_coefficients = dict(zip(feature_names, coefficients))
    else:
        feature_coefficients = coefficients.tolist()

    # Calculate feature importance based on absolute coefficient values
    feature_importance = {
        feature: abs(coef) for feature, coef in feature_coefficients.items()
    } if isinstance(feature_coefficients, dict) else [abs(coef) for coef in coefficients]

    # Return dictionary with coefficients, intercept, and feature importance
    return {
        "coefficients": feature_coefficients,
        "intercept": intercept,
        "feature_importance": feature_importance,
    }


def evaluate_model(model: LinearRegression, test_features: pd.DataFrame, test_target: pd.Series) -> Dict:
    """Evaluates model performance on test data

    Args:
        model (LinearRegression): Trained linear model
        test_features (pd.DataFrame): Test feature DataFrame
        test_target (pd.Series): Test target Series

    Returns:
        Dict: Dictionary of evaluation metrics
    """
    # Check if model is trained (has coef_ attribute)
    if not hasattr(model, "coef_"):
        raise ModelExecutionError("Model has not been trained", "unknown", 0, str(model))

    # Generate predictions on test data
    predictions = model.predict(test_features)

    # Calculate evaluation metrics (RMSE, R²)
    rmse = calculate_rmse(test_target.tolist(), predictions.tolist())
    r2 = calculate_r2(test_target.tolist(), predictions.tolist())

    # Return dictionary of evaluation metrics
    return {"rmse": rmse, "r2": r2}


class LinearModelExecutor:
    """Class for executing linear models with caching capability"""

    def __init__(self):
        """Initializes the linear model executor"""
        # Initialize empty model cache dictionary
        self._model_cache: Dict = {}
        # Initialize empty result cache dictionary
        self._result_cache: Dict = {}

    @timing_decorator
    def execute_model(self, model: LinearRegression, features: pd.DataFrame, product: str, hour: int,
                      use_cache: bool = True) -> float:
        """Executes a model for a specific product and hour

        Args:
            model (LinearRegression): Trained linear model
            features (pd.DataFrame): Feature DataFrame
            product (str): The product being forecasted
            hour (int): The hour being forecasted
            use_cache (bool): Whether to use the result cache. Defaults to True.

        Returns:
            float: Point forecast value
        """
        # Create cache key from product, hour, and feature values
        cache_key = f"{product}_{hour}_{features.values.tobytes()}"

        # If use_cache is True and result is in cache, return cached result
        if use_cache and cache_key in self._result_cache:
            logger.debug(f"Returning cached result for {product} hour {hour}")
            return self._result_cache[cache_key]

        # Otherwise, execute the model using execute_linear_model
        forecast_value = execute_linear_model(model, features, product, hour)

        # If use_cache is True, store result in cache
        if use_cache:
            self._result_cache[cache_key] = forecast_value
            logger.debug(f"Stored result in cache for {product} hour {hour}")

        # Return the forecast value
        return forecast_value

    @log_exceptions
    def generate_forecast(self, product: str, hour: int, features: pd.DataFrame) -> float:
        """Generates a forecast for a specific product and hour

        Args:
            product (str): The product being forecasted
            hour (int): The hour being forecasted
            features (pd.DataFrame): Feature DataFrame

        Returns:
            float: Point forecast value
        """
        # Check if product is valid (in FORECAST_PRODUCTS)
        if product not in FORECAST_PRODUCTS:
            raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")

        # Check if model for this product/hour is in cache
        model = self.get_model(product, hour)
        if model is None:
            logger.warning(f"No model found in cache for {product} hour {hour}")
            raise ModelExecutionError(f"No model found in cache for {product} hour {hour}", product, hour, "None")

        # Execute the model using execute_model method
        forecast_value = self.execute_model(model, features, product, hour)

        # Return the forecast value
        return forecast_value

    @log_exceptions
    def register_model(self, product: str, hour: int, model: LinearRegression) -> None:
        """Registers a model for a specific product and hour

        Args:
            product (str): The product being forecasted
            hour (int): The hour being forecasted
            model (LinearRegression): Trained linear model
        """
        # Check if product is valid (in FORECAST_PRODUCTS)
        if product not in FORECAST_PRODUCTS:
            raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")

        # Create cache key from product and hour
        cache_key = f"{product}_{hour}"

        # Store model in model cache with the cache key
        self._model_cache[cache_key] = model

        # Log model registration
        logger.info(f"Registered model for {product} hour {hour}")

    def get_model(self, product: str, hour: int) -> Optional[LinearRegression]:
        """Retrieves a model for a specific product and hour

        Args:
            product (str): The product being forecasted
            hour (int): The hour being forecasted

        Returns:
            Optional[LinearRegression]: Cached model or None if not found
        """
        # Create cache key from product and hour
        cache_key = f"{product}_{hour}"

        # Return model from cache if it exists, otherwise return None
        if cache_key in self._model_cache:
            logger.debug(f"Returning cached model for {product} hour {hour}")
            return self._model_cache[cache_key]
        else:
            logger.debug(f"No model found in cache for {product} hour {hour}")
            return None

    def clear_cache(self) -> None:
        """Clears the result cache"""
        # Clear the result cache dictionary
        self._result_cache.clear()

        # Log cache clearing
        logger.info("Cleared result cache")