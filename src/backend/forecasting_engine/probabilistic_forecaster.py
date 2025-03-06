# src/backend/forecasting_engine/probabilistic_forecaster.py
"""Implements the main probabilistic forecasting functionality for the Electricity Market Price Forecasting System.
This module orchestrates the entire forecasting process by combining model selection, linear model execution,
uncertainty estimation, and sample generation to produce probabilistic price forecasts for electricity market products.
"""

import typing
from datetime import datetime

import pandas  # package_version: 2.0.0+
import numpy  # package_version: 1.24.0+

# Internal imports
from .exceptions import ForecastGenerationError, ModelExecutionError, UncertaintyEstimationError, SampleGenerationError
from .model_selector import select_model_for_product_hour
from .linear_model import execute_linear_model
from .uncertainty_estimator import estimate_uncertainty
from .sample_generator import generate_samples, create_probabilistic_forecast
from ..utils.logging_utils import get_logger, log_execution_time
from ..utils.decorators import memoize, log_exceptions
from ..models.forecast_models import ProbabilisticForecast, ForecastEnsemble
from ..config.settings import FORECAST_PRODUCTS, FORECAST_HORIZON_HOURS

# Global logger
logger = get_logger(__name__)

# Default uncertainty method
DEFAULT_UNCERTAINTY_METHOD = "historical_residuals"

# Default distribution type
DEFAULT_DISTRIBUTION_TYPE = "normal"


@log_execution_time
@log_exceptions
def generate_probabilistic_forecast(
    product: str,
    hour: int,
    features: pandas.DataFrame,
    historical_data: Dict,
    timestamp: datetime,
    uncertainty_method: str = DEFAULT_UNCERTAINTY_METHOD,
    distribution_type: str = DEFAULT_DISTRIBUTION_TYPE
) -> ProbabilisticForecast:
    """Main function to generate a probabilistic forecast for a specific product and hour

    Args:
        product (str): The price product
        hour (int): The target hour
        features (pandas.DataFrame): Feature DataFrame
        historical_data (Dict): Historical data
        timestamp (datetime): Forecast timestamp
        uncertainty_method (str): Uncertainty estimation method
        distribution_type (str): Distribution type for sample generation

    Returns:
        ProbabilisticForecast: Probabilistic forecast for the specified product and hour

    Raises:
        ForecastGenerationError: If any step in forecast generation fails
    """
    try:
        # 1. Validate input parameters
        logger.debug(f"Validating inputs for {product} at hour {hour}")

        # 2. Select appropriate model
        logger.debug(f"Selecting model for {product} at hour {hour}")
        model, feature_names, metrics = select_model_for_product_hour(product, hour)

        # 3. Execute linear model
        logger.debug(f"Executing linear model for {product} at hour {hour}")
        point_forecast = execute_linear_model(model, features[feature_names], product, hour)

        # 4. Estimate uncertainty
        logger.debug(f"Estimating uncertainty for {product} at hour {hour}")
        uncertainty_params = estimate_uncertainty(point_forecast, product, hour, historical_data, method=uncertainty_method)

        # 5. Generate probabilistic samples
        logger.debug(f"Generating samples for {product} at hour {hour}")
        samples = generate_samples(point_forecast, uncertainty_params, product, hour, distribution_type=distribution_type)

        # 6. Create ProbabilisticForecast object
        logger.debug(f"Creating ProbabilisticForecast object for {product} at hour {hour}")
        probabilistic_forecast = create_probabilistic_forecast(point_forecast, samples, product, timestamp)

        return probabilistic_forecast

    except Exception as e:
        # Handle exceptions at each step and raise appropriate ForecastGenerationError
        error_msg = f"Failed to generate forecast for {product} at hour {hour}: {str(e)}"
        logger.error(error_msg)
        raise ForecastGenerationError(error_msg, product, hour, stage=str(e))


@log_execution_time
@log_exceptions
def generate_forecast_ensemble(
    product: str,
    features: pandas.DataFrame,
    historical_data: Dict,
    start_time: datetime,
    uncertainty_method: str = DEFAULT_UNCERTAINTY_METHOD,
    distribution_type: str = DEFAULT_DISTRIBUTION_TYPE
) -> ForecastEnsemble:
    """Generate a complete ensemble of forecasts for a product over the forecast horizon

    Args:
        product (str): The price product
        features (pandas.DataFrame): Feature DataFrame
        historical_data (Dict): Historical data
        start_time (datetime): Start time for the forecast horizon
        uncertainty_method (str): Uncertainty estimation method
        distribution_type (str): Distribution type for sample generation

    Returns:
        ForecastEnsemble: Ensemble of forecasts covering the forecast horizon
    """
    try:
        # 1. Validate that product is in FORECAST_PRODUCTS
        if product not in FORECAST_PRODUCTS:
            raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")

        # 2. Initialize empty list to store individual forecasts
        forecasts: List[ProbabilisticForecast] = []

        # 3. Calculate end_time as start_time + FORECAST_HORIZON_HOURS
        end_time = start_time + pandas.Timedelta(hours=FORECAST_HORIZON_HOURS)

        # 4. For each hour in the forecast horizon:
        current_time = start_time
        while current_time < end_time:
            # 5. Generate probabilistic forecast for the current hour
            forecast = generate_probabilistic_forecast(
                product=product,
                hour=current_time.hour,
                features=features,
                historical_data=historical_data,
                timestamp=current_time,
                uncertainty_method=uncertainty_method,
                distribution_type=distribution_type
            )

            # 6. Append forecast to the list
            forecasts.append(forecast)

            # Increment current_time by one hour
            current_time += pandas.Timedelta(hours=1)

        # 7. Create and return ForecastEnsemble with all generated forecasts
        forecast_ensemble = ForecastEnsemble(
            product=product,
            start_time=start_time,
            end_time=end_time,
            forecasts=forecasts,
            generation_timestamp=datetime.now()
        )
        return forecast_ensemble

    except Exception as e:
        # Handle exceptions and set is_fallback flag if any forecast generation fails
        error_msg = f"Failed to generate forecast ensemble for {product}: {str(e)}"
        logger.error(error_msg)
        raise ForecastGenerationError(error_msg, product, hour=0, stage=str(e))


def validate_forecast_inputs(product: str, hour: int, features: pandas.DataFrame) -> bool:
    """Validates inputs for forecast generation

    Args:
        product (str): The price product
        hour (int): The target hour
        features (pandas.DataFrame): Feature DataFrame

    Returns:
        bool: True if inputs are valid, raises exception otherwise

    Raises:
        ValueError: If any validation fails
    """
    # 1. Check if product is in FORECAST_PRODUCTS
    if product not in FORECAST_PRODUCTS:
        raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")

    # 2. Check if hour is between 0 and 23
    if not 0 <= hour <= 23:
        raise ValueError(f"Invalid hour: {hour}. Must be between 0 and 23")

    # 3. Check if features is a non-empty DataFrame
    if not isinstance(features, pandas.DataFrame) or features.empty:
        raise ValueError("Features must be a non-empty DataFrame")

    # 4. Return True if all validations pass
    return True


def handle_forecast_error(error: Exception, product: str, hour: int, stage: str) -> None:
    """Handles errors during forecast generation and logs appropriate information

    Args:
        error (Exception): The exception that occurred
        product (str): The price product
        hour (int): The target hour
        stage (str): The stage of forecast generation where the error occurred

    Returns:
        None: Function raises ForecastGenerationError

    Raises:
        ForecastGenerationError: Always raised after logging
    """
    # 1. Log detailed error information
    logger.error(f"Error during forecast generation for {product} at hour {hour} in stage {stage}: {str(error)}")

    # 2. If error is already a ForecastGenerationError, re-raise it
    if isinstance(error, ForecastGenerationError):
        raise error

    # 3. Otherwise, wrap error in ForecastGenerationError with appropriate context
    raise ForecastGenerationError(f"Error during forecast generation: {str(error)}", product, hour, stage)


class ProbabilisticForecaster:
    """Class for generating probabilistic forecasts with caching capability"""

    def __init__(self):
        """Initializes the probabilistic forecaster"""
        # 1. Initialize empty forecast cache dictionary
        self._forecast_cache: Dict = {}

        # 2. Initialize uncertainty methods dictionary with default method
        self._uncertainty_methods: Dict = {}

        # 3. Initialize distribution types dictionary with default type
        self._distribution_types: Dict = {}

        # 4. Set up logger for the class
        self.logger = get_logger(__name__)

    @log_execution_time
    @log_exceptions
    def generate_forecast(
        self,
        product: str,
        hour: int,
        features: pandas.DataFrame,
        historical_data: Dict,
        timestamp: datetime,
        use_cache: bool = True
    ) -> ProbabilisticForecast:
        """Generates a probabilistic forecast for a specific product and hour

        Args:
            product (str): The price product
            hour (int): The target hour
            features (pandas.DataFrame): Feature DataFrame
            historical_data (Dict): Historical data
            timestamp (datetime): Forecast timestamp
            use_cache (bool): Whether to use the cache

        Returns:
            ProbabilisticForecast: Probabilistic forecast for the specified product and hour
        """
        # 1. Create cache key from product, hour, and timestamp
        cache_key = f"{product}_{hour}_{timestamp}"

        # 2. If use_cache is True and forecast is in cache, return cached forecast
        if use_cache and cache_key in self._forecast_cache:
            self.logger.debug(f"Returning cached forecast for {product} at hour {hour}")
            return self._forecast_cache[cache_key]

        # 3. Get uncertainty method and distribution type for this product
        uncertainty_method = self.get_uncertainty_method(product)
        distribution_type = self.get_distribution_type(product)

        # 4. Generate forecast using generate_probabilistic_forecast
        forecast = generate_probabilistic_forecast(
            product=product,
            hour=hour,
            features=features,
            historical_data=historical_data,
            timestamp=timestamp,
            uncertainty_method=uncertainty_method,
            distribution_type=distribution_type
        )

        # 5. If use_cache is True, store forecast in cache
        if use_cache:
            self._forecast_cache[cache_key] = forecast
            self.logger.debug(f"Stored forecast in cache for {product} at hour {hour}")

        # 6. Return the generated forecast
        return forecast

    @log_execution_time
    @log_exceptions
    def generate_ensemble(
        self,
        product: str,
        features: pandas.DataFrame,
        historical_data: Dict,
        start_time: datetime,
        use_cache: bool = True
    ) -> ForecastEnsemble:
        """Generates a complete ensemble of forecasts for a product

        Args:
            product (str): The price product
            features (pandas.DataFrame): Feature DataFrame
            historical_data (Dict): Historical data
            start_time (datetime): Start time for the forecast horizon
            use_cache (bool): Whether to use the cache

        Returns:
            ForecastEnsemble: Ensemble of forecasts covering the forecast horizon
        """
        # 1. Get uncertainty method and distribution type for this product
        uncertainty_method = self.get_uncertainty_method(product)
        distribution_type = self.get_distribution_type(product)

        # 2. Generate ensemble using generate_forecast_ensemble
        ensemble = generate_forecast_ensemble(
            product=product,
            features=features,
            historical_data=historical_data,
            start_time=start_time,
            uncertainty_method=uncertainty_method,
            distribution_type=distribution_type
        )

        # 3. Return the generated ensemble
        return ensemble

    def register_uncertainty_method(self, product: str, method: str) -> None:
        """Registers a specific uncertainty method for a product

        Args:
            product (str): The price product
            method (str): The uncertainty method
        """
        # 1. Validate that product is in FORECAST_PRODUCTS
        if product not in FORECAST_PRODUCTS:
            raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")

        # 2. Store method for the product in uncertainty_methods dictionary
        self._uncertainty_methods[product] = method

        # 3. Log registration of uncertainty method for product
        self.logger.info(f"Registered uncertainty method '{method}' for product {product}")

    def register_distribution_type(self, product: str, distribution_type: str) -> None:
        """Registers a specific distribution type for a product

        Args:
            product (str): The price product
            distribution_type (str): The distribution type
        """
        # 1. Validate that product is in FORECAST_PRODUCTS
        if product not in FORECAST_PRODUCTS:
            raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")

        # 2. Store distribution type for the product in distribution_types dictionary
        self._distribution_types[product] = distribution_type

        # 3. Log registration of distribution type for product
        self.logger.info(f"Registered distribution type '{distribution_type}' for product {product}")

    def get_uncertainty_method(self, product: str) -> str:
        """Gets the uncertainty method for a specific product

        Args:
            product (str): The price product

        Returns:
            str: Uncertainty method for the product
        """
        # 1. Return method from uncertainty_methods dictionary if present
        if product in self._uncertainty_methods:
            return self._uncertainty_methods[product]

        # 2. Otherwise return DEFAULT_UNCERTAINTY_METHOD
        return DEFAULT_UNCERTAINTY_METHOD

    def get_distribution_type(self, product: str) -> str:
        """Gets the distribution type for a specific product

        Args:
            product (str): The price product

        Returns:
            str: Distribution type for the product
        """
        # 1. Return type from distribution_types dictionary if present
        if product in self._distribution_types:
            return self._distribution_types[product]

        # 2. Otherwise return DEFAULT_DISTRIBUTION_TYPE
        return DEFAULT_DISTRIBUTION_TYPE

    def clear_cache(self) -> None:
        """Clears the forecast cache"""
        # 1. Clear the forecast_cache dictionary
        self._forecast_cache.clear()

        # 2. Log cache clearing
        self.logger.info("Cleared forecast cache")