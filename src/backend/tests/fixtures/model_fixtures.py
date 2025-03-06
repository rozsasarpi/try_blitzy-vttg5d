# src/backend/tests/fixtures/model_fixtures.py
"""Provides test fixtures for forecasting models to be used in unit and integration tests
for the Electricity Market Price Forecasting System. These fixtures include mock linear models,
model registry entries, uncertainty parameters, and probabilistic forecasts for testing the forecasting engine components."""

import datetime
from typing import Dict, List, Optional, Any, Tuple
import tempfile
import pathlib

import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
from sklearn.linear_model import LinearRegression  # scikit-learn: 1.2.0+

from src.backend.forecasting_engine.exceptions import ModelRegistryError  # Custom exception for model registry issues
from src.backend.forecasting_engine.exceptions import ModelExecutionError  # Exception for model execution failures
from src.backend.forecasting_engine.exceptions import UncertaintyEstimationError  # Exception for uncertainty estimation failures
from src.backend.forecasting_engine.exceptions import SampleGenerationError  # Exception for sample generation failures
from src.backend.forecasting_engine.model_registry import ModelRegistry  # Registry for managing linear models
from src.backend.forecasting_engine.linear_model import create_linear_model  # Create a new linear model instance
from src.backend.forecasting_engine.linear_model import LinearModelExecutor  # Class for executing linear models with caching
from src.backend.forecasting_engine.model_selector import ModelSelector  # Class for selecting appropriate models
from src.backend.forecasting_engine.uncertainty_estimator import UncertaintyEstimator  # Class for estimating forecast uncertainty
from src.backend.forecasting_engine.sample_generator import SampleGenerator  # Class for generating probabilistic samples
from src.backend.forecasting_engine.probabilistic_forecaster import ProbabilisticForecaster  # Class for generating probabilistic forecasts
from src.backend.models.forecast_models import ProbabilisticForecast  # Model class for probabilistic forecasts
from src.backend.models.forecast_models import ForecastEnsemble  # Class for ensemble of forecasts
from src.backend.tests.fixtures.feature_fixtures import create_mock_feature_data  # Create mock feature data for tests
from src.backend.tests.fixtures.feature_fixtures import create_mock_product_hour_features  # Create mock product/hour-specific features
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid price products
from src.backend.config.settings import FORECAST_HORIZON_HOURS  # Number of hours in forecast horizon
from src.backend.config.settings import PROBABILISTIC_SAMPLE_COUNT  # Number of probabilistic samples
from src.backend.utils.date_utils import localize_to_cst  # Convert datetime to CST timezone

DEFAULT_MODEL_PARAMS = {"fit_intercept": True, "copy_X": True, "n_jobs": None}
DEFAULT_FEATURE_NAMES = ["load_mw", "wind_generation", "solar_generation", "thermal_generation", "hour", "day_of_week", "is_weekend"]
DEFAULT_METRICS = {"rmse": 5.23, "r2": 0.87, "mae": 4.12}

def create_mock_linear_model(model_params: Optional[Dict] = None, feature_names: Optional[List[str]] = None, coefficients: Optional[numpy.ndarray] = None, intercept: Optional[float] = None) -> LinearRegression:
    """Creates a mock linear regression model for testing

    Args:
        model_params: Dictionary of model parameters
        feature_names: List of feature names
        coefficients: Array of coefficients
        intercept: Intercept value

    Returns:
        Mock linear model with specified parameters
    """
    # If model_params is None, use DEFAULT_MODEL_PARAMS
    params = model_params if model_params else DEFAULT_MODEL_PARAMS

    # Create a new LinearRegression model using create_linear_model
    model = create_linear_model(params)

    # If coefficients are provided, set model.coef_ to the coefficients
    if coefficients is not None:
        model.coef_ = coefficients

    # If intercept is provided, set model.intercept_ to the intercept
    if intercept is not None:
        model.intercept_ = intercept

    # If feature_names is provided, store them as model.feature_names_
    if feature_names is not None:
        model.feature_names_ = feature_names

    # Return the configured model
    return model

def create_mock_model_registry(models_dict: Optional[Dict] = None, registry_dir: Optional[pathlib.Path] = None) -> ModelRegistry:
    """Creates a mock model registry with predefined models

    Args:
        models_dict: Dictionary of models to register
        registry_dir: Directory to store the registry

    Returns:
        Mock model registry with registered models
    """
    # If registry_dir is None, create a temporary directory
    if registry_dir is None:
        registry_dir = pathlib.Path(tempfile.mkdtemp())

    # Create a new ModelRegistry instance with the registry_dir
    registry = ModelRegistry(registry_dir=str(registry_dir))

    # If models_dict is provided, register each model in the registry
    if models_dict is not None:
        # For each (product, hour) key in models_dict:
        for (product, hour), value in models_dict.items():
            # Extract model, feature_names, and metrics
            model = value["model"]
            feature_names = value["feature_names"]
            metrics = value["metrics"]

            # Register the model in the registry
            registry.register(product, hour, model, feature_names, metrics)

    # Return the configured registry
    return registry

def create_mock_model_selector(registry: ModelRegistry) -> ModelSelector:
    """Creates a mock model selector with predefined model selections

    Args:
        registry: ModelRegistry instance

    Returns:
        Mock model selector using the provided registry
    """
    # Create a new ModelSelector instance
    model_selector = ModelSelector()

    # Patch the select_model method to use the provided registry
    model_selector.select_model = lambda product, hour: registry.get(product, hour)

    # Return the configured model selector
    return model_selector

def create_mock_uncertainty_params(mean: Optional[float] = None, std_dev: Optional[float] = None, distribution_type: Optional[str] = None) -> Dict[str, float]:
    """Creates mock uncertainty parameters for testing

    Args:
        mean: Mean value
        std_dev: Standard deviation value
        distribution_type: Distribution type

    Returns:
        Dictionary of uncertainty parameters
    """
    # If mean is None, use 0.0 as default
    mean = mean if mean is not None else 0.0

    # If std_dev is None, use 5.0 as default
    std_dev = std_dev if std_dev is not None else 5.0

    # If distribution_type is None, use 'normal' as default
    distribution_type = distribution_type if distribution_type is not None else 'normal'

    # Create uncertainty_params dictionary with mean, std_dev, and distribution_type
    uncertainty_params = {"mean": mean, "std_dev": std_dev, "distribution_type": distribution_type}

    # Return the uncertainty parameters dictionary
    return uncertainty_params

def create_mock_uncertainty_estimator(product_uncertainties: Optional[Dict] = None, error: Optional[Exception] = None) -> UncertaintyEstimator:
    """Creates a mock uncertainty estimator with predefined responses

    Args:
        product_uncertainties: Dictionary of product uncertainties
        error: Exception to raise

    Returns:
        Mock uncertainty estimator
    """
    # Create a new UncertaintyEstimator instance
    uncertainty_estimator = UncertaintyEstimator()

    # If product_uncertainties is provided, configure the estimator to return these values
    if product_uncertainties is not None:
        uncertainty_estimator.estimate_uncertainty = lambda point_forecast, product, hour, historical_data, method: product_uncertainties.get((product, hour))

    # If error is provided, configure the estimator to raise this error
    if error is not None:
        uncertainty_estimator.estimate_uncertainty = lambda point_forecast, product, hour, historical_data, method: exec("raise error")

    # Return the configured uncertainty estimator
    return uncertainty_estimator

def create_mock_sample_generator(product_samples: Optional[Dict] = None, error: Optional[Exception] = None) -> SampleGenerator:
    """Creates a mock sample generator with predefined samples

    Args:
        product_samples: Dictionary of product samples
        error: Exception to raise

    Returns:
        Mock sample generator
    """
    # Create a new SampleGenerator instance
    sample_generator = SampleGenerator()

    # If product_samples is provided, configure the generator to return these samples
    if product_samples is not None:
        sample_generator.generate_samples = lambda point_forecast, uncertainty_params, product, hour, distribution_type: product_samples.get((product, hour))

    # If error is provided, configure the generator to raise this error
    if error is not None:
        sample_generator.generate_samples = lambda point_forecast, uncertainty_params, product, hour, distribution_type: exec("raise error")

    # Return the configured sample generator
    return sample_generator

def create_mock_probabilistic_forecast(product: Optional[str] = None, timestamp: Optional[datetime.datetime] = None, point_forecast: Optional[float] = None, samples: Optional[List] = None, is_fallback: Optional[bool] = None) -> ProbabilisticForecast:
    """Creates a mock probabilistic forecast for testing

    Args:
        product: Price product identifier
        timestamp: Forecast timestamp
        point_forecast: Point forecast value
        samples: List of probabilistic samples
        is_fallback: Whether the forecast is a fallback

    Returns:
        Mock probabilistic forecast
    """
    # If product is None, use 'DALMP' as default
    product = product if product is not None else 'DALMP'

    # If timestamp is None, use current time in CST
    timestamp = timestamp if timestamp is not None else localize_to_cst(datetime.datetime.now())

    # If point_forecast is None, use 45.0 as default
    point_forecast = point_forecast if point_forecast is not None else 45.0

    # If samples is None, generate random samples around point_forecast
    if samples is None:
        samples = numpy.random.normal(loc=point_forecast, scale=5.0, size=PROBABILISTIC_SAMPLE_COUNT).tolist()

    # Ensure samples has length equal to PROBABILISTIC_SAMPLE_COUNT
    if len(samples) != PROBABILISTIC_SAMPLE_COUNT:
        samples = numpy.random.normal(loc=point_forecast, scale=5.0, size=PROBABILISTIC_SAMPLE_COUNT).tolist()

    # If is_fallback is None, use False as default
    is_fallback = is_fallback if is_fallback is not None else False

    # Create and return a new ProbabilisticForecast with the specified parameters
    return ProbabilisticForecast(timestamp=timestamp, product=product, point_forecast=point_forecast, samples=samples, generation_timestamp=datetime.datetime.now(), is_fallback=is_fallback)

def create_mock_forecast_ensemble(product: Optional[str] = None, start_time: Optional[datetime.datetime] = None, hours: Optional[int] = None, is_fallback: Optional[bool] = None) -> ForecastEnsemble:
    """Creates a mock forecast ensemble for testing

    Args:
        product: Price product identifier
        start_time: Start time for the ensemble
        hours: Number of hours in the ensemble
        is_fallback: Whether the ensemble is a fallback

    Returns:
        Mock forecast ensemble
    """
    # If product is None, use 'DALMP' as default
    product = product if product is not None else 'DALMP'

    # If start_time is None, use current time in CST
    start_time = start_time if start_time is not None else localize_to_cst(datetime.datetime.now())

    # If hours is None, use FORECAST_HORIZON_HOURS as default
    hours = hours if hours is not None else FORECAST_HORIZON_HOURS

    # If is_fallback is None, use False as default
    is_fallback = is_fallback if is_fallback is not None else False

    # Calculate end_time as start_time + hours
    end_time = start_time + datetime.timedelta(hours=hours)

    # Create a list of mock probabilistic forecasts for each hour
    forecasts = [create_mock_probabilistic_forecast(product=product, timestamp=start_time + datetime.timedelta(hours=i), is_fallback=is_fallback) for i in range(hours)]

    # Create and return a new ForecastEnsemble with the forecasts
    return ForecastEnsemble(product=product, start_time=start_time, end_time=end_time, forecasts=forecasts, is_fallback=is_fallback)

def create_mock_historical_data(product_hour_errors: Optional[Dict] = None) -> Dict:
    """Creates mock historical data for uncertainty estimation

    Args:
        product_hour_errors: Dictionary of product/hour errors

    Returns:
        Dictionary of historical data for uncertainty estimation
    """
    # If product_hour_errors is None, create default errors for each product and hour
    if product_hour_errors is None:
        product_hour_errors = {}
        for product in FORECAST_PRODUCTS:
            for hour in range(24):
                product_hour_errors[(product, hour)] = {"residuals": numpy.random.normal(loc=0, scale=5, size=100).tolist(), "percentage_errors": numpy.random.normal(loc=0, scale=0.1, size=100).tolist()}

    # Create a historical_data dictionary with residuals and percentage errors
    historical_data = product_hour_errors

    # Return the historical_data dictionary
    return historical_data

def create_mock_probabilistic_forecaster(product_forecasts: Optional[Dict] = None, error: Optional[Exception] = None) -> ProbabilisticForecaster:
    """Creates a mock probabilistic forecaster with predefined responses

    Args:
        product_forecasts: Dictionary of product forecasts
        error: Exception to raise

    Returns:
        Mock probabilistic forecaster
    """
    # Create a new ProbabilisticForecaster instance
    probabilistic_forecaster = ProbabilisticForecaster()

    # If product_forecasts is provided, configure the forecaster to return these forecasts
    if product_forecasts is not None:
        probabilistic_forecaster.generate_forecast = lambda product, hour, features, historical_data, timestamp, use_cache: product_forecasts.get((product, hour))

    # If error is provided, configure the forecaster to raise this error
    if error is not None:
        probabilistic_forecaster.generate_forecast = lambda product, hour, features, historical_data, timestamp, use_cache: exec("raise error")

    # Return the configured forecaster
    return probabilistic_forecaster

class MockModelRegistry:
    """Mock class for model registry to use in tests"""

    def __init__(self, models: Optional[Dict] = None, error: Optional[Exception] = None):
        """Initialize the mock model registry with predefined models

        Args:
            models: Dictionary of models to return
            error: Exception to raise
        """
        # Initialize _models dictionary with provided models or empty dict
        self._models = models if models else {}
        # Store optional error to raise during operations
        self._error = error if error is not None else None

    def register(self, product: str, hour: int, model: LinearRegression, feature_names: List[str], metrics: Dict[str, float]) -> bool:
        """Mock implementation of register that stores models or raises errors

        Args:
            product: The price product
            hour: The target hour
            model: The trained LinearRegression model
            feature_names: List of feature names used by the model
            metrics: Dictionary of model performance metrics

        Returns:
            True if successful
        """
        # If self._error is set, raise the specified error
        if self._error:
            raise self._error

        # Create a key from product and hour
        key = (product, hour)

        # Store model, feature_names, and metrics in _models dictionary
        self._models[key] = {"model": model, "feature_names": feature_names, "metrics": metrics}

        # Return True
        return True

    def get(self, product: str, hour: int) -> Tuple[Optional[LinearRegression], Optional[List[str]], Optional[Dict[str, float]]]:
        """Mock implementation of get that returns models or raises errors

        Args:
            product: The price product
            hour: The target hour

        Returns:
            Tuple of (model, feature_names, metrics) or (None, None, None)
        """
        # If self._error is set, raise the specified error
        if self._error:
            raise self._error

        # Create a key from product and hour
        key = (product, hour)

        # If key exists in _models, return the stored model, feature_names, and metrics
        if key in self._models:
            model_entry = self._models[key]
            return model_entry["model"], model_entry["feature_names"], model_entry["metrics"]

        # Otherwise, return (None, None, None)
        return None, None, None

    def has_model(self, product: str, hour: int) -> bool:
        """Mock implementation of has_model that checks for models or raises errors

        Args:
            product: The price product
            hour: The target hour

        Returns:
            True if model exists, False otherwise
        """
        # If self._error is set, raise the specified error
        if self._error:
            raise self._error

        # Create a key from product and hour
        key = (product, hour)

        # Return True if key exists in _models, False otherwise
        return key in self._models

    def set_error(self, error: Exception) -> None:
        """Sets an error to be raised on operations

        Args:
            error: Exception to raise
        """
        # Store the provided error in self._error
        self._error = error

    def clear_error(self) -> None:
        """Clears any set error"""
        # Set self._error to None
        self._error = None

    def add_model(self, product: str, hour: int, model: LinearRegression, feature_names: List[str], metrics: Dict[str, float]) -> None:
        """Adds a model to the registry

        Args:
            product: The price product
            hour: The target hour
            model: The trained LinearRegression model
            feature_names: List of feature names used by the model
            metrics: Dictionary of model performance metrics
        """
        # Create a key from product and hour
        key = (product, hour)

        # Store model, feature_names, and metrics in _models dictionary
        self._models[key] = {"model": model, "feature_names": feature_names, "metrics": metrics}

class MockModelExecutor:
    """Mock class for linear model execution to use in tests"""

    def __init__(self, models: Optional[Dict] = None, results: Optional[Dict] = None, error: Optional[Exception] = None):
        """Initialize the mock model executor with predefined results

        Args:
            models: Dictionary of models to return
            results: Dictionary of results to return
            error: Exception to raise
        """
        # Initialize _models dictionary with provided models or empty dict
        self._models = models if models else {}
        # Initialize _results dictionary with provided results or empty dict
        self._results = results if results else {}
        # Store optional error to raise during execution
        self._error = error if error is not None else None

    def execute_model(self, model: LinearRegression, features: pandas.DataFrame, product: str, hour: int, use_cache: bool) -> float:
        """Mock implementation of execute_model that returns predefined results

        Args:
            model: The LinearRegression model
            features: The features DataFrame
            product: The price product
            hour: The target hour
            use_cache: Whether to use the cache

        Returns:
            Predefined forecast value
        """
        # If self._error is set, raise the specified error
        if self._error:
            raise self._error

        # Create a key from product and hour
        key = (product, hour)

        # If key exists in _results, return the stored result
        if key in self._results:
            return self._results[key]

        # Otherwise, return a default value (45.0)
        return 45.0

    def register_model(self, product: str, hour: int, model: LinearRegression) -> None:
        """Mock implementation of register_model

        Args:
            product: The price product
            hour: The target hour
            model: The LinearRegression model
        """
        # If self._error is set, raise the specified error
        if self._error:
            raise self._error

        # Create a key from product and hour
        key = (product, hour)

        # Store model in _models dictionary
        self._models[key] = model

    def set_result(self, product: str, hour: int, result: float) -> None:
        """Sets a predefined result for a product/hour combination

        Args:
            product: The price product
            hour: The target hour
            result: The result to return
        """
        # Create a key from product and hour
        key = (product, hour)

        # Store result in _results dictionary
        self._results[key] = result

    def set_error(self, error: Exception) -> None:
        """Sets an error to be raised on execution

        Args:
            error: Exception to raise
        """
        # Store the provided error in self._error
        self._error = error

    def clear_error(self) -> None:
        """Clears any set error"""
        # Set self._error to None
        self._error = None

class MockUncertaintyEstimator:
    """Mock class for uncertainty estimation to use in tests"""

    def __init__(self, product_uncertainties: Optional[Dict] = None, error: Optional[Exception] = None):
        """Initialize the mock uncertainty estimator with predefined uncertainties

        Args:
            uncertainties: Dictionary of uncertainties to return
            error: Exception to raise
        """
        # Initialize _uncertainties dictionary with provided uncertainties or empty dict
        self._uncertainties = product_uncertainties if product_uncertainties else {}
        # Store optional error to raise during estimation
        self._error = error if error is not None else None

    def estimate_uncertainty(self, point_forecast: float, product: str, hour: int, historical_data: Dict, method: str) -> Dict[str, float]:
        """Mock implementation of estimate_uncertainty

        Args:
            point_forecast: The point forecast value
            product: The price product
            hour: The target hour
            historical_data: Historical data
            method: The uncertainty estimation method

        Returns:
            Predefined uncertainty parameters
        """
        # If self._error is set, raise the specified error
        if self._error:
            raise self._error

        # Create a key from product and hour
        key = (product, hour)

        # If key exists in _uncertainties, return the stored uncertainty
        if key in self._uncertainties:
            return self._uncertainties[key]

        # Otherwise, return default uncertainty parameters
        return {"mean": point_forecast, "std_dev": 5.0}

    def set_uncertainty(self, product: str, hour: int, uncertainty: Dict[str, float]) -> None:
        """Sets predefined uncertainty for a product/hour combination

        Args:
            product: The price product
            hour: The target hour
            uncertainty: The uncertainty parameters to return
        """
        # Create a key from product and hour
        key = (product, hour)

        # Store uncertainty in _uncertainties dictionary
        self._uncertainties[key] = uncertainty

    def set_error(self, error: Exception) -> None:
        """Sets an error to be raised on estimation

        Args:
            error: Exception to raise
        """
        # Store the provided error in self._error
        self._error = error

    def clear_error(self) -> None:
        """Clears any set error"""
        # Set self._error to None
        self._error = None

class MockSampleGenerator:
    """Mock class for sample generation to use in tests"""

    def __init__(self, samples: Optional[Dict] = None, error: Optional[Exception] = None):
        """Initialize the mock sample generator with predefined samples

        Args:
            samples: Dictionary of samples to return
            error: Exception to raise
        """
        # Initialize _samples dictionary with provided samples or empty dict
        self._samples = samples if samples else {}
        # Store optional error to raise during generation
        self._error = error if error is not None else None

    def generate_samples(self, point_forecast: float, uncertainty_params: Dict, product: str, hour: int, distribution_type: str) -> List[float]:
        """Mock implementation of generate_samples

        Args:
            point_forecast: The point forecast value
            uncertainty_params: The uncertainty parameters
            product: The price product
            hour: The target hour
            distribution_type: The distribution type

        Returns:
            Predefined samples
        """
        # If self._error is set, raise the specified error
        if self._error:
            raise self._error

        # Create a key from product and hour
        key = (product, hour)

        # If key exists in _samples, return the stored samples
        if key in self._samples:
            return self._samples[key]

        # Otherwise, generate random samples around point_forecast
        samples = numpy.random.normal(loc=point_forecast, scale=5.0, size=PROBABILISTIC_SAMPLE_COUNT).tolist()
        return samples

    def set_samples(self, product: str, hour: int, samples: List[float]) -> None:
        """Sets predefined samples for a product/hour combination

        Args:
            product: The price product
            hour: The target hour
            samples: The samples to return
        """
        # Create a key from product and hour
        key = (product, hour)

        # Store samples in _samples dictionary
        self._samples[key] = samples

    def set_error(self, error: Exception) -> None:
        """Sets an error to be raised on sample generation

        Args:
            error: Exception to raise
        """
        # Store the provided error in self._error
        self._error = error

    def clear_error(self) -> None:
        """Clears any set error"""
        # Set self._error to None
        self._error = None