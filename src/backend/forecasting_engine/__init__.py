"""
Initialization file for the forecasting engine module of the Electricity Market Price Forecasting System.
This module exports the core functionality for generating probabilistic price forecasts using linear models tailored to specific market products and hours.
"""

from .exceptions import (
    ForecastingEngineError,
    ModelSelectionError,
    ModelExecutionError,
    UncertaintyEstimationError,
    SampleGenerationError,
    ModelRegistryError,
    InvalidFeatureError,
    ForecastGenerationError,
)
from .model_registry import (
    initialize_registry,
    register_model,
    get_model,
    has_model,
    list_available_models,
    ModelRegistry,
)
from .linear_model import (
    create_linear_model,
    train_linear_model,
    execute_linear_model,
    get_model_coefficients,
    evaluate_model,
    LinearModelExecutor,
)
from .model_selector import (
    select_model_for_product_hour,
    validate_product_hour,
    get_model_info,
    is_model_available,
    ModelSelector,
)
from .sample_generator import (
    generate_samples,
    create_probabilistic_forecast,
    SampleGenerator,
    generate_normal_samples,
    generate_lognormal_samples,
    generate_truncated_normal_samples,
    generate_skewed_normal_samples,
)
from .uncertainty_estimator import (
    estimate_uncertainty,
    UncertaintyEstimator,
    estimate_uncertainty_from_residuals,
    estimate_uncertainty_from_percentage,
    estimate_uncertainty_fixed,
    estimate_uncertainty_adaptive,
)
from ..utils.logging_utils import get_logger

# Initialize logger
logger = get_logger(__name__)

# Define the version of the forecasting engine
__version__ = "0.1.0"

# Expose all the exception classes
__all__ = [
    "ForecastingEngineError",
    "ModelSelectionError",
    "ModelExecutionError",
    "UncertaintyEstimationError",
    "SampleGenerationError",
    "ModelRegistryError",
    "InvalidFeatureError",
    "ForecastGenerationError",
    "initialize_registry",
    "register_model",
    "get_model",
    "has_model",
    "list_available_models",
    "ModelRegistry",
    "create_linear_model",
    "train_linear_model",
    "execute_linear_model",
    "get_model_coefficients",
    "evaluate_model",
    "LinearModelExecutor",
    "select_model_for_product_hour",
    "validate_product_hour",
    "get_model_info",
    "is_model_available",
    "ModelSelector",
    "generate_samples",
    "create_probabilistic_forecast",
    "SampleGenerator",
    "generate_normal_samples",
    "generate_lognormal_samples",
    "generate_truncated_normal_samples",
    "generate_skewed_normal_samples",
    "estimate_uncertainty",
    "UncertaintyEstimator",
    "estimate_uncertainty_from_residuals",
    "estimate_uncertainty_from_percentage",
    "estimate_uncertainty_fixed",
    "estimate_uncertainty_adaptive",
]