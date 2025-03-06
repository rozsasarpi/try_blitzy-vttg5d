"""
Initializes the models package and exports all model classes and utility functions for the
Electricity Market Price Forecasting System.

This module serves as the entry point for accessing data models, validation models, and
forecast models throughout the application, providing a clean interface to the models layer.
"""

# Import data models
from .data_models import (
    BaseDataModel,
    LoadForecast,
    HistoricalPrice,
    GenerationForecast,
    PriceForecast,
    create_empty_forecast_dataframe,
    create_sample_columns
)

# Import validation models
from .validation_models import (
    ValidationResult,
    DataFrameValidator,
    ModelValidator,
    validate_dataframe,
    validate_model,
    format_validation_errors
)

# Import forecast models
from .forecast_models import (
    ProbabilisticForecast,
    ForecastEnsemble,
    ForecastComparison,
    calculate_forecast_statistics,
    aggregate_forecasts,
    create_forecast_dataframe,
    forecasts_from_dataframe,
    CONFIDENCE_LEVELS,
    FORECAST_METRICS
)

# Export all models and utility functions
__all__ = [
    # Data models
    'BaseDataModel',
    'LoadForecast',
    'HistoricalPrice',
    'GenerationForecast',
    'PriceForecast',
    'create_empty_forecast_dataframe',
    'create_sample_columns',
    
    # Validation models
    'ValidationResult',
    'DataFrameValidator',
    'ModelValidator',
    'validate_dataframe',
    'validate_model',
    'format_validation_errors',
    
    # Forecast models
    'ProbabilisticForecast',
    'ForecastEnsemble',
    'ForecastComparison',
    'calculate_forecast_statistics',
    'aggregate_forecasts',
    'create_forecast_dataframe',
    'forecasts_from_dataframe',
    'CONFIDENCE_LEVELS',
    'FORECAST_METRICS'
]