"""
Initialization file for the test fixtures package that makes fixtures easily importable throughout the test suite.
This file exposes key test fixtures from individual fixture modules to simplify imports in test files for the Electricity Market Price Forecasting System.
"""

from .load_forecast_fixtures import (
    create_mock_load_forecast_data,
    create_mock_load_forecast_models,
    create_incomplete_load_forecast_data,
    create_invalid_load_forecast_data,
    create_mock_api_response,
    create_invalid_api_response,
    MockLoadForecastClient,
)
from .historical_prices_fixtures import (
    create_mock_historical_price_data,
    create_mock_historical_price_models,
    create_incomplete_historical_price_data,
    create_invalid_historical_price_data,
    generate_price_value,
    MockHistoricalPriceClient,
)
from .forecast_fixtures import (
    create_mock_price_samples,
    create_mock_probabilistic_forecast,
    create_mock_forecast_data,
    create_mock_probabilistic_forecasts,
    create_mock_forecast_ensemble,
    create_incomplete_forecast_data,
    create_invalid_forecast_data,
    create_inconsistent_forecast_data,
    MockForecastValidator,
    SAMPLE_PRODUCTS,
    BASE_PRICE_PATTERNS,
    VOLATILITY_FACTORS,
)

__all__ = [
    "create_mock_load_forecast_data",
    "create_mock_load_forecast_models",
    "create_incomplete_load_forecast_data",
    "create_invalid_load_forecast_data",
    "create_mock_api_response",
    "create_invalid_api_response",
    "MockLoadForecastClient",
    "create_mock_historical_price_data",
    "create_mock_historical_price_models",
    "create_incomplete_historical_price_data",
    "create_invalid_historical_price_data",
    "generate_price_value",
    "MockHistoricalPriceClient",
    "create_mock_price_samples",
    "create_mock_probabilistic_forecast",
    "create_mock_forecast_data",
    "create_mock_probabilistic_forecasts",
    "create_mock_forecast_ensemble",
    "create_incomplete_forecast_data",
    "create_invalid_forecast_data",
    "create_inconsistent_forecast_data",
    "MockForecastValidator",
    "SAMPLE_PRODUCTS",
    "BASE_PRICE_PATTERNS",
    "VOLATILITY_FACTORS",
]