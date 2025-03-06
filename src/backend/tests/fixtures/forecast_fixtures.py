# src/backend/tests/fixtures/forecast_fixtures.py
"""
Provides test fixtures for forecast data to be used in unit and integration tests
for the Electricity Market Price Forecasting System. This module contains functions to generate
mock probabilistic forecasts, forecast ensembles, and various test data scenarios including
normal, incomplete, invalid, and inconsistent forecast data.
"""

import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
from typing import List, Optional, Dict
from datetime import datetime, timedelta  # standard library

from src.backend.models.forecast_models import ProbabilisticForecast, ForecastEnsemble  # Model class for probabilistic forecasts
from src.backend.models.validation_models import ValidationResult  # Class for representing validation results
from src.backend.config.settings import FORECAST_PRODUCTS, FORECAST_HORIZON_HOURS, PROBABILISTIC_SAMPLE_COUNT  # List of valid price products for validation
from src.backend.tests.fixtures.feature_fixtures import create_mock_product_hour_features  # Create mock product/hour-specific features for tests
from src.backend.utils.date_utils import localize_to_cst  # Convert datetime to CST timezone

SAMPLE_PRODUCTS = ['DALMP', 'RTLMP', 'RegUp', 'RegDown', 'RRS', 'NSRS']
BASE_PRICE_PATTERNS = {
    "DALMP": {"base": 45.0, "daily_pattern": [0.8, 0.7, 0.65, 0.6, 0.65, 0.7, 0.9, 1.1, 1.2, 1.1, 1.0, 1.0, 1.0, 0.95, 0.9, 0.95, 1.1, 1.3, 1.4, 1.3, 1.2, 1.1, 1.0, 0.9]},
    "RTLMP": {"base": 48.0, "daily_pattern": [0.8, 0.7, 0.65, 0.6, 0.65, 0.7, 0.9, 1.2, 1.3, 1.2, 1.1, 1.0, 1.0, 0.95, 0.9, 0.95, 1.2, 1.4, 1.5, 1.4, 1.3, 1.2, 1.1, 0.9]},
    "RegUp": {"base": 12.0, "daily_pattern": [0.7, 0.6, 0.6, 0.6, 0.6, 0.7, 0.8, 1.0, 1.2, 1.3, 1.4, 1.3, 1.2, 1.1, 1.0, 1.1, 1.3, 1.5, 1.6, 1.5, 1.4, 1.2, 1.0, 0.8]},
    "RegDown": {"base": 8.0, "daily_pattern": [1.0, 1.1, 1.2, 1.2, 1.1, 1.0, 0.9, 0.8, 0.7, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.1, 0.9, 0.8, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]},
    "RRS": {"base": 10.0, "daily_pattern": [0.8, 0.7, 0.7, 0.7, 0.7, 0.8, 0.9, 1.1, 1.2, 1.3, 1.3, 1.2, 1.1, 1.0, 0.9, 1.0, 1.2, 1.4, 1.5, 1.4, 1.3, 1.1, 0.9, 0.8]},
    "NSRS": {"base": 6.0, "daily_pattern": [0.9, 0.8, 0.8, 0.8, 0.8, 0.9, 1.0, 1.1, 1.2, 1.2, 1.2, 1.1, 1.0, 0.9, 0.9, 1.0, 1.1, 1.3, 1.4, 1.3, 1.2, 1.1, 1.0, 0.9]}
}
VOLATILITY_FACTORS = {
    "DALMP": 0.1,
    "RTLMP": 0.2,
    "RegUp": 0.15,
    "RegDown": 0.12,
    "RRS": 0.14,
    "NSRS": 0.13
}


def create_mock_price_samples(point_forecast: float = None, volatility: float = None, sample_count: int = None) -> List[float]:
    """
    Creates a list of mock price samples for probabilistic forecasts
    """
    if point_forecast is None:
        point_forecast = 45.0
    if volatility is None:
        volatility = 0.1
    if sample_count is None:
        sample_count = PROBABILISTIC_SAMPLE_COUNT

    samples = numpy.random.normal(point_forecast, point_forecast * volatility, sample_count).tolist()
    # Ensure all samples are non-negative for products that can't have negative prices
    samples = [max(0, s) for s in samples]
    return samples


def create_mock_probabilistic_forecast(product: str = None, timestamp: datetime = None, point_forecast: float = None, samples: List[float] = None, generation_timestamp: datetime = None, is_fallback: bool = None) -> ProbabilisticForecast:
    """
    Creates a single mock probabilistic forecast for testing
    """
    if product is None:
        product = 'DALMP'
    if timestamp is None:
        timestamp = datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    if point_forecast is None:
        hour = timestamp.hour
        point_forecast = BASE_PRICE_PATTERNS[product]["base"] * BASE_PRICE_PATTERNS[product]["daily_pattern"][hour]
    if samples is None:
        volatility = VOLATILITY_FACTORS[product]
        samples = create_mock_price_samples(point_forecast, volatility)
    if generation_timestamp is None:
        generation_timestamp = datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    if is_fallback is None:
        is_fallback = False

    return ProbabilisticForecast(
        timestamp=timestamp,
        product=product,
        point_forecast=point_forecast,
        samples=samples,
        generation_timestamp=generation_timestamp,
        is_fallback=is_fallback
    )


def create_mock_forecast_data(product: str = None, start_time: datetime = None, hours: int = None, is_fallback: bool = None) -> pandas.DataFrame:
    """
    Creates a pandas DataFrame with mock forecast data
    """
    if product is None:
        product = 'DALMP'
    if start_time is None:
        start_time = datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    if is_fallback is None:
        is_fallback = False

    forecasts = []
    for hour in range(hours):
        forecast_time = start_time + timedelta(hours=hour)
        point_forecast = BASE_PRICE_PATTERNS[product]["base"] * BASE_PRICE_PATTERNS[product]["daily_pattern"][forecast_time.hour]
        volatility = VOLATILITY_FACTORS[product]
        samples = create_mock_price_samples(point_forecast, volatility)
        forecast = ProbabilisticForecast(
            timestamp=forecast_time,
            product=product,
            point_forecast=point_forecast,
            samples=samples,
            generation_timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo),
            is_fallback=is_fallback
        )
        forecasts.append(forecast.to_dataframe_row())

    df = pandas.DataFrame(forecasts)
    df['timestamp'] = pandas.to_datetime(df['timestamp'])
    df['generation_timestamp'] = pandas.to_datetime(df['generation_timestamp'])
    df['product'] = df['product'].astype(str)
    df['point_forecast'] = df['point_forecast'].astype(float)
    df['is_fallback'] = df['is_fallback'].astype(bool)
    return df


def create_mock_probabilistic_forecasts(product: str = None, start_time: datetime = None, hours: int = None, is_fallback: bool = None) -> List[ProbabilisticForecast]:
    """
    Creates a list of mock probabilistic forecasts for testing
    """
    if product is None:
        product = 'DALMP'
    if start_time is None:
        start_time = datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    if is_fallback is None:
        is_fallback = False

    forecast_list = []
    for hour in range(hours):
        forecast_time = start_time + timedelta(hours=hour)
        point_forecast = BASE_PRICE_PATTERNS[product]["base"] * BASE_PRICE_PATTERNS[product]["daily_pattern"][forecast_time.hour]
        volatility = VOLATILITY_FACTORS[product]
        samples = create_mock_price_samples(point_forecast, volatility)
        forecast = ProbabilisticForecast(
            timestamp=forecast_time,
            product=product,
            point_forecast=point_forecast,
            samples=samples,
            generation_timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo),
            is_fallback=is_fallback
        )
        forecast_list.append(forecast)
    return forecast_list


def create_mock_forecast_ensemble(product: str = None, start_time: datetime = None, hours: int = None, is_fallback: bool = None) -> ForecastEnsemble:
    """
    Creates a mock forecast ensemble for a product
    """
    if product is None:
        product = 'DALMP'
    if start_time is None:
        start_time = datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    if is_fallback is None:
        is_fallback = False

    forecasts = create_mock_probabilistic_forecasts(product, start_time, hours, is_fallback)
    end_time = start_time + timedelta(hours=hours)
    return ForecastEnsemble(
        product=product,
        start_time=start_time,
        end_time=end_time,
        forecasts=forecasts,
        is_fallback=is_fallback
    )


def create_incomplete_forecast_data(forecast_df: pandas.DataFrame = None, hours_to_remove: List[int] = None) -> pandas.DataFrame:
    """
    Creates incomplete forecast data for testing validation
    """
    if forecast_df is None:
        forecast_df = create_mock_forecast_data()
    if hours_to_remove is None:
        hours_to_remove = numpy.random.choice(range(FORECAST_HORIZON_HOURS), size=5, replace=False)

    df = forecast_df.copy()
    df = df[~df['timestamp'].dt.hour.isin(hours_to_remove)]
    return df


def create_invalid_forecast_data(forecast_df: pandas.DataFrame = None, invalid_columns: Dict = None) -> pandas.DataFrame:
    """
    Creates invalid forecast data for testing validation
    """
    if forecast_df is None:
        forecast_df = create_mock_forecast_data()
    if invalid_columns is None:
        invalid_columns = {'point_forecast': -100, 'is_fallback': 'invalid'}

    df = forecast_df.copy()
    for column, value in invalid_columns.items():
        df[column] = value
    return df


def create_inconsistent_forecast_data(forecast_df: pandas.DataFrame = None, inconsistency_params: Dict = None) -> pandas.DataFrame:
    """
    Creates inconsistent forecast data for testing validation
    """
    if forecast_df is None:
        forecast_df = create_mock_forecast_data()
    if inconsistency_params is None:
        inconsistency_params = {'price_spike_hour': 12, 'spike_factor': 2}

    df = forecast_df.copy()
    # Apply inconsistency modifications based on parameters
    return df


class MockForecastValidator:
    """Mock validator for testing forecast validation"""

    def __init__(self, validation_results: Dict = None, error: Exception = None):
        """
        Initialize the mock forecast validator with predefined validation results
        """
        self._validation_results = validation_results if validation_results is not None else {}
        self._error = error

    def validate_forecast(self, forecast: ProbabilisticForecast) -> ValidationResult:
        """
        Mock implementation of forecast validation
        """
        if self._error:
            raise self._error
        if forecast in self._validation_results:
            return self._validation_results[forecast]
        return ValidationResult(is_valid=True)

    def add_validation_result(self, forecast: ProbabilisticForecast, result: ValidationResult):
        """
        Adds a predefined validation result for a forecast
        """
        self._validation_results[forecast] = result

    def set_error(self, error: Exception):
        """
        Sets an error to be raised during validation
        """
        self._error = error

    def clear_error(self):
        """
        Clears any set error
        """
        self._error = None