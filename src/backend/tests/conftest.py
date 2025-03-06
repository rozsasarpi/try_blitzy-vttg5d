"""Defines pytest fixtures for the Electricity Market Price Forecasting System test suite.
This file provides reusable test fixtures for data sources, models, and components that can be shared across unit, integration, and end-to-end tests. The fixtures include mock data, model instances, and test utilities to facilitate comprehensive testing of the forecasting system."""

import pytest  # package_version: 7.0.0+
import pandas  # package_version: 2.0.0+
import numpy  # package_version: 1.24.0+
from datetime import datetime  # package_version: standard library
import pathlib  # package_version: standard library
import tempfile  # package_version: standard library
import os  # package_version: standard library

from .fixtures.load_forecast_fixtures import create_mock_load_forecast_data  # Create mock load forecast data for tests
from .fixtures.load_forecast_fixtures import MockLoadForecastClient  # Mock client for load forecast API testing
from .fixtures.historical_prices_fixtures import create_mock_historical_price_data  # Create mock historical price data for tests
from .fixtures.historical_prices_fixtures import MockHistoricalPriceClient  # Mock client for historical price API testing
from .fixtures.generation_forecast_fixtures import create_mock_generation_forecast_data  # Create mock generation forecast data for tests
from .fixtures.generation_forecast_fixtures import MockGenerationForecastClient  # Mock client for generation forecast API testing
from .fixtures.feature_fixtures import create_mock_feature_data  # Create mock feature data for tests
from .fixtures.feature_fixtures import create_mock_product_hour_features  # Create mock product/hour-specific features
from .fixtures.feature_fixtures import MockFeatureCreator  # Mock class for feature creation testing
from .fixtures.model_fixtures import create_mock_linear_model  # Create a mock linear model for testing
from .fixtures.model_fixtures import create_mock_model_registry  # Create a mock model registry with predefined models
from .fixtures.model_fixtures import MockModelRegistry  # Mock class for model registry testing
from .fixtures.forecast_fixtures import create_mock_probabilistic_forecast  # Create a single mock probabilistic forecast
from .fixtures.forecast_fixtures import create_mock_forecast_ensemble  # Create a mock forecast ensemble for a product
from .fixtures.forecast_fixtures import MockForecastValidator  # Mock validator for testing forecast validation
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid price products for validation
from src.backend.config.settings import FORECAST_HORIZON_HOURS  # Number of hours in the forecast horizon (72)
from src.backend.utils.date_utils import localize_to_cst  # Convert datetime to CST timezone

TEST_DATA_DIR = pathlib.Path(__file__).parent / 'test_data'

@pytest.fixture
def reference_datetime() -> datetime:
    """Fixture providing a reference datetime for tests"""
    return localize_to_cst(datetime(2023, 1, 1, 0, 0, 0))

@pytest.fixture
def temp_dir() -> pathlib.Path:
    """Fixture providing a temporary directory for test file operations"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield pathlib.Path(tmpdir)

@pytest.fixture
def mock_load_forecast_data(reference_datetime: datetime) -> pandas.DataFrame:
    """Fixture providing mock load forecast data"""
    return create_mock_load_forecast_data(reference_datetime)

@pytest.fixture
def mock_load_forecast_client(mock_load_forecast_data: pandas.DataFrame) -> MockLoadForecastClient:
    """Fixture providing a mock load forecast API client"""
    client = MockLoadForecastClient()
    client.add_response({}, {"data": mock_load_forecast_data.to_dict('records')})
    return client

@pytest.fixture
def mock_historical_price_data(reference_datetime: datetime) -> pandas.DataFrame:
    """Fixture providing mock historical price data"""
    return create_mock_historical_price_data(reference_datetime, reference_datetime + pandas.Timedelta(hours=FORECAST_HORIZON_HOURS))

@pytest.fixture
def mock_historical_price_client(mock_historical_price_data: pandas.DataFrame) -> MockHistoricalPriceClient:
    """Fixture providing a mock historical price API client"""
    client = MockHistoricalPriceClient()
    client.add_response({}, {"data": mock_historical_price_data.to_dict('records')})
    return client

@pytest.fixture
def mock_generation_forecast_data(reference_datetime: datetime) -> pandas.DataFrame:
    """Fixture providing mock generation forecast data"""
    return create_mock_generation_forecast_data(reference_datetime)

@pytest.fixture
def mock_generation_forecast_client(mock_generation_forecast_data: pandas.DataFrame) -> MockGenerationForecastClient:
    """Fixture providing a mock generation forecast API client"""
    client = MockGenerationForecastClient()
    client.add_response({}, {"data": mock_generation_forecast_data.to_dict('records')})
    return client

@pytest.fixture
def mock_feature_data(reference_datetime: datetime) -> pandas.DataFrame:
    """Fixture providing mock feature data"""
    return create_mock_feature_data(reference_datetime)

@pytest.fixture
def mock_product_hour_features(mock_feature_data: pandas.DataFrame) -> pandas.DataFrame:
    """Fixture providing mock product/hour-specific features"""
    return create_mock_product_hour_features()

@pytest.fixture
def mock_feature_creator(mock_feature_data: pandas.DataFrame) -> MockFeatureCreator:
    """Fixture providing a mock feature creator"""
    feature_creator = MockFeatureCreator()
    feature_creator.set_features(mock_feature_data)
    return feature_creator

@pytest.fixture
def mock_linear_model() -> LinearRegression:
    """Fixture providing a mock linear model"""
    return create_mock_linear_model()

@pytest.fixture
def mock_model_registry(mock_linear_model: LinearRegression) -> MockModelRegistry:
    """Fixture providing a mock model registry"""
    models = {}
    for product in FORECAST_PRODUCTS:
        for hour in range(24):
            models[(product, hour)] = {"model": mock_linear_model, "feature_names": [], "metrics": {}}
    return create_mock_model_registry(models)

@pytest.fixture
def mock_probabilistic_forecast() -> ProbabilisticForecast:
    """Fixture providing a mock probabilistic forecast"""
    return create_mock_probabilistic_forecast()

@pytest.fixture
def mock_forecast_ensemble() -> ForecastEnsemble:
    """Fixture providing a mock forecast ensemble"""
    return create_mock_forecast_ensemble()

@pytest.fixture
def mock_forecast_validator() -> MockForecastValidator:
    """Fixture providing a mock forecast validator"""
    return MockForecastValidator()

@pytest.fixture
def mock_input_data(mock_load_forecast_data, mock_historical_price_data, mock_generation_forecast_data):
    """Fixture providing a complete set of mock input data"""
    return {
        'load_forecast': mock_load_forecast_data,
        'historical_prices': mock_historical_price_data,
        'generation_forecast': mock_generation_forecast_data
    }