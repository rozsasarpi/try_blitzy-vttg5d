"""
Integration tests for the feature-to-forecast pipeline in the Electricity Market Price Forecasting System.
This module tests the end-to-end flow from engineered features to probabilistic forecasts, ensuring that the
feature vectors can be properly consumed by the forecasting engine to produce valid probabilistic forecasts.
"""

import pandas  # package_version: 2.0.0+
import numpy  # package_version: 1.24.0+
from datetime import datetime  # package_version: standard library
from unittest import mock  # package_version: standard library
import pytest  # package_version: 7.0.0+

from src.backend.tests.fixtures.feature_fixtures import create_mock_product_hour_features  # Create mock product/hour-specific features for tests
from src.backend.tests.fixtures.feature_fixtures import create_mock_feature_data  # Create mock feature data for tests
from src.backend.tests.fixtures.feature_fixtures import create_incomplete_feature_data  # Create incomplete feature data for testing validation
from src.backend.tests.fixtures.feature_fixtures import create_invalid_feature_data  # Create invalid feature data for testing validation
from src.backend.tests.fixtures.feature_fixtures import MockFeatureCreator  # Mock class for feature creation testing
from src.backend.tests.fixtures.forecast_fixtures import create_mock_probabilistic_forecast  # Create a single mock probabilistic forecast
from src.backend.tests.fixtures.forecast_fixtures import MockForecastValidator  # Mock validator for testing forecast validation
from src.backend.tests.fixtures.forecast_fixtures import SAMPLE_PRODUCTS  # Sample list of products for testing
from src.backend.forecasting_engine.probabilistic_forecaster import generate_probabilistic_forecast  # Main function to generate a probabilistic forecast
from src.backend.forecasting_engine.probabilistic_forecaster import generate_forecast_ensemble  # Generate a complete ensemble of forecasts for a product
from src.backend.forecasting_engine.probabilistic_forecaster import ProbabilisticForecaster  # Class for generating probabilistic forecasts with caching capability
from src.backend.forecasting_engine.exceptions import ForecastGenerationError  # Exception for overall forecast generation failures
from src.backend.forecasting_engine.exceptions import ModelExecutionError  # Exception for model execution failures
from src.backend.forecasting_engine.model_selector import select_model_for_product_hour  # Select appropriate model for a product/hour combination
from src.backend.forecasting_engine.model_registry import initialize_registry  # Initialize the model registry
from src.backend.forecasting_engine.model_registry import register_model  # Register a model for a product/hour combination
from src.backend.forecasting_engine.linear_model import create_linear_model  # Create a new linear model instance
from src.backend.models.forecast_models import ProbabilisticForecast  # Model class for probabilistic forecasts
from src.backend.models.forecast_models import ForecastEnsemble  # Class representing an ensemble of forecasts for the same product
from src.backend.feature_engineering import create_feature_pipeline  # Function to create a complete feature engineering pipeline
from src.backend.feature_engineering.exceptions import FeatureEngineeringError  # Base exception for feature engineering errors
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid price products for validation
from src.backend.utils.date_utils import localize_to_cst  # Convert datetime to CST timezone

TEST_PRODUCTS = ['DALMP', 'RTLMP', 'RegUp']
TEST_HOURS = [0, 6, 12, 18]

def setup_model_registry():
    """Sets up the model registry with test models for integration testing"""
    # Initialize the model registry
    initialize_registry()

    # Create linear models for each test product and hour combination
    for product in TEST_PRODUCTS:
        for hour in TEST_HOURS:
            model = create_linear_model()
            feature_names = ['load_mw', 'hour']  # Example feature names
            metrics = {'rmse': 10.0, 'r2': 0.8}  # Example metrics
            register_model(product, hour, model, feature_names, metrics)

    # Return None as function performs setup only
    return None

def create_test_historical_data(product: str) -> dict:
    """Creates test historical data for uncertainty estimation"""
    # Create a dictionary with historical residuals for the product
    historical_data = {
        f"{product}_12": {
            "residuals": numpy.random.normal(0, 5, 100).tolist(),
            "percentage_errors": numpy.random.normal(0, 0.1, 100).tolist()
        }
    }
    # Add historical point forecasts and actual values
    historical_data[f"{product}_12"]['point_forecasts'] = numpy.random.rand(100).tolist()
    historical_data[f"{product}_12"]['actual_values'] = numpy.random.rand(100).tolist()

    # Return the historical data dictionary
    return historical_data

@pytest.mark.integration
def test_feature_to_forecast_basic_flow():
    """Tests the basic flow from features to forecast for a single product and hour"""
    # Set up the model registry with test models
    setup_model_registry()

    # Create mock product/hour features for 'DALMP' and hour 12
    features = create_mock_product_hour_features(product='DALMP', hour=12)

    # Create test historical data for uncertainty estimation
    historical_data = create_test_historical_data(product='DALMP')

    # Generate a probabilistic forecast using the features
    forecast = generate_probabilistic_forecast(
        product='DALMP',
        hour=12,
        features=features,
        historical_data=historical_data,
        timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    )

    # Validate that the forecast is a valid ProbabilisticForecast instance
    assert isinstance(forecast, ProbabilisticForecast)

    # Validate that the forecast has the correct product and timestamp
    assert forecast.product == 'DALMP'
    assert forecast.timestamp.hour == 12

    # Validate that the forecast contains the expected number of samples
    assert len(forecast.samples) == PROBABILISTIC_SAMPLE_COUNT

    # Validate that the forecast passes its own validation method
    assert forecast.validate().is_valid

@pytest.mark.integration
@pytest.mark.parametrize('product', TEST_PRODUCTS)
def test_feature_to_forecast_all_products(product):
    """Tests the feature to forecast flow for all test products"""
    # Set up the model registry with test models
    setup_model_registry()

    # Create mock product/hour features for the specified product and hour 12
    features = create_mock_product_hour_features(product=product, hour=12)

    # Create test historical data for uncertainty estimation
    historical_data = create_test_historical_data(product=product)

    # Generate a probabilistic forecast using the features
    forecast = generate_probabilistic_forecast(
        product=product,
        hour=12,
        features=features,
        historical_data=historical_data,
        timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    )

    # Validate that the forecast is a valid ProbabilisticForecast instance
    assert isinstance(forecast, ProbabilisticForecast)

    # Validate that the forecast has the correct product and timestamp
    assert forecast.product == product
    assert forecast.timestamp.hour == 12

    # Validate that the forecast contains the expected number of samples
    assert len(forecast.samples) == PROBABILISTIC_SAMPLE_COUNT

    # Validate that the forecast passes its own validation method
    assert forecast.validate().is_valid

@pytest.mark.integration
@pytest.mark.parametrize('hour', TEST_HOURS)
def test_feature_to_forecast_all_hours(hour):
    """Tests the feature to forecast flow for all test hours"""
    # Set up the model registry with test models
    setup_model_registry()

    # Create mock product/hour features for 'DALMP' and the specified hour
    features = create_mock_product_hour_features(product='DALMP', hour=hour)

    # Create test historical data for uncertainty estimation
    historical_data = create_test_historical_data(product='DALMP')

    # Generate a probabilistic forecast using the features
    forecast = generate_probabilistic_forecast(
        product='DALMP',
        hour=hour,
        features=features,
        historical_data=historical_data,
        timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    )

    # Validate that the forecast is a valid ProbabilisticForecast instance
    assert isinstance(forecast, ProbabilisticForecast)

    # Validate that the forecast has the correct product and timestamp
    assert forecast.product == 'DALMP'
    assert forecast.timestamp.hour == hour

    # Validate that the forecast contains the expected number of samples
    assert len(forecast.samples) == PROBABILISTIC_SAMPLE_COUNT

    # Validate that the forecast passes its own validation method
    assert forecast.validate().is_valid

@pytest.mark.integration
def test_feature_to_forecast_ensemble():
    """Tests generating a complete forecast ensemble from features"""
    # Set up the model registry with test models
    setup_model_registry()

    # Create mock feature data for a 72-hour period
    start_time = datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    features = create_mock_feature_data(start_time=start_time)

    # Create test historical data for uncertainty estimation
    historical_data = create_test_historical_data(product='DALMP')

    # Generate a forecast ensemble using the features
    ensemble = generate_forecast_ensemble(
        product='DALMP',
        features=features,
        historical_data=historical_data,
        start_time=start_time
    )

    # Validate that the ensemble is a valid ForecastEnsemble instance
    assert isinstance(ensemble, ForecastEnsemble)

    # Validate that the ensemble has the correct product
    assert ensemble.product == 'DALMP'

    # Validate that the ensemble contains forecasts for all hours in the horizon
    assert len(ensemble.forecasts) == FORECAST_HORIZON_HOURS

    # Validate that the ensemble passes its own validation method
    assert ensemble.validate().is_valid

@pytest.mark.integration
def test_feature_to_forecast_with_forecaster_class():
    """Tests using the ProbabilisticForecaster class for forecast generation"""
    # Set up the model registry with test models
    setup_model_registry()

    # Create mock product/hour features for 'DALMP' and hour 12
    features = create_mock_product_hour_features(product='DALMP', hour=12)

    # Create test historical data for uncertainty estimation
    historical_data = create_test_historical_data(product='DALMP')

    # Create a ProbabilisticForecaster instance
    forecaster = ProbabilisticForecaster()

    # Generate a forecast using the forecaster's generate_forecast method
    forecast = forecaster.generate_forecast(
        product='DALMP',
        hour=12,
        features=features,
        historical_data=historical_data,
        timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    )

    # Validate that the forecast is a valid ProbabilisticForecast instance
    assert isinstance(forecast, ProbabilisticForecast)

    # Generate an ensemble using the forecaster's generate_ensemble method
    ensemble = forecaster.generate_ensemble(
        product='DALMP',
        features=features,
        historical_data=historical_data,
        start_time=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    )

    # Validate that the ensemble is a valid ForecastEnsemble instance
    assert isinstance(ensemble, ForecastEnsemble)

    # Test the forecaster's caching capability by generating the same forecast twice
    forecast2 = forecaster.generate_forecast(
        product='DALMP',
        hour=12,
        features=features,
        historical_data=historical_data,
        timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    )
    assert forecast is forecast2  # Should return the same object from cache

@pytest.mark.integration
def test_feature_to_forecast_with_incomplete_features():
    """Tests forecast generation with incomplete feature data"""
    # Set up the model registry with test models
    setup_model_registry()

    # Create mock product/hour features for 'DALMP' and hour 12
    features = create_mock_product_hour_features(product='DALMP', hour=12)

    # Create incomplete feature data by removing essential columns
    incomplete_features = create_incomplete_feature_data(features, columns_to_remove=['load_mw', 'hour'])

    # Create test historical data for uncertainty estimation
    historical_data = create_test_historical_data(product='DALMP')

    # Attempt to generate a forecast with incomplete features
    with pytest.raises((ForecastGenerationError, ModelExecutionError)) as exc_info:
        generate_probabilistic_forecast(
            product='DALMP',
            hour=12,
            features=incomplete_features,
            historical_data=historical_data,
            timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
        )

    # Verify that appropriate exception is raised (ForecastGenerationError or ModelExecutionError)
    assert isinstance(exc_info.value, (ForecastGenerationError, ModelExecutionError))

    # Verify that the exception message indicates the missing features
    assert "Missing required columns" in str(exc_info.value) or "not in list" in str(exc_info.value)

@pytest.mark.integration
def test_feature_to_forecast_with_invalid_features():
    """Tests forecast generation with invalid feature data"""
    # Set up the model registry with test models
    setup_model_registry()

    # Create mock product/hour features for 'DALMP' and hour 12
    features = create_mock_product_hour_features(product='DALMP', hour=12)

    # Create invalid feature data by setting invalid values for essential columns
    invalid_features = create_invalid_feature_data(features, invalid_columns={'load_mw': -100, 'hour': 'invalid'})

    # Create test historical data for uncertainty estimation
    historical_data = create_test_historical_data(product='DALMP')

    # Attempt to generate a forecast with invalid features
    with pytest.raises((ForecastGenerationError, ModelExecutionError)) as exc_info:
        generate_probabilistic_forecast(
            product='DALMP',
            hour=12,
            features=invalid_features,
            historical_data=historical_data,
            timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
        )

    # Verify that appropriate exception is raised (ForecastGenerationError or ModelExecutionError)
    assert isinstance(exc_info.value, (ForecastGenerationError, ModelExecutionError))

    # Verify that the exception message indicates the invalid features
    assert "must be a pandas DataFrame" in str(exc_info.value) or "could not convert string to float" in str(exc_info.value)

@pytest.mark.integration
def test_feature_to_forecast_with_feature_pipeline():
    """Tests the end-to-end flow from raw data through feature pipeline to forecast"""
    # Set up the model registry with test models
    setup_model_registry()

    # Create mock input data for the feature pipeline
    start_time = datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
    input_data = create_mock_feature_data(start_time=start_time)

    # Process the data through the feature pipeline using create_feature_pipeline
    processed_features = create_feature_pipeline(
        input_data=input_data,
        start_date=start_time,
        product='DALMP',
        hour=12
    )

    # Create test historical data for uncertainty estimation
    historical_data = create_test_historical_data(product='DALMP')

    # Generate a probabilistic forecast using the processed features
    forecast = generate_probabilistic_forecast(
        product='DALMP',
        hour=12,
        features=processed_features,
        historical_data=historical_data,
        timestamp=start_time
    )

    # Validate that the forecast is a valid ProbabilisticForecast instance
    assert isinstance(forecast, ProbabilisticForecast)

    # Validate that the forecast passes its own validation method
    assert forecast.validate().is_valid

@pytest.mark.integration
def test_feature_to_forecast_with_mocked_model_selector():
    """Tests forecast generation with a mocked model selector"""
    # Create mock product/hour features for 'DALMP' and hour 12
    features = create_mock_product_hour_features(product='DALMP', hour=12)

    # Create a mock model using create_linear_model
    mock_model = create_linear_model()

    # Mock the select_model_for_product_hour function to return the mock model
    with mock.patch('src.backend.forecasting_engine.probabilistic_forecaster.select_model_for_product_hour') as mock_selector:
        mock_selector.return_value = (mock_model, ['load_mw', 'hour'], {'rmse': 10.0})

        # Create test historical data for uncertainty estimation
        historical_data = create_test_historical_data(product='DALMP')

        # Generate a probabilistic forecast using the features and mocked model selector
        forecast = generate_probabilistic_forecast(
            product='DALMP',
            hour=12,
            features=features,
            historical_data=historical_data,
            timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
        )

        # Verify that the mock model was used by checking the call count
        assert mock_selector.call_count == 1

        # Validate that the forecast is a valid ProbabilisticForecast instance
        assert isinstance(forecast, ProbabilisticForecast)

@pytest.mark.integration
def test_feature_to_forecast_error_handling():
    """Tests error handling in the feature to forecast flow"""
    # Set up the model registry with test models
    setup_model_registry()

    # Create mock product/hour features for 'DALMP' and hour 12
    features = create_mock_product_hour_features(product='DALMP', hour=12)

    # Create test historical data for uncertainty estimation
    historical_data = create_test_historical_data(product='DALMP')

    # Mock various components to raise exceptions at different stages
    with mock.patch('src.backend.forecasting_engine.probabilistic_forecaster.select_model_for_product_hour') as mock_selector, \
         mock.patch('src.backend.forecasting_engine.probabilistic_forecaster.execute_linear_model') as mock_execute, \
         mock.patch('src.backend.forecasting_engine.probabilistic_forecaster.estimate_uncertainty') as mock_uncertainty, \
         mock.patch('src.backend.forecasting_engine.probabilistic_forecaster.generate_samples') as mock_samples:

        # Set up mock side effects
        mock_selector.side_effect = ValueError("Model selection failed")
        mock_execute.side_effect = ValueError("Model execution failed")
        mock_uncertainty.side_effect = ValueError("Uncertainty estimation failed")
        mock_samples.side_effect = ValueError("Sample generation failed")

        # Attempt to generate a forecast and verify appropriate exceptions are raised
        with pytest.raises(ForecastGenerationError) as exc_info:
            generate_probabilistic_forecast(
                product='DALMP',
                hour=12,
                features=features,
                historical_data=historical_data,
                timestamp=datetime.now(tz=localize_to_cst(datetime.now()).tzinfo)
            )

        # Verify that exceptions are properly wrapped in ForecastGenerationError
        assert isinstance(exc_info.value, ForecastGenerationError)
        assert "Failed to generate forecast" in str(exc_info.value)