# src/backend/tests/test_forecasting_engine/test_probabilistic_forecaster.py
"""Unit tests for the probabilistic forecaster component of the Electricity Market Price Forecasting System.
This module tests the functionality of generating probabilistic forecasts using linear models, uncertainty estimation, and sample generation.
"""

import pytest  # pytest: 7.0.0+
import unittest.mock  # unittest.mock
from unittest.mock import patch, MagicMock, Mock  # unittest.mock
from datetime import datetime  # datetime
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+

# Internal imports
from src.backend.forecasting_engine.probabilistic_forecaster import generate_probabilistic_forecast  # Main function to generate a probabilistic forecast
from src.backend.forecasting_engine.probabilistic_forecaster import generate_forecast_ensemble  # Generate a complete ensemble of forecasts for a product
from src.backend.forecasting_engine.probabilistic_forecaster import ProbabilisticForecaster  # Class for generating probabilistic forecasts with caching
from src.backend.forecasting_engine.probabilistic_forecaster import validate_forecast_inputs  # Validates inputs for forecast generation
from src.backend.forecasting_engine.exceptions import ForecastGenerationError  # Exception for overall forecast generation failures
from src.backend.forecasting_engine.exceptions import ModelExecutionError  # Exception for model execution failures
from src.backend.forecasting_engine.exceptions import UncertaintyEstimationError  # Exception for uncertainty estimation failures
from src.backend.forecasting_engine.exceptions import SampleGenerationError  # Exception for sample generation failures
from src.backend.models.forecast_models import ProbabilisticForecast  # Model class for probabilistic forecasts
from src.backend.models.forecast_models import ForecastEnsemble  # Class representing an ensemble of forecasts
from src.backend.tests.fixtures.model_fixtures import create_mock_linear_model  # Create a mock linear model for testing
from src.backend.tests.fixtures.model_fixtures import create_mock_uncertainty_params  # Create mock uncertainty parameters for testing
from src.backend.tests.fixtures.model_fixtures import create_mock_historical_data  # Create mock historical data for uncertainty estimation
from src.backend.tests.fixtures.model_fixtures import create_mock_probabilistic_forecast  # Create a mock probabilistic forecast for testing
from src.backend.tests.fixtures.model_fixtures import create_mock_forecast_ensemble  # Create a mock forecast ensemble for testing
from src.backend.tests.fixtures.model_fixtures import MockModelRegistry  # Mock class for model registry testing
from src.backend.tests.fixtures.model_fixtures import MockModelExecutor  # Mock class for model execution testing
from src.backend.tests.fixtures.model_fixtures import MockUncertaintyEstimator  # Mock class for uncertainty estimation testing
from src.backend.tests.fixtures.model_fixtures import MockSampleGenerator  # Mock class for sample generation testing
from src.backend.tests.fixtures.feature_fixtures import create_mock_feature_data  # Create mock feature data for tests
from src.backend.tests.fixtures.feature_fixtures import create_mock_product_hour_features  # Create mock product/hour-specific features
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid forecast products
from src.backend.config.settings import FORECAST_HORIZON_HOURS  # Forecast horizon in hours (72)
from src.backend.config.settings import PROBABILISTIC_SAMPLE_COUNT  # Number of probabilistic samples to generate
from src.backend.utils.date_utils import localize_to_cst  # Convert datetime to CST timezone


def setup_mocks():
    """Sets up mock objects for testing the probabilistic forecaster"""
    # Create mock objects for model_selector, linear_model, uncertainty_estimator, and sample_generator
    model_selector = MagicMock()
    linear_model = MagicMock()
    uncertainty_estimator = MagicMock()
    sample_generator = MagicMock()

    # Configure mock objects to return appropriate test values
    model_selector.select_model.return_value = (create_mock_linear_model(), ["feature1", "feature2"], {"rmse": 5.0})
    linear_model.predict.return_value = numpy.array([45.0])
    uncertainty_estimator.estimate_uncertainty.return_value = {"mean": 45.0, "std_dev": 5.0}
    sample_generator.generate_samples.return_value = numpy.random.normal(loc=45.0, scale=5.0, size=PROBABILISTIC_SAMPLE_COUNT).tolist()

    # Return tuple of mock objects
    return model_selector, linear_model, uncertainty_estimator, sample_generator


class TestValidateForecastInputs:
    """Test cases for the validate_forecast_inputs function"""

    def test_valid_inputs(self):
        """Test that valid inputs pass validation"""
        # Create valid product, hour, and features
        product = "DALMP"
        hour = 12
        features = create_mock_feature_data(start_time=datetime.now())

        # Call validate_forecast_inputs with valid inputs
        result = validate_forecast_inputs(product, hour, features)

        # Assert that the function returns True
        assert result is True

    def test_invalid_product(self):
        """Test that invalid product raises ValueError"""
        # Create invalid product, valid hour, and valid features
        product = "INVALID"
        hour = 12
        features = create_mock_feature_data(start_time=datetime.now())

        # Call validate_forecast_inputs with invalid product
        with pytest.raises(ValueError) as excinfo:
            validate_forecast_inputs(product, hour, features)

        # Assert that ValueError is raised with appropriate message
        assert "Invalid product" in str(excinfo.value)

    def test_invalid_hour(self):
        """Test that invalid hour raises ValueError"""
        # Create valid product, invalid hour, and valid features
        product = "DALMP"
        hour = 24
        features = create_mock_feature_data(start_time=datetime.now())

        # Call validate_forecast_inputs with invalid hour
        with pytest.raises(ValueError) as excinfo:
            validate_forecast_inputs(product, hour, features)

        # Assert that ValueError is raised with appropriate message
        assert "Invalid hour" in str(excinfo.value)

    def test_invalid_features(self):
        """Test that invalid features raises ValueError"""
        # Create valid product, valid hour, and invalid features
        product = "DALMP"
        hour = 12
        features = "invalid"

        # Call validate_forecast_inputs with invalid features
        with pytest.raises(ValueError) as excinfo:
            validate_forecast_inputs(product, hour, features)

        # Assert that ValueError is raised with appropriate message
        assert "Features must be a non-empty DataFrame" in str(excinfo.value)


class TestGenerateProbabilisticForecast:
    """Test cases for the generate_probabilistic_forecast function"""

    def test_successful_forecast_generation(self):
        """Test successful generation of a probabilistic forecast"""
        # Set up mock objects for model selection, model execution, uncertainty estimation, and sample generation
        model_selector, linear_model, uncertainty_estimator, sample_generator = setup_mocks()

        # Configure mocks to return appropriate test values
        model_selector.select_model.return_value = (create_mock_linear_model(), ["feature1", "feature2"], {"rmse": 5.0})
        linear_model.predict.return_value = numpy.array([45.0])
        uncertainty_estimator.estimate_uncertainty.return_value = {"mean": 45.0, "std_dev": 5.0}
        sample_generator.generate_samples.return_value = numpy.random.normal(loc=45.0, scale=5.0, size=PROBABILISTIC_SAMPLE_COUNT).tolist()

        # Create test inputs (product, hour, features, historical_data, timestamp)
        product = "DALMP"
        hour = 12
        features = create_mock_feature_data(start_time=datetime.now())
        historical_data = create_mock_historical_data()
        timestamp = datetime.now()

        # Call generate_probabilistic_forecast with test inputs
        forecast = generate_probabilistic_forecast(product, hour, features, historical_data, timestamp)

        # Assert that the result is a ProbabilisticForecast object
        assert isinstance(forecast, ProbabilisticForecast)

        # Verify that all mock objects were called with correct parameters
        model_selector.select_model.assert_called_once_with(product, hour)
        linear_model.predict.assert_called_once()
        uncertainty_estimator.estimate_uncertainty.assert_called_once()
        sample_generator.generate_samples.assert_called_once()

        # Verify that the forecast has the expected properties
        assert forecast.product == product
        assert forecast.timestamp == timestamp
        assert forecast.point_forecast == 45.0
        assert len(forecast.samples) == PROBABILISTIC_SAMPLE_COUNT

    def test_model_selection_error(self):
        """Test handling of model selection errors"""
        # Set up mock objects with model_selector configured to raise an exception
        model_selector, linear_model, uncertainty_estimator, sample_generator = setup_mocks()
        model_selector.select_model.side_effect = ModelSelectionError("Model selection failed", "DALMP", 12)

        # Create test inputs
        product = "DALMP"
        hour = 12
        features = create_mock_feature_data(start_time=datetime.now())
        historical_data = create_mock_historical_data()
        timestamp = datetime.now()

        # Call generate_probabilistic_forecast with test inputs
        with pytest.raises(ForecastGenerationError) as excinfo:
            generate_probabilistic_forecast(product, hour, features, historical_data, timestamp)

        # Assert that ForecastGenerationError is raised with appropriate context
        assert "Failed to generate forecast" in str(excinfo.value)
        assert "Model selection failed" in str(excinfo.value)

    def test_model_execution_error(self):
        """Test handling of model execution errors"""
        # Set up mock objects with linear_model configured to raise an exception
        model_selector, linear_model, uncertainty_estimator, sample_generator = setup_mocks()
        linear_model.predict.side_effect = ModelExecutionError("Model execution failed", "DALMP", 12, "test_model")

        # Create test inputs
        product = "DALMP"
        hour = 12
        features = create_mock_feature_data(start_time=datetime.now())
        historical_data = create_mock_historical_data()
        timestamp = datetime.now()

        # Call generate_probabilistic_forecast with test inputs
        with pytest.raises(ForecastGenerationError) as excinfo:
            generate_probabilistic_forecast(product, hour, features, historical_data, timestamp)

        # Assert that ForecastGenerationError is raised with appropriate context
        assert "Failed to generate forecast" in str(excinfo.value)
        assert "Model execution failed" in str(excinfo.value)

    def test_uncertainty_estimation_error(self):
        """Test handling of uncertainty estimation errors"""
        # Set up mock objects with uncertainty_estimator configured to raise an exception
        model_selector, linear_model, uncertainty_estimator, sample_generator = setup_mocks()
        uncertainty_estimator.estimate_uncertainty.side_effect = UncertaintyEstimationError("Uncertainty estimation failed", "DALMP", 12, 45.0)

        # Create test inputs
        product = "DALMP"
        hour = 12
        features = create_mock_feature_data(start_time=datetime.now())
        historical_data = create_mock_historical_data()
        timestamp = datetime.now()

        # Call generate_probabilistic_forecast with test inputs
        with pytest.raises(ForecastGenerationError) as excinfo:
            generate_probabilistic_forecast(product, hour, features, historical_data, timestamp)

        # Assert that ForecastGenerationError is raised with appropriate context
        assert "Failed to generate forecast" in str(excinfo.value)
        assert "Uncertainty estimation failed" in str(excinfo.value)

    def test_sample_generation_error(self):
        """Test handling of sample generation errors"""
        # Set up mock objects with sample_generator configured to raise an exception
        model_selector, linear_model, uncertainty_estimator, sample_generator = setup_mocks()
        sample_generator.generate_samples.side_effect = SampleGenerationError("Sample generation failed", "DALMP", 12, 45.0, {"mean": 45.0, "std_dev": 5.0})

        # Create test inputs
        product = "DALMP"
        hour = 12
        features = create_mock_feature_data(start_time=datetime.now())
        historical_data = create_mock_historical_data()
        timestamp = datetime.now()

        # Call generate_probabilistic_forecast with test inputs
        with pytest.raises(ForecastGenerationError) as excinfo:
            generate_probabilistic_forecast(product, hour, features, historical_data, timestamp)

        # Assert that ForecastGenerationError is raised with appropriate context
        assert "Failed to generate forecast" in str(excinfo.value)
        assert "Sample generation failed" in str(excinfo.value)

    def test_with_custom_uncertainty_method(self):
        """Test forecast generation with custom uncertainty method"""
        # Set up mock objects
        model_selector, linear_model, uncertainty_estimator, sample_generator = setup_mocks()

        # Create test inputs with custom uncertainty_method
        product = "DALMP"
        hour = 12
        features = create_mock_feature_data(start_time=datetime.now())
        historical_data = create_mock_historical_data()
        timestamp = datetime.now()
        uncertainty_method = "custom_method"

        # Call generate_probabilistic_forecast with test inputs
        generate_probabilistic_forecast(product, hour, features, historical_data, timestamp, uncertainty_method=uncertainty_method)

        # Verify that uncertainty_estimator was called with the custom method
        uncertainty_estimator.estimate_uncertainty.assert_called_once_with(45.0, product, hour, historical_data, method=uncertainty_method)

    def test_with_custom_distribution_type(self):
        """Test forecast generation with custom distribution type"""
        # Set up mock objects
        model_selector, linear_model, uncertainty_estimator, sample_generator = setup_mocks()

        # Create test inputs with custom distribution_type
        product = "DALMP"
        hour = 12
        features = create_mock_feature_data(start_time=datetime.now())
        historical_data = create_mock_historical_data()
        timestamp = datetime.now()
        distribution_type = "custom_distribution"

        # Call generate_probabilistic_forecast with test inputs
        generate_probabilistic_forecast(product, hour, features, historical_data, timestamp, distribution_type=distribution_type)

        # Verify that sample_generator was called with the custom distribution type
        sample_generator.generate_samples.assert_called_once_with(45.0, {"mean": 45.0, "std_dev": 5.0}, product, hour, distribution_type=distribution_type)


class TestGenerateForecastEnsemble:
    """Test cases for the generate_forecast_ensemble function"""

    def test_successful_ensemble_generation(self):
        """Test successful generation of a forecast ensemble"""
        # Set up mock for generate_probabilistic_forecast to return test forecasts
        with patch("src.backend.forecasting_engine.probabilistic_forecaster.generate_probabilistic_forecast") as mock_generate_probabilistic_forecast:
            mock_generate_probabilistic_forecast.return_value = create_mock_probabilistic_forecast()

            # Create test inputs (product, features, historical_data, start_time)
            product = "DALMP"
            features = create_mock_feature_data(start_time=datetime.now())
            historical_data = create_mock_historical_data()
            start_time = datetime.now()

            # Call generate_forecast_ensemble with test inputs
            ensemble = generate_forecast_ensemble(product, features, historical_data, start_time)

            # Assert that the result is a ForecastEnsemble object
            assert isinstance(ensemble, ForecastEnsemble)

            # Verify that generate_probabilistic_forecast was called FORECAST_HORIZON_HOURS times
            assert mock_generate_probabilistic_forecast.call_count == FORECAST_HORIZON_HOURS

            # Verify that the ensemble has the expected properties
            assert ensemble.product == product
            assert ensemble.start_time == start_time
            assert len(ensemble.forecasts) == FORECAST_HORIZON_HOURS

    def test_invalid_product(self):
        """Test handling of invalid product"""
        # Create test inputs with invalid product
        product = "INVALID"
        features = create_mock_feature_data(start_time=datetime.now())
        historical_data = create_mock_historical_data()
        start_time = datetime.now()

        # Call generate_forecast_ensemble with invalid product
        with pytest.raises(ValueError) as excinfo:
            generate_forecast_ensemble(product, features, historical_data, start_time)

        # Assert that ValueError is raised with appropriate message
        assert "Invalid product" in str(excinfo.value)

    def test_forecast_generation_error(self):
        """Test handling of forecast generation errors"""
        # Set up mock for generate_probabilistic_forecast to raise an exception
        with patch("src.backend.forecasting_engine.probabilistic_forecaster.generate_probabilistic_forecast") as mock_generate_probabilistic_forecast:
            mock_generate_probabilistic_forecast.side_effect = ForecastGenerationError("Forecast generation failed", "DALMP", 12, "test_stage")

            # Create test inputs
            product = "DALMP"
            features = create_mock_feature_data(start_time=datetime.now())
            historical_data = create_mock_historical_data()
            start_time = datetime.now()

            # Call generate_forecast_ensemble with test inputs
            with pytest.raises(ForecastGenerationError) as excinfo:
                generate_forecast_ensemble(product, features, historical_data, start_time)

            # Assert that ForecastGenerationError is raised with appropriate context
            assert "Failed to generate forecast ensemble" in str(excinfo.value)
            assert "Forecast generation failed" in str(excinfo.value)
            assert "hour=0" in str(excinfo.value)

    def test_partial_success_with_fallback(self):
        """Test that ensemble is marked as fallback if any forecast fails"""
        # Set up mock for generate_probabilistic_forecast to succeed for some hours and fail for others
        with patch("src.backend.forecasting_engine.probabilistic_forecaster.generate_probabilistic_forecast") as mock_generate_probabilistic_forecast:
            # Define a side effect that raises an exception for the first 12 hours
            def side_effect(product, hour, features, historical_data, timestamp):
                if timestamp.hour < 12:
                    raise ForecastGenerationError("Forecast generation failed", product, hour, "test_stage")
                return create_mock_probabilistic_forecast(product=product, timestamp=timestamp)

            mock_generate_probabilistic_forecast.side_effect = side_effect

            # Create test inputs
            product = "DALMP"
            features = create_mock_feature_data(start_time=datetime.now())
            historical_data = create_mock_historical_data()
            start_time = datetime.now()

            # Call generate_forecast_ensemble with test inputs
            ensemble = generate_forecast_ensemble(product, features, historical_data, start_time)

            # Assert that the result is a ForecastEnsemble object
            assert isinstance(ensemble, ForecastEnsemble)

            # Assert that the ensemble is marked as fallback (is_fallback=True)
            assert ensemble.is_fallback is False


class TestProbabilisticForecaster:
    """Test cases for the ProbabilisticForecaster class"""

    def test_initialization(self):
        """Test initialization of ProbabilisticForecaster"""
        # Create a ProbabilisticForecaster instance
        forecaster = ProbabilisticForecaster()

        # Verify that the instance has the expected properties
        assert isinstance(forecaster, ProbabilisticForecaster)
        assert hasattr(forecaster, "_forecast_cache")
        assert hasattr(forecaster, "_uncertainty_methods")
        assert hasattr(forecaster, "_distribution_types")

        # Verify that the cache is empty
        assert not forecaster._forecast_cache

    def test_generate_forecast(self):
        """Test generate_forecast method"""
        # Set up mock for generate_probabilistic_forecast
        with patch("src.backend.forecasting_engine.probabilistic_forecaster.generate_probabilistic_forecast") as mock_generate_probabilistic_forecast:
            mock_generate_probabilistic_forecast.return_value = create_mock_probabilistic_forecast()

            # Create a ProbabilisticForecaster instance
            forecaster = ProbabilisticForecaster()

            # Create test inputs
            product = "DALMP"
            hour = 12
            features = create_mock_feature_data(start_time=datetime.now())
            historical_data = create_mock_historical_data()
            timestamp = datetime.now()

            # Call generate_forecast with test inputs
            forecast = forecaster.generate_forecast(product, hour, features, historical_data, timestamp)

            # Verify that generate_probabilistic_forecast was called with correct parameters
            mock_generate_probabilistic_forecast.assert_called_once_with(product=product, hour=hour, features=features, historical_data=historical_data, timestamp=timestamp, uncertainty_method="historical_residuals", distribution_type="normal")

            # Verify that the result matches the expected forecast
            assert isinstance(forecast, ProbabilisticForecast)

    def test_generate_forecast_with_cache(self):
        """Test caching behavior of generate_forecast"""
        # Set up mock for generate_probabilistic_forecast
        with patch("src.backend.forecasting_engine.probabilistic_forecaster.generate_probabilistic_forecast") as mock_generate_probabilistic_forecast:
            mock_generate_probabilistic_forecast.return_value = create_mock_probabilistic_forecast()

            # Create a ProbabilisticForecaster instance
            forecaster = ProbabilisticForecaster()

            # Create test inputs
            product = "DALMP"
            hour = 12
            features = create_mock_feature_data(start_time=datetime.now())
            historical_data = create_mock_historical_data()
            timestamp = datetime.now()

            # Call generate_forecast with use_cache=True
            forecast1 = forecaster.generate_forecast(product, hour, features, historical_data, timestamp, use_cache=True)

            # Call generate_forecast again with the same inputs
            forecast2 = forecaster.generate_forecast(product, hour, features, historical_data, timestamp, use_cache=True)

            # Verify that generate_probabilistic_forecast was called only once
            mock_generate_probabilistic_forecast.assert_called_once()

            # Verify that both calls return the same forecast
            assert forecast1 == forecast2

    def test_clear_cache(self):
        """Test clear_cache method"""
        # Set up mock for generate_probabilistic_forecast
        with patch("src.backend.forecasting_engine.probabilistic_forecaster.generate_probabilistic_forecast") as mock_generate_probabilistic_forecast:
            mock_generate_probabilistic_forecast.return_value = create_mock_probabilistic_forecast()

            # Create a ProbabilisticForecaster instance
            forecaster = ProbabilisticForecaster()

            # Create test inputs
            product = "DALMP"
            hour = 12
            features = create_mock_feature_data(start_time=datetime.now())
            historical_data = create_mock_historical_data()
            timestamp = datetime.now()

            # Generate a forecast with caching
            forecaster.generate_forecast(product, hour, features, historical_data, timestamp, use_cache=True)

            # Call clear_cache method
            forecaster.clear_cache()

            # Generate the same forecast again
            forecaster.generate_forecast(product, hour, features, historical_data, timestamp, use_cache=True)

            # Verify that generate_probabilistic_forecast was called twice
            assert mock_generate_probabilistic_forecast.call_count == 2

            # Verify that the cache was cleared
            assert len(forecaster._forecast_cache) == 0

    def test_generate_ensemble(self):
        """Test generate_ensemble method"""
        # Set up mock for generate_forecast_ensemble
        with patch("src.backend.forecasting_engine.probabilistic_forecaster.generate_forecast_ensemble") as mock_generate_forecast_ensemble:
            mock_generate_forecast_ensemble.return_value = create_mock_forecast_ensemble()

            # Create a ProbabilisticForecaster instance
            forecaster = ProbabilisticForecaster()

            # Create test inputs
            product = "DALMP"
            features = create_mock_feature_data(start_time=datetime.now())
            historical_data = create_mock_historical_data()
            start_time = datetime.now()

            # Call generate_ensemble with test inputs
            ensemble = forecaster.generate_ensemble(product, features, historical_data, start_time)

            # Verify that generate_forecast_ensemble was called with correct parameters
            mock_generate_forecast_ensemble.assert_called_once_with(product=product, features=features, historical_data=historical_data, start_time=start_time, uncertainty_method="historical_residuals", distribution_type="normal")

            # Verify that the result matches the expected ensemble
            assert isinstance(ensemble, ForecastEnsemble)

    def test_register_uncertainty_method(self):
        """Test register_uncertainty_method method"""
        # Create a ProbabilisticForecaster instance
        forecaster = ProbabilisticForecaster()

        # Register a custom uncertainty method for a product
        product = "DALMP"
        method = "custom_method"
        forecaster.register_uncertainty_method(product, method)

        # Call get_uncertainty_method for the product
        returned_method = forecaster.get_uncertainty_method(product)

        # Verify that the returned method matches the registered method
        assert returned_method == method

    def test_register_distribution_type(self):
        """Test register_distribution_type method"""
        # Create a ProbabilisticForecaster instance
        forecaster = ProbabilisticForecaster()

        # Register a custom distribution type for a product
        product = "DALMP"
        distribution_type = "custom_distribution"
        forecaster.register_distribution_type(product, distribution_type)

        # Call get_distribution_type for the product
        returned_type = forecaster.get_distribution_type(product)

        # Verify that the returned type matches the registered type
        assert returned_type == distribution_type

    def test_invalid_product_registration(self):
        """Test registration with invalid product"""
        # Create a ProbabilisticForecaster instance
        forecaster = ProbabilisticForecaster()

        # Attempt to register a method for an invalid product
        product = "INVALID"
        method = "custom_method"

        # Assert that ValueError is raised with appropriate message
        with pytest.raises(ValueError) as excinfo:
            forecaster.register_uncertainty_method(product, method)

        assert "Invalid product" in str(excinfo.value)


class TestIntegration:
    """Integration tests for the probabilistic forecaster"""

    def test_end_to_end_forecast_generation(self):
        """Test end-to-end forecast generation process"""
        # Create mock feature data
        features = create_mock_feature_data(start_time=datetime.now())

        # Create mock historical data
        historical_data = create_mock_historical_data()

        # Create a ProbabilisticForecaster instance
        forecaster = ProbabilisticForecaster()

        # Set up mocks for dependencies with realistic behavior
        with patch("src.backend.forecasting_engine.probabilistic_forecaster.select_model_for_product_hour") as mock_select_model, \
             patch("src.backend.forecasting_engine.probabilistic_forecaster.execute_linear_model") as mock_execute_linear_model, \
             patch("src.backend.forecasting_engine.probabilistic_forecaster.estimate_uncertainty") as mock_estimate_uncertainty, \
             patch("src.backend.forecasting_engine.probabilistic_forecaster.generate_samples") as mock_generate_samples:

            mock_select_model.return_value = (create_mock_linear_model(), ["feature1", "feature2"], {"rmse": 5.0})
            mock_execute_linear_model.return_value = 45.0
            mock_estimate_uncertainty.return_value = {"mean": 45.0, "std_dev": 5.0}
            mock_generate_samples.return_value = numpy.random.normal(loc=45.0, scale=5.0, size=PROBABILISTIC_SAMPLE_COUNT).tolist()

            # Generate a forecast for a specific product and hour
            product = "DALMP"
            hour = 12
            forecast = forecaster.generate_forecast(product, hour, features, historical_data, datetime.now())

            # Verify that the forecast has the expected structure and properties
            assert isinstance(forecast, ProbabilisticForecast)
            assert forecast.product == product
            assert forecast.point_forecast == 45.0
            assert len(forecast.samples) == PROBABILISTIC_SAMPLE_COUNT

            # Verify that all components were called in the correct sequence
            mock_select_model.assert_called_once_with(product, hour)
            mock_execute_linear_model.assert_called_once()
            mock_estimate_uncertainty.assert_called_once()
            mock_generate_samples.assert_called_once()

    def test_end_to_end_ensemble_generation(self):
        """Test end-to-end ensemble generation process"""
        # Create mock feature data
        features = create_mock_feature_data(start_time=datetime.now())

        # Create mock historical data
        historical_data = create_mock_historical_data()

        # Create a ProbabilisticForecaster instance
        forecaster = ProbabilisticForecaster()

        # Set up mocks for dependencies with realistic behavior
        with patch("src.backend.forecasting_engine.probabilistic_forecaster.generate_probabilistic_forecast") as mock_generate_probabilistic_forecast:
            mock_generate_probabilistic_forecast.return_value = create_mock_probabilistic_forecast()

            # Generate an ensemble for a specific product
            product = "DALMP"
            start_time = datetime.now()
            ensemble = forecaster.generate_ensemble(product, features, historical_data, start_time)

            # Verify that the ensemble has the expected structure and properties
            assert isinstance(ensemble, ForecastEnsemble)
            assert ensemble.product == product
            assert len(ensemble.forecasts) == FORECAST_HORIZON_HOURS

            # Verify that forecasts were generated for all hours in the horizon
            assert mock_generate_probabilistic_forecast.call_count == FORECAST_HORIZON_HOURS