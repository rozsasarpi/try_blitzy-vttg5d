"""Unit tests for the linear model functionality in the forecasting engine.
Tests the creation, training, execution, and evaluation of linear models, as well as the LinearModelExecutor class with its caching capabilities.
Ensures that the linear models correctly handle various input scenarios, error conditions, and produce expected outputs.
"""

import pytest  # pytest: 7.0.0+
import numpy  # numpy: 1.24.0+
import pandas  # pandas: 2.0.0+
from sklearn.linear_model import LinearRegression  # scikit-learn: 1.2.0+
from unittest import mock  # standard library

# Internal imports
from src.backend.forecasting_engine.linear_model import create_linear_model  # Function to create linear models for testing
from src.backend.forecasting_engine.linear_model import train_linear_model  # Function to train linear models for testing
from src.backend.forecasting_engine.linear_model import execute_linear_model  # Function to execute linear models for testing
from src.backend.forecasting_engine.linear_model import get_model_coefficients  # Function to extract model coefficients for testing
from src.backend.forecasting_engine.linear_model import evaluate_model  # Function to evaluate model performance for testing
from src.backend.forecasting_engine.linear_model import validate_features  # Function to validate feature inputs for testing
from src.backend.forecasting_engine.linear_model import LinearModelExecutor  # Class for executing linear models with caching capability
from src.backend.forecasting_engine.exceptions import ModelExecutionError  # Exception for model execution failures
from src.backend.forecasting_engine.exceptions import InvalidFeatureError  # Exception for invalid feature inputs
from src.backend.tests.fixtures.feature_fixtures import create_mock_feature_data  # Create mock feature data for tests
from src.backend.tests.fixtures.feature_fixtures import create_mock_product_hour_features  # Create mock product/hour-specific features
from src.backend.tests.fixtures.feature_fixtures import create_incomplete_feature_data  # Create incomplete feature data for testing validation
from src.backend.tests.fixtures.feature_fixtures import create_invalid_feature_data  # Create invalid feature data for testing validation
from src.backend.tests.fixtures.model_fixtures import create_mock_linear_model  # Create a mock linear model for testing
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid price products for testing


def test_create_linear_model_default_params():
    """Tests that create_linear_model creates a model with default parameters when none are provided"""
    # Call create_linear_model with no parameters
    model = create_linear_model()

    # Assert that the returned model is a LinearRegression instance
    assert isinstance(model, LinearRegression)

    # Assert that the model has default parameters (fit_intercept=True, copy_X=True)
    assert model.fit_intercept is True
    assert model.copy_X is True


def test_create_linear_model_custom_params():
    """Tests that create_linear_model creates a model with custom parameters when provided"""
    # Define custom model parameters (fit_intercept=False, n_jobs=2)
    custom_params = {"fit_intercept": False, "n_jobs": 2}

    # Call create_linear_model with custom parameters
    model = create_linear_model(custom_params)

    # Assert that the returned model is a LinearRegression instance
    assert isinstance(model, LinearRegression)

    # Assert that the model has the custom parameters
    assert model.fit_intercept is False
    # n_jobs cannot be directly accessed, but its presence implies it was set
    assert model.n_jobs == 2


def test_validate_features_valid_data():
    """Tests that validate_features returns True for valid feature data"""
    # Create mock feature data with create_mock_feature_data
    feature_data = create_mock_feature_data(start_time=datetime.datetime.now(), hours=24)

    # Define required columns list
    required_columns = ["timestamp", "load_mw", "hour"]

    # Call validate_features with valid data
    is_valid = validate_features(feature_data, required_columns)

    # Assert that the function returns True
    assert is_valid is True


def test_validate_features_missing_columns():
    """Tests that validate_features raises InvalidFeatureError when required columns are missing"""
    # Create incomplete feature data with create_incomplete_feature_data
    incomplete_data = create_incomplete_feature_data()

    # Define required columns list including missing columns
    required_columns = ["timestamp", "load_mw", "hour", "wind_generation"]

    # Use pytest.raises to assert that InvalidFeatureError is raised
    with pytest.raises(InvalidFeatureError) as exc_info:
        # Call validate_features with incomplete data inside the context manager
        validate_features(incomplete_data, required_columns)
    assert "Missing required columns" in str(exc_info.value)


def test_validate_features_invalid_data():
    """Tests that validate_features raises InvalidFeatureError when data contains invalid values"""
    # Create invalid feature data with create_invalid_feature_data
    invalid_data = create_invalid_feature_data()

    # Define required columns list
    required_columns = ["timestamp", "load_mw", "hour"]

    # Use pytest.raises to assert that InvalidFeatureError is raised
    with pytest.raises(InvalidFeatureError) as exc_info:
        # Call validate_features with invalid data inside the context manager
        validate_features(invalid_data, required_columns)
    assert "Missing values found in columns" in str(exc_info.value)


def test_train_linear_model_success():
    """Tests that train_linear_model successfully trains a model with valid data"""
    # Create mock feature data with create_mock_feature_data
    feature_data = create_mock_feature_data(start_time=datetime.datetime.now(), hours=24)

    # Create target variable (pandas Series)
    target_variable = pandas.Series(numpy.random.rand(24))

    # Create initial model with create_linear_model
    initial_model = create_linear_model()

    # Call train_linear_model with features, target, product, and hour
    trained_model = train_linear_model(initial_model, feature_data, target_variable, "DALMP", 12)

    # Assert that the returned model is trained (has coef_ attribute)
    assert hasattr(trained_model, "coef_")

    # Assert that the model coefficients have expected shape
    assert trained_model.coef_.shape == (feature_data.shape[1],)


def test_train_linear_model_invalid_features():
    """Tests that train_linear_model raises InvalidFeatureError with invalid features"""
    # Create invalid feature data with create_invalid_feature_data
    invalid_data = create_invalid_feature_data()

    # Create target variable (pandas Series)
    target_variable = pandas.Series(numpy.random.rand(24))

    # Create initial model with create_linear_model
    initial_model = create_linear_model()

    # Use pytest.raises to assert that InvalidFeatureError is raised
    with pytest.raises(InvalidFeatureError) as exc_info:
        # Call train_linear_model with invalid features inside the context manager
        train_linear_model(initial_model, invalid_data, target_variable, "DALMP", 12)
    assert "Missing values found in columns" in str(exc_info.value)


def test_execute_linear_model_success():
    """Tests that execute_linear_model returns expected forecast value"""
    # Create mock feature data with create_mock_feature_data
    feature_data = create_mock_feature_data(start_time=datetime.datetime.now(), hours=1)

    # Create mock trained model with create_mock_linear_model
    trained_model = create_mock_linear_model(coefficients=numpy.array([0.5, 0.3, 0.2]), intercept=10.0)

    # Call execute_linear_model with model, features, product, and hour
    forecast_value = execute_linear_model(trained_model, feature_data, "DALMP", 12)

    # Assert that the returned value is a float
    assert isinstance(forecast_value, float)

    # Assert that the returned value is within expected range
    assert 0 < forecast_value < 100


def test_execute_linear_model_untrained_model():
    """Tests that execute_linear_model raises ModelExecutionError with untrained model"""
    # Create mock feature data with create_mock_feature_data
    feature_data = create_mock_feature_data(start_time=datetime.datetime.now(), hours=1)

    # Create untrained model with create_linear_model
    untrained_model = create_linear_model()

    # Use pytest.raises to assert that ModelExecutionError is raised
    with pytest.raises(ModelExecutionError) as exc_info:
        # Call execute_linear_model with untrained model inside the context manager
        execute_linear_model(untrained_model, feature_data, "DALMP", 12)
    assert "Model has not been trained" in str(exc_info.value)


def test_get_model_coefficients():
    """Tests that get_model_coefficients returns correct coefficient information"""
    # Create mock trained model with create_mock_linear_model
    trained_model = create_mock_linear_model(coefficients=numpy.array([0.5, 0.3, 0.2]), intercept=10.0)

    # Define feature names list
    feature_names = ["load_mw", "wind_generation", "hour"]

    # Call get_model_coefficients with model and feature names
    coefficient_info = get_model_coefficients(trained_model, feature_names)

    # Assert that the returned value is a dictionary
    assert isinstance(coefficient_info, dict)

    # Assert that the dictionary contains expected keys (coefficients, intercept, feature_importance)
    assert "coefficients" in coefficient_info
    assert "intercept" in coefficient_info
    assert "feature_importance" in coefficient_info

    # Assert that the coefficients match the model's coefficients
    assert coefficient_info["coefficients"]["load_mw"] == 0.5
    assert coefficient_info["intercept"] == 10.0


def test_evaluate_model():
    """Tests that evaluate_model returns correct evaluation metrics"""
    # Create mock feature data with create_mock_feature_data
    feature_data = create_mock_feature_data(start_time=datetime.datetime.now(), hours=24)

    # Create target variable (pandas Series)
    target_variable = pandas.Series(numpy.random.rand(24))

    # Create mock trained model with create_mock_linear_model
    trained_model = create_mock_linear_model()

    # Call evaluate_model with model, features, and target
    evaluation_metrics = evaluate_model(trained_model, feature_data, target_variable)

    # Assert that the returned value is a dictionary
    assert isinstance(evaluation_metrics, dict)

    # Assert that the dictionary contains expected metrics (rmse, r2, mae)
    assert "rmse" in evaluation_metrics
    assert "r2" in evaluation_metrics
    assert "mae" in evaluation_metrics

    # Assert that the metric values are within expected ranges
    assert 0 <= evaluation_metrics["rmse"] <= 10
    assert -1 <= evaluation_metrics["r2"] <= 1
    assert 0 <= evaluation_metrics["mae"] <= 10


def test_linear_model_executor_register_model():
    """Tests that LinearModelExecutor correctly registers models"""
    # Create a LinearModelExecutor instance
    executor = LinearModelExecutor()

    # Create mock trained model with create_mock_linear_model
    trained_model = create_mock_linear_model()

    # Register the model for a specific product and hour
    executor.register_model("DALMP", 12, trained_model)

    # Retrieve the model using get_model
    retrieved_model = executor.get_model("DALMP", 12)

    # Assert that the retrieved model is the same as the registered model
    assert retrieved_model is trained_model


def test_linear_model_executor_execute_model():
    """Tests that LinearModelExecutor.execute_model correctly executes models"""
    # Create a LinearModelExecutor instance
    executor = LinearModelExecutor()

    # Create mock feature data with create_mock_feature_data
    feature_data = create_mock_feature_data(start_time=datetime.datetime.now(), hours=1)

    # Create mock trained model with create_mock_linear_model
    trained_model = create_mock_linear_model(coefficients=numpy.array([0.5, 0.3, 0.2]), intercept=10.0)

    # Call execute_model with model, features, product, and hour
    forecast_value = executor.execute_model(trained_model, feature_data, "DALMP", 12)

    # Assert that the returned value is a float
    assert isinstance(forecast_value, float)

    # Assert that the returned value is within expected range
    assert 0 < forecast_value < 100


def test_linear_model_executor_caching():
    """Tests that LinearModelExecutor correctly caches and retrieves results"""
    # Create a LinearModelExecutor instance
    executor = LinearModelExecutor()

    # Create mock feature data with create_mock_feature_data
    feature_data = create_mock_feature_data(start_time=datetime.datetime.now(), hours=1)

    # Create mock trained model with create_mock_linear_model
    trained_model = create_mock_linear_model(coefficients=numpy.array([0.5, 0.3, 0.2]), intercept=10.0)

    # Mock the execute_linear_model function to track calls
    with mock.patch("src.backend.forecasting_engine.linear_model.execute_linear_model") as mock_execute:
        mock_execute.return_value = 50.0  # Set a default return value

        # Call execute_model with use_cache=True
        result1 = executor.execute_model(trained_model, feature_data, "DALMP", 12, use_cache=True)

        # Assert that the result is a float
        assert isinstance(result1, float)

        # Assert that the result is within expected range
        assert 0 < result1 < 100

        # Assert that execute_linear_model was called once
        assert mock_execute.call_count == 1

        # Call execute_model again with the same parameters
        result2 = executor.execute_model(trained_model, feature_data, "DALMP", 12, use_cache=True)

        # Assert that the second result is identical to the first (cached)
        assert result2 == result1

        # Assert that execute_linear_model was still called only once (cached)
        assert mock_execute.call_count == 1

        # Call clear_cache to clear the cache
        executor.clear_cache()

        # Call execute_model again
        result3 = executor.execute_model(trained_model, feature_data, "DALMP", 12, use_cache=True)

        # Assert that the result is a float
        assert isinstance(result3, float)

        # Assert that the result is within expected range
        assert 0 < result3 < 100

        # Assert that execute_linear_model was called again (recalculated)
        assert mock_execute.call_count == 2


def test_linear_model_executor_generate_forecast():
    """Tests that LinearModelExecutor.generate_forecast correctly generates forecasts"""
    # Create a LinearModelExecutor instance
    executor = LinearModelExecutor()

    # Create mock feature data with create_mock_feature_data
    feature_data = create_mock_feature_data(start_time=datetime.datetime.now(), hours=1)

    # Create mock trained model with create_mock_linear_model
    trained_model = create_mock_linear_model(coefficients=numpy.array([0.5, 0.3, 0.2]), intercept=10.0)

    # Register the model for a specific product and hour
    executor.register_model("DALMP", 12, trained_model)

    # Call generate_forecast with product, hour, and features
    forecast_value = executor.generate_forecast("DALMP", 12, feature_data)

    # Assert that the returned value is a float
    assert isinstance(forecast_value, float)

    # Assert that the returned value is within expected range
    assert 0 < forecast_value < 100


def test_linear_model_executor_generate_forecast_invalid_product():
    """Tests that generate_forecast raises ModelExecutionError with invalid product"""
    # Create a LinearModelExecutor instance
    executor = LinearModelExecutor()

    # Create mock feature data with create_mock_feature_data
    feature_data = create_mock_feature_data(start_time=datetime.datetime.now(), hours=1)

    # Use pytest.raises to assert that ModelExecutionError is raised
    with pytest.raises(ValueError) as exc_info:
        # Call generate_forecast with invalid product inside the context manager
        executor.generate_forecast("INVALID", 12, feature_data)
    assert "Invalid product" in str(exc_info.value)


def test_linear_model_executor_generate_forecast_missing_model():
    """Tests that generate_forecast raises ModelExecutionError when model is not registered"""
    # Create a LinearModelExecutor instance
    executor = LinearModelExecutor()

    # Create mock feature data with create_mock_feature_data
    feature_data = create_mock_feature_data(start_time=datetime.datetime.now(), hours=1)

    # Use pytest.raises to assert that ModelExecutionError is raised
    with pytest.raises(ModelExecutionError) as exc_info:
        # Call generate_forecast with valid product but unregistered model inside the context manager
        executor.generate_forecast("DALMP", 12, feature_data)
    assert "No model found in cache" in str(exc_info.value)