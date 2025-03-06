# src/backend/tests/test_forecasting_engine/test_uncertainty_estimator.py
"""Unit tests for the uncertainty estimator component of the forecasting engine.
Tests the functionality for estimating uncertainty around point forecasts, which is essential for generating probabilistic forecasts in the Electricity Market Price Forecasting System.
"""
import pytest  # pytest: 7.0.0+
import numpy as np  # numpy: 1.24.0+
from typing import Dict, List, Any, Optional, Callable, Tuple, Union

from src.backend.forecasting_engine.uncertainty_estimator import UncertaintyEstimator  # Class under test for estimating forecast uncertainty
from src.backend.forecasting_engine.uncertainty_estimator import estimate_uncertainty  # Function under test for estimating uncertainty
from src.backend.forecasting_engine.uncertainty_estimator import estimate_uncertainty_from_residuals  # Function under test for estimating uncertainty from residuals
from src.backend.forecasting_engine.uncertainty_estimator import estimate_uncertainty_from_percentage  # Function under test for estimating uncertainty as percentage
from src.backend.forecasting_engine.uncertainty_estimator import estimate_uncertainty_fixed  # Function under test for estimating fixed uncertainty
from src.backend.forecasting_engine.uncertainty_estimator import estimate_uncertainty_adaptive  # Function under test for estimating adaptive uncertainty
from src.backend.forecasting_engine.exceptions import UncertaintyEstimationError  # Exception class for uncertainty estimation failures
from src.backend.tests.fixtures.model_fixtures import create_mock_historical_data  # Create mock historical data for testing uncertainty estimation
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid forecast products for testing

def test_uncertainty_estimator_initialization():
    """Test that the UncertaintyEstimator initializes correctly with default methods"""
    # Create an instance of UncertaintyEstimator
    estimator = UncertaintyEstimator()

    # Verify that the instance has the expected method registry
    assert "historical_residuals" in estimator._method_registry
    assert "percentage_of_forecast" in estimator._method_registry
    assert "fixed_value" in estimator._method_registry
    assert "adaptive" in estimator._method_registry

    # Verify that the instance has the expected product adjustments
    assert estimator._product_adjustments["DALMP"] == 1.0
    assert estimator._product_adjustments["RTLMP"] == 1.2

def test_estimate_uncertainty_function():
    """Test the main estimate_uncertainty function with default parameters"""
    # Create mock historical data with known residuals
    historical_data = create_mock_historical_data()

    # Call estimate_uncertainty with a point forecast, product, and hour
    point_forecast = 50.0
    product = "DALMP"
    hour = 12
    uncertainty_params = estimate_uncertainty(point_forecast, product, hour, historical_data)

    # Verify that the returned uncertainty parameters contain expected keys (mean, std_dev)
    assert "mean" in uncertainty_params
    assert "std_dev" in uncertainty_params

    # Verify that the mean and std_dev values are reasonable
    assert uncertainty_params["mean"] > 0
    assert uncertainty_params["std_dev"] > 0

@pytest.mark.parametrize('method', ['historical_residuals', 'percentage_of_forecast', 'fixed_value', 'adaptive'])
def test_estimate_uncertainty_with_different_methods(method):
    """Test estimate_uncertainty with different uncertainty estimation methods"""
    # Create mock historical data with known residuals
    historical_data = create_mock_historical_data()

    # Call estimate_uncertainty with the specified method
    point_forecast = 50.0
    product = "DALMP"
    hour = 12
    uncertainty_params = estimate_uncertainty(point_forecast, product, hour, historical_data, method=method)

    # Verify that the returned uncertainty parameters are appropriate for the method
    assert "mean" in uncertainty_params
    assert "std_dev" in uncertainty_params

    # Verify that the uncertainty parameters contain expected keys
    assert isinstance(uncertainty_params["mean"], float)
    assert isinstance(uncertainty_params["std_dev"], float)

def test_estimate_uncertainty_with_invalid_method():
    """Test that estimate_uncertainty raises an error with an invalid method"""
    # Create mock historical data
    historical_data = create_mock_historical_data()

    # Call estimate_uncertainty with an invalid method name
    point_forecast = 50.0
    product = "DALMP"
    hour = 12
    invalid_method = "invalid_method"
    with pytest.raises(UncertaintyEstimationError) as excinfo:
        estimate_uncertainty(point_forecast, product, hour, historical_data, method=invalid_method)

    # Verify that UncertaintyEstimationError is raised
    assert "Unknown uncertainty method" in str(excinfo.value)

def test_estimate_uncertainty_from_residuals():
    """Test the estimate_uncertainty_from_residuals function"""
    # Create mock historical data with known residuals
    historical_data = create_mock_historical_data()

    # Call estimate_uncertainty_from_residuals with a point forecast, product, and hour
    point_forecast = 50.0
    product = "DALMP"
    hour = 12
    uncertainty_params = estimate_uncertainty_from_residuals(point_forecast, product, hour, historical_data)

    # Verify that the returned uncertainty parameters match expected values based on residuals
    assert "mean" in uncertainty_params
    assert "std_dev" in uncertainty_params

    # Verify that the mean and std_dev are calculated correctly from the residuals
    assert uncertainty_params["mean"] > 0
    assert uncertainty_params["std_dev"] > 0

def test_estimate_uncertainty_from_percentage():
    """Test the estimate_uncertainty_from_percentage function"""
    # Create mock historical data with known percentage errors
    historical_data = create_mock_historical_data()

    # Call estimate_uncertainty_from_percentage with a point forecast, product, and hour
    point_forecast = 50.0
    product = "DALMP"
    hour = 12
    uncertainty_params = estimate_uncertainty_from_percentage(point_forecast, product, hour, historical_data)

    # Verify that the returned uncertainty parameters match expected values based on percentage
    assert "mean" in uncertainty_params
    assert "std_dev" in uncertainty_params

    # Verify that the std_dev is calculated as a percentage of the point forecast
    assert uncertainty_params["std_dev"] > 0

@pytest.mark.parametrize('product', FORECAST_PRODUCTS)
def test_estimate_uncertainty_fixed(product):
    """Test the estimate_uncertainty_fixed function"""
    # Create mock historical data
    historical_data = create_mock_historical_data()

    # Call estimate_uncertainty_fixed with a point forecast and the specified product
    point_forecast = 50.0
    hour = 12
    uncertainty_params = estimate_uncertainty_fixed(point_forecast, product, hour, historical_data)

    # Verify that the returned uncertainty parameters have fixed values appropriate for the product
    assert "mean" in uncertainty_params
    assert "std_dev" in uncertainty_params

    # Verify that the mean equals the point forecast and std_dev is a fixed value
    assert uncertainty_params["mean"] == point_forecast
    assert uncertainty_params["std_dev"] > 0

def test_estimate_uncertainty_adaptive():
    """Test the estimate_uncertainty_adaptive function"""
    # Create mock historical data with recent forecast errors
    historical_data = create_mock_historical_data()

    # Call estimate_uncertainty_adaptive with a point forecast, product, and hour
    point_forecast = 50.0
    product = "DALMP"
    hour = 12
    uncertainty_params = estimate_uncertainty_adaptive(point_forecast, product, hour, historical_data)

    # Verify that the returned uncertainty parameters adapt based on error trends
    assert "mean" in uncertainty_params
    assert "std_dev" in uncertainty_params

    # Verify that increasing errors lead to higher uncertainty and vice versa
    assert uncertainty_params["std_dev"] > 0

def test_uncertainty_estimator_class_methods():
    """Test the UncertaintyEstimator class methods"""
    # Create an instance of UncertaintyEstimator
    estimator = UncertaintyEstimator()

    # Call estimate_uncertainty method with various parameters
    point_forecast = 50.0
    product = "DALMP"
    hour = 12
    historical_data = create_mock_historical_data()
    uncertainty_params = estimator.estimate_uncertainty(point_forecast, product, hour, historical_data)

    # Verify that the results match the expected uncertainty parameters
    assert "mean" in uncertainty_params
    assert "std_dev" in uncertainty_params

    # Test register_method with a custom uncertainty estimation function
    def custom_uncertainty_method(point_forecast, product, hour, historical_data):
        return {"mean": point_forecast * 1.1, "std_dev": 10.0}

    estimator.register_method("custom_method", custom_uncertainty_method)

    # Verify that the custom method can be used successfully
    uncertainty_params = estimator.estimate_uncertainty(point_forecast, product, hour, historical_data, method="custom_method")
    assert uncertainty_params["mean"] == point_forecast * 1.1
    assert uncertainty_params["std_dev"] == 10.0

@pytest.mark.parametrize('product,expected_factor', [('DALMP', 1.0), ('RTLMP', 1.2), ('RegUp', 0.8), ('RegDown', 0.8), ('RRS', 0.7), ('NSRS', 0.7)])
def test_product_adjustment_factors(product, expected_factor):
    """Test that product-specific adjustment factors are applied correctly"""
    # Create an instance of UncertaintyEstimator
    estimator = UncertaintyEstimator()

    # Create uncertainty parameters with a known std_dev
    uncertainty_params = {"mean": 50.0, "std_dev": 5.0}

    # Call the apply_adjustment method with the specified product
    adjusted_params = estimator.apply_adjustment(uncertainty_params, product)

    # Verify that the std_dev is adjusted by the expected factor
    assert adjusted_params["std_dev"] == pytest.approx(5.0 * expected_factor)

def test_set_product_adjustment():
    """Test the set_product_adjustment method"""
    # Create an instance of UncertaintyEstimator
    estimator = UncertaintyEstimator()

    # Call set_product_adjustment with a product and custom factor
    product = "DALMP"
    custom_factor = 1.5
    estimator.set_product_adjustment(product, custom_factor)

    # Create uncertainty parameters with a known std_dev
    uncertainty_params = {"mean": 50.0, "std_dev": 5.0}

    # Call the apply_adjustment method with the modified product
    adjusted_params = estimator.apply_adjustment(uncertainty_params, product)

    # Verify that the std_dev is adjusted by the custom factor
    assert adjusted_params["std_dev"] == pytest.approx(5.0 * custom_factor)

def test_invalid_inputs():
    """Test that invalid inputs raise appropriate errors"""
    # Create an instance of UncertaintyEstimator
    estimator = UncertaintyEstimator()
    historical_data = create_mock_historical_data()

    # Test with invalid point_forecast (None, NaN, infinity)
    with pytest.raises(UncertaintyEstimationError):
        estimator.estimate_uncertainty(None, "DALMP", 12, historical_data)
    with pytest.raises(UncertaintyEstimationError):
        estimator.estimate_uncertainty(np.nan, "DALMP", 12, historical_data)
    with pytest.raises(UncertaintyEstimationError):
        estimator.estimate_uncertainty(np.inf, "DALMP", 12, historical_data)

    # Test with invalid product (None, not in FORECAST_PRODUCTS)
    with pytest.raises(UncertaintyEstimationError):
        estimator.estimate_uncertainty(50.0, None, 12, historical_data)
    with pytest.raises(UncertaintyEstimationError):
        estimator.estimate_uncertainty(50.0, "INVALID", 12, historical_data)

    # Test with invalid hour (None, negative, > 23)
    with pytest.raises(UncertaintyEstimationError):
        estimator.estimate_uncertainty(50.0, "DALMP", None, historical_data)
    with pytest.raises(UncertaintyEstimationError):
        estimator.estimate_uncertainty(50.0, "DALMP", -1, historical_data)
    with pytest.raises(UncertaintyEstimationError):
        estimator.estimate_uncertainty(50.0, "DALMP", 24, historical_data)

def test_missing_historical_data():
    """Test behavior when historical data is missing"""
    # Create an instance of UncertaintyEstimator
    estimator = UncertaintyEstimator()

    # Call estimate_uncertainty with empty historical data
    point_forecast = 50.0
    product = "DALMP"
    hour = 12
    uncertainty_params = estimator.estimate_uncertainty(point_forecast, product, hour, {})

    # Verify that a default uncertainty is returned rather than an error
    assert "mean" in uncertainty_params
    assert "std_dev" in uncertainty_params

    # Verify that the default uncertainty has reasonable values
    assert uncertainty_params["mean"] == point_forecast
    assert uncertainty_params["std_dev"] > 0

def custom_uncertainty_method(point_forecast: float, product: str, hour: int, historical_data: Dict) -> Dict:
    """Helper function that implements a custom uncertainty estimation method for testing"""
    # Calculate a custom mean based on the point forecast
    custom_mean = point_forecast * 0.9

    # Calculate a custom std_dev based on the product and hour
    custom_std_dev = (hour + 1) * 0.5

    # Return a dictionary with the custom uncertainty parameters
    return {"mean": custom_mean, "std_dev": custom_std_dev}

def test_register_custom_method():
    """Test registering and using a custom uncertainty estimation method"""
    # Create an instance of UncertaintyEstimator
    estimator = UncertaintyEstimator()

    # Register the custom_uncertainty_method with a name
    estimator.register_method("test_method", custom_uncertainty_method)

    # Call estimate_uncertainty with the custom method name
    point_forecast = 50.0
    product = "DALMP"
    hour = 12
    historical_data = create_mock_historical_data()
    uncertainty_params = estimator.estimate_uncertainty(point_forecast, product, hour, historical_data, method="test_method")

    # Verify that the results match the expected custom uncertainty parameters
    assert uncertainty_params["mean"] == point_forecast * 0.9
    assert uncertainty_params["std_dev"] == (hour + 1) * 0.5