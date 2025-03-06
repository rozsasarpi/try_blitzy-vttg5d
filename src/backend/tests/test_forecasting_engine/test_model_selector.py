# src/backend/tests/test_forecasting_engine/test_model_selector.py
"""
Implements unit tests for the model_selector module of the Electricity Market Price Forecasting System.
Tests the functionality for selecting appropriate linear models for specific product/hour combinations,
validating inputs, and handling error conditions.
"""

import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import numpy as np  # numpy: 1.24.0+

# Internal imports
from src.backend.forecasting_engine.model_selector import select_model_for_product_hour  # Function to select model for product/hour combination
from src.backend.forecasting_engine.model_selector import validate_product_hour  # Function to validate product and hour parameters
from src.backend.forecasting_engine.model_selector import get_model_info  # Function to get model information
from src.backend.forecasting_engine.model_selector import is_model_available  # Function to check if model is available
from src.backend.forecasting_engine.model_selector import ModelSelector  # Class for selecting appropriate models
from src.backend.forecasting_engine.exceptions import ModelSelectionError  # Exception for model selection failures
from src.backend.forecasting_engine.model_registry import has_model  # Function to check if model exists in registry
from src.backend.forecasting_engine.model_registry import get_model  # Function to get model from registry
from src.backend.tests.fixtures.model_fixtures import create_mock_linear_model  # Create mock linear model for testing
from src.backend.tests.fixtures.model_fixtures import create_mock_model_registry  # Create mock model registry with predefined models
from src.backend.tests.fixtures.model_fixtures import MockModelRegistry  # Mock class for model registry testing
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid forecast products


def test_validate_product_hour_valid_inputs():
    """Tests that validate_product_hour accepts valid product and hour inputs"""
    # Test with valid product ('DALMP') and hour (12)
    assert validate_product_hour('DALMP', 12) is True  # Assert that validate_product_hour returns True

    # Test with other valid products and hours
    assert validate_product_hour('RTLMP', 0) is True
    assert validate_product_hour('RegUp', 23) is True
    assert validate_product_hour('NSRS', 5) is True  # Assert that validate_product_hour returns True for all valid combinations


def test_validate_product_hour_invalid_product():
    """Tests that validate_product_hour rejects invalid product inputs"""
    # Test with invalid product ('INVALID') and valid hour (12)
    with pytest.raises(ModelSelectionError) as excinfo:
        validate_product_hour('INVALID', 12)  # Assert that validate_product_hour raises ModelSelectionError

    assert "Invalid product" in str(excinfo.value)  # Verify error message mentions invalid product


def test_validate_product_hour_invalid_hour():
    """Tests that validate_product_hour rejects invalid hour inputs"""
    # Test with valid product ('DALMP') and invalid hour (24)
    with pytest.raises(ModelSelectionError) as excinfo:
        validate_product_hour('DALMP', 24)  # Assert that validate_product_hour raises ModelSelectionError

    assert "Invalid hour" in str(excinfo.value)  # Verify error message mentions invalid hour

    # Test with valid product ('DALMP') and negative hour (-1)
    with pytest.raises(ModelSelectionError) as excinfo:
        validate_product_hour('DALMP', -1)  # Assert that validate_product_hour raises ModelSelectionError

    assert "Invalid hour" in str(excinfo.value)  # Verify error message mentions invalid hour


@pytest.mark.parametrize('product,hour', [('DALMP', 0), ('RTLMP', 12), ('RegUp', 23)])
def test_select_model_for_product_hour_success(product, hour):
    """Tests successful model selection for valid product/hour"""
    # Create mock model, feature_names, and metrics
    mock_model = create_mock_linear_model()
    feature_names = ['feature1', 'feature2']
    metrics = {'rmse': 1.0, 'r2': 0.8}

    # Mock has_model to return True
    with unittest.mock.patch('src.backend.forecasting_engine.model_selector.has_model', return_value=True):
        # Mock get_model to return (mock_model, feature_names, metrics)
        with unittest.mock.patch('src.backend.forecasting_engine.model_selector.get_model', return_value=(mock_model, feature_names, metrics)):
            # Call select_model_for_product_hour with product and hour
            model, returned_feature_names, returned_metrics = select_model_for_product_hour(product, hour)

            # Assert that returned model, feature_names, and metrics match expected values
            assert model == mock_model
            assert returned_feature_names == feature_names
            assert returned_metrics == metrics

            # Verify has_model was called with correct product and hour
            has_model.assert_called_with(product, hour)

            # Verify get_model was called with correct product and hour
            get_model.assert_called_with(product, hour)


def test_select_model_for_product_hour_model_not_found():
    """Tests model selection when model is not found in registry"""
    # Mock has_model to return False
    with unittest.mock.patch('src.backend.forecasting_engine.model_selector.has_model', return_value=False):
        # Call select_model_for_product_hour with valid product and hour
        with pytest.raises(ModelSelectionError) as excinfo:
            select_model_for_product_hour('DALMP', 12)

        # Assert that ModelSelectionError is raised
        assert "No model found" in str(excinfo.value)  # Verify error message mentions model not found


@pytest.mark.parametrize('product,hour,error_type', [('INVALID', 12, 'product'), ('DALMP', 24, 'hour')])
def test_select_model_for_product_hour_invalid_inputs(product, hour, error_type):
    """Tests model selection with invalid product/hour inputs"""
    # Call select_model_for_product_hour with invalid product or hour
    with pytest.raises(ModelSelectionError) as excinfo:
        select_model_for_product_hour(product, hour)

    # Assert that ModelSelectionError is raised
    assert "Invalid" in str(excinfo.value)  # Verify error message mentions the specific invalid input (product or hour)


def test_get_model_info_success():
    """Tests successful retrieval of model information"""
    # Create mock model, feature_names, and metrics
    mock_model = create_mock_linear_model()
    feature_names = ['feature1', 'feature2']
    metrics = {'rmse': 1.0, 'r2': 0.8}

    # Mock select_model_for_product_hour to return (mock_model, feature_names, metrics)
    with unittest.mock.patch('src.backend.forecasting_engine.model_selector.select_model_for_product_hour', return_value=(mock_model, feature_names, metrics)):
        # Call get_model_info with valid product and hour
        info = get_model_info('DALMP', 12)

        # Assert that returned info contains expected model details
        assert info['product'] == 'DALMP'
        assert info['hour'] == 12
        assert info['feature_names'] == feature_names
        assert info['metrics'] == metrics

        # Verify info contains model_type, feature_count, metrics, and other relevant details
        select_model_for_product_hour.assert_called_with('DALMP', 12)


def test_get_model_info_model_not_found():
    """Tests model info retrieval when model is not found"""
    # Mock select_model_for_product_hour to raise ModelSelectionError
    with unittest.mock.patch('src.backend.forecasting_engine.model_selector.select_model_for_product_hour', side_effect=ModelSelectionError("No model found", "DALMP", 12)):
        # Call get_model_info with valid product and hour
        info = get_model_info('DALMP', 12)

        # Assert that None is returned (graceful failure)
        assert info is None


def test_is_model_available_true():
    """Tests is_model_available when model is available"""
    # Mock has_model to return True
    with unittest.mock.patch('src.backend.forecasting_engine.model_selector.has_model', return_value=True):
        # Call is_model_available with valid product and hour
        available = is_model_available('DALMP', 12)

        # Assert that True is returned
        assert available is True

        # Verify has_model was called with correct product and hour
        has_model.assert_called_with('DALMP', 12)


def test_is_model_available_false():
    """Tests is_model_available when model is not available"""
    # Mock has_model to return False
    with unittest.mock.patch('src.backend.forecasting_engine.model_selector.has_model', return_value=False):
        # Call is_model_available with valid product and hour
        available = is_model_available('DALMP', 12)

        # Assert that False is returned
        assert available is False

        # Verify has_model was called with correct product and hour
        has_model.assert_called_with('DALMP', 12)


@pytest.mark.parametrize('product,hour', [('INVALID', 12), ('DALMP', 24)])
def test_is_model_available_invalid_inputs(product, hour):
    """Tests is_model_available with invalid inputs"""
    # Call is_model_available with invalid product or hour
    available = is_model_available(product, hour)

    # Assert that False is returned (graceful failure)
    assert available is False


def test_model_selector_class_select_model():
    """Tests ModelSelector class select_model method"""
    # Create a ModelSelector instance
    selector = ModelSelector()

    # Mock select_model_for_product_hour to return expected values
    with unittest.mock.patch('src.backend.forecasting_engine.model_selector.select_model_for_product_hour', return_value=('model', ['feature1'], {'rmse': 1.0})):
        # Call selector.select_model with valid product and hour
        model, feature_names, metrics = selector.select_model('DALMP', 12)

        # Assert that returned values match expected values
        assert model == 'model'
        assert feature_names == ['feature1']
        assert metrics == {'rmse': 1.0}

        # Verify select_model_for_product_hour was called with correct parameters
        select_model_for_product_hour.assert_called_with('DALMP', 12)


def test_model_selector_class_get_model_info():
    """Tests ModelSelector class get_model_info method"""
    # Create a ModelSelector instance
    selector = ModelSelector()

    # Mock get_model_info function to return expected info
    with unittest.mock.patch('src.backend.forecasting_engine.model_selector.get_model_info', return_value={'product': 'DALMP', 'hour': 12, 'feature_names': ['feature1'], 'metrics': {'rmse': 1.0}}):
        # Call selector.get_model_info with valid product and hour
        info = selector.get_model_info('DALMP', 12)

        # Assert that returned info matches expected info
        assert info == {'product': 'DALMP', 'hour': 12, 'feature_names': ['feature1'], 'metrics': {'rmse': 1.0}}

        # Verify get_model_info function was called with correct parameters
        get_model_info.assert_called_with('DALMP', 12)


def test_model_selector_class_is_model_available():
    """Tests ModelSelector class is_model_available method"""
    # Create a ModelSelector instance
    selector = ModelSelector()

    # Mock is_model_available function to return True
    with unittest.mock.patch('src.backend.forecasting_engine.model_selector.is_model_available', return_value=True):
        # Call selector.is_model_available with valid product and hour
        available = selector.is_model_available('DALMP', 12)

        # Assert that True is returned
        assert available is True

        # Verify is_model_available function was called with correct parameters
        is_model_available.assert_called_with('DALMP', 12)


def test_integration_with_mock_registry():
    """Integration test with mock model registry"""
    # Create mock models for different product/hour combinations
    model_dalmp_0 = create_mock_linear_model()
    model_rtlmp_12 = create_mock_linear_model()

    # Create a mock model registry with these models
    models = {
        ('DALMP', 0): {'model': model_dalmp_0, 'feature_names': ['f1', 'f2'], 'metrics': {'rmse': 1.0}},
        ('RTLMP', 12): {'model': model_rtlmp_12, 'feature_names': ['f3', 'f4'], 'metrics': {'rmse': 2.0}}
    }
    registry = create_mock_model_registry(models)

    # Patch model_registry functions to use mock registry
    with unittest.mock.patch('src.backend.forecasting_engine.model_selector.has_model', side_effect=registry.has_model):
        with unittest.mock.patch('src.backend.forecasting_engine.model_selector.get_model', side_effect=registry.get):
            # Test select_model_for_product_hour with various products and hours
            model, _, _ = select_model_for_product_hour('DALMP', 0)
            assert model == model_dalmp_0

            model, _, _ = select_model_for_product_hour('RTLMP', 12)
            assert model == model_rtlmp_12

            # Verify correct models are returned for each product/hour
            with pytest.raises(ModelSelectionError):
                select_model_for_product_hour('RegUp', 5)

            # Test is_model_available returns correct availability status
            assert is_model_available('DALMP', 0) is True
            assert is_model_available('RTLMP', 12) is True
            assert is_model_available('RegUp', 5) is False

            # Test get_model_info returns correct model information
            info = get_model_info('DALMP', 0)
            assert info['product'] == 'DALMP'
            assert info['hour'] == 0
            assert info['feature_names'] == ['f1', 'f2']
            assert info['metrics'] == {'rmse': 1.0}