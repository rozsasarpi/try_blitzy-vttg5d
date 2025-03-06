"""
Unit tests for the model registry module of the Electricity Market Price Forecasting System.
Tests the functionality for registering, retrieving, and managing linear models for specific product/hour combinations.
"""
import os  # Operating system interfaces for file path operations
import tempfile  # Generate temporary files and directories for testing
import pathlib  # Object-oriented filesystem path manipulation
import shutil  # High-level file operations for cleanup

import pytest  # pytest: 7.0.0+ Testing framework for writing and executing tests
import numpy  # numpy: 1.24.0+ Numerical operations for test data generation
import pandas  # pandas: 2.0.0+ Data manipulation for test data
from sklearn.linear_model import LinearRegression  # scikit-learn: 1.2.0+ Linear regression model implementation

from src.backend.forecasting_engine.model_registry import initialize_registry  # Initialize the model registry
from src.backend.forecasting_engine.model_registry import register_model  # Register a model for a product/hour combination
from src.backend.forecasting_engine.model_registry import get_model  # Get a model for a product/hour combination
from src.backend.forecasting_engine.model_registry import has_model  # Check if a model exists for a product/hour combination
from src.backend.forecasting_engine.model_registry import list_available_models  # List all available models in the registry
from src.backend.forecasting_engine.model_registry import delete_model  # Delete a model from the registry
from src.backend.forecasting_engine.model_registry import clear_registry  # Clear all models from the registry
from src.backend.forecasting_engine.model_registry import save_registry_to_disk  # Save all models in the registry to disk
from src.backend.forecasting_engine.model_registry import load_registry_from_disk  # Load all models from disk into the registry
from src.backend.forecasting_engine.model_registry import ModelRegistry  # Class for managing linear models for price forecasting
from src.backend.forecasting_engine.exceptions import ModelRegistryError  # Exception for model registry issues
from src.backend.tests.fixtures.model_fixtures import create_mock_linear_model  # Create a mock linear model for testing
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid forecast products
from src.backend.config.settings import MODEL_REGISTRY_DIR  # Directory for storing model registry files

TEST_PRODUCT = "DALMP"
TEST_HOUR = 12
TEST_FEATURE_NAMES = ['load_mw', 'wind_generation', 'solar_generation', 'thermal_generation', 'hour', 'day_of_week', 'is_weekend']
TEST_METRICS = {'rmse': 5.23, 'r2': 0.87, 'mae': 4.12}

def setup_test_registry_dir() -> pathlib.Path:
    """Creates a temporary directory for testing the model registry"""
    # Create a temporary directory using tempfile.mkdtemp()
    temp_dir = tempfile.mkdtemp()
    # Convert the path to a pathlib.Path object
    registry_dir = pathlib.Path(temp_dir)
    # Return the path
    return registry_dir

def cleanup_test_registry_dir(registry_dir: pathlib.Path) -> None:
    """Cleans up the temporary directory after tests"""
    # Check if the directory exists
    if registry_dir.exists():
        # If it exists, remove it and all its contents using shutil.rmtree()
        shutil.rmtree(registry_dir)

class TestModelRegistry:
    """Test class for the model registry module"""
    def setup_method(self, method):
        """Setup method that runs before each test"""
        # Create a temporary directory for the test registry
        self.registry_dir = setup_test_registry_dir()
        # Set self.registry_dir to the temporary directory
        os.environ['MODEL_REGISTRY_DIR'] = str(self.registry_dir)
        # Create a mock linear model for testing
        self.model = create_mock_linear_model()
        # Set self.model to the mock model
        self.feature_names = TEST_FEATURE_NAMES
        # Set self.feature_names to TEST_FEATURE_NAMES
        self.metrics = TEST_METRICS
        # Set self.metrics to TEST_METRICS
        initialize_registry()
        # Reset any global state in the model_registry module

    def teardown_method(self, method):
        """Teardown method that runs after each test"""
        # Clean up the temporary directory
        cleanup_test_registry_dir(self.registry_dir)
        # Reset any global state in the model_registry module
        initialize_registry()

    def test_initialize_registry(self):
        """Test initializing the model registry"""
        # Call initialize_registry()
        registry = initialize_registry()
        # Assert that the returned value is a dictionary
        assert isinstance(registry, dict)
        # Assert that the dictionary is empty (no models yet)
        assert not registry

    def test_register_model(self):
        """Test registering a model in the registry"""
        # Call register_model with test product, hour, model, feature_names, and metrics
        result = register_model(TEST_PRODUCT, TEST_HOUR, self.model, self.feature_names, self.metrics)
        # Assert that the function returns True
        assert result is True
        # Check that the model file exists on disk
        model_path = self.registry_dir / f"{TEST_PRODUCT}_{TEST_HOUR}.joblib"
        assert model_path.exists()
        # Verify that has_model returns True for the registered model
        assert has_model(TEST_PRODUCT, TEST_HOUR) is True

    def test_register_model_invalid_product(self):
        """Test registering a model with an invalid product"""
        # With pytest.raises(ModelRegistryError):
        with pytest.raises(ModelRegistryError):
            # Call register_model with an invalid product
            register_model("InvalidProduct", TEST_HOUR, self.model, self.feature_names, self.metrics)
        # Verify that has_model returns False for the invalid product
        assert has_model("InvalidProduct", TEST_HOUR) is False

    def test_register_model_invalid_hour(self):
        """Test registering a model with an invalid hour"""
        # With pytest.raises(ModelRegistryError):
        with pytest.raises(ModelRegistryError):
            # Call register_model with a valid product but invalid hour (e.g., 24)
            register_model(TEST_PRODUCT, 24, self.model, self.feature_names, self.metrics)
        # Verify that has_model returns False for the invalid hour
        assert has_model(TEST_PRODUCT, 24) is False

    def test_get_model(self):
        """Test retrieving a model from the registry"""
        # Register a test model
        register_model(TEST_PRODUCT, TEST_HOUR, self.model, self.feature_names, self.metrics)
        # Call get_model with the same product and hour
        model, feature_names, metrics = get_model(TEST_PRODUCT, TEST_HOUR)
        # Assert that the returned model, feature_names, and metrics match the registered ones
        assert model == self.model
        assert feature_names == self.feature_names
        assert metrics == self.metrics

    def test_get_model_not_found(self):
        """Test retrieving a non-existent model"""
        # Call get_model with a product and hour that hasn't been registered
        model, feature_names, metrics = get_model("NonExistentProduct", 1)
        # Assert that the returned values are (None, None, None)
        assert model is None
        assert feature_names is None
        assert metrics is None

    def test_has_model(self):
        """Test checking if a model exists in the registry"""
        # Register a test model
        register_model(TEST_PRODUCT, TEST_HOUR, self.model, self.feature_names, self.metrics)
        # Assert that has_model returns True for the registered model
        assert has_model(TEST_PRODUCT, TEST_HOUR) is True
        # Assert that has_model returns False for an unregistered model
        assert has_model("NonExistentProduct", 1) is False

    def test_list_available_models(self):
        """Test listing all available models in the registry"""
        # Register multiple test models with different product/hour combinations
        register_model("DALMP", 1, self.model, self.feature_names, self.metrics)
        register_model("RTLMP", 2, self.model, self.feature_names, self.metrics)
        # Call list_available_models()
        available_models = list_available_models()
        # Assert that the returned list contains all registered product/hour combinations
        assert ("DALMP", 1) in available_models
        assert ("RTLMP", 2) in available_models

    def test_delete_model(self):
        """Test deleting a model from the registry"""
        # Register a test model
        register_model(TEST_PRODUCT, TEST_HOUR, self.model, self.feature_names, self.metrics)
        # Verify that has_model returns True for the registered model
        assert has_model(TEST_PRODUCT, TEST_HOUR) is True
        # Call delete_model with the same product and hour
        result = delete_model(TEST_PRODUCT, TEST_HOUR)
        # Assert that the function returns True
        assert result is True
        # Verify that has_model now returns False for the deleted model
        assert has_model(TEST_PRODUCT, TEST_HOUR) is False
        # Check that the model file no longer exists on disk
        model_path = self.registry_dir / f"{TEST_PRODUCT}_{TEST_HOUR}.joblib"
        assert not model_path.exists()

    def test_delete_model_not_found(self):
        """Test deleting a non-existent model"""
        # Call delete_model with a product and hour that hasn't been registered
        result = delete_model("NonExistentProduct", 1)
        # Assert that the function returns False
        assert result is False

    def test_clear_registry(self):
        """Test clearing all models from the registry"""
        # Register multiple test models
        register_model("DALMP", 1, self.model, self.feature_names, self.metrics)
        register_model("RTLMP", 2, self.model, self.feature_names, self.metrics)
        # Verify that list_available_models returns a non-empty list
        assert len(list_available_models()) > 0
        # Call clear_registry()
        cleared_count = clear_registry()
        # Assert that the function returns the correct number of cleared models
        assert cleared_count == 2
        # Verify that list_available_models now returns an empty list
        assert len(list_available_models()) == 0

    def test_save_and_load_registry(self):
        """Test saving and loading the registry to/from disk"""
        # Register multiple test models
        register_model("DALMP", 1, self.model, self.feature_names, self.metrics)
        register_model("RTLMP", 2, self.model, self.feature_names, self.metrics)
        # Call save_registry_to_disk()
        save_registry_to_disk()
        # Clear the registry using clear_registry()
        clear_registry()
        # Verify that list_available_models returns an empty list
        assert len(list_available_models()) == 0
        # Call load_registry_from_disk()
        load_registry_from_disk()
        # Verify that all previously registered models are now available again
        assert ("DALMP", 1) in list_available_models()
        assert ("RTLMP", 2) in list_available_models()
        # Check that get_model returns the correct models, feature_names, and metrics
        model, feature_names, metrics = get_model("DALMP", 1)
        assert model == self.model
        assert feature_names == self.feature_names
        assert metrics == self.metrics

    def test_model_registry_class(self):
        """Test the ModelRegistry class directly"""
        # Create a ModelRegistry instance with the test registry directory
        registry = ModelRegistry(registry_dir=str(self.registry_dir))
        # Register a model using the register method
        registry.register(TEST_PRODUCT, TEST_HOUR, self.model, self.feature_names, self.metrics)
        # Verify that has_model returns True for the registered model
        assert registry.has_model(TEST_PRODUCT, TEST_HOUR) is True
        # Retrieve the model using the get method
        model, feature_names, metrics = registry.get(TEST_PRODUCT, TEST_HOUR)
        # Assert that the returned model, feature_names, and metrics match the registered ones
        assert model == self.model
        assert feature_names == self.feature_names
        assert metrics == self.metrics
        # List all models using the list_models method
        available_models = registry.list_models()
        # Assert that the list contains the registered product/hour combination
        assert (TEST_PRODUCT, TEST_HOUR) in available_models
        # Delete the model using the delete method
        registry.delete(TEST_PRODUCT, TEST_HOUR)
        # Verify that has_model now returns False for the deleted model
        assert registry.has_model(TEST_PRODUCT, TEST_HOUR) is False

    def test_model_registry_save_load(self):
        """Test saving and loading using the ModelRegistry class"""
        # Create a ModelRegistry instance
        registry = ModelRegistry(registry_dir=str(self.registry_dir))
        # Register multiple models
        registry.register("DALMP", 1, self.model, self.feature_names, self.metrics)
        registry.register("RTLMP", 2, self.model, self.feature_names, self.metrics)
        # Save all models using the save_all method
        registry.save_all()
        # Create a new ModelRegistry instance
        new_registry = ModelRegistry(registry_dir=str(self.registry_dir))
        # Load all models using the load_all method
        new_registry.load_all()
        # Verify that all previously registered models are available in the new instance
        assert new_registry.has_model("DALMP", 1) is True
        assert new_registry.has_model("RTLMP", 2) is True