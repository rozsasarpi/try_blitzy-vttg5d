"""
Implements a registry for managing linear models used in the Electricity Market Price Forecasting System.
This module provides functionality to register, retrieve, and manage models for specific product/hour combinations,
supporting the requirement for separate linear models for each unique product/hour pair.
"""

import os
import pathlib
from typing import Dict, List, Optional, Tuple, Union, Any
import pandas as pd  # version: 2.0.0+
import joblib  # version: 1.2.0+
from sklearn.linear_model import LinearRegression  # version: 1.2.0+

# Internal imports
from .exceptions import ModelRegistryError
from ..utils.logging_utils import get_logger, log_execution_time, log_exceptions
from ..utils.decorators import log_exceptions, memoize
from ..utils.file_utils import save_dataframe, load_dataframe, ensure_directory_exists
from ..config.settings import FORECAST_PRODUCTS, MODEL_REGISTRY_DIR

# Configure logger
logger = get_logger(__name__)

# Global registry cache
_registry = None

# File extension for saved models
MODEL_FILE_EXTENSION = '.joblib'


@log_exceptions
def initialize_registry():
    """
    Initializes the model registry, loading existing models if available.
    
    Returns:
        dict: Dictionary mapping product/hour combinations to models
    """
    global _registry
    
    # Return existing registry if already initialized
    if _registry is not None:
        logger.debug("Registry already initialized")
        return _registry
    
    logger.info("Initializing model registry")
    
    # Create a new empty registry
    _registry = {}
    
    # Ensure registry directory exists
    ensure_directory_exists(MODEL_REGISTRY_DIR)
    
    # Attempt to load existing registry from disk
    try:
        load_registry_from_disk()
        logger.info(f"Loaded {len(_registry)} models from registry")
    except Exception as e:
        logger.warning(f"Failed to load existing registry: {str(e)}")
        logger.info("Starting with empty registry")
    
    return _registry


@log_execution_time
@log_exceptions
def register_model(
    product: str,
    hour: int,
    model: LinearRegression,
    feature_names: List[str],
    metrics: Dict[str, float]
) -> bool:
    """
    Registers a model for a specific product/hour combination.
    
    Args:
        product: The price product identifier (e.g., DALMP, RTLMP)
        hour: The target hour (0-23)
        model: The trained LinearRegression model
        feature_names: List of feature names used by the model
        metrics: Dictionary of model performance metrics
        
    Returns:
        bool: True if registration was successful
        
    Raises:
        ModelRegistryError: If product is invalid or hour is out of range
    """
    # Validate product and hour
    _validate_product_hour(product, hour)
    
    # Validate model type
    if not isinstance(model, LinearRegression):
        raise ModelRegistryError(f"Model must be a LinearRegression instance", "register_model")
    
    # Validate feature_names
    if not isinstance(feature_names, list) or len(feature_names) == 0:
        raise ModelRegistryError(f"Feature names must be a non-empty list", "register_model")
    
    # Initialize registry if needed
    initialize_registry()
    
    # Create model entry
    model_entry = {
        "model": model,
        "feature_names": feature_names,
        "metrics": metrics,
        "created_at": pd.Timestamp.now()
    }
    
    # Add to registry with (product, hour) key
    _registry[(product, hour)] = model_entry
    
    # Save the model to disk
    model_path = _get_model_path(product, hour)
    try:
        joblib.dump(model_entry, model_path)
        logger.info(f"Saved model for {product}, hour {hour} to {model_path}")
    except Exception as e:
        logger.error(f"Failed to save model for {product}, hour {hour}: {str(e)}")
        return False
    
    logger.info(f"Registered model for {product}, hour {hour}")
    return True


@log_exceptions
@memoize
def get_model(
    product: str,
    hour: int
) -> Tuple[Optional[LinearRegression], Optional[List[str]], Optional[Dict[str, float]]]:
    """
    Gets a model for a specific product/hour combination.
    
    Args:
        product: The price product identifier (e.g., DALMP, RTLMP)
        hour: The target hour (0-23)
        
    Returns:
        Tuple of (model, feature_names, metrics) or (None, None, None) if not found
        
    Raises:
        ModelRegistryError: If product is invalid or hour is out of range
    """
    # Validate product and hour
    _validate_product_hour(product, hour)
    
    # Initialize registry if needed
    initialize_registry()
    
    # Check if model exists in registry
    if (product, hour) in _registry:
        model_entry = _registry[(product, hour)]
        logger.debug(f"Retrieved model for {product}, hour {hour} from registry")
        return model_entry["model"], model_entry["feature_names"], model_entry["metrics"]
    
    # Model not found
    logger.warning(f"No model found for {product}, hour {hour}")
    return None, None, None


@log_exceptions
def has_model(product: str, hour: int) -> bool:
    """
    Checks if a model exists for a specific product/hour combination.
    
    Args:
        product: The price product identifier (e.g., DALMP, RTLMP)
        hour: The target hour (0-23)
        
    Returns:
        bool: True if model exists, False otherwise
        
    Raises:
        ModelRegistryError: If product is invalid or hour is out of range
    """
    # Validate product and hour
    _validate_product_hour(product, hour)
    
    # Initialize registry if needed
    initialize_registry()
    
    # Check if model exists in registry
    return (product, hour) in _registry


@log_exceptions
def list_available_models() -> List[Tuple[str, int]]:
    """
    Lists all available models in the registry.
    
    Returns:
        list: List of (product, hour) tuples for available models
    """
    # Initialize registry if needed
    initialize_registry()
    
    # Return list of keys (product, hour tuples)
    return list(_registry.keys())


@log_exceptions
def delete_model(product: str, hour: int) -> bool:
    """
    Deletes a model from the registry.
    
    Args:
        product: The price product identifier (e.g., DALMP, RTLMP)
        hour: The target hour (0-23)
        
    Returns:
        bool: True if successful, False if model not found
        
    Raises:
        ModelRegistryError: If product is invalid or hour is out of range
    """
    # Validate product and hour
    _validate_product_hour(product, hour)
    
    # Initialize registry if needed
    initialize_registry()
    
    # Check if model exists in registry
    if (product, hour) not in _registry:
        logger.warning(f"Cannot delete: No model found for {product}, hour {hour}")
        return False
    
    # Remove from registry
    del _registry[(product, hour)]
    
    # Remove file from disk
    model_path = _get_model_path(product, hour)
    try:
        if os.path.exists(model_path):
            os.remove(model_path)
            logger.info(f"Deleted model file: {model_path}")
    except Exception as e:
        logger.error(f"Failed to delete model file {model_path}: {str(e)}")
        # We still return True since the model was removed from the registry
    
    logger.info(f"Deleted model for {product}, hour {hour}")
    return True


@log_exceptions
def clear_registry() -> int:
    """
    Clears all models from the registry.
    
    Returns:
        int: Number of models cleared
    """
    global _registry
    
    # Initialize registry if needed
    initialize_registry()
    
    # Count the number of models
    model_count = len(_registry)
    
    # Clear the registry
    _registry = {}
    
    logger.info(f"Cleared registry ({model_count} models removed)")
    return model_count


@log_execution_time
@log_exceptions
def save_registry_to_disk() -> int:
    """
    Saves all models in the registry to disk.
    
    Returns:
        int: Number of models saved
    """
    # Initialize registry if needed
    initialize_registry()
    
    # Ensure registry directory exists
    ensure_directory_exists(MODEL_REGISTRY_DIR)
    
    # Save each model
    saved_count = 0
    for (product, hour), model_entry in _registry.items():
        model_path = _get_model_path(product, hour)
        try:
            joblib.dump(model_entry, model_path)
            saved_count += 1
        except Exception as e:
            logger.error(f"Failed to save model for {product}, hour {hour}: {str(e)}")
    
    # Create a registry index file with metadata
    try:
        index_data = []
        for (product, hour), model_entry in _registry.items():
            index_data.append({
                "product": product,
                "hour": hour,
                "features": len(model_entry["feature_names"]),
                "created_at": model_entry.get("created_at", pd.Timestamp.now()),
                "file_path": str(_get_model_path(product, hour))
            })
        
        if index_data:
            index_df = pd.DataFrame(index_data)
            index_path = os.path.join(MODEL_REGISTRY_DIR, "registry_index.parquet")
            save_dataframe(index_df, index_path)
            logger.info(f"Saved registry index with {len(index_data)} entries")
    except Exception as e:
        logger.error(f"Failed to save registry index: {str(e)}")
    
    logger.info(f"Saved {saved_count} models to disk")
    return saved_count


@log_execution_time
@log_exceptions
def load_registry_from_disk() -> int:
    """
    Loads all models from disk into the registry.
    
    Returns:
        int: Number of models loaded
    """
    global _registry
    
    # Initialize empty registry
    _registry = {}
    
    # Check if registry directory exists
    if not os.path.exists(MODEL_REGISTRY_DIR):
        logger.warning(f"Registry directory does not exist: {MODEL_REGISTRY_DIR}")
        return 0
    
    # Try to load registry index if it exists
    index_path = os.path.join(MODEL_REGISTRY_DIR, "registry_index.parquet")
    if os.path.exists(index_path):
        try:
            index_df = load_dataframe(index_path)
            logger.info(f"Loaded registry index with {len(index_df)} entries")
        except Exception as e:
            logger.warning(f"Failed to load registry index: {str(e)}")
            index_df = None
    else:
        index_df = None
    
    # Load model files from directory
    loaded_count = 0
    
    # If we have an index, use it to load models
    if index_df is not None:
        for _, row in index_df.iterrows():
            product = row["product"]
            hour = row["hour"]
            
            # Skip if product or hour is invalid
            try:
                _validate_product_hour(product, hour)
            except ModelRegistryError:
                logger.warning(f"Skipping invalid product/hour in index: {product}, {hour}")
                continue
            
            model_path = _get_model_path(product, hour)
            if os.path.exists(model_path):
                try:
                    model_entry = joblib.load(model_path)
                    _registry[(product, hour)] = model_entry
                    loaded_count += 1
                except Exception as e:
                    logger.error(f"Failed to load model for {product}, hour {hour}: {str(e)}")
    else:
        # No index, scan directory for model files
        for file_name in os.listdir(MODEL_REGISTRY_DIR):
            if file_name.endswith(MODEL_FILE_EXTENSION) and "_" in file_name:
                try:
                    # Parse product and hour from filename
                    base_name = file_name.replace(MODEL_FILE_EXTENSION, "")
                    product, hour_str = base_name.split("_", 1)
                    hour = int(hour_str)
                    
                    # Validate product and hour
                    _validate_product_hour(product, hour)
                    
                    # Load the model
                    model_path = os.path.join(MODEL_REGISTRY_DIR, file_name)
                    model_entry = joblib.load(model_path)
                    _registry[(product, hour)] = model_entry
                    loaded_count += 1
                except Exception as e:
                    logger.error(f"Failed to load model file {file_name}: {str(e)}")
    
    logger.info(f"Loaded {loaded_count} models from disk")
    return loaded_count


def _get_model_path(product: str, hour: int) -> pathlib.Path:
    """
    Gets the file path for a model.
    
    Args:
        product: The price product identifier
        hour: The target hour
        
    Returns:
        pathlib.Path: Path to the model file
    """
    # Construct filename as product_hour.joblib
    filename = f"{product}_{hour}{MODEL_FILE_EXTENSION}"
    return pathlib.Path(MODEL_REGISTRY_DIR) / filename


def _validate_product_hour(product: str, hour: int) -> bool:
    """
    Validates product and hour parameters.
    
    Args:
        product: The price product identifier
        hour: The target hour
        
    Returns:
        bool: True if valid, raises exception if invalid
        
    Raises:
        ModelRegistryError: If product is invalid or hour is out of range
    """
    # Check product is in the list of valid products
    if product not in FORECAST_PRODUCTS:
        raise ModelRegistryError(
            f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}",
            "validate_product_hour"
        )
    
    # Check hour is between 0 and 23
    if not 0 <= hour <= 23:
        raise ModelRegistryError(
            f"Invalid hour: {hour}. Must be between 0 and 23",
            "validate_product_hour"
        )
    
    return True


class ModelRegistry:
    """
    Class for managing linear models for price forecasting.
    
    Provides an object-oriented interface for registering, retrieving, and
    managing models for specific product/hour combinations.
    """
    
    def __init__(self, registry_dir: str = MODEL_REGISTRY_DIR):
        """
        Initializes the model registry.
        
        Args:
            registry_dir: Directory to store model files
        """
        self._models = {}
        self._registry_dir = pathlib.Path(registry_dir)
        
        # Ensure registry directory exists
        ensure_directory_exists(self._registry_dir)
        
        # Attempt to load existing models
        try:
            self.load_all()
            logger.info(f"Loaded {len(self._models)} models into registry")
        except Exception as e:
            logger.warning(f"Failed to load existing models: {str(e)}")
            logger.info("Starting with empty registry")
    
    def register(
        self,
        product: str,
        hour: int,
        model: LinearRegression,
        feature_names: List[str],
        metrics: Dict[str, float]
    ) -> bool:
        """
        Registers a model for a product/hour combination.
        
        Args:
            product: The price product identifier (e.g., DALMP, RTLMP)
            hour: The target hour (0-23)
            model: The trained LinearRegression model
            feature_names: List of feature names used by the model
            metrics: Dictionary of model performance metrics
            
        Returns:
            bool: True if registration was successful
            
        Raises:
            ModelRegistryError: If product is invalid or hour is out of range
        """
        # Validate product and hour
        self._validate_product_hour(product, hour)
        
        # Validate model type
        if not isinstance(model, LinearRegression):
            raise ModelRegistryError(f"Model must be a LinearRegression instance", "register")
        
        # Validate feature_names
        if not isinstance(feature_names, list) or len(feature_names) == 0:
            raise ModelRegistryError(f"Feature names must be a non-empty list", "register")
        
        # Create model entry
        model_entry = {
            "model": model,
            "feature_names": feature_names,
            "metrics": metrics,
            "created_at": pd.Timestamp.now()
        }
        
        # Add to registry with (product, hour) key
        self._models[(product, hour)] = model_entry
        
        # Save the model to disk
        model_path = self._get_model_path(product, hour)
        try:
            joblib.dump(model_entry, model_path)
            logger.info(f"Saved model for {product}, hour {hour} to {model_path}")
        except Exception as e:
            logger.error(f"Failed to save model for {product}, hour {hour}: {str(e)}")
            return False
        
        logger.info(f"Registered model for {product}, hour {hour}")
        return True
    
    def get(
        self,
        product: str,
        hour: int
    ) -> Tuple[Optional[LinearRegression], Optional[List[str]], Optional[Dict[str, float]]]:
        """
        Gets a model for a specific product/hour combination.
        
        Args:
            product: The price product identifier (e.g., DALMP, RTLMP)
            hour: The target hour (0-23)
            
        Returns:
            Tuple of (model, feature_names, metrics) or (None, None, None) if not found
            
        Raises:
            ModelRegistryError: If product is invalid or hour is out of range
        """
        # Validate product and hour
        self._validate_product_hour(product, hour)
        
        # Check if model exists in registry
        if (product, hour) in self._models:
            model_entry = self._models[(product, hour)]
            logger.debug(f"Retrieved model for {product}, hour {hour} from registry")
            return model_entry["model"], model_entry["feature_names"], model_entry["metrics"]
        
        # Model not found
        logger.warning(f"No model found for {product}, hour {hour}")
        return None, None, None
    
    def has_model(self, product: str, hour: int) -> bool:
        """
        Checks if a model exists for a product/hour combination.
        
        Args:
            product: The price product identifier (e.g., DALMP, RTLMP)
            hour: The target hour (0-23)
            
        Returns:
            bool: True if model exists, False otherwise
            
        Raises:
            ModelRegistryError: If product is invalid or hour is out of range
        """
        # Validate product and hour
        self._validate_product_hour(product, hour)
        
        # Check if model exists in registry
        return (product, hour) in self._models
    
    def list_models(self) -> List[Tuple[str, int]]:
        """
        Lists all available models in the registry.
        
        Returns:
            list: List of (product, hour) tuples for available models
        """
        # Return list of keys (product, hour tuples)
        return list(self._models.keys())
    
    def delete(self, product: str, hour: int) -> bool:
        """
        Deletes a model from the registry.
        
        Args:
            product: The price product identifier (e.g., DALMP, RTLMP)
            hour: The target hour (0-23)
            
        Returns:
            bool: True if successful, False if model not found
            
        Raises:
            ModelRegistryError: If product is invalid or hour is out of range
        """
        # Validate product and hour
        self._validate_product_hour(product, hour)
        
        # Check if model exists in registry
        if (product, hour) not in self._models:
            logger.warning(f"Cannot delete: No model found for {product}, hour {hour}")
            return False
        
        # Remove from registry
        del self._models[(product, hour)]
        
        # Remove file from disk
        model_path = self._get_model_path(product, hour)
        try:
            if os.path.exists(model_path):
                os.remove(model_path)
                logger.info(f"Deleted model file: {model_path}")
        except Exception as e:
            logger.error(f"Failed to delete model file {model_path}: {str(e)}")
            # We still return True since the model was removed from the registry
        
        logger.info(f"Deleted model for {product}, hour {hour}")
        return True
    
    def clear(self) -> int:
        """
        Clears all models from the registry.
        
        Returns:
            int: Number of models cleared
        """
        # Count the number of models
        model_count = len(self._models)
        
        # Clear the registry
        self._models = {}
        
        logger.info(f"Cleared registry ({model_count} models removed)")
        return model_count
    
    def save_all(self) -> int:
        """
        Saves all models to disk.
        
        Returns:
            int: Number of models saved
        """
        # Ensure registry directory exists
        ensure_directory_exists(self._registry_dir)
        
        # Save each model
        saved_count = 0
        for (product, hour), model_entry in self._models.items():
            model_path = self._get_model_path(product, hour)
            try:
                joblib.dump(model_entry, model_path)
                saved_count += 1
            except Exception as e:
                logger.error(f"Failed to save model for {product}, hour {hour}: {str(e)}")
        
        # Create a registry index file with metadata
        try:
            index_data = []
            for (product, hour), model_entry in self._models.items():
                index_data.append({
                    "product": product,
                    "hour": hour,
                    "features": len(model_entry["feature_names"]),
                    "created_at": model_entry.get("created_at", pd.Timestamp.now()),
                    "file_path": str(self._get_model_path(product, hour))
                })
            
            if index_data:
                index_df = pd.DataFrame(index_data)
                index_path = self._registry_dir / "registry_index.parquet"
                save_dataframe(index_df, index_path)
                logger.info(f"Saved registry index with {len(index_data)} entries")
        except Exception as e:
            logger.error(f"Failed to save registry index: {str(e)}")
        
        logger.info(f"Saved {saved_count} models to disk")
        return saved_count
    
    def load_all(self) -> int:
        """
        Loads all models from disk.
        
        Returns:
            int: Number of models loaded
        """
        # Clear current models
        self._models = {}
        
        # Check if registry directory exists
        if not os.path.exists(self._registry_dir):
            logger.warning(f"Registry directory does not exist: {self._registry_dir}")
            return 0
        
        # Try to load registry index if it exists
        index_path = self._registry_dir / "registry_index.parquet"
        if os.path.exists(index_path):
            try:
                index_df = load_dataframe(index_path)
                logger.info(f"Loaded registry index with {len(index_df)} entries")
            except Exception as e:
                logger.warning(f"Failed to load registry index: {str(e)}")
                index_df = None
        else:
            index_df = None
        
        # Load model files from directory
        loaded_count = 0
        
        # If we have an index, use it to load models
        if index_df is not None:
            for _, row in index_df.iterrows():
                product = row["product"]
                hour = row["hour"]
                
                # Skip if product or hour is invalid
                try:
                    self._validate_product_hour(product, hour)
                except ModelRegistryError:
                    logger.warning(f"Skipping invalid product/hour in index: {product}, {hour}")
                    continue
                
                model_path = self._get_model_path(product, hour)
                if os.path.exists(model_path):
                    try:
                        model_entry = joblib.load(model_path)
                        self._models[(product, hour)] = model_entry
                        loaded_count += 1
                    except Exception as e:
                        logger.error(f"Failed to load model for {product}, hour {hour}: {str(e)}")
        else:
            # No index, scan directory for model files
            for file_name in os.listdir(self._registry_dir):
                if file_name.endswith(MODEL_FILE_EXTENSION) and "_" in file_name:
                    try:
                        # Parse product and hour from filename
                        base_name = file_name.replace(MODEL_FILE_EXTENSION, "")
                        product, hour_str = base_name.split("_", 1)
                        hour = int(hour_str)
                        
                        # Validate product and hour
                        self._validate_product_hour(product, hour)
                        
                        # Load the model
                        model_path = self._registry_dir / file_name
                        model_entry = joblib.load(model_path)
                        self._models[(product, hour)] = model_entry
                        loaded_count += 1
                    except Exception as e:
                        logger.error(f"Failed to load model file {file_name}: {str(e)}")
        
        logger.info(f"Loaded {loaded_count} models from disk")
        return loaded_count
    
    def _get_model_path(self, product: str, hour: int) -> pathlib.Path:
        """
        Gets the file path for a model.
        
        Args:
            product: The price product identifier
            hour: The target hour
            
        Returns:
            pathlib.Path: Path to the model file
        """
        # Construct filename as product_hour.joblib
        filename = f"{product}_{hour}{MODEL_FILE_EXTENSION}"
        return self._registry_dir / filename
    
    def _validate_product_hour(self, product: str, hour: int) -> bool:
        """
        Validates product and hour parameters.
        
        Args:
            product: The price product identifier
            hour: The target hour
            
        Returns:
            bool: True if valid, raises exception if invalid
            
        Raises:
            ModelRegistryError: If product is invalid or hour is out of range
        """
        # Check product is in the list of valid products
        if product not in FORECAST_PRODUCTS:
            raise ModelRegistryError(
                f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}",
                "validate_product_hour"
            )
        
        # Check hour is between 0 and 23
        if not 0 <= hour <= 23:
            raise ModelRegistryError(
                f"Invalid hour: {hour}. Must be between 0 and 23",
                "validate_product_hour"
            )
        
        return True