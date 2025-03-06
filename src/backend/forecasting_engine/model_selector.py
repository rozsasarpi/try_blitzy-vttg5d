"""
Implements model selection functionality for the Electricity Market Price Forecasting System.
This module is responsible for selecting the appropriate linear model for each product/hour combination,
supporting the requirement for separate models tailored to specific market products and hours.
"""

import typing
import sklearn.linear_model  # scikit-learn version: 1.2.0+

# Internal imports
from .exceptions import ModelSelectionError
from ..utils.logging_utils import get_logger, log_execution_time
from ..utils.decorators import log_exceptions, memoize
from .model_registry import get_model, has_model
from ..config.settings import FORECAST_PRODUCTS

# Global logger for this module
logger = get_logger(__name__)


@log_execution_time
@log_exceptions
@memoize
def select_model_for_product_hour(
    product: str,
    hour: int
) -> typing.Tuple[typing.Optional[sklearn.linear_model.LinearRegression], typing.Optional[typing.List[str]], typing.Optional[typing.Dict[str, float]]]:
    """
    Selects the appropriate model for a specific product and hour

    Args:
        product: The price product (e.g., DALMP, RTLMP)
        hour: The target hour (0-23)

    Returns:
        Tuple of (model, feature_names, metrics) for the selected product/hour

    Raises:
        ModelSelectionError: If no model is found for the given product and hour
    """
    # Validate that product is in FORECAST_PRODUCTS
    if product not in FORECAST_PRODUCTS:
        error_message = f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}"
        logger.error(error_message)
        raise ModelSelectionError(error_message, product, hour)

    # Validate that hour is between 0 and 23
    if not 0 <= hour <= 23:
        error_message = f"Invalid hour: {hour}. Must be between 0 and 23"
        logger.error(error_message)
        raise ModelSelectionError(error_message, product, hour)

    # Check if model exists in registry using has_model
    if not has_model(product, hour):
        error_message = f"No model found for product {product} and hour {hour}"
        logger.error(error_message)
        raise ModelSelectionError(error_message, product, hour)

    # If model exists, retrieve it using get_model
    model, feature_names, metrics = get_model(product, hour)

    # Return the tuple of (model, feature_names, metrics)
    return model, feature_names, metrics


def validate_product_hour(product: str, hour: int) -> bool:
    """
    Validates that product and hour parameters are valid

    Args:
        product: The price product (e.g., DALMP, RTLMP)
        hour: The target hour (0-23)

    Returns:
        True if valid, raises exception if invalid

    Raises:
        ModelSelectionError: If product is invalid or hour is out of range
    """
    # Check if product is in FORECAST_PRODUCTS
    if product not in FORECAST_PRODUCTS:
        error_message = f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}"
        logger.error(error_message)
        raise ModelSelectionError(error_message, product, hour)

    # Check if hour is between 0 and 23
    if not 0 <= hour <= 23:
        error_message = f"Invalid hour: {hour}. Must be between 0 and 23"
        logger.error(error_message)
        raise ModelSelectionError(error_message, product, hour)

    return True


@log_exceptions
def get_model_info(product: str, hour: int) -> typing.Optional[typing.Dict[str, typing.Any]]:
    """
    Gets information about a model for a specific product/hour

    Args:
        product: The price product (e.g., DALMP, RTLMP)
        hour: The target hour (0-23)

    Returns:
        Dictionary with model information or None if not found
    """
    try:
        # Validate product and hour using validate_product_hour
        validate_product_hour(product, hour)

        # Try to get model using select_model_for_product_hour
        model, feature_names, metrics = select_model_for_product_hour(product, hour)

        # If successful, extract model, feature_names, and metrics
        if model:
            # Create and return info dictionary with model details
            info = {
                "product": product,
                "hour": hour,
                "feature_names": feature_names,
                "metrics": metrics
            }
            return info
        else:
            logger.warning(f"No model found for {product}, hour {hour}")
            return None

    except ModelSelectionError:
        # If model selection fails, log warning and return None
        logger.warning(f"No model found for {product}, hour {hour}")
        return None


@log_exceptions
def is_model_available(product: str, hour: int) -> bool:
    """
    Checks if a model is available for a specific product/hour

    Args:
        product: The price product (e.g., DALMP, RTLMP)
        hour: The target hour (0-23)

    Returns:
        True if model is available, False otherwise
    """
    try:
        # Try to validate product and hour using validate_product_hour
        validate_product_hour(product, hour)

    except ModelSelectionError:
        # If validation fails, return False
        return False

    # Check if model exists using has_model
    return has_model(product, hour)


class ModelSelector:
    """
    Class for selecting appropriate models for product/hour combinations
    """

    def __init__(self):
        """
        Initializes the model selector
        """
        # Initialize logger for this class
        self.logger = get_logger(__name__)

    def select_model(
        self,
        product: str,
        hour: int
    ) -> typing.Tuple[typing.Optional[sklearn.linear_model.LinearRegression], typing.Optional[typing.List[str]], typing.Optional[typing.Dict[str, float]]]:
        """
        Selects the appropriate model for a product/hour combination

        Args:
            product: The price product (e.g., DALMP, RTLMP)
            hour: The target hour (0-23)

        Returns:
            Tuple of (model, feature_names, metrics) for the selected product/hour
        """
        # Delegate to select_model_for_product_hour function
        return select_model_for_product_hour(product, hour)

    def get_model_info(self, product: str, hour: int) -> typing.Optional[typing.Dict[str, typing.Any]]:
        """
        Gets information about a model for a specific product/hour

        Args:
            product: The price product (e.g., DALMP, RTLMP)
            hour: The target hour (0-23)

        Returns:
            Dictionary with model information or None if not found
        """
        # Delegate to get_model_info function
        return get_model_info(product, hour)

    def is_model_available(self, product: str, hour: int) -> bool:
        """
        Checks if a model is available for a specific product/hour

        Args:
            product: The price product (e.g., DALMP, RTLMP)
            hour: The target hour (0-23)

        Returns:
            bool: True if model is available, False otherwise
        """
        # Delegate to is_model_available function
        return is_model_available(product, hour)


# Expose functions for external use
__all__ = [
    "select_model_for_product_hour",
    "validate_product_hour",
    "get_model_info",
    "is_model_available",
    "ModelSelector"
]