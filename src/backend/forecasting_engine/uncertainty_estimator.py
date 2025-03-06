"""
Implements uncertainty estimation for the forecasting engine of the Electricity Market Price Forecasting System.
This module is responsible for estimating the uncertainty around point forecasts, which is a critical component for generating probabilistic forecasts. It provides various methods for uncertainty estimation based on historical model performance and product-specific characteristics.
"""

from typing import Dict, List, Any, Optional, Callable, Tuple, Union
import numpy as np
import functools

# Internal imports
from .exceptions import UncertaintyEstimationError
from ..utils.logging_utils import get_logger, log_execution_time
from ..utils.decorators import validate_input
from ..config.settings import FORECAST_PRODUCTS

# Configure logger
logger = get_logger(__name__)

# Default uncertainty estimation method
DEFAULT_UNCERTAINTY_METHOD = "historical_residuals"

# Product-specific adjustment factors for uncertainty
# Higher values increase uncertainty, lower values decrease it
PRODUCT_ADJUSTMENTS = {
    "DALMP": 1.0,    # Day-Ahead Locational Marginal Price (baseline)
    "RTLMP": 1.2,    # Real-Time Locational Marginal Price (more volatile)
    "RegUp": 0.8,    # Regulation Up (less volatile)
    "RegDown": 0.8,  # Regulation Down (less volatile)
    "RRS": 0.7,      # Responsive Reserve Service (less volatile)
    "NSRS": 0.7      # Non-Spinning Reserve Service (less volatile)
}


def validate_point_forecast(point_forecast: float) -> bool:
    """
    Validates that a point forecast is a valid number.
    
    Args:
        point_forecast: The point forecast value to validate
        
    Returns:
        True if valid, False otherwise
    """
    if point_forecast is None:
        logger.error("Point forecast cannot be None")
        return False
    
    if not isinstance(point_forecast, (int, float)):
        logger.error(f"Point forecast must be a number, got {type(point_forecast)}")
        return False
    
    if np.isnan(point_forecast) or np.isinf(point_forecast):
        logger.error(f"Point forecast cannot be NaN or infinity, got {point_forecast}")
        return False
    
    return True


def validate_product(product: str) -> bool:
    """
    Validates that a product is in the list of valid products.
    
    Args:
        product: The product name to validate
        
    Returns:
        True if valid, False otherwise
    """
    if product is None:
        logger.error("Product cannot be None")
        return False
    
    if not isinstance(product, str):
        logger.error(f"Product must be a string, got {type(product)}")
        return False
    
    if product not in FORECAST_PRODUCTS:
        logger.error(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")
        return False
    
    return True


def validate_hour(hour: int) -> bool:
    """
    Validates that an hour is within valid range (0-23).
    
    Args:
        hour: The hour to validate
        
    Returns:
        True if valid, False otherwise
    """
    if hour is None:
        logger.error("Hour cannot be None")
        return False
    
    if not isinstance(hour, int):
        logger.error(f"Hour must be an integer, got {type(hour)}")
        return False
    
    if hour < 0 or hour > 23:
        logger.error(f"Hour must be between 0 and 23, got {hour}")
        return False
    
    return True


@log_execution_time
@validate_input([validate_point_forecast, validate_product, validate_hour])
def estimate_uncertainty(
    point_forecast: float,
    product: str,
    hour: int,
    historical_data: Dict[str, Any],
    method: str = DEFAULT_UNCERTAINTY_METHOD
) -> Dict[str, float]:
    """
    Main function to estimate uncertainty for a point forecast.
    
    Args:
        point_forecast: The point forecast value
        product: The price product (e.g., DALMP, RTLMP, RegUp)
        hour: The target hour (0-23)
        historical_data: Dictionary containing historical forecast performance data
        method: Uncertainty estimation method to use
        
    Returns:
        Dictionary of uncertainty parameters (mean, std_dev, etc.)
        
    Raises:
        UncertaintyEstimationError: If uncertainty estimation fails
    """
    logger.debug(f"Estimating uncertainty for {product} at hour {hour} with method {method}")
    
    try:
        # Get the appropriate estimation function
        if method not in UNCERTAINTY_METHODS:
            logger.warning(
                f"Unknown uncertainty method: {method}, using default: {DEFAULT_UNCERTAINTY_METHOD}"
            )
            method = DEFAULT_UNCERTAINTY_METHOD
        
        estimation_func = UNCERTAINTY_METHODS[method]
        
        # Estimate uncertainty using the selected method
        uncertainty_params = estimation_func(point_forecast, product, hour, historical_data)
        
        # Apply product-specific adjustment
        adjusted_params = apply_product_adjustment(uncertainty_params, product)
        
        logger.info(
            f"Estimated uncertainty for {product} hour {hour}: "
            f"mean={adjusted_params.get('mean', 'N/A')}, "
            f"std_dev={adjusted_params.get('std_dev', 'N/A')}"
        )
        
        return adjusted_params
        
    except Exception as e:
        logger.error(f"Error estimating uncertainty for {product} hour {hour}: {str(e)}")
        raise UncertaintyEstimationError(
            f"Failed to estimate uncertainty: {str(e)}",
            product,
            hour,
            point_forecast
        )


def estimate_uncertainty_from_residuals(
    point_forecast: float,
    product: str,
    hour: int,
    historical_data: Dict[str, Any]
) -> Dict[str, float]:
    """
    Estimates uncertainty based on historical model residuals.
    
    Args:
        point_forecast: The point forecast value
        product: The price product
        hour: The target hour
        historical_data: Dictionary containing historical residuals by product and hour
        
    Returns:
        Dictionary of uncertainty parameters
    """
    # Extract historical residuals for this product and hour
    key = f"{product}_{hour}"
    if key not in historical_data or 'residuals' not in historical_data[key]:
        logger.warning(f"No historical residuals found for {product} hour {hour}, using default uncertainty")
        # Default to 10% of point forecast if no historical data
        mean = point_forecast
        std_dev = abs(point_forecast) * 0.10
        return {"mean": mean, "std_dev": std_dev}
    
    residuals = historical_data[key]['residuals']
    
    # Calculate mean and standard deviation of residuals
    mean = point_forecast + np.mean(residuals)
    std_dev = np.std(residuals)
    
    # Ensure std_dev is positive and reasonable
    std_dev = max(std_dev, abs(point_forecast) * 0.05)
    
    logger.debug(
        f"Estimated uncertainty from residuals for {product} hour {hour}: "
        f"mean={mean}, std_dev={std_dev}"
    )
    
    return {"mean": mean, "std_dev": std_dev}


def estimate_uncertainty_from_percentage(
    point_forecast: float,
    product: str,
    hour: int,
    historical_data: Dict[str, Any]
) -> Dict[str, float]:
    """
    Estimates uncertainty as a percentage of the point forecast.
    
    Args:
        point_forecast: The point forecast value
        product: The price product
        hour: The target hour
        historical_data: Dictionary containing historical percentage errors
        
    Returns:
        Dictionary of uncertainty parameters
    """
    # Extract historical percentage errors
    key = f"{product}_{hour}"
    if key not in historical_data or 'percentage_errors' not in historical_data[key]:
        logger.warning(f"No historical percentage errors found for {product} hour {hour}, using default percentage")
        # Default percentages if no historical data
        mean_percentage_error = 0
        std_percentage = 0.10  # 10% of point forecast
    else:
        percentage_errors = historical_data[key]['percentage_errors']
        mean_percentage_error = np.mean(percentage_errors)
        std_percentage = np.std(percentage_errors)
        # Ensure std_percentage is reasonable
        std_percentage = max(std_percentage, 0.05)
    
    # Calculate mean and standard deviation based on percentages
    mean = point_forecast * (1 + mean_percentage_error)
    std_dev = abs(point_forecast) * std_percentage
    
    logger.debug(
        f"Estimated uncertainty from percentage for {product} hour {hour}: "
        f"mean={mean}, std_dev={std_dev}"
    )
    
    return {"mean": mean, "std_dev": std_dev}


def estimate_uncertainty_fixed(
    point_forecast: float,
    product: str,
    hour: int,
    historical_data: Dict[str, Any]
) -> Dict[str, float]:
    """
    Estimates uncertainty using fixed values based on product type.
    
    Args:
        point_forecast: The point forecast value
        product: The price product
        hour: The target hour
        historical_data: Not used for this method, included for API consistency
        
    Returns:
        Dictionary of uncertainty parameters
    """
    # Define fixed uncertainty values by product
    fixed_uncertainty = {
        "DALMP": 5.0,    # $5 standard deviation
        "RTLMP": 8.0,    # $8 standard deviation (more volatile)
        "RegUp": 3.0,    # $3 standard deviation
        "RegDown": 3.0,  # $3 standard deviation
        "RRS": 2.5,      # $2.5 standard deviation
        "NSRS": 2.0      # $2 standard deviation
    }
    
    # Use the fixed value for this product, or default to DALMP if not found
    std_dev = fixed_uncertainty.get(product, fixed_uncertainty["DALMP"])
    
    # Set mean to point forecast (no bias)
    mean = point_forecast
    
    logger.debug(
        f"Estimated fixed uncertainty for {product} hour {hour}: "
        f"mean={mean}, std_dev={std_dev}"
    )
    
    return {"mean": mean, "std_dev": std_dev}


def estimate_uncertainty_adaptive(
    point_forecast: float,
    product: str,
    hour: int,
    historical_data: Dict[str, Any]
) -> Dict[str, float]:
    """
    Estimates uncertainty adaptively based on recent forecast performance.
    
    Args:
        point_forecast: The point forecast value
        product: The price product
        hour: The target hour
        historical_data: Dictionary containing recent forecast errors
        
    Returns:
        Dictionary of uncertainty parameters
    """
    # Extract recent forecast errors
    key = f"{product}_{hour}"
    if key not in historical_data or 'recent_errors' not in historical_data[key]:
        logger.warning(f"No recent errors found for {product} hour {hour}, using fallback method")
        # Fallback to historical residuals method
        return estimate_uncertainty_from_residuals(point_forecast, product, hour, historical_data)
    
    recent_errors = historical_data[key]['recent_errors']
    
    # Calculate error trend (is error increasing or decreasing?)
    if len(recent_errors) < 3:
        # Not enough data for trend analysis
        error_trend = 0
    else:
        # Simple trend calculation: compare recent errors to older errors
        recent_window = recent_errors[-3:]
        older_window = recent_errors[-6:-3] if len(recent_errors) >= 6 else recent_errors[:-3]
        
        recent_mean = np.mean(np.abs(recent_window))
        older_mean = np.mean(np.abs(older_window))
        
        # Positive trend means errors are increasing
        error_trend = (recent_mean - older_mean) / max(older_mean, 1.0)
    
    # Base uncertainty estimate from recent errors
    if recent_errors:
        mean = point_forecast + np.mean(recent_errors)
        std_dev = np.std(recent_errors)
        # Ensure std_dev is positive and reasonable
        std_dev = max(std_dev, abs(point_forecast) * 0.05)
    else:
        # Default if no recent errors
        mean = point_forecast
        std_dev = abs(point_forecast) * 0.10
    
    # Adjust uncertainty based on error trend
    trend_adjustment = 1.0 + max(0, error_trend)  # Only increase for positive trends
    std_dev = std_dev * trend_adjustment
    
    logger.debug(
        f"Estimated adaptive uncertainty for {product} hour {hour}: "
        f"mean={mean}, std_dev={std_dev}, trend_adjustment={trend_adjustment:.2f}"
    )
    
    return {"mean": mean, "std_dev": std_dev}


def apply_product_adjustment(
    uncertainty_params: Dict[str, float],
    product: str
) -> Dict[str, float]:
    """
    Applies product-specific adjustment to uncertainty parameters.
    
    Args:
        uncertainty_params: Dictionary of uncertainty parameters
        product: The price product
        
    Returns:
        Adjusted uncertainty parameters
    """
    # Create a copy to avoid modifying the original
    adjusted_params = uncertainty_params.copy()
    
    # Get adjustment factor for this product (default to 1.0 if not found)
    adjustment_factor = PRODUCT_ADJUSTMENTS.get(product, 1.0)
    
    # Apply adjustment to standard deviation
    if 'std_dev' in adjusted_params:
        adjusted_params['std_dev'] = adjusted_params['std_dev'] * adjustment_factor
    
    logger.debug(f"Applied adjustment factor {adjustment_factor} for product {product}")
    
    return adjusted_params


# Register uncertainty estimation methods
UNCERTAINTY_METHODS = {
    "historical_residuals": estimate_uncertainty_from_residuals,
    "percentage_of_forecast": estimate_uncertainty_from_percentage,
    "fixed_value": estimate_uncertainty_fixed,
    "adaptive": estimate_uncertainty_adaptive
}


class UncertaintyEstimator:
    """
    Class responsible for estimating uncertainty for price forecasts.
    
    Provides a flexible interface for registering and using different
    uncertainty estimation methods.
    """
    
    def __init__(self):
        """Initializes the uncertainty estimator with method registry."""
        # Initialize method registry with supported estimation methods
        self._method_registry = {
            "historical_residuals": estimate_uncertainty_from_residuals,
            "percentage_of_forecast": estimate_uncertainty_from_percentage,
            "fixed_value": estimate_uncertainty_fixed,
            "adaptive": estimate_uncertainty_adaptive
        }
        
        # Set up product-specific adjustment factors
        self._product_adjustments = PRODUCT_ADJUSTMENTS.copy()
        
        # Set up logger
        self._logger = get_logger(f"{__name__}.UncertaintyEstimator")
        self._logger.info("Initialized UncertaintyEstimator")
    
    def estimate_uncertainty(
        self,
        point_forecast: float,
        product: str,
        hour: int,
        historical_data: Dict[str, Any],
        method: str = "historical_residuals"
    ) -> Dict[str, float]:
        """
        Estimates uncertainty for a given point forecast.
        
        Args:
            point_forecast: The point forecast value
            product: The price product
            hour: The target hour
            historical_data: Dictionary containing historical forecast performance data
            method: Uncertainty estimation method to use
            
        Returns:
            Dictionary of uncertainty parameters
            
        Raises:
            UncertaintyEstimationError: If uncertainty estimation fails
        """
        self._logger.debug(f"Estimating uncertainty for {product} at hour {hour} with method {method}")
        
        # Validate inputs
        if not validate_point_forecast(point_forecast):
            raise UncertaintyEstimationError(
                "Invalid point forecast",
                product,
                hour,
                point_forecast
            )
        
        if not validate_product(product):
            raise UncertaintyEstimationError(
                f"Invalid product: {product}",
                product,
                hour,
                point_forecast
            )
        
        if not validate_hour(hour):
            raise UncertaintyEstimationError(
                f"Invalid hour: {hour}",
                product,
                hour,
                point_forecast
            )
        
        try:
            # Get the appropriate estimation method
            estimation_func = self.get_method(method)
            
            # Estimate uncertainty using the method
            uncertainty_params = estimation_func(point_forecast, product, hour, historical_data)
            
            # Apply product-specific adjustment
            adjusted_params = self.apply_adjustment(uncertainty_params, product)
            
            self._logger.info(
                f"Estimated uncertainty for {product} hour {hour}: "
                f"mean={adjusted_params.get('mean', 'N/A')}, "
                f"std_dev={adjusted_params.get('std_dev', 'N/A')}"
            )
            
            return adjusted_params
            
        except Exception as e:
            self._logger.error(f"Error estimating uncertainty for {product} hour {hour}: {str(e)}")
            raise UncertaintyEstimationError(
                f"Failed to estimate uncertainty: {str(e)}",
                product,
                hour,
                point_forecast
            )
    
    def register_method(self, name: str, method_function: callable) -> None:
        """
        Registers a new uncertainty estimation method.
        
        Args:
            name: Name for the method
            method_function: Function implementing the method
            
        Raises:
            ValueError: If the method function is not callable
        """
        if not callable(method_function):
            raise ValueError(f"Method function must be callable, got {type(method_function)}")
        
        self._method_registry[name] = method_function
        self._logger.info(f"Registered new uncertainty estimation method: {name}")
    
    def get_method(self, method_name: str) -> callable:
        """
        Gets an uncertainty estimation method from the registry.
        
        Args:
            method_name: Name of the method to retrieve
            
        Returns:
            The method function
            
        Raises:
            UncertaintyEstimationError: If the method is not found
        """
        if method_name not in self._method_registry:
            self._logger.warning(
                f"Unknown uncertainty method: {method_name}, using default: historical_residuals"
            )
            method_name = "historical_residuals"
        
        return self._method_registry[method_name]
    
    def set_product_adjustment(self, product: str, adjustment_factor: float) -> None:
        """
        Sets adjustment factor for a specific product.
        
        Args:
            product: The product to set the adjustment for
            adjustment_factor: The adjustment factor to apply
            
        Raises:
            ValueError: If the product is invalid or adjustment factor is not a positive number
        """
        if product not in FORECAST_PRODUCTS:
            raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")
        
        if not isinstance(adjustment_factor, (int, float)) or adjustment_factor <= 0:
            raise ValueError(f"Adjustment factor must be a positive number, got {adjustment_factor}")
        
        self._product_adjustments[product] = adjustment_factor
        self._logger.info(f"Set adjustment factor for {product} to {adjustment_factor}")
    
    def apply_adjustment(
        self,
        uncertainty_params: Dict[str, float],
        product: str
    ) -> Dict[str, float]:
        """
        Applies product-specific adjustment to uncertainty parameters.
        
        Args:
            uncertainty_params: Dictionary of uncertainty parameters
            product: The price product
            
        Returns:
            Adjusted uncertainty parameters
        """
        # Create a copy to avoid modifying the original
        adjusted_params = uncertainty_params.copy()
        
        # Get adjustment factor for this product (default to 1.0 if not found)
        adjustment_factor = self._product_adjustments.get(product, 1.0)
        
        # Apply adjustment to standard deviation
        if 'std_dev' in adjusted_params:
            adjusted_params['std_dev'] = adjusted_params['std_dev'] * adjustment_factor
        
        self._logger.debug(f"Applied adjustment factor {adjustment_factor} for product {product}")
        
        return adjusted_params