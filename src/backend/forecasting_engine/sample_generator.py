"""
Implements probabilistic sample generation for the forecasting engine of the Electricity Market Price Forecasting System.
This module is responsible for generating sample-based probabilistic forecasts from point forecasts and uncertainty
parameters, which is a critical component of the system's probabilistic forecasting capability.
"""

import numpy as np  # version: 1.24.0
import scipy.stats  # version: 1.10.0
from typing import List, Dict, Optional, Union, Tuple
from datetime import datetime

# Internal imports
from .exceptions import SampleGenerationError
from ..utils.logging_utils import get_logger, log_execution_time
from ..utils.decorators import validate_input
from ..models.forecast_models import ProbabilisticForecast
from ..config.settings import PROBABILISTIC_SAMPLE_COUNT, FORECAST_PRODUCTS

# Global logger
logger = get_logger(__name__)

def generate_normal_samples(point_forecast: float, uncertainty_params: Dict, sample_count: int) -> List[float]:
    """
    Generates samples from a normal distribution.
    
    Args:
        point_forecast: The point forecast value (mean)
        uncertainty_params: Dictionary containing uncertainty parameters ('std_dev')
        sample_count: Number of samples to generate
    
    Returns:
        List of samples from normal distribution
    """
    # Extract standard deviation from uncertainty_params
    std_dev = uncertainty_params.get('std_dev', 0.1 * abs(point_forecast))
    
    # Generate samples using numpy's normal distribution
    samples = np.random.normal(loc=point_forecast, scale=std_dev, size=sample_count)
    
    return samples.tolist()

def generate_lognormal_samples(point_forecast: float, uncertainty_params: Dict, sample_count: int) -> List[float]:
    """
    Generates samples from a lognormal distribution.
    
    Args:
        point_forecast: The point forecast value
        uncertainty_params: Dictionary containing uncertainty parameters
        sample_count: Number of samples to generate
    
    Returns:
        List of samples from lognormal distribution
    """
    # Extract parameters or use defaults
    # For lognormal, we need to convert the parameters
    cv = uncertainty_params.get('coefficient_of_variation', 0.1)
    
    # Convert to lognormal parameters (mu and sigma)
    # For lognormal, the mean is exp(mu + sigma^2/2)
    # and variance is [exp(sigma^2) - 1] * exp(2*mu + sigma^2)
    # We want mean = point_forecast and CV = std_dev/mean
    
    if point_forecast <= 0:
        # Lognormal is only for positive values, so use a small positive number
        point_forecast = max(0.01, point_forecast)
        logger.warning(f"Adjusting point forecast to {point_forecast} for lognormal distribution")
    
    sigma = np.sqrt(np.log(1 + cv**2))
    mu = np.log(point_forecast) - sigma**2 / 2
    
    # Generate samples
    samples = np.random.lognormal(mean=mu, sigma=sigma, size=sample_count)
    
    return samples.tolist()

def generate_truncated_normal_samples(point_forecast: float, uncertainty_params: Dict, sample_count: int) -> List[float]:
    """
    Generates samples from a truncated normal distribution.
    
    Args:
        point_forecast: The point forecast value
        uncertainty_params: Dictionary containing uncertainty parameters
        sample_count: Number of samples to generate
    
    Returns:
        List of samples from truncated normal distribution
    """
    # Extract parameters
    std_dev = uncertainty_params.get('std_dev', 0.1 * abs(point_forecast))
    lower_bound = uncertainty_params.get('lower_bound', point_forecast - 3 * std_dev)
    upper_bound = uncertainty_params.get('upper_bound', point_forecast + 3 * std_dev)
    
    # Calculate normalized bounds
    a = (lower_bound - point_forecast) / std_dev
    b = (upper_bound - point_forecast) / std_dev
    
    # Generate samples using scipy's truncated normal
    samples = scipy.stats.truncnorm.rvs(a, b, loc=point_forecast, scale=std_dev, size=sample_count)
    
    return samples.tolist()

def generate_skewed_normal_samples(point_forecast: float, uncertainty_params: Dict, sample_count: int) -> List[float]:
    """
    Generates samples from a skewed normal distribution.
    
    Args:
        point_forecast: The point forecast value
        uncertainty_params: Dictionary containing uncertainty parameters
        sample_count: Number of samples to generate
    
    Returns:
        List of samples from skewed normal distribution
    """
    # Extract parameters
    std_dev = uncertainty_params.get('std_dev', 0.1 * abs(point_forecast))
    skewness = uncertainty_params.get('skewness', 0)  # 0 means no skew
    
    # Generate samples using scipy's skewnorm
    samples = scipy.stats.skewnorm.rvs(a=skewness, loc=point_forecast, scale=std_dev, size=sample_count)
    
    return samples.tolist()

def apply_product_constraints(samples: List[float], product: str) -> List[float]:
    """
    Applies product-specific constraints to samples.
    
    Args:
        samples: List of samples to constrain
        product: Price product identifier
    
    Returns:
        List of constrained samples
    """
    # Convert to numpy array for vectorized operations
    samples_array = np.array(samples)
    
    # Apply constraints based on product
    # Energy prices (DALMP, RTLMP) can be negative
    # Ancillary services (RegUp, RegDown, RRS, NSRS) must be non-negative
    if product in ['RegUp', 'RegDown', 'RRS', 'NSRS']:
        # Ensure all values are non-negative
        samples_array = np.maximum(samples_array, 0)
    
    # For DALMP and RTLMP, we allow negative values
    # No additional constraints needed
    
    return samples_array.tolist()

def create_probabilistic_forecast(point_forecast: float, samples: List[float], product: str, 
                                 timestamp: datetime, is_fallback: bool = False) -> ProbabilisticForecast:
    """
    Creates a ProbabilisticForecast object from point forecast and samples.
    
    Args:
        point_forecast: Point forecast value
        samples: List of probabilistic samples
        product: Price product identifier
        timestamp: Forecast timestamp
        is_fallback: Whether this is a fallback forecast
    
    Returns:
        ProbabilisticForecast object
    """
    # Create and return a new ProbabilisticForecast
    return ProbabilisticForecast(
        timestamp=timestamp,
        product=product,
        point_forecast=point_forecast,
        samples=samples,
        generation_timestamp=datetime.now(),
        is_fallback=is_fallback
    )

def validate_point_forecast(point_forecast: float) -> bool:
    """
    Validates that a point forecast is a valid number.
    
    Args:
        point_forecast: Point forecast value to validate
    
    Returns:
        True if valid, False otherwise
    """
    # Check if it's None
    if point_forecast is None:
        logger.error("Point forecast is None")
        return False
    
    # Check if it's a number
    if not isinstance(point_forecast, (int, float)):
        logger.error(f"Point forecast must be a number, got {type(point_forecast)}")
        return False
    
    # Check if it's not NaN or infinity
    if np.isnan(point_forecast) or np.isinf(point_forecast):
        logger.error(f"Point forecast cannot be NaN or infinity, got {point_forecast}")
        return False
    
    return True

def validate_uncertainty_params(uncertainty_params: Dict) -> bool:
    """
    Validates that uncertainty parameters are valid.
    
    Args:
        uncertainty_params: Dictionary of uncertainty parameters
    
    Returns:
        True if valid, False otherwise
    """
    # Check if it's None or not a dictionary
    if uncertainty_params is None or not isinstance(uncertainty_params, dict):
        logger.error(f"Uncertainty parameters must be a dictionary, got {type(uncertainty_params)}")
        return False
    
    # Check if required parameters are present
    if 'std_dev' not in uncertainty_params:
        logger.warning("Standard deviation ('std_dev') not found in uncertainty parameters, will use default")
    
    # Validate numeric parameters
    for param in ['std_dev', 'lower_bound', 'upper_bound', 'skewness', 'coefficient_of_variation']:
        if param in uncertainty_params:
            value = uncertainty_params[param]
            if not isinstance(value, (int, float)) or np.isnan(value) or np.isinf(value):
                logger.error(f"Parameter '{param}' must be a valid number, got {value}")
                return False
    
    # If std_dev is provided, check if it's positive
    if 'std_dev' in uncertainty_params and uncertainty_params['std_dev'] <= 0:
        logger.error(f"Standard deviation must be positive, got {uncertainty_params['std_dev']}")
        return False
    
    return True

def validate_product(product: str) -> bool:
    """
    Validates that a product is in the list of valid products.
    
    Args:
        product: Product to validate
    
    Returns:
        True if valid, False otherwise
    """
    # Check if it's None or not a string
    if product is None or not isinstance(product, str):
        logger.error(f"Product must be a string, got {type(product)}")
        return False
    
    # Check if it's in the list of valid products
    if product not in FORECAST_PRODUCTS:
        logger.error(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")
        return False
    
    return True

# Define a mapping of distribution types to generator functions
DISTRIBUTION_TYPES = {
    "normal": generate_normal_samples,
    "lognormal": generate_lognormal_samples,
    "truncated_normal": generate_truncated_normal_samples,
    "skewed_normal": generate_skewed_normal_samples
}

# Default distribution type
DEFAULT_DISTRIBUTION_TYPE = "normal"

@log_execution_time
@validate_input([validate_point_forecast, validate_uncertainty_params, validate_product])
def generate_samples(point_forecast: float, uncertainty_params: Dict, product: str, 
                    hour: int, distribution_type: str = DEFAULT_DISTRIBUTION_TYPE) -> List[float]:
    """
    Main function to generate probabilistic samples from a point forecast and uncertainty parameters.
    
    Args:
        point_forecast: The point forecast value
        uncertainty_params: Dictionary containing uncertainty parameters
        product: Price product identifier
        hour: Target hour
        distribution_type: Type of distribution to use (default: 'normal')
    
    Returns:
        List of probabilistic samples
    """
    try:
        logger.debug(f"Generating samples for {product}, hour {hour} using {distribution_type} distribution")
        
        # Select distribution type, default to normal if not found
        if distribution_type not in DISTRIBUTION_TYPES:
            logger.warning(f"Distribution type {distribution_type} not found, using {DEFAULT_DISTRIBUTION_TYPE}")
            distribution_type = DEFAULT_DISTRIBUTION_TYPE
        
        # Get the corresponding generator function
        generator_func = DISTRIBUTION_TYPES[distribution_type]
        
        # Generate samples
        samples = generator_func(point_forecast, uncertainty_params, PROBABILISTIC_SAMPLE_COUNT)
        
        # Apply product-specific constraints
        constrained_samples = apply_product_constraints(samples, product)
        
        logger.debug(f"Generated {len(constrained_samples)} samples for {product}, hour {hour}")
        return constrained_samples
        
    except Exception as e:
        # Log the error and reraise as SampleGenerationError
        error_msg = f"Error generating samples for {product}, hour {hour}: {str(e)}"
        logger.error(error_msg)
        raise SampleGenerationError(error_msg, product, hour, point_forecast, uncertainty_params)

class SampleGenerator:
    """
    Class responsible for generating probabilistic samples for price forecasts.
    """
    
    def __init__(self):
        """
        Initializes the sample generator with distribution registry.
        """
        # Initialize distribution registry
        self._distribution_registry = {
            "normal": generate_normal_samples,
            "lognormal": generate_lognormal_samples,
            "truncated_normal": generate_truncated_normal_samples,
            "skewed_normal": generate_skewed_normal_samples
        }
        
        # Initialize product constraints
        self._product_constraints = {
            "DALMP": {},  # No special constraints for DALMP
            "RTLMP": {},  # No special constraints for RTLMP
            "RegUp": {"non_negative": True},
            "RegDown": {"non_negative": True},
            "RRS": {"non_negative": True},
            "NSRS": {"non_negative": True}
        }
        
        # Set up logger
        self.logger = get_logger(f"{__name__}.SampleGenerator")
        self.logger.info("SampleGenerator initialized")
    
    def generate_samples(self, point_forecast: float, uncertainty_params: Dict, product: str, 
                         hour: int, distribution_type: str = DEFAULT_DISTRIBUTION_TYPE, 
                         sample_count: int = PROBABILISTIC_SAMPLE_COUNT) -> List[float]:
        """
        Generates probabilistic samples for a given point forecast.
        
        Args:
            point_forecast: The point forecast value
            uncertainty_params: Dictionary containing uncertainty parameters
            product: Price product identifier
            hour: Target hour
            distribution_type: Type of distribution to use (default: 'normal')
            sample_count: Number of samples to generate (default: from settings)
        
        Returns:
            List of probabilistic samples
        """
        try:
            # Validate inputs
            if not validate_point_forecast(point_forecast):
                raise ValueError(f"Invalid point forecast: {point_forecast}")
            
            if not validate_uncertainty_params(uncertainty_params):
                raise ValueError(f"Invalid uncertainty parameters: {uncertainty_params}")
            
            if not validate_product(product):
                raise ValueError(f"Invalid product: {product}")
            
            # Get the distribution function
            try:
                distribution_func = self.get_distribution(distribution_type)
            except SampleGenerationError:
                self.logger.warning(f"Distribution {distribution_type} not found, using {DEFAULT_DISTRIBUTION_TYPE}")
                distribution_func = self.get_distribution(DEFAULT_DISTRIBUTION_TYPE)
            
            # Generate samples
            self.logger.debug(f"Generating {sample_count} samples for {product}, hour {hour}")
            samples = distribution_func(point_forecast, uncertainty_params, sample_count)
            
            # Apply constraints
            constrained_samples = self.apply_constraints(samples, product)
            
            self.logger.debug(f"Generated {len(constrained_samples)} samples for {product}, hour {hour}")
            return constrained_samples
            
        except Exception as e:
            error_msg = f"Error generating samples in SampleGenerator for {product}, hour {hour}: {str(e)}"
            self.logger.error(error_msg)
            raise SampleGenerationError(error_msg, product, hour, point_forecast, uncertainty_params)
    
    def register_distribution(self, name: str, distribution_function: callable) -> None:
        """
        Registers a new distribution type for sample generation.
        
        Args:
            name: Name of the distribution
            distribution_function: Function to generate samples for this distribution
            
        Returns:
            None: Function performs side effects only
        """
        self._distribution_registry[name] = distribution_function
        self.logger.info(f"Registered new distribution: {name}")
    
    def get_distribution(self, distribution_name: str) -> callable:
        """
        Gets a distribution function from the registry.
        
        Args:
            distribution_name: Name of the distribution to retrieve
            
        Returns:
            Distribution function
        
        Raises:
            SampleGenerationError: If distribution is not found
        """
        if distribution_name not in self._distribution_registry:
            error_msg = f"Distribution '{distribution_name}' not found in registry"
            self.logger.error(error_msg)
            raise SampleGenerationError(error_msg, "unknown", 0, 0, {})
        
        return self._distribution_registry[distribution_name]
    
    def set_product_constraint(self, product: str, constraints: Dict) -> None:
        """
        Sets constraints for a specific product.
        
        Args:
            product: Price product identifier
            constraints: Dictionary of constraints
            
        Returns:
            None: Function performs side effects only
            
        Raises:
            ValueError: If product is not valid
        """
        if product not in FORECAST_PRODUCTS:
            error_msg = f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        self._product_constraints[product] = constraints
        self.logger.info(f"Set constraints for product {product}: {constraints}")
    
    def apply_constraints(self, samples: List[float], product: str) -> List[float]:
        """
        Applies constraints to generated samples.
        
        Args:
            samples: List of samples to constrain
            product: Price product identifier
            
        Returns:
            Constrained samples
        """
        # Get constraints for this product
        constraints = self._product_constraints.get(product, {})
        
        # Convert to numpy array for vectorized operations
        samples_array = np.array(samples)
        
        # Apply non-negative constraint if specified
        if constraints.get("non_negative", False):
            samples_array = np.maximum(samples_array, 0)
        
        # Apply min value constraint if specified
        if "min_value" in constraints:
            samples_array = np.maximum(samples_array, constraints["min_value"])
        
        # Apply max value constraint if specified
        if "max_value" in constraints:
            samples_array = np.minimum(samples_array, constraints["max_value"])
        
        return samples_array.tolist()