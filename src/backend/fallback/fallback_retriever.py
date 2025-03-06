"""
Module responsible for retrieving previous forecasts to use as fallbacks when current forecast generation fails.
Implements search strategies to find suitable historical forecasts, handles error conditions, and coordinates
with the timestamp adjuster to prepare fallback forecasts for current use.
"""

import logging
import datetime
from typing import Dict, List, Optional, Tuple, Union
import time
import pandas as pd  # version: 2.0.0+

# Internal imports
from .exceptions import FallbackRetrievalError, NoFallbackAvailableError
from .fallback_logger import log_fallback_retrieval, log_fallback_error
from .timestamp_adjuster import adjust_timestamps
from ..storage.storage_manager import get_forecast, check_forecast_availability, get_forecast_info
from ..utils.date_utils import get_previous_day_date, localize_to_cst
from ..utils.logging_utils import get_logger
from ..config.settings import FORECAST_PRODUCTS

# Configure logger
logger = get_logger(__name__)

# Default maximum number of days to search for a fallback forecast
DEFAULT_MAX_SEARCH_DAYS = 7


def retrieve_fallback_forecast(
    product: str, 
    target_date: datetime.datetime, 
    max_search_days: int = DEFAULT_MAX_SEARCH_DAYS
) -> pd.DataFrame:
    """
    Main function to retrieve a fallback forecast for a specific product and target date.
    
    Args:
        product: The price product to retrieve a fallback for
        target_date: The target date for which a forecast is needed
        max_search_days: Maximum number of days to search backward for a fallback
        
    Returns:
        Adjusted fallback forecast dataframe
        
    Raises:
        FallbackRetrievalError: If fallback retrieval fails
        NoFallbackAvailableError: If no suitable fallback can be found
    """
    # Validate input parameters
    if not validate_fallback_parameters(product, target_date):
        raise FallbackRetrievalError(
            "Invalid parameters for fallback retrieval", 
            product, 
            target_date
        )
    
    # Log the start of fallback retrieval
    logger.info(f"Starting fallback forecast retrieval for {product} on {target_date.strftime('%Y-%m-%d')}")
    start_time = time.time()
    
    try:
        # Find a suitable previous forecast to use as fallback
        source_date, metadata = find_suitable_fallback(product, target_date, max_search_days)
        
        # Get the forecast data
        logger.debug(f"Retrieving fallback forecast for {product} from {source_date.strftime('%Y-%m-%d')}")
        fallback_df = get_forecast(source_date, product)
        
        # Adjust timestamps to the target date
        logger.debug(f"Adjusting timestamps from {source_date.strftime('%Y-%m-%d')} to {target_date.strftime('%Y-%m-%d')}")
        adjusted_df = adjust_timestamps(fallback_df, product, source_date, target_date)
        
        # Log successful fallback retrieval
        log_fallback_retrieval(product, target_date, source_date, metadata)
        
        elapsed_time = time.time() - start_time
        logger.info(
            f"Successfully retrieved and adjusted fallback forecast for {product} "
            f"(source: {source_date.strftime('%Y-%m-%d')}, "
            f"target: {target_date.strftime('%Y-%m-%d')}) "
            f"in {elapsed_time:.2f} seconds"
        )
        
        return adjusted_df
        
    except NoFallbackAvailableError as e:
        # Log the error and re-raise
        log_fallback_error("retrieval", e, {
            "product": product,
            "target_date": target_date.strftime('%Y-%m-%d'),
            "max_search_days": max_search_days
        })
        raise
        
    except Exception as e:
        # Catch any other errors, log them, and wrap in FallbackRetrievalError
        log_fallback_error("retrieval", e, {
            "product": product,
            "target_date": target_date.strftime('%Y-%m-%d')
        })
        raise FallbackRetrievalError(
            f"Failed to retrieve fallback forecast: {str(e)}", 
            product, 
            target_date, 
            e
        )


def find_suitable_fallback(
    product: str, 
    target_date: datetime.datetime, 
    max_search_days: int
) -> Tuple[datetime.datetime, Dict]:
    """
    Searches for a suitable previous forecast to use as fallback.
    
    Args:
        product: The price product to find a fallback for
        target_date: The target date for which a forecast is needed
        max_search_days: Maximum number of days to search backward
        
    Returns:
        Tuple of (source_date, metadata) for the suitable fallback
        
    Raises:
        NoFallbackAvailableError: If no suitable fallback can be found
    """
    # Ensure target_date is in CST timezone
    target_date = localize_to_cst(target_date)
    
    # Start with the previous day
    search_date = get_previous_day_date(target_date)
    
    logger.debug(f"Searching for fallback forecast for {product} starting from {search_date.strftime('%Y-%m-%d')}")
    
    # Try each day up to max_search_days
    for days_back in range(1, max_search_days + 1):
        logger.debug(f"Checking for forecast on day -{days_back}: {search_date.strftime('%Y-%m-%d')}")
        
        # Check if a forecast exists for this date
        if check_forecast_availability(search_date, product):
            # Get metadata about the forecast
            forecast_metadata = get_forecast_info(search_date, product)
            
            # Check if this forecast is suitable for use as a fallback
            if is_forecast_suitable(forecast_metadata, allow_fallback_cascading=True):
                logger.info(
                    f"Found suitable fallback forecast for {product} "
                    f"from {search_date.strftime('%Y-%m-%d')} "
                    f"({days_back} days before target)"
                )
                
                # Prepare metadata about the fallback
                fallback_metadata = get_fallback_metadata(forecast_metadata, search_date, target_date)
                
                return search_date, fallback_metadata
            else:
                logger.debug(
                    f"Forecast for {product} on {search_date.strftime('%Y-%m-%d')} "
                    f"exists but is not suitable for use as fallback"
                )
        
        # Move to the previous day
        search_date = get_previous_day_date(search_date)
    
    # If we reach here, no suitable fallback was found
    logger.error(
        f"No suitable fallback forecast found for {product} "
        f"after searching {max_search_days} days before {target_date.strftime('%Y-%m-%d')}"
    )
    
    raise NoFallbackAvailableError(
        "No suitable fallback forecast found", 
        product, 
        target_date, 
        max_search_days
    )


def validate_fallback_parameters(product: str, target_date: datetime.datetime) -> bool:
    """
    Validates parameters for fallback retrieval.
    
    Args:
        product: The price product to retrieve a fallback for
        target_date: The target date for which a forecast is needed
        
    Returns:
        True if parameters are valid, False otherwise
    """
    # Check product
    if product is None:
        logger.error("Product cannot be None")
        return False
    
    if product not in FORECAST_PRODUCTS:
        logger.error(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")
        return False
    
    # Check target_date
    if target_date is None:
        logger.error("Target date cannot be None")
        return False
    
    if not isinstance(target_date, datetime.datetime):
        logger.error(f"Target date must be a datetime object, got {type(target_date)}")
        return False
    
    return True


def is_forecast_suitable(forecast_metadata: Dict, allow_fallback_cascading: bool = True) -> bool:
    """
    Determines if a forecast is suitable for use as a fallback.
    
    Args:
        forecast_metadata: Metadata about the forecast to evaluate
        allow_fallback_cascading: Whether to allow using a fallback of a fallback
        
    Returns:
        True if the forecast is suitable for use as a fallback, False otherwise
    """
    # Check if metadata is valid
    if not forecast_metadata:
        logger.warning("Cannot evaluate empty forecast metadata")
        return False
    
    # Check if this is already a fallback and we don't allow cascading
    if not allow_fallback_cascading and forecast_metadata.get('is_fallback', False):
        logger.debug("Forecast is already a fallback and cascading is not allowed")
        return False
    
    # Check for required metadata fields
    required_fields = ['timestamp', 'product', 'generation_timestamp']
    for field in required_fields:
        if field not in forecast_metadata:
            logger.warning(f"Forecast metadata missing required field: {field}")
            return False
    
    # Check that the forecast has sufficient horizon coverage
    # This could be more sophisticated based on actual requirements
    if 'horizon_hours' in forecast_metadata:
        horizon_hours = forecast_metadata['horizon_hours']
        if horizon_hours < 24:  # For example, require at least 24 hours
            logger.warning(f"Forecast has insufficient horizon coverage: {horizon_hours} hours")
            return False
    
    # For now, all forecasts that pass these basic checks are considered suitable
    return True


def get_fallback_metadata(
    forecast_metadata: Dict, 
    source_date: datetime.datetime, 
    target_date: datetime.datetime
) -> Dict:
    """
    Extracts and formats metadata about a fallback forecast.
    
    Args:
        forecast_metadata: Metadata about the source forecast
        source_date: The date of the source forecast
        target_date: The target date for the fallback
        
    Returns:
        Formatted metadata dictionary
    """
    # Calculate age of fallback in days
    fallback_age_days = (target_date.date() - source_date.date()).days
    
    # Determine if this is a cascading fallback (fallback of a fallback)
    is_cascading = forecast_metadata.get('is_fallback', False)
    
    # Extract generation timestamp if available
    generation_timestamp = forecast_metadata.get('generation_timestamp', 'unknown')
    generation_str = generation_timestamp
    if isinstance(generation_timestamp, datetime.datetime):
        generation_str = generation_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    # Prepare metadata dictionary
    metadata = {
        'source_date': source_date.strftime('%Y-%m-%d'),
        'target_date': target_date.strftime('%Y-%m-%d'),
        'fallback_age_days': fallback_age_days,
        'is_cascading_fallback': is_cascading,
        'original_generation_time': generation_str
    }
    
    # Add any other relevant fields from the original metadata
    for key in ['horizon_hours', 'product']:
        if key in forecast_metadata:
            metadata[key] = forecast_metadata[key]
    
    return metadata