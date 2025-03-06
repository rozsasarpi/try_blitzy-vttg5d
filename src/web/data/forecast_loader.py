"""
Module responsible for loading electricity market price forecasts from various sources, including 
the backend API and local cache. Provides a unified interface for retrieving forecast data with 
appropriate error handling, caching, and data transformation for visualization purposes.
"""

import pandas as pd  # version 2.0.0+
import datetime
import logging
from typing import List, Dict, Optional, Union, Any, Tuple
import functools  # standard library

from .forecast_client import get_forecast_by_date, get_latest_forecast, get_forecasts_by_date_range
from .cache_manager import forecast_cache_manager
from .schema import prepare_dataframe_for_visualization, extract_samples_from_dataframe, validate_forecast_dataframe
from ..config.product_config import PRODUCTS, DEFAULT_PRODUCT
from ..config.settings import CACHE_ENABLED
from ..utils.error_handlers import handle_data_loading_error

# Set up module logger
logger = logging.getLogger(__name__)

# Default percentiles for uncertainty bands
DEFAULT_PERCENTILES = [10, 90]


def validate_product(product: str) -> bool:
    """
    Validates that a product is in the list of supported products.
    
    Args:
        product: Product identifier to validate
        
    Returns:
        True if valid, raises exception otherwise
    """
    if product not in PRODUCTS:
        logger.error(f"Invalid product: {product}")
        raise ValueError(f"Invalid product: {product}. Must be one of: {', '.join(PRODUCTS)}")
    return True


def load_forecast_by_date(
    product: str,
    date: Union[str, datetime.date, datetime.datetime],
    percentiles: Optional[List[int]] = None
) -> pd.DataFrame:
    """
    Loads forecast data for a specific date and product.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        date: The date to retrieve the forecast for
        percentiles: Optional list of percentiles to extract (default: [10, 90])
        
    Returns:
        Forecast dataframe for visualization
    """
    try:
        # Validate the product
        validate_product(product)
        
        # Check cache first if enabled
        if CACHE_ENABLED:
            cached_forecast = forecast_cache_manager.get_forecast(product, date)
            if cached_forecast is not None:
                logger.info(f"Using cached forecast for {product} on {date}")
                return prepare_dataframe_for_visualization(cached_forecast, percentiles or DEFAULT_PERCENTILES)
        
        # Not in cache, fetch from API
        logger.info(f"Fetching forecast from API for {product} on {date}")
        forecast_df = get_forecast_by_date(product, date)
        
        # Transform for visualization
        viz_df = prepare_dataframe_for_visualization(forecast_df, percentiles or DEFAULT_PERCENTILES)
        
        # Cache the raw forecast
        if CACHE_ENABLED:
            forecast_cache_manager.cache_forecast(product, forecast_df, date)
            logger.debug(f"Cached forecast for {product} on {date}")
        
        return viz_df
        
    except Exception as e:
        logger.error(f"Error loading forecast for {product} on {date}: {str(e)}")
        # Use handle_data_loading_error for consistent error handling across the application
        return handle_data_loading_error(e, f"loading forecast for {product} on {date}")


def load_latest_forecast(
    product: str,
    percentiles: Optional[List[int]] = None
) -> pd.DataFrame:
    """
    Loads the latest forecast for a specific product.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        percentiles: Optional list of percentiles to extract (default: [10, 90])
        
    Returns:
        Latest forecast dataframe for visualization
    """
    try:
        # Validate the product
        validate_product(product)
        
        # Check cache first if enabled
        if CACHE_ENABLED:
            cached_forecast = forecast_cache_manager.get_forecast(product, "latest")
            if cached_forecast is not None:
                logger.info(f"Using cached latest forecast for {product}")
                return prepare_dataframe_for_visualization(cached_forecast, percentiles or DEFAULT_PERCENTILES)
        
        # Not in cache, fetch from API
        logger.info(f"Fetching latest forecast from API for {product}")
        forecast_df = get_latest_forecast(product)
        
        # Transform for visualization
        viz_df = prepare_dataframe_for_visualization(forecast_df, percentiles or DEFAULT_PERCENTILES)
        
        # Cache the raw forecast
        if CACHE_ENABLED:
            forecast_cache_manager.cache_forecast(product, forecast_df)
            logger.debug(f"Cached latest forecast for {product}")
        
        return viz_df
        
    except Exception as e:
        logger.error(f"Error loading latest forecast for {product}: {str(e)}")
        return handle_data_loading_error(e, f"loading latest forecast for {product}")


def load_forecast_by_date_range(
    product: str,
    start_date: Union[str, datetime.date, datetime.datetime],
    end_date: Union[str, datetime.date, datetime.datetime],
    percentiles: Optional[List[int]] = None
) -> pd.DataFrame:
    """
    Loads forecast data for a specific product within a date range.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        start_date: The start date for the forecast range
        end_date: The end date for the forecast range
        percentiles: Optional list of percentiles to extract (default: [10, 90])
        
    Returns:
        Forecast dataframe for visualization covering the specified date range
    """
    try:
        # Validate the product
        validate_product(product)
        
        # Check cache first if enabled
        if CACHE_ENABLED:
            cached_forecast = forecast_cache_manager.get_forecast(product, start_date, end_date)
            if cached_forecast is not None:
                logger.info(f"Using cached forecast for {product} from {start_date} to {end_date}")
                return prepare_dataframe_for_visualization(cached_forecast, percentiles or DEFAULT_PERCENTILES)
        
        # Not in cache, fetch from API
        logger.info(f"Fetching forecast from API for {product} from {start_date} to {end_date}")
        forecast_df = get_forecasts_by_date_range(product, start_date, end_date)
        
        # Transform for visualization
        viz_df = prepare_dataframe_for_visualization(forecast_df, percentiles or DEFAULT_PERCENTILES)
        
        # Cache the raw forecast
        if CACHE_ENABLED:
            forecast_cache_manager.cache_forecast(product, forecast_df, start_date, end_date)
            logger.debug(f"Cached forecast for {product} from {start_date} to {end_date}")
        
        return viz_df
        
    except Exception as e:
        logger.error(f"Error loading forecast for {product} from {start_date} to {end_date}: {str(e)}")
        return handle_data_loading_error(e, f"loading forecast for {product} from {start_date} to {end_date}")


def extract_forecast_percentiles(
    df: pd.DataFrame,
    percentiles: Optional[List[int]] = None
) -> pd.DataFrame:
    """
    Extracts percentile values from a forecast dataframe.
    
    Args:
        df: Forecast dataframe
        percentiles: Optional list of percentiles to extract (default: [10, 90])
        
    Returns:
        Dataframe with extracted percentile values
    """
    try:
        if percentiles is None:
            percentiles = DEFAULT_PERCENTILES
            
        return extract_samples_from_dataframe(df, percentiles)
        
    except Exception as e:
        logger.error(f"Error extracting percentiles from forecast: {str(e)}")
        raise ValueError(f"Failed to extract percentiles: {str(e)}")


def get_forecast_metadata(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Extracts metadata from a forecast dataframe.
    
    Args:
        df: Forecast dataframe
        
    Returns:
        Dictionary containing forecast metadata (generation time, horizon, fallback status, etc.)
    """
    try:
        metadata = {}
        
        # Get forecast generation timestamp
        if 'generation_timestamp' in df.columns:
            metadata['generation_timestamp'] = df['generation_timestamp'].iloc[0]
        
        # Check if this is a fallback forecast
        if 'is_fallback' in df.columns:
            metadata['is_fallback'] = bool(df['is_fallback'].iloc[0])
        else:
            metadata['is_fallback'] = False
        
        # Get forecast horizon (time range)
        if 'timestamp' in df.columns:
            timestamps = df['timestamp'].sort_values()
            metadata['start_time'] = timestamps.iloc[0]
            metadata['end_time'] = timestamps.iloc[-1]
            metadata['hours'] = len(timestamps.unique())
        
        # Get products included
        if 'product' in df.columns:
            metadata['products'] = df['product'].unique().tolist()
        
        return metadata
        
    except Exception as e:
        logger.error(f"Error extracting forecast metadata: {str(e)}")
        return {'error': str(e)}


def get_available_forecast_dates(product: str) -> List[datetime.date]:
    """
    Retrieves a list of dates for which forecasts are available.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        
    Returns:
        List of available forecast dates
    """
    try:
        # Validate the product
        validate_product(product)
        
        # This would typically call an API endpoint to get available dates
        # In a real implementation, this would query the backend for actual availability
        today = datetime.date.today()
        
        # Return the last 7 days as available
        available_dates = [(today - datetime.timedelta(days=i)) for i in range(7)]
        
        logger.info(f"Retrieved {len(available_dates)} available forecast dates for {product}")
        return available_dates
        
    except Exception as e:
        logger.error(f"Error retrieving available forecast dates for {product}: {str(e)}")
        return handle_data_loading_error(e, f"retrieving available forecast dates for {product}")


def check_forecast_availability(
    product: str,
    date: Union[str, datetime.date, datetime.datetime]
) -> bool:
    """
    Checks if a forecast is available for a specific date and product.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        date: The date to check
        
    Returns:
        True if forecast is available, False otherwise
    """
    try:
        # Validate the product
        validate_product(product)
        
        # Convert date to datetime.date if it's a string
        if isinstance(date, str):
            parsed_date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
        elif isinstance(date, datetime.datetime):
            parsed_date = date.date()
        else:
            parsed_date = date
        
        # Get available dates
        available_dates = get_available_forecast_dates(product)
        
        # Check if the requested date is available
        is_available = parsed_date in available_dates
        
        logger.info(f"Forecast for {product} on {date} is {'available' if is_available else 'not available'}")
        return is_available
        
    except Exception as e:
        logger.error(f"Error checking forecast availability for {product} on {date}: {str(e)}")
        return False


class ForecastLoader:
    """
    Class that provides methods for loading forecast data from various sources.
    """
    
    def __init__(self):
        """
        Initializes the ForecastLoader.
        """
        self.logger = logging.getLogger(__name__ + '.ForecastLoader')
        self.logger.info("Initialized ForecastLoader")
    
    def load_forecast_by_date(
        self,
        product: str,
        date: Union[str, datetime.date, datetime.datetime],
        percentiles: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        Loads forecast data for a specific date and product.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            date: The date to retrieve the forecast for
            percentiles: Optional list of percentiles to extract
            
        Returns:
            Forecast dataframe for visualization
        """
        return load_forecast_by_date(product, date, percentiles)
    
    def load_latest_forecast(
        self,
        product: str,
        percentiles: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        Loads the latest forecast for a specific product.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            percentiles: Optional list of percentiles to extract
            
        Returns:
            Latest forecast dataframe for visualization
        """
        return load_latest_forecast(product, percentiles)
    
    def load_forecast_by_date_range(
        self,
        product: str,
        start_date: Union[str, datetime.date, datetime.datetime],
        end_date: Union[str, datetime.date, datetime.datetime],
        percentiles: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        Loads forecast data for a specific product within a date range.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            start_date: The start date for the forecast range
            end_date: The end date for the forecast range
            percentiles: Optional list of percentiles to extract
            
        Returns:
            Forecast dataframe for visualization
        """
        return load_forecast_by_date_range(product, start_date, end_date, percentiles)
    
    def extract_forecast_percentiles(
        self,
        df: pd.DataFrame,
        percentiles: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        Extracts percentile values from a forecast dataframe.
        
        Args:
            df: Forecast dataframe
            percentiles: Optional list of percentiles to extract
            
        Returns:
            Dataframe with extracted percentile values
        """
        return extract_forecast_percentiles(df, percentiles)
    
    def get_forecast_metadata(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extracts metadata from a forecast dataframe.
        
        Args:
            df: Forecast dataframe
            
        Returns:
            Dictionary containing forecast metadata
        """
        return get_forecast_metadata(df)
    
    def get_available_forecast_dates(self, product: str) -> List[datetime.date]:
        """
        Retrieves a list of dates for which forecasts are available.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            
        Returns:
            List of available forecast dates
        """
        return get_available_forecast_dates(product)
    
    def check_forecast_availability(
        self,
        product: str,
        date: Union[str, datetime.date, datetime.datetime]
    ) -> bool:
        """
        Checks if a forecast is available for a specific date and product.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            date: The date to check
            
        Returns:
            True if forecast is available, False otherwise
        """
        return check_forecast_availability(product, date)
    
    def clear_cache(self, product: Optional[str] = None) -> int:
        """
        Clears the forecast cache.
        
        Args:
            product: If provided, only clears cache for this product
            
        Returns:
            Number of cache entries cleared
        """
        count = forecast_cache_manager.clear_cache(product)
        self.logger.info(f"Cleared {count} cache entries for {product if product else 'all products'}")
        return count


# Create a singleton instance for application-wide use
forecast_loader = ForecastLoader()