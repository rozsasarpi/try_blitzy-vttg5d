"""
Service module that provides a high-level API for accessing electricity market price forecasts.

Acts as an abstraction layer between the web visualization components and the backend data sources,
handling caching, error management, and data transformation to ensure consistent forecast data delivery.
"""

import pandas as pd  # version 2.0.0+
import logging
import datetime
from typing import Dict, List, Optional, Union, Any
import functools

from ..config.settings import (
    API_BASE_URL,
    FORECAST_API_TIMEOUT,
    CACHE_ENABLED,
    MAX_FORECAST_DAYS
)
from ..data.forecast_client import ForecastClient
from ..data.cache_manager import forecast_cache_manager
from ..data.schema import (
    prepare_dataframe_for_visualization,
    validate_forecast_dataframe,
    add_unit_information
)
from ..utils.error_handlers import (
    handle_data_loading_error,
    is_fallback_data
)
from ..utils.url_helpers import build_api_url

# Set up logger
logger = logging.getLogger(__name__)

# Default percentiles for forecast uncertainty bands
DEFAULT_PERCENTILES = [10, 90]

# Create a shared forecast client instance
_forecast_client = ForecastClient(API_BASE_URL, FORECAST_API_TIMEOUT)


def get_forecast_by_date(
    product: str,
    date: Union[str, datetime.date, datetime.datetime],
    percentiles: Optional[List[int]] = None,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    Retrieves and prepares forecast data for a specific date and product.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        date: The date to retrieve the forecast for
        percentiles: Optional list of percentiles for uncertainty bands [lower, upper]
        use_cache: Whether to use cached data if available
        
    Returns:
        Prepared forecast dataframe ready for visualization
    """
    try:
        logger.info(f"Retrieving forecast for product {product} on date {date}")
        
        # Use cache if enabled and requested
        if CACHE_ENABLED and use_cache:
            # Convert date to string if needed
            if isinstance(date, (datetime.date, datetime.datetime)):
                date_str = date.strftime("%Y-%m-%d")
            else:
                date_str = str(date)
                
            # Try to get from cache first
            df = forecast_cache_manager.get_forecast(product, date=date_str)
            if df is not None:
                logger.info(f"Using cached forecast for {product} on {date_str}")
                # Process the dataframe for visualization
                return process_forecast_data(df, percentiles)
        
        # Not in cache or cache disabled, fetch from API
        df = _forecast_client.get_forecast_by_date(product, date)
        
        # Process the dataframe for visualization
        result_df = process_forecast_data(df, percentiles)
        
        # Cache the result if caching is enabled
        if CACHE_ENABLED and use_cache:
            if isinstance(date, (datetime.date, datetime.datetime)):
                date_str = date.strftime("%Y-%m-%d")
            else:
                date_str = str(date)
                
            forecast_cache_manager.cache_forecast(product, df, date=date_str)
            logger.info(f"Cached forecast for {product} on {date_str}")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Error retrieving forecast for {product} on {date}: {str(e)}")
        return handle_data_loading_error(e, f"product={product}, date={date}")


def get_latest_forecast(
    product: str,
    percentiles: Optional[List[int]] = None,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    Retrieves and prepares the latest forecast for a specific product.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        percentiles: Optional list of percentiles for uncertainty bands [lower, upper]
        use_cache: Whether to use cached data if available
        
    Returns:
        Prepared latest forecast dataframe ready for visualization
    """
    try:
        logger.info(f"Retrieving latest forecast for product {product}")
        
        # Use cache if enabled and requested
        if CACHE_ENABLED and use_cache:
            # Try to get from cache first
            df = forecast_cache_manager.get_forecast(product, date="latest")
            if df is not None:
                logger.info(f"Using cached latest forecast for {product}")
                # Process the dataframe for visualization
                return process_forecast_data(df, percentiles)
        
        # Not in cache or cache disabled, fetch from API
        df = _forecast_client.get_latest_forecast(product)
        
        # Process the dataframe for visualization
        result_df = process_forecast_data(df, percentiles)
        
        # Cache the result if caching is enabled
        if CACHE_ENABLED and use_cache:
            forecast_cache_manager.cache_forecast(product, df, date="latest")
            logger.info(f"Cached latest forecast for {product}")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Error retrieving latest forecast for {product}: {str(e)}")
        return handle_data_loading_error(e, f"product={product}, latest=True")


def get_forecast_range(
    product: str,
    start_date: Union[str, datetime.date, datetime.datetime],
    end_date: Union[str, datetime.date, datetime.datetime],
    percentiles: Optional[List[int]] = None,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    Retrieves and prepares forecast data for a date range and product.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        start_date: The start date for the forecast range
        end_date: The end date for the forecast range
        percentiles: Optional list of percentiles for uncertainty bands [lower, upper]
        use_cache: Whether to use cached data if available
        
    Returns:
        Prepared forecast dataframe for date range ready for visualization
    """
    try:
        # Convert dates to strings if needed
        if isinstance(start_date, (datetime.date, datetime.datetime)):
            start_str = start_date.strftime("%Y-%m-%d")
        else:
            start_str = str(start_date)
            
        if isinstance(end_date, (datetime.date, datetime.datetime)):
            end_str = end_date.strftime("%Y-%m-%d")
        else:
            end_str = str(end_date)
            
        logger.info(f"Retrieving forecast range for product {product} from {start_str} to {end_str}")
        
        # Use cache if enabled and requested
        if CACHE_ENABLED and use_cache:
            # Try to get from cache first
            df = forecast_cache_manager.get_forecast(product, date=start_str, end_date=end_str)
            if df is not None:
                logger.info(f"Using cached forecast range for {product} from {start_str} to {end_str}")
                # Process the dataframe for visualization
                return process_forecast_data(df, percentiles)
        
        # Not in cache or cache disabled, fetch from API
        df = _forecast_client.get_forecasts_by_date_range(product, start_date, end_date)
        
        # Process the dataframe for visualization
        result_df = process_forecast_data(df, percentiles)
        
        # Cache the result if caching is enabled
        if CACHE_ENABLED and use_cache:
            forecast_cache_manager.cache_forecast(product, df, date=start_str, end_date=end_str)
            logger.info(f"Cached forecast range for {product} from {start_str} to {end_str}")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Error retrieving forecast range for {product} from {start_date} to {end_date}: {str(e)}")
        return handle_data_loading_error(e, f"product={product}, date_range={start_date} to {end_date}")


def check_api_health() -> bool:
    """
    Checks if the forecast API is available and responding.
    
    Returns:
        True if API is healthy, False otherwise
    """
    try:
        return _forecast_client.check_api_health()
    except Exception as e:
        logger.error(f"API health check failed: {str(e)}")
        return False


def clear_cache(product: Optional[str] = None) -> int:
    """
    Clears the forecast cache for all products or a specific product.
    
    Args:
        product: If provided, only clears cache for this product
        
    Returns:
        Number of cache entries cleared
    """
    count = forecast_cache_manager.clear_cache(product)
    if product:
        logger.info(f"Cleared forecast cache for product {product}: {count} entries removed")
    else:
        logger.info(f"Cleared all forecast cache: {count} entries removed")
    return count


def get_cache_stats() -> Dict[str, Any]:
    """
    Returns statistics about the forecast cache usage.
    
    Returns:
        Dictionary containing cache statistics
    """
    return forecast_cache_manager.get_stats()


def process_forecast_data(df: pd.DataFrame, percentiles: Optional[List[int]] = None) -> pd.DataFrame:
    """
    Processes raw forecast data into visualization-ready format.
    
    Args:
        df: Raw forecast dataframe
        percentiles: Optional list of percentiles for uncertainty bands [lower, upper]
        
    Returns:
        Processed dataframe ready for visualization
    """
    try:
        # Use default percentiles if none provided
        if percentiles is None:
            percentiles = DEFAULT_PERCENTILES
            
        # Validate the dataframe
        is_valid, errors = validate_forecast_dataframe(df)
        if not is_valid:
            logger.warning(f"Invalid forecast dataframe: {errors}")
            
        # Transform for visualization
        vis_df = prepare_dataframe_for_visualization(df, percentiles)
        
        # Add unit information
        vis_df = add_unit_information(vis_df)
        
        return vis_df
    except Exception as e:
        logger.error(f"Error processing forecast data: {str(e)}")
        raise


def is_using_fallback(df: pd.DataFrame) -> bool:
    """
    Checks if the current forecast data is from the fallback mechanism.
    
    Args:
        df: Forecast dataframe
        
    Returns:
        True if using fallback data, False otherwise
    """
    if "is_fallback" in df.columns:
        return df["is_fallback"].any()
    return False


class ForecastService:
    """
    Service class that provides access to electricity market price forecasts.
    
    This class provides an abstraction layer between the web visualization components
    and the backend data sources, handling caching, error management, and data
    transformation to ensure consistent forecast data delivery.
    """
    
    def __init__(self, client: Optional[ForecastClient] = None):
        """
        Initializes the ForecastService with a client and cache manager.
        
        Args:
            client: Optional ForecastClient instance to use. If None, creates a new one.
        """
        self._client = client or ForecastClient(API_BASE_URL, FORECAST_API_TIMEOUT)
        self.logger = logging.getLogger(__name__ + '.ForecastService')
        self.logger.info("Initialized ForecastService")
    
    def get_forecast_by_date(
        self,
        product: str,
        date: Union[str, datetime.date, datetime.datetime],
        percentiles: Optional[List[int]] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Retrieves and prepares forecast data for a specific date and product.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            date: The date to retrieve the forecast for
            percentiles: Optional list of percentiles for uncertainty bands [lower, upper]
            use_cache: Whether to use cached data if available
            
        Returns:
            Prepared forecast dataframe ready for visualization
        """
        try:
            self.logger.info(f"Retrieving forecast for product {product} on date {date}")
            
            # Use cache if enabled and requested
            if CACHE_ENABLED and use_cache:
                # Convert date to string if needed
                if isinstance(date, (datetime.date, datetime.datetime)):
                    date_str = date.strftime("%Y-%m-%d")
                else:
                    date_str = str(date)
                    
                # Try to get from cache first
                df = forecast_cache_manager.get_forecast(product, date=date_str)
                if df is not None:
                    self.logger.info(f"Using cached forecast for {product} on {date_str}")
                    # Process the dataframe for visualization
                    return process_forecast_data(df, percentiles)
            
            # Not in cache or cache disabled, fetch from API
            df = self._client.get_forecast_by_date(product, date)
            
            # Process the dataframe for visualization
            result_df = process_forecast_data(df, percentiles)
            
            # Cache the result if caching is enabled
            if CACHE_ENABLED and use_cache:
                if isinstance(date, (datetime.date, datetime.datetime)):
                    date_str = date.strftime("%Y-%m-%d")
                else:
                    date_str = str(date)
                    
                forecast_cache_manager.cache_forecast(product, df, date=date_str)
                self.logger.info(f"Cached forecast for {product} on {date_str}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error retrieving forecast for {product} on {date}: {str(e)}")
            return handle_data_loading_error(e, f"product={product}, date={date}")
    
    def get_latest_forecast(
        self,
        product: str,
        percentiles: Optional[List[int]] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Retrieves and prepares the latest forecast for a specific product.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            percentiles: Optional list of percentiles for uncertainty bands [lower, upper]
            use_cache: Whether to use cached data if available
            
        Returns:
            Prepared latest forecast dataframe ready for visualization
        """
        try:
            self.logger.info(f"Retrieving latest forecast for product {product}")
            
            # Use cache if enabled and requested
            if CACHE_ENABLED and use_cache:
                # Try to get from cache first
                df = forecast_cache_manager.get_forecast(product, date="latest")
                if df is not None:
                    self.logger.info(f"Using cached latest forecast for {product}")
                    # Process the dataframe for visualization
                    return process_forecast_data(df, percentiles)
            
            # Not in cache or cache disabled, fetch from API
            df = self._client.get_latest_forecast(product)
            
            # Process the dataframe for visualization
            result_df = process_forecast_data(df, percentiles)
            
            # Cache the result if caching is enabled
            if CACHE_ENABLED and use_cache:
                forecast_cache_manager.cache_forecast(product, df, date="latest")
                self.logger.info(f"Cached latest forecast for {product}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error retrieving latest forecast for {product}: {str(e)}")
            return handle_data_loading_error(e, f"product={product}, latest=True")
    
    def get_forecast_range(
        self,
        product: str,
        start_date: Union[str, datetime.date, datetime.datetime],
        end_date: Union[str, datetime.date, datetime.datetime],
        percentiles: Optional[List[int]] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Retrieves and prepares forecast data for a date range and product.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            start_date: The start date for the forecast range
            end_date: The end date for the forecast range
            percentiles: Optional list of percentiles for uncertainty bands [lower, upper]
            use_cache: Whether to use cached data if available
            
        Returns:
            Prepared forecast dataframe for date range ready for visualization
        """
        try:
            # Convert dates to strings if needed
            if isinstance(start_date, (datetime.date, datetime.datetime)):
                start_str = start_date.strftime("%Y-%m-%d")
            else:
                start_str = str(start_date)
                
            if isinstance(end_date, (datetime.date, datetime.datetime)):
                end_str = end_date.strftime("%Y-%m-%d")
            else:
                end_str = str(end_date)
                
            self.logger.info(f"Retrieving forecast range for product {product} from {start_str} to {end_str}")
            
            # Use cache if enabled and requested
            if CACHE_ENABLED and use_cache:
                # Try to get from cache first
                df = forecast_cache_manager.get_forecast(product, date=start_str, end_date=end_str)
                if df is not None:
                    self.logger.info(f"Using cached forecast range for {product} from {start_str} to {end_str}")
                    # Process the dataframe for visualization
                    return process_forecast_data(df, percentiles)
            
            # Not in cache or cache disabled, fetch from API
            df = self._client.get_forecasts_by_date_range(product, start_date, end_date)
            
            # Process the dataframe for visualization
            result_df = process_forecast_data(df, percentiles)
            
            # Cache the result if caching is enabled
            if CACHE_ENABLED and use_cache:
                forecast_cache_manager.cache_forecast(product, df, date=start_str, end_date=end_str)
                self.logger.info(f"Cached forecast range for {product} from {start_str} to {end_str}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error retrieving forecast range for {product} from {start_date} to {end_date}: {str(e)}")
            return handle_data_loading_error(e, f"product={product}, date_range={start_date} to {end_date}")
    
    def check_api_health(self) -> bool:
        """
        Checks if the forecast API is available and responding.
        
        Returns:
            True if API is healthy, False otherwise
        """
        try:
            return self._client.check_api_health()
        except Exception as e:
            self.logger.error(f"API health check failed: {str(e)}")
            return False
    
    def clear_cache(self, product: Optional[str] = None) -> int:
        """
        Clears the forecast cache for all products or a specific product.
        
        Args:
            product: If provided, only clears cache for this product
            
        Returns:
            Number of cache entries cleared
        """
        count = forecast_cache_manager.clear_cache(product)
        if product:
            self.logger.info(f"Cleared forecast cache for product {product}: {count} entries removed")
        else:
            self.logger.info(f"Cleared all forecast cache: {count} entries removed")
        return count
    
    def is_using_fallback(self, df: pd.DataFrame) -> bool:
        """
        Checks if the current forecast data is from the fallback mechanism.
        
        Args:
            df: Forecast dataframe
            
        Returns:
            True if using fallback data, False otherwise
        """
        return is_fallback_data(df)
    
    def close(self) -> None:
        """
        Closes the forecast client connection.
        """
        if self._client:
            self._client.close()
            self.logger.info("Closed forecast client connection")


# Create a singleton instance for application-wide use
forecast_service = ForecastService()