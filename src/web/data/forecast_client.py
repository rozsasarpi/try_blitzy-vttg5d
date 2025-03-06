"""
Client module for retrieving electricity market price forecasts from the backend API.
Provides a clean interface for fetching forecast data by date, date range, or latest
available, with appropriate error handling and request formatting.
"""
import logging
import requests  # version ^2.28.0
import pandas as pd  # version 2.0.0+
from datetime import date, datetime
from typing import Union, Dict, Any, Optional
import io  # standard library

from ..config.settings import API_BASE_URL, FORECAST_API_TIMEOUT
from ..config.product_config import PRODUCTS
from ..utils.url_helpers import build_api_url, build_forecast_api_url
from ..utils.error_handlers import handle_data_loading_error

# Set up logger
logger = logging.getLogger(__name__)

# Default response format
DEFAULT_FORMAT = "json"

# Default number of retries for API requests
DEFAULT_RETRIES = 3


class ForecastClient:
    """
    Client for retrieving forecast data from the backend API.
    """
    
    def __init__(self, base_url: str = None, timeout: int = None):
        """
        Initializes the ForecastClient with API settings.
        
        Args:
            base_url: Base URL for the forecast API
            timeout: Timeout for API requests in seconds
        """
        self.base_url = base_url or API_BASE_URL
        self.timeout = timeout or FORECAST_API_TIMEOUT
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'ElectricityMarketForecastClient/1.0'
        })
        self.logger = logging.getLogger(__name__ + '.ForecastClient')
        self.logger.info(f"Initialized ForecastClient with base URL: {self.base_url}")
    
    def get_forecast_by_date(
        self, 
        product: str, 
        date: Union[str, date, datetime], 
        format: str = DEFAULT_FORMAT
    ) -> pd.DataFrame:
        """
        Retrieves a forecast for a specific date and product.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            date: The date to retrieve the forecast for
            format: Response format (json, csv, excel, parquet)
            
        Returns:
            Forecast dataframe for the specified product and date
        """
        try:
            # Validate the product
            validate_product(product)
            
            # Convert date to string if needed
            if isinstance(date, (datetime, date)):
                date = date.strftime("%Y-%m-%d")
            
            # Build the API URL
            url = build_forecast_api_url(product, start_date=date, end_date=date)
            
            # Make the API request
            self.logger.info(f"Retrieving forecast for product: {product}, date: {date}")
            response = self.session.get(url, timeout=self.timeout)
            
            # Parse and return the response
            return self.parse_response(response, format)
        
        except Exception as e:
            self.logger.error(f"Error retrieving forecast for {product} on {date}: {e}")
            raise
    
    def get_latest_forecast(self, product: str, format: str = DEFAULT_FORMAT) -> pd.DataFrame:
        """
        Retrieves the latest forecast for a specific product.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            format: Response format (json, csv, excel, parquet)
            
        Returns:
            Latest forecast dataframe for the specified product
        """
        try:
            # Validate the product
            validate_product(product)
            
            # Build the API URL for latest forecast
            url = build_api_url(f"forecasts/{product}/latest")
            
            # Make the API request
            self.logger.info(f"Retrieving latest forecast for product: {product}")
            response = self.session.get(url, timeout=self.timeout)
            
            # Parse and return the response
            return self.parse_response(response, format)
        
        except Exception as e:
            self.logger.error(f"Error retrieving latest forecast for {product}: {e}")
            raise
    
    def get_forecasts_by_date_range(
        self, 
        product: str, 
        start_date: Union[str, date, datetime], 
        end_date: Union[str, date, datetime], 
        format: str = DEFAULT_FORMAT
    ) -> pd.DataFrame:
        """
        Retrieves forecasts for a specific product within a date range.
        
        Args:
            product: The price product (e.g., 'DALMP', 'RTLMP')
            start_date: The start date for the forecast range
            end_date: The end date for the forecast range
            format: Response format (json, csv, excel, parquet)
            
        Returns:
            Combined forecast dataframe for the specified product and date range
        """
        try:
            # Validate the product
            validate_product(product)
            
            # Convert dates to strings if needed
            if isinstance(start_date, (datetime, date)):
                start_date = start_date.strftime("%Y-%m-%d")
            if isinstance(end_date, (datetime, date)):
                end_date = end_date.strftime("%Y-%m-%d")
            
            # Build the API URL
            url = build_forecast_api_url(product, start_date=start_date, end_date=end_date)
            
            # Make the API request
            self.logger.info(f"Retrieving forecasts for product: {product}, date range: {start_date} to {end_date}")
            response = self.session.get(url, timeout=self.timeout)
            
            # Parse and return the response
            return self.parse_response(response, format)
        
        except Exception as e:
            self.logger.error(f"Error retrieving forecasts for {product} from {start_date} to {end_date}: {e}")
            raise
    
    def parse_response(self, response: requests.Response, format: str = DEFAULT_FORMAT) -> pd.DataFrame:
        """
        Parses API response based on the requested format.
        
        Args:
            response: API response object
            format: Format of the response (json, csv, excel, parquet)
            
        Returns:
            Parsed response as a pandas DataFrame
        """
        return parse_response(response, format)
    
    def check_api_health(self) -> bool:
        """
        Checks if the forecast API is available and responding.
        
        Returns:
            True if API is healthy, False otherwise
        """
        try:
            # Build health check endpoint URL
            url = build_api_url("health")
            
            # Make the API request
            self.logger.debug(f"Checking API health at: {url}")
            response = self.session.get(url, timeout=self.timeout)
            
            # Check if response is successful
            return response.status_code == 200
        
        except Exception as e:
            self.logger.error(f"API health check failed: {e}")
            return False
    
    def close(self) -> None:
        """
        Closes the client session.
        """
        self.session.close()
        self.logger.info("Closed ForecastClient session")


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


def parse_response(response: requests.Response, format: str = DEFAULT_FORMAT) -> pd.DataFrame:
    """
    Parses API response based on the requested format.
    
    Args:
        response: API response object
        format: Format of the response (json, csv, excel, parquet)
        
    Returns:
        Parsed response as a pandas DataFrame
    """
    if response.status_code != 200:
        logger.error(f"API request failed with status code: {response.status_code}")
        raise Exception(f"API request failed with status code: {response.status_code}. Response: {response.text}")
    
    try:
        if format.lower() == "json":
            return pd.DataFrame(response.json())
        
        elif format.lower() == "csv":
            return pd.read_csv(io.StringIO(response.text))
        
        elif format.lower() == "excel":
            return pd.read_excel(io.BytesIO(response.content))
        
        elif format.lower() == "parquet":
            return pd.read_parquet(io.BytesIO(response.content))
        
        else:
            logger.warning(f"Unsupported format: {format}, using default parser")
            return pd.DataFrame(response.json())
    
    except Exception as e:
        logger.error(f"Failed to parse API response: {e}")
        raise Exception(f"Failed to parse API response: {e}")


# Standalone functions that use the client
def get_forecast_by_date(
    product: str, 
    date: Union[str, date, datetime], 
    format: str = DEFAULT_FORMAT
) -> pd.DataFrame:
    """
    Retrieves a forecast for a specific date and product.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        date: The date to retrieve the forecast for
        format: Response format (json, csv, excel, parquet)
        
    Returns:
        Forecast dataframe for the specified product and date
    """
    client = ForecastClient()
    try:
        return client.get_forecast_by_date(product, date, format)
    finally:
        client.close()


def get_latest_forecast(product: str, format: str = DEFAULT_FORMAT) -> pd.DataFrame:
    """
    Retrieves the latest forecast for a specific product.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        format: Response format (json, csv, excel, parquet)
        
    Returns:
        Latest forecast dataframe for the specified product
    """
    client = ForecastClient()
    try:
        return client.get_latest_forecast(product, format)
    finally:
        client.close()


def get_forecasts_by_date_range(
    product: str, 
    start_date: Union[str, date, datetime], 
    end_date: Union[str, date, datetime], 
    format: str = DEFAULT_FORMAT
) -> pd.DataFrame:
    """
    Retrieves forecasts for a specific product within a date range.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        start_date: The start date for the forecast range
        end_date: The end date for the forecast range
        format: Response format (json, csv, excel, parquet)
        
    Returns:
        Combined forecast dataframe for the specified product and date range
    """
    client = ForecastClient()
    try:
        return client.get_forecasts_by_date_range(product, start_date, end_date, format)
    finally:
        client.close()


# Create a singleton instance for application-wide use
forecast_client = ForecastClient()