"""
Implements the core API functionality for retrieving and formatting electricity market price forecasts.
This module provides functions to access forecast data from the storage system, convert it to various
formats, and handle errors gracefully. It serves as the bridge between the storage layer and the API
endpoints defined in routes.py.
"""

import datetime
from typing import Union, List, Dict, Any, Optional
import pandas as pd
import io
import json

# Internal imports
from ..storage.storage_manager import (
    get_forecast,
    get_latest_forecast,
    get_forecasts_for_period,
    get_storage_info
)
from ..storage.exceptions import DataFrameNotFoundError
from ..models.forecast_models import (
    ProbabilisticForecast,
    ForecastEnsemble
)
from ..utils.date_utils import parse_timestamp, format_timestamp
from ..utils.logging_utils import get_logger, log_execution_time
from ..config.settings import FORECAST_PRODUCTS
from .exceptions import (
    ForecastRetrievalError,
    RequestValidationError,
    InvalidFormatError,
    ResourceNotFoundError
)

# Configure logger
logger = get_logger(__name__)

# Define supported output formats
SUPPORTED_FORMATS = ['json', 'csv', 'excel', 'parquet']


@log_execution_time
def get_forecast_by_date(date_str: str, product: str, format: str = 'json') -> Union[pd.DataFrame, dict]:
    """
    Retrieves a forecast for a specific date and product.
    
    Args:
        date_str: Date string in ISO format (YYYY-MM-DD)
        product: Price product identifier (e.g., DALMP, RTLMP)
        format: Output format (json, csv, excel, parquet)
        
    Returns:
        Forecast data in the requested format
        
    Raises:
        RequestValidationError: If product is invalid
        ResourceNotFoundError: If forecast is not found
        InvalidFormatError: If format is not supported
    """
    # Validate inputs
    validate_product(product)
    validate_format(format)
    
    try:
        # Parse date string to datetime
        date = parse_timestamp(date_str)
        
        # Get the forecast from storage
        df = get_forecast(date, product)
        
        logger.info(f"Retrieved forecast for {product} on {date_str}")
        
        # Return DataFrame or formatted data
        if format == 'json':
            return format_forecast_response(df, format)
        return df
    
    except DataFrameNotFoundError as e:
        logger.warning(f"Forecast not found for {product} on {date_str}")
        raise ResourceNotFoundError(
            f"No forecast found for {product} on {date_str}",
            "forecast",
            f"{product}_{date_str}"
        )
    except Exception as e:
        logger.error(f"Error retrieving forecast for {product} on {date_str}: {str(e)}")
        raise ForecastRetrievalError(f"Failed to retrieve forecast: {str(e)}", product, date)


@log_execution_time
def get_latest_forecast(product: str, format: str = 'json') -> Union[pd.DataFrame, dict]:
    """
    Retrieves the latest forecast for a product.
    
    Args:
        product: Price product identifier (e.g., DALMP, RTLMP)
        format: Output format (json, csv, excel, parquet)
        
    Returns:
        Latest forecast data in the requested format
        
    Raises:
        RequestValidationError: If product is invalid
        ResourceNotFoundError: If forecast is not found
        InvalidFormatError: If format is not supported
    """
    # Validate inputs
    validate_product(product)
    validate_format(format)
    
    try:
        # Get the latest forecast from storage
        df = get_latest_forecast(product)
        
        logger.info(f"Retrieved latest forecast for {product}")
        
        # Return DataFrame or formatted data
        if format == 'json':
            return format_forecast_response(df, format)
        return df
    
    except DataFrameNotFoundError as e:
        logger.warning(f"Latest forecast not found for {product}")
        raise ResourceNotFoundError(
            f"No latest forecast found for {product}",
            "forecast",
            f"latest_{product}"
        )
    except Exception as e:
        logger.error(f"Error retrieving latest forecast for {product}: {str(e)}")
        raise ForecastRetrievalError(f"Failed to retrieve latest forecast: {str(e)}", product)


@log_execution_time
def get_forecasts_by_date_range(
    start_date_str: str, 
    end_date_str: str, 
    product: str, 
    format: str = 'json'
) -> Union[pd.DataFrame, dict]:
    """
    Retrieves forecasts within a date range for a product.
    
    Args:
        start_date_str: Start date string in ISO format (YYYY-MM-DD)
        end_date_str: End date string in ISO format (YYYY-MM-DD)
        product: Price product identifier (e.g., DALMP, RTLMP)
        format: Output format (json, csv, excel, parquet)
        
    Returns:
        Forecast data for the date range in the requested format
        
    Raises:
        RequestValidationError: If product is invalid
        ResourceNotFoundError: If no forecasts are found
        InvalidFormatError: If format is not supported
    """
    # Validate inputs
    validate_product(product)
    validate_format(format)
    
    try:
        # Parse date strings to datetimes
        start_date = parse_timestamp(start_date_str)
        end_date = parse_timestamp(end_date_str)
        
        # Check that end date is not before start date
        if end_date < start_date:
            raise RequestValidationError(
                f"End date {end_date_str} cannot be before start date {start_date_str}",
                {"date_range": ["End date must be on or after start date"]}
            )
        
        # Get the forecasts from storage
        forecasts_dict = get_forecasts_for_period(start_date, end_date, product)
        
        # Combine results into a single DataFrame
        if not forecasts_dict:
            raise ResourceNotFoundError(
                f"No forecasts found for {product} between {start_date_str} and {end_date_str}",
                "forecast",
                f"{product}_{start_date_str}_to_{end_date_str}"
            )
        
        # Combine all dataframes into one
        dfs = list(forecasts_dict.values())
        combined_df = pd.concat(dfs, ignore_index=True)
        
        logger.info(f"Retrieved {len(combined_df)} forecast entries for {product} between {start_date_str} and {end_date_str}")
        
        # Return DataFrame or formatted data
        if format == 'json':
            return format_forecast_response(combined_df, format)
        return combined_df
    
    except ResourceNotFoundError:
        # Re-raise this exception type
        raise
    except DataFrameNotFoundError as e:
        logger.warning(f"No forecasts found for {product} between {start_date_str} and {end_date_str}")
        raise ResourceNotFoundError(
            f"No forecasts found for {product} between {start_date_str} and {end_date_str}",
            "forecast",
            f"{product}_{start_date_str}_to_{end_date_str}"
        )
    except RequestValidationError:
        # Re-raise this exception type
        raise
    except Exception as e:
        logger.error(f"Error retrieving forecasts for {product} between {start_date_str} and {end_date_str}: {str(e)}")
        raise ForecastRetrievalError(
            f"Failed to retrieve forecasts: {str(e)}", 
            product, 
            None, 
            (parse_timestamp(start_date_str), parse_timestamp(end_date_str))
        )


@log_execution_time
def get_forecast_as_model(date_str: str, product: str) -> List[ProbabilisticForecast]:
    """
    Retrieves a forecast as ProbabilisticForecast objects.
    
    Args:
        date_str: Date string in ISO format (YYYY-MM-DD)
        product: Price product identifier (e.g., DALMP, RTLMP)
        
    Returns:
        List of ProbabilisticForecast objects
        
    Raises:
        RequestValidationError: If product is invalid
        ResourceNotFoundError: If forecast is not found
    """
    # Validate inputs
    validate_product(product)
    
    try:
        # Retrieve the forecast data
        df = get_forecast_by_date(date_str, product, format='dataframe')
        
        # Convert each row to a ProbabilisticForecast object
        forecasts = []
        for _, row in df.iterrows():
            forecast = ProbabilisticForecast.from_dataframe_row(row)
            forecasts.append(forecast)
        
        logger.info(f"Retrieved {len(forecasts)} forecast models for {product} on {date_str}")
        return forecasts
    
    except ResourceNotFoundError:
        # Re-raise this exception type
        raise
    except Exception as e:
        logger.error(f"Error retrieving forecast models for {product} on {date_str}: {str(e)}")
        raise ForecastRetrievalError(f"Failed to retrieve forecast models: {str(e)}", product, parse_timestamp(date_str))


@log_execution_time
def get_latest_forecast_as_model(product: str) -> List[ProbabilisticForecast]:
    """
    Retrieves the latest forecast as ProbabilisticForecast objects.
    
    Args:
        product: Price product identifier (e.g., DALMP, RTLMP)
        
    Returns:
        List of ProbabilisticForecast objects
        
    Raises:
        RequestValidationError: If product is invalid
        ResourceNotFoundError: If forecast is not found
    """
    # Validate inputs
    validate_product(product)
    
    try:
        # Retrieve the latest forecast data
        df = get_latest_forecast(product, format='dataframe')
        
        # Convert each row to a ProbabilisticForecast object
        forecasts = []
        for _, row in df.iterrows():
            forecast = ProbabilisticForecast.from_dataframe_row(row)
            forecasts.append(forecast)
        
        logger.info(f"Retrieved {len(forecasts)} latest forecast models for {product}")
        return forecasts
    
    except ResourceNotFoundError:
        # Re-raise this exception type
        raise
    except Exception as e:
        logger.error(f"Error retrieving latest forecast models for {product}: {str(e)}")
        raise ForecastRetrievalError(f"Failed to retrieve latest forecast models: {str(e)}", product)


@log_execution_time
def get_forecast_ensemble(start_date_str: str, end_date_str: str, product: str) -> ForecastEnsemble:
    """
    Retrieves a forecast ensemble for a date range and product.
    
    Args:
        start_date_str: Start date string in ISO format (YYYY-MM-DD)
        end_date_str: End date string in ISO format (YYYY-MM-DD)
        product: Price product identifier (e.g., DALMP, RTLMP)
        
    Returns:
        Forecast ensemble for the date range
        
    Raises:
        RequestValidationError: If product is invalid
        ResourceNotFoundError: If no forecasts are found
    """
    # Validate inputs
    validate_product(product)
    
    try:
        # Parse date strings to datetimes
        start_date = parse_timestamp(start_date_str)
        end_date = parse_timestamp(end_date_str)
        
        # Retrieve forecasts for the date range
        df = get_forecasts_by_date_range(start_date_str, end_date_str, product, format='dataframe')
        
        # Create a ForecastEnsemble from the dataframe
        ensemble = ForecastEnsemble.from_dataframe(df)
        
        logger.info(f"Created forecast ensemble for {product} between {start_date_str} and {end_date_str}")
        return ensemble
    
    except ResourceNotFoundError:
        # Re-raise this exception type
        raise
    except Exception as e:
        logger.error(f"Error creating forecast ensemble for {product} between {start_date_str} and {end_date_str}: {str(e)}")
        raise ForecastRetrievalError(
            f"Failed to create forecast ensemble: {str(e)}", 
            product, 
            None, 
            (parse_timestamp(start_date_str), parse_timestamp(end_date_str))
        )


def format_forecast_response(df: pd.DataFrame, format: str) -> Union[dict, bytes, str]:
    """
    Formats forecast data in the requested format.
    
    Args:
        df: DataFrame containing forecast data
        format: Output format (json, csv, excel, parquet)
        
    Returns:
        Formatted forecast data
        
    Raises:
        InvalidFormatError: If format is not supported
    """
    # Validate the requested format
    validate_format(format)
    
    try:
        if format == 'json':
            # Convert to JSON (through dict to handle datetime serialization)
            result = df.to_dict(orient='records')
            return result
        
        elif format == 'csv':
            # Convert to CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            return csv_buffer.getvalue()
        
        elif format == 'excel':
            # Convert to Excel
            excel_buffer = io.BytesIO()
            df.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            return excel_buffer.getvalue()
        
        elif format == 'parquet':
            # Convert to Parquet
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            return parquet_buffer.getvalue()
        
        else:
            # This should not happen due to validate_format, but included for robustness
            raise InvalidFormatError(
                f"Unsupported format: {format}", 
                format, 
                SUPPORTED_FORMATS
            )
    
    except Exception as e:
        logger.error(f"Error formatting forecast data to {format}: {str(e)}")
        raise InvalidFormatError(
            f"Failed to format forecast data: {str(e)}", 
            format, 
            SUPPORTED_FORMATS
        )


def validate_product(product: str) -> bool:
    """
    Validates that a product is in the list of supported products.
    
    Args:
        product: Product identifier to validate
        
    Returns:
        True if valid, raises exception otherwise
        
    Raises:
        RequestValidationError: If product is invalid
    """
    if product not in FORECAST_PRODUCTS:
        raise RequestValidationError(
            f"Invalid product: {product}",
            {"product": [f"Must be one of {FORECAST_PRODUCTS}"]}
        )
    return True


def validate_format(format: str) -> bool:
    """
    Validates that a format is supported.
    
    Args:
        format: Format to validate
        
    Returns:
        True if valid, raises exception otherwise
        
    Raises:
        InvalidFormatError: If format is not supported
    """
    if format not in SUPPORTED_FORMATS:
        raise InvalidFormatError(
            f"Unsupported format: {format}",
            format,
            SUPPORTED_FORMATS
        )
    return True


@log_execution_time
def get_storage_status() -> dict:
    """
    Retrieves information about the forecast storage system.
    
    Returns:
        Storage system information and statistics
    """
    try:
        # Get storage information
        info = get_storage_info()
        
        logger.info("Retrieved storage system information")
        return info
    
    except Exception as e:
        logger.error(f"Error retrieving storage status: {str(e)}")
        raise ForecastRetrievalError(f"Failed to retrieve storage status: {str(e)}", "system")


class ForecastAPI:
    """
    Class that encapsulates forecast API functionality.
    
    This class provides a unified interface to the forecast API functions.
    """
    
    def __init__(self):
        """
        Initializes the ForecastAPI class.
        """
        self.logger = get_logger(__name__ + ".ForecastAPI")
    
    def get_forecast_by_date(self, date_str: str, product: str, format: str = 'json') -> Union[pd.DataFrame, dict]:
        """
        Retrieves a forecast for a specific date and product.
        
        Args:
            date_str: Date string in ISO format (YYYY-MM-DD)
            product: Price product identifier (e.g., DALMP, RTLMP)
            format: Output format (json, csv, excel, parquet)
            
        Returns:
            Forecast data in the requested format
        """
        return get_forecast_by_date(date_str, product, format)
    
    def get_latest_forecast(self, product: str, format: str = 'json') -> Union[pd.DataFrame, dict]:
        """
        Retrieves the latest forecast for a product.
        
        Args:
            product: Price product identifier (e.g., DALMP, RTLMP)
            format: Output format (json, csv, excel, parquet)
            
        Returns:
            Latest forecast data in the requested format
        """
        return get_latest_forecast(product, format)
    
    def get_forecasts_by_date_range(self, start_date_str: str, end_date_str: str, product: str, format: str = 'json') -> Union[pd.DataFrame, dict]:
        """
        Retrieves forecasts within a date range for a product.
        
        Args:
            start_date_str: Start date string in ISO format (YYYY-MM-DD)
            end_date_str: End date string in ISO format (YYYY-MM-DD)
            product: Price product identifier (e.g., DALMP, RTLMP)
            format: Output format (json, csv, excel, parquet)
            
        Returns:
            Forecast data for the date range in the requested format
        """
        return get_forecasts_by_date_range(start_date_str, end_date_str, product, format)
    
    def get_forecast_as_model(self, date_str: str, product: str) -> List[ProbabilisticForecast]:
        """
        Retrieves a forecast as ProbabilisticForecast objects.
        
        Args:
            date_str: Date string in ISO format (YYYY-MM-DD)
            product: Price product identifier (e.g., DALMP, RTLMP)
            
        Returns:
            List of ProbabilisticForecast objects
        """
        return get_forecast_as_model(date_str, product)
    
    def get_latest_forecast_as_model(self, product: str) -> List[ProbabilisticForecast]:
        """
        Retrieves the latest forecast as ProbabilisticForecast objects.
        
        Args:
            product: Price product identifier (e.g., DALMP, RTLMP)
            
        Returns:
            List of ProbabilisticForecast objects
        """
        return get_latest_forecast_as_model(product)
    
    def get_forecast_ensemble(self, start_date_str: str, end_date_str: str, product: str) -> ForecastEnsemble:
        """
        Retrieves a forecast ensemble for a date range and product.
        
        Args:
            start_date_str: Start date string in ISO format (YYYY-MM-DD)
            end_date_str: End date string in ISO format (YYYY-MM-DD)
            product: Price product identifier (e.g., DALMP, RTLMP)
            
        Returns:
            Forecast ensemble for the date range
        """
        return get_forecast_ensemble(start_date_str, end_date_str, product)
    
    def format_forecast_response(self, df: pd.DataFrame, format: str) -> Union[dict, bytes, str]:
        """
        Formats forecast data in the requested format.
        
        Args:
            df: DataFrame containing forecast data
            format: Output format (json, csv, excel, parquet)
            
        Returns:
            Formatted forecast data
        """
        return format_forecast_response(df, format)
    
    def get_storage_status(self) -> dict:
        """
        Retrieves information about the forecast storage system.
        
        Returns:
            Storage system information and statistics
        """
        return get_storage_status()