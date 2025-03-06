"""
Module for fetching, validating, and processing generation forecast data from external sources for the 
Electricity Market Price Forecasting System. Provides functionality to retrieve generation forecasts 
by fuel type, which is a critical input for the forecasting models.
"""

# External imports
import pandas as pd  # version: 2.0.0+
import datetime
from typing import Dict, List, Any, Optional
import time

# Internal imports
from .api_client import APIClient, fetch_data
from .exceptions import (
    DataValidationError, 
    APIConnectionError, 
    APIResponseError, 
    MissingDataError
)
from .data_validator import validate_generation_forecast_data
from .data_transformer import normalize_generation_forecast_data, pivot_generation_data
from ..models.data_models import GenerationForecast
from ..utils.date_utils import localize_to_cst
from ..utils.logging_utils import ComponentLogger, log_execution_time
from ..config.settings import DATA_SOURCES

# Global variables
logger = ComponentLogger('generation_forecast', {'component': 'data_ingestion'})
SOURCE_NAME = "generation_forecast"

@log_execution_time
def fetch_generation_forecast(
    start_date: datetime.datetime, 
    end_date: datetime.datetime,
    additional_params: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Fetches generation forecast data from the external API.
    
    Args:
        start_date: Start date for the forecast period
        end_date: End date for the forecast period
        additional_params: Additional parameters for the API request
        
    Returns:
        DataFrame containing generation forecast data
        
    Raises:
        APIConnectionError: If connection to the API fails
        APIResponseError: If the API returns an error response
        DataValidationError: If the data fails validation
    """
    logger.log_start("Fetching generation forecast data", {
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat()
    })
    
    # Ensure dates are in CST timezone
    start_date = localize_to_cst(start_date)
    end_date = localize_to_cst(end_date)
    
    # Initialize empty params dict if None provided
    params = additional_params or {}
    
    try:
        # Fetch data from the API
        response_data = fetch_data(
            SOURCE_NAME, 
            params=params, 
            start_date=start_date, 
            end_date=end_date
        )
        
        # Convert API response to DataFrame
        df = pd.DataFrame(response_data.get('data', []))
        
        if df.empty:
            logger.log_data_event("received_empty", df, {"source": SOURCE_NAME})
            return df
        
        # Validate the data
        validation_result = validate_generation_forecast_data(df)
        if not validation_result.is_valid:
            error_message = f"Generation forecast data validation failed: {validation_result.format_errors()}"
            logger.log_failure("Data validation", time.time(), DataValidationError(SOURCE_NAME, list(validation_result.errors.keys())), {
                'source': SOURCE_NAME
            })
            raise DataValidationError(SOURCE_NAME, list(validation_result.errors.keys()))
        
        # Normalize the data
        normalized_df = normalize_generation_forecast_data(df)
        
        logger.log_data_event("received", normalized_df, {
            'source': SOURCE_NAME,
            'rows': len(normalized_df),
            'start_date': normalized_df['timestamp'].min(),
            'end_date': normalized_df['timestamp'].max()
        })
        
        return normalized_df
        
    except APIConnectionError as e:
        logger.log_failure("API connection", time.time(), e, {
            'source': SOURCE_NAME,
            'api_endpoint': e.api_endpoint
        })
        raise
        
    except APIResponseError as e:
        logger.log_failure("API response", time.time(), e, {
            'source': SOURCE_NAME,
            'status_code': e.status_code
        })
        raise
        
    except Exception as e:
        logger.log_failure("Generation forecast fetching", time.time(), e, {
            'source': SOURCE_NAME
        })
        raise

@log_execution_time
def get_generation_forecast_by_fuel_type(
    start_date: datetime.datetime, 
    end_date: datetime.datetime,
    fuel_types: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Retrieves generation forecast data organized by fuel type.
    
    Args:
        start_date: Start date for the forecast period
        end_date: End date for the forecast period
        fuel_types: Optional list of specific fuel types to include
        
    Returns:
        Pivoted DataFrame with fuel types as columns
        
    Raises:
        APIConnectionError: If connection to the API fails
        APIResponseError: If the API returns an error response
        DataValidationError: If the data fails validation
    """
    logger.log_start("Getting generation forecast by fuel type", {
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'fuel_types': fuel_types
    })
    
    try:
        # Fetch raw generation forecast data
        gen_forecast_df = fetch_generation_forecast(start_date, end_date)
        
        if gen_forecast_df.empty:
            logger.log_data_event("empty_result", gen_forecast_df, {
                'source': SOURCE_NAME
            })
            return gen_forecast_df
        
        # Filter by fuel types if specified
        if fuel_types:
            gen_forecast_df = gen_forecast_df[gen_forecast_df['fuel_type'].isin(fuel_types)]
            
            if gen_forecast_df.empty:
                logger.log_data_event("empty_after_filtering", gen_forecast_df, {
                    'source': SOURCE_NAME,
                    'fuel_types': fuel_types
                })
                return gen_forecast_df
        
        # Pivot the data to create columns for each fuel type
        pivoted_df = pivot_generation_data(gen_forecast_df)
        
        logger.log_data_event("processed", pivoted_df, {
            'source': SOURCE_NAME,
            'rows': len(pivoted_df),
            'columns': list(pivoted_df.columns)
        })
        
        return pivoted_df
        
    except Exception as e:
        logger.log_failure("Generation forecast by fuel type", time.time(), e, {
            'source': SOURCE_NAME
        })
        raise

def get_fuel_type_list(df: pd.DataFrame) -> List[str]:
    """
    Retrieves a list of available fuel types from the generation forecast data.
    
    Args:
        df: DataFrame containing generation forecast data
        
    Returns:
        List of unique fuel types
    """
    # Check if DataFrame is None or empty
    if df is None or df.empty:
        return []
    
    # Extract unique fuel types
    if 'fuel_type' in df.columns:
        fuel_types = sorted(df['fuel_type'].unique().tolist())
        return fuel_types
    
    return []

def calculate_total_generation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates total generation across all fuel types for each timestamp.
    
    Args:
        df: DataFrame containing generation forecast data
        
    Returns:
        DataFrame with total generation column added
    """
    # Check if DataFrame is None or empty
    if df is None or df.empty:
        return pd.DataFrame()
    
    # Group by timestamp and sum generation_mw
    if 'timestamp' in df.columns and 'generation_mw' in df.columns:
        total_gen = df.groupby('timestamp')['generation_mw'].sum().reset_index()
        total_gen = total_gen.rename(columns={'generation_mw': 'total_generation_mw'})
        return total_gen
    
    return pd.DataFrame()

class GenerationForecastClient:
    """
    Client for fetching generation forecast data from external API.
    """
    
    def __init__(self):
        """
        Initializes the generation forecast client.
        """
        self._api_client = APIClient(SOURCE_NAME)
        self._logger = ComponentLogger('generation_forecast_client', {'component': 'data_ingestion'})
    
    def fetch_data(
        self, 
        start_date: datetime.datetime, 
        end_date: datetime.datetime,
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Fetches generation forecast data for a specified date range.
        
        Args:
            start_date: Start date for the forecast period
            end_date: End date for the forecast period
            params: Additional parameters for the API request
            
        Returns:
            DataFrame containing generation forecast data
            
        Raises:
            APIConnectionError: If connection to the API fails
            APIResponseError: If the API returns an error response
            DataValidationError: If the data fails validation
        """
        self._logger.log_start("Fetching generation forecast data", {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat()
        })
        
        try:
            # Use the module-level function for consistency
            return fetch_generation_forecast(start_date, end_date, params)
            
        except Exception as e:
            # Exceptions are already logged by fetch_generation_forecast
            raise
    
    def get_by_fuel_type(
        self, 
        start_date: datetime.datetime, 
        end_date: datetime.datetime,
        fuel_types: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Retrieves generation forecast data organized by fuel type.
        
        Args:
            start_date: Start date for the forecast period
            end_date: End date for the forecast period
            fuel_types: Optional list of specific fuel types to include
            
        Returns:
            Pivoted DataFrame with fuel types as columns
        """
        self._logger.log_start("Getting generation forecast by fuel type", {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'fuel_types': fuel_types
        })
        
        try:
            # Use the module-level function for consistency
            return get_generation_forecast_by_fuel_type(start_date, end_date, fuel_types)
            
        except Exception as e:
            # Exceptions are already logged by get_generation_forecast_by_fuel_type
            raise
    
    def get_available_fuel_types(self, reference_date: datetime.datetime) -> List[str]:
        """
        Retrieves a list of available fuel types.
        
        Args:
            reference_date: Reference date to fetch a sample around
            
        Returns:
            List of available fuel types
        """
        try:
            # Fetch a small sample of data (just one day) around the reference date
            start_date = reference_date - datetime.timedelta(hours=12)
            end_date = reference_date + datetime.timedelta(hours=12)
            
            sample_df = self.fetch_data(start_date, end_date)
            
            # Extract unique fuel types using the module-level function
            return get_fuel_type_list(sample_df)
            
        except Exception as e:
            self._logger.log_failure("Getting available fuel types", time.time(), e, {
                'source': SOURCE_NAME
            })
            return []
    
    def get_total_generation(
        self, 
        start_date: datetime.datetime, 
        end_date: datetime.datetime
    ) -> pd.DataFrame:
        """
        Retrieves total generation across all fuel types.
        
        Args:
            start_date: Start date for the forecast period
            end_date: End date for the forecast period
            
        Returns:
            DataFrame with total generation by timestamp
        """
        try:
            # Fetch raw generation forecast data
            gen_forecast_df = self.fetch_data(start_date, end_date)
            
            if gen_forecast_df.empty:
                return pd.DataFrame()
            
            # Calculate total generation using the module-level function
            return calculate_total_generation(gen_forecast_df)
            
        except Exception as e:
            self._logger.log_failure("Getting total generation", time.time(), e, {
                'source': SOURCE_NAME
            })
            raise