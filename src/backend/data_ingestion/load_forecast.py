"""
Implements functionality for retrieving load forecast data from external sources for the Electricity Market Price Forecasting System. This module provides a client for connecting to load forecast APIs, fetching data for the required time range, validating the data structure, and transforming it into a standardized format suitable for the forecasting engine.
"""

# External imports
import pandas as pd  # version: 2.0.0+
import datetime
from typing import Dict, List, Any, Optional, Union
import time

# Internal imports
from .api_client import APIClient
from .exceptions import (
    DataIngestionError, 
    APIConnectionError, 
    APIResponseError, 
    MissingDataError
)
from .data_validator import validate_load_forecast_data
from .data_transformer import normalize_load_forecast_data
from ..models.data_models import LoadForecast
from ..config.settings import DATA_SOURCES, FORECAST_HORIZON_HOURS
from ..utils.date_utils import localize_to_cst, generate_forecast_date_range
from ..utils.logging_utils import (
    get_logger, 
    log_execution_time, 
    ComponentLogger
)

# Global variables
logger = get_logger(__name__)
LOAD_FORECAST_SOURCE = "load_forecast"

@log_execution_time
def fetch_load_forecast(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    additional_params: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Fetches load forecast data from the external API for a specified date range.
    
    Args:
        start_date: Start date for the forecast period
        end_date: End date for the forecast period
        additional_params: Optional parameters to pass to the API.
                         This can include filters, specific data fields, etc.
        
    Returns:
        DataFrame containing load forecast data
        
    Raises:
        DataIngestionError: If there is an error fetching or processing the data
    """
    logger.info(f"Fetching load forecast data from {start_date} to {end_date}")
    
    try:
        # Create client and get forecast
        client = LoadForecastClient()
        df = client.get_forecast(start_date, end_date, additional_params)
        
        # Validate the data
        validation_result = validate_load_forecast_data(df)
        if not validation_result.is_valid:
            logger.error(f"Load forecast validation failed: {validation_result.format_errors()}")
            raise DataIngestionError(f"Load forecast validation failed: {validation_result.format_errors()}")
        
        # Normalize the data
        normalized_df = normalize_load_forecast_data(df)
        
        # Validate the normalized data as well
        validation_result = validate_load_forecast_data(normalized_df)
        if not validation_result.is_valid:
            logger.error(f"Normalized load forecast validation failed: {validation_result.format_errors()}")
            raise DataIngestionError(f"Normalized load forecast validation failed: {validation_result.format_errors()}")
        
        logger.info(f"Successfully fetched and processed load forecast data with {len(normalized_df)} rows")
        
        # Log some basic statistics about the data
        if not normalized_df.empty and 'load_mw' in normalized_df.columns:
            min_load = normalized_df['load_mw'].min()
            max_load = normalized_df['load_mw'].max()
            avg_load = normalized_df['load_mw'].mean()
            logger.info(f"Load forecast statistics: min={min_load:.2f}, max={max_load:.2f}, avg={avg_load:.2f}")
        
        return normalized_df
        
    except Exception as e:
        if not isinstance(e, DataIngestionError):
            logger.error(f"Error fetching load forecast data: {str(e)}")
            raise DataIngestionError(f"Failed to fetch load forecast data: {str(e)}")
        raise

@log_execution_time
def get_load_forecast_for_horizon(
    start_date: datetime.datetime,
    horizon_hours: Optional[int] = None
) -> pd.DataFrame:
    """
    Retrieves load forecast data for the entire forecast horizon.
    
    Args:
        start_date: Start date for the forecast period
        horizon_hours: Number of hours in the forecast horizon (defaults to FORECAST_HORIZON_HOURS)
        
    Returns:
        DataFrame containing load forecast for the entire horizon
        
    Raises:
        DataIngestionError: If there is an error fetching or processing the data
    """
    logger.info(f"Retrieving load forecast for horizon from {start_date}")
    
    # Use default horizon if not specified
    if horizon_hours is None:
        horizon_hours = FORECAST_HORIZON_HOURS
        logger.debug(f"Using default horizon hours: {horizon_hours}")
    
    try:
        # Calculate end date
        start_date = localize_to_cst(start_date)
        end_date = start_date + datetime.timedelta(hours=horizon_hours)
        
        # Fetch the forecast data
        forecast_df = fetch_load_forecast(start_date, end_date)
        
        # Verify that we have data for the entire horizon
        if not forecast_df.empty and 'timestamp' in forecast_df.columns:
            # Convert to pandas datetime if not already
            forecast_df['timestamp'] = pd.to_datetime(forecast_df['timestamp'])
            
            # Generate the expected date range
            expected_dates = generate_forecast_date_range(start_date, horizon_hours)
            
            # Check for missing timestamps
            forecast_timestamps = set(pd.to_datetime(forecast_df['timestamp']).dt.tz_localize(None))
            expected_timestamps = set(pd.to_datetime(expected_dates).tz_localize(None))
            missing_timestamps = expected_timestamps - forecast_timestamps
            
            if missing_timestamps:
                logger.warning(f"Missing {len(missing_timestamps)} timestamps in horizon forecast")
                # Here we could implement gap filling, but for now just log the issue
        
        return forecast_df
        
    except Exception as e:
        if not isinstance(e, DataIngestionError):
            logger.error(f"Error retrieving load forecast for horizon: {str(e)}")
            raise DataIngestionError(f"Failed to retrieve load forecast for horizon: {str(e)}")
        raise

def create_api_client() -> APIClient:
    """
    Creates an API client for the load forecast data source.
    
    Returns:
        Configured API client for load forecast
        
    Raises:
        ValueError: If the load forecast configuration is not found
    """
    # Get load forecast configuration from settings
    if LOAD_FORECAST_SOURCE not in DATA_SOURCES:
        logger.error(f"Missing configuration for {LOAD_FORECAST_SOURCE}")
        raise ValueError(f"Configuration for {LOAD_FORECAST_SOURCE} not found in DATA_SOURCES")
    
    # Create and return an APIClient for load forecast
    return APIClient(LOAD_FORECAST_SOURCE)

class LoadForecastClient:
    """
    Client for retrieving load forecast data from external API.
    """
    
    def __init__(self):
        """Initializes the load forecast client."""
        self._api_client = create_api_client()
        self._logger = ComponentLogger('load_forecast', {'component': 'data_ingestion'})
    
    def get_forecast(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        additional_params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Retrieves load forecast data for a specified date range.
        
        Args:
            start_date: Start date for the forecast period
            end_date: End date for the forecast period
            additional_params: Optional parameters to pass to the API.
                             This can include filters, specific data fields, etc.
            
        Returns:
            DataFrame containing load forecast data
            
        Raises:
            APIConnectionError: If connection to the API fails
            APIResponseError: If the API returns an error response
            MissingDataError: If required data is missing from the response
            DataIngestionError: If there is an error processing the data
        """
        self._logger.log_start("Load forecast retrieval", {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat()
        })
        
        # Ensure dates are in CST timezone
        start_date = localize_to_cst(start_date)
        end_date = localize_to_cst(end_date)
        
        # Initialize additional params if None
        additional_params = additional_params or {}
        
        try:
            # Fetch data from API
            response = self._api_client.get_data(start_date, end_date, additional_params)
            
            # Convert to DataFrame
            if not response or 'data' not in response:
                raise MissingDataError(LOAD_FORECAST_SOURCE, ["No data returned from API"])
            
            # Extract load forecast data from response
            load_data = response.get('data', [])
            
            if not load_data:
                raise MissingDataError(LOAD_FORECAST_SOURCE, ["Empty data returned from API"])
            
            # Convert to DataFrame
            df = pd.DataFrame(load_data)
            
            self._logger.log_data_event("received", df, {
                'rows': len(df),
                'date_range': f"{start_date.isoformat()} to {end_date.isoformat()}"
            })
            
            return df
            
        except (APIConnectionError, APIResponseError) as e:
            self._logger.log_failure("Load forecast retrieval", time.time(), e, {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            })
            raise
        except Exception as e:
            self._logger.log_failure("Load forecast retrieval", time.time(), e, {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            })
            raise DataIngestionError(f"Failed to retrieve load forecast data: {str(e)}")
    
    def get_forecast_for_horizon(
        self,
        start_date: datetime.datetime,
        horizon_hours: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Retrieves load forecast data for the entire forecast horizon.
        
        Args:
            start_date: Start date for the forecast period
            horizon_hours: Number of hours in the forecast horizon (defaults to FORECAST_HORIZON_HOURS)
            
        Returns:
            DataFrame containing load forecast for the entire horizon
            
        Raises:
            APIConnectionError: If connection to the API fails
            APIResponseError: If the API returns an error response
            MissingDataError: If required data is missing from the response
            DataIngestionError: If there is an error processing the data
        """
        self._logger.log_start("Load forecast horizon retrieval", {
            'start_date': start_date.isoformat(),
            'horizon_hours': horizon_hours or FORECAST_HORIZON_HOURS
        })
        
        # Use default horizon if not specified
        if horizon_hours is None:
            horizon_hours = FORECAST_HORIZON_HOURS
        
        # Calculate end date
        start_date = localize_to_cst(start_date)
        end_date = start_date + datetime.timedelta(hours=horizon_hours)
        
        try:
            # Get forecast for the date range
            forecast_df = self.get_forecast(start_date, end_date)
            
            # Generate the expected date range for verification
            expected_dates = generate_forecast_date_range(start_date, horizon_hours)
            
            # Check if all expected timestamps are in the data
            if 'timestamp' in forecast_df.columns:
                # Convert to pandas datetime if not already
                forecast_df['timestamp'] = pd.to_datetime(forecast_df['timestamp'])
                
                # Check for missing timestamps
                forecast_timestamps = set(pd.to_datetime(forecast_df['timestamp']).dt.tz_localize(None))
                expected_timestamps = set(pd.to_datetime(expected_dates).tz_localize(None))
                missing_timestamps = expected_timestamps - forecast_timestamps
                
                if missing_timestamps:
                    self._logger.adapter.warning(
                        f"Missing {len(missing_timestamps)} timestamps in load forecast data."
                    )
                    # In a real implementation, we might attempt to fill gaps here
            
            return forecast_df
            
        except Exception as e:
            self._logger.log_failure("Load forecast horizon retrieval", time.time(), e, {
                'start_date': start_date.isoformat(),
                'horizon_hours': horizon_hours
            })
            if isinstance(e, (APIConnectionError, APIResponseError, MissingDataError)):
                raise
            raise DataIngestionError(f"Failed to retrieve load forecast for horizon: {str(e)}")
    
    def get_latest_forecast(self) -> pd.DataFrame:
        """
        Retrieves the latest available load forecast data.
        
        Returns:
            DataFrame containing latest load forecast data
            
        Raises:
            APIConnectionError: If connection to the API fails
            APIResponseError: If the API returns an error response
            MissingDataError: If required data is missing from the response
            DataIngestionError: If there is an error processing the data
        """
        self._logger.log_start("Latest load forecast retrieval")
        
        try:
            # Request latest data from API
            response = self._api_client.get_latest_data({'latest': True})
            
            # Convert to DataFrame
            if not response or 'data' not in response:
                raise MissingDataError(LOAD_FORECAST_SOURCE, ["No data returned from API"])
            
            # Extract load forecast data from response
            load_data = response.get('data', [])
            
            if not load_data:
                raise MissingDataError(LOAD_FORECAST_SOURCE, ["Empty data returned from API"])
            
            # Convert to DataFrame
            df = pd.DataFrame(load_data)
            
            self._logger.log_data_event("received", df, {
                'rows': len(df),
                'latest': True
            })
            
            return df
            
        except (APIConnectionError, APIResponseError) as e:
            self._logger.log_failure("Latest load forecast retrieval", time.time(), e)
            raise
        except Exception as e:
            self._logger.log_failure("Latest load forecast retrieval", time.time(), e)
            raise DataIngestionError(f"Failed to retrieve latest load forecast data: {str(e)}")