"""
Provides test fixtures for load forecast data to be used in unit and integration tests
for the Electricity Market Price Forecasting System. These fixtures include mock data,
sample responses, and utility functions to generate test data with various characteristics
for testing the load forecast data ingestion pipeline.
"""

import datetime
from typing import Dict, List, Optional, Any

import pytest  # version: 7.0.0+
import pandas as pd  # version: 2.0.0+
import numpy as np  # version: 1.24.0+

from ../../models.data_models import LoadForecast
from ../../config.settings import FORECAST_HORIZON_HOURS, TIMEZONE
from ../../data_ingestion.exceptions import (
    APIConnectionError,
    APIResponseError,
    DataValidationError,
    MissingDataError,
)
from ../../utils.date_utils import localize_to_cst, generate_forecast_date_range

# Sample constants for test data generation
SAMPLE_REGION = "ERCOT"
SAMPLE_LOAD_PATTERN = [
    35420.5, 34150.2, 33275.8, 32890.3, 32750.6, 33120.8,  # Hours 0-5
    34560.2, 36780.5, 38950.3, 40120.5, 41250.8, 42380.2,  # Hours 6-11
    43150.5, 43680.2, 43520.8, 43180.5, 42950.3, 43250.8,  # Hours 12-17
    43780.2, 42950.5, 41250.3, 39850.5, 38250.8, 36780.2   # Hours 18-23
]


def create_mock_load_forecast_data(
    start_time: datetime.datetime,
    hours: Optional[int] = None,
    region: Optional[str] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with mock load forecast data for testing.
    
    Args:
        start_time: Starting timestamp for the forecast data
        hours: Number of hours to generate (defaults to FORECAST_HORIZON_HOURS)
        region: Region for the forecast data (defaults to SAMPLE_REGION)
        
    Returns:
        DataFrame with mock load forecast data
    """
    # Ensure start_time is in CST timezone
    start_time = localize_to_cst(start_time)
    
    # Use defaults if not provided
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    if region is None:
        region = SAMPLE_REGION
    
    # Generate date range for the forecast period
    date_range = generate_forecast_date_range(start_time, hours)
    
    # Create data rows
    data = []
    for i, timestamp in enumerate(date_range):
        # Calculate the hour of day to use the appropriate load pattern value
        hour_of_day = timestamp.hour
        
        # Get base load from pattern and add some random variation
        load_mw = generate_load_value(hour_of_day, 0.05)  # 5% volatility
        
        # Create row
        row = {
            'timestamp': timestamp,
            'load_mw': load_mw,
            'region': region
        }
        data.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Ensure correct data types
    df = df.astype({
        'timestamp': 'datetime64[ns, America/Chicago]',
        'load_mw': 'float64',
        'region': 'string'
    })
    
    return df


def create_mock_load_forecast_models(
    start_time: datetime.datetime,
    hours: Optional[int] = None,
    region: Optional[str] = None
) -> List[LoadForecast]:
    """
    Creates a list of LoadForecast model instances for testing.
    
    Args:
        start_time: Starting timestamp for the forecast data
        hours: Number of hours to generate (defaults to FORECAST_HORIZON_HOURS)
        region: Region for the forecast data (defaults to SAMPLE_REGION)
        
    Returns:
        List of LoadForecast model instances
    """
    # Ensure start_time is in CST timezone
    start_time = localize_to_cst(start_time)
    
    # Use defaults if not provided
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    if region is None:
        region = SAMPLE_REGION
    
    # Generate date range for the forecast period
    date_range = generate_forecast_date_range(start_time, hours)
    
    # Create LoadForecast instances
    forecasts = []
    for i, timestamp in enumerate(date_range):
        # Calculate the hour of day to use the appropriate load pattern value
        hour_of_day = timestamp.hour
        
        # Generate load value with some randomness
        load_mw = generate_load_value(hour_of_day, 0.05)  # 5% volatility
        
        # Create LoadForecast instance
        forecast = LoadForecast(
            timestamp=timestamp.to_pydatetime(),
            load_mw=load_mw,
            region=region
        )
        forecasts.append(forecast)
    
    return forecasts


def create_incomplete_load_forecast_data(
    start_time: datetime.datetime,
    hours: Optional[int] = None,
    region: Optional[str] = None,
    missing_indices: Optional[List[int]] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with incomplete load forecast data (missing hours).
    
    Args:
        start_time: Starting timestamp for the forecast data
        hours: Number of hours to generate (defaults to FORECAST_HORIZON_HOURS)
        region: Region for the forecast data (defaults to SAMPLE_REGION)
        missing_indices: List of indices to remove from the data
        
    Returns:
        DataFrame with incomplete load forecast data
    """
    # Create complete data first
    df = create_mock_load_forecast_data(start_time, hours, region)
    
    # If missing_indices provided, remove those rows
    if missing_indices:
        df = df.drop(missing_indices).reset_index(drop=True)
    
    return df


def create_invalid_load_forecast_data(
    start_time: datetime.datetime,
    hours: Optional[int] = None,
    region: Optional[str] = None,
    invalid_indices: Optional[Dict[int, Dict[str, Any]]] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with invalid load forecast data (negative or extreme values).
    
    Args:
        start_time: Starting timestamp for the forecast data
        hours: Number of hours to generate (defaults to FORECAST_HORIZON_HOURS)
        region: Region for the forecast data (defaults to SAMPLE_REGION)
        invalid_indices: Dictionary mapping indices to dictionaries of invalid values
            e.g., {0: {'load_mw': -100}, 5: {'region': 'INVALID'}}
        
    Returns:
        DataFrame with invalid load forecast data
    """
    # Create complete data first
    df = create_mock_load_forecast_data(start_time, hours, region)
    
    # If invalid_indices provided, replace values at those indices
    if invalid_indices:
        for idx, values in invalid_indices.items():
            if idx < len(df):
                for col, val in values.items():
                    df.at[idx, col] = val
    
    return df


def create_mock_api_response(
    start_time: datetime.datetime,
    hours: Optional[int] = None,
    region: Optional[str] = None
) -> Dict[str, Any]:
    """
    Creates a mock API response dictionary for load forecast data.
    
    Args:
        start_time: Starting timestamp for the forecast data
        hours: Number of hours to generate (defaults to FORECAST_HORIZON_HOURS)
        region: Region for the forecast data (defaults to SAMPLE_REGION)
        
    Returns:
        Dictionary mimicking the API response format
    """
    # Create DataFrame with mock data
    df = create_mock_load_forecast_data(start_time, hours, region)
    
    # Convert to the expected API response format
    forecasts = []
    for _, row in df.iterrows():
        forecast = {
            'timestamp': row['timestamp'].isoformat(),
            'load_mw': float(row['load_mw']),
            'region': row['region']
        }
        forecasts.append(forecast)
    
    # Create full response structure
    response = {
        'status': 'success',
        'timestamp': datetime.datetime.now(TIMEZONE).isoformat(),
        'data': {
            'forecasts': forecasts,
            'region': region or SAMPLE_REGION,
            'count': len(forecasts)
        }
    }
    
    return response


def create_invalid_api_response(error_type: str) -> Dict[str, Any]:
    """
    Creates an invalid mock API response for testing error handling.
    
    Args:
        error_type: Type of error to simulate:
            - 'missing_data': Response with empty data
            - 'wrong_format': Response with incorrectly formatted data
            - 'empty_response': Empty response
            - 'server_error': Server error response
            - 'invalid_region': Response with invalid region
        
    Returns:
        Dictionary with invalid API response structure
    """
    if error_type == 'missing_data':
        return {
            'status': 'success',
            'timestamp': datetime.datetime.now(TIMEZONE).isoformat(),
            'data': {
                'forecasts': [],
                'region': SAMPLE_REGION,
                'count': 0
            }
        }
    
    elif error_type == 'wrong_format':
        return {
            'status': 'success',
            'timestamp': datetime.datetime.now(TIMEZONE).isoformat(),
            'data': {
                'forecasts': [
                    {
                        # Missing timestamp
                        'load': 35000,  # Wrong field name
                        'area': SAMPLE_REGION  # Wrong field name
                    }
                ],
                'region': SAMPLE_REGION,
                'count': 1
            }
        }
    
    elif error_type == 'empty_response':
        return {}
    
    elif error_type == 'server_error':
        return {
            'status': 'error',
            'error': 'Internal server error',
            'code': 500
        }
    
    elif error_type == 'invalid_region':
        return {
            'status': 'success',
            'timestamp': datetime.datetime.now(TIMEZONE).isoformat(),
            'data': {
                'forecasts': [
                    {
                        'timestamp': datetime.datetime.now(TIMEZONE).isoformat(),
                        'load_mw': 35000,
                        'region': 'INVALID_REGION'
                    }
                ],
                'region': 'INVALID_REGION',
                'count': 1
            }
        }
    
    else:
        raise ValueError(f"Unknown error type: {error_type}")


def generate_load_value(hour_of_day: int, volatility_factor: float = 0.05) -> float:
    """
    Generates a realistic load value for a specific hour of day.
    
    Args:
        hour_of_day: Hour of day (0-23)
        volatility_factor: Factor to control random variation (default 5%)
        
    Returns:
        Generated load value
    """
    # Ensure hour_of_day is within valid range
    hour_of_day = hour_of_day % 24
    
    # Get base load from pattern
    base_load = SAMPLE_LOAD_PATTERN[hour_of_day]
    
    # Add random variation
    variation = np.random.normal(0, base_load * volatility_factor)
    load_value = base_load + variation
    
    # Ensure load is positive
    return max(100, load_value)


class MockLoadForecastClient:
    """
    Mock client for load forecast API to use in tests.
    """
    
    def __init__(
        self,
        responses: Optional[Dict[str, Dict[str, Any]]] = None,
        error: Optional[Exception] = None
    ):
        """
        Initialize the mock client with predefined responses.
        
        Args:
            responses: Dictionary mapping parameter strings to response dictionaries
            error: Optional exception to raise during fetch operations
        """
        self._responses = responses or {}
        self._error = error
    
    def fetch_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mock implementation of fetch_data that returns predefined responses or raises errors.
        
        Args:
            params: Request parameters
            
        Returns:
            Predefined response for the given parameters
            
        Raises:
            Exception: If an error was set during initialization
            KeyError: If no response was defined for the given parameters
        """
        if self._error:
            raise self._error
        
        # Convert params to a string key for lookup
        param_key = str(sorted([(k, v) for k, v in params.items()]))
        
        if param_key in self._responses:
            return self._responses[param_key]
        
        raise KeyError(f"No mock response defined for parameters: {params}")
    
    def add_response(self, params: Dict[str, Any], response: Dict[str, Any]) -> None:
        """
        Adds a new mock response for specific parameters.
        
        Args:
            params: Request parameters
            response: Response to return for those parameters
        """
        # Convert params to a string key for storage
        param_key = str(sorted([(k, v) for k, v in params.items()]))
        self._responses[param_key] = response
    
    def set_error(self, error: Exception) -> None:
        """
        Sets an error to be raised on fetch operations.
        
        Args:
            error: Exception to raise
        """
        self._error = error
    
    def clear_error(self) -> None:
        """
        Clears any set error.
        """
        self._error = None