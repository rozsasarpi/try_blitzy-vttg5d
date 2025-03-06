"""
Provides test fixtures for generation forecast data to be used in unit and integration tests
for the Electricity Market Price Forecasting System. These fixtures include mock data,
sample responses, and utility functions to generate test data with various characteristics
for testing the generation forecast data ingestion pipeline.
"""

import pytest  # version: 7.0.0+
import pandas as pd  # version: 2.0.0+
import numpy as np  # version: 1.24.0+
from datetime import datetime  # standard library
from typing import List, Dict, Optional, Union, Any  # standard library

from ../../models.data_models import GenerationForecast
from ../../config.settings import FORECAST_HORIZON_HOURS, TIMEZONE
from ../../data_ingestion.exceptions import (
    APIConnectionError,
    APIResponseError,
    DataValidationError,
    MissingDataError
)
from ../../utils.date_utils import localize_to_cst, generate_forecast_date_range

# Sample region for test data
SAMPLE_REGION = "ERCOT"

# Sample fuel types for test data
SAMPLE_FUEL_TYPES = ["WIND", "SOLAR", "GAS", "COAL", "NUCLEAR"]

# Sample hourly generation patterns for each fuel type
SAMPLE_GENERATION_PATTERNS = {
    "WIND": [8500.5, 8200.3, 7900.8, 7600.2, 7400.5, 7200.8, 7100.2, 7300.5, 7600.8, 
             8000.2, 8400.5, 8700.8, 9000.2, 9200.5, 9400.8, 9300.2, 9100.5, 8900.8, 
             8700.2, 8500.5, 8300.8, 8200.2, 8400.5, 8600.8],
    "SOLAR": [0.0, 0.0, 0.0, 0.0, 0.0, 100.2, 500.5, 1200.8, 2500.2, 3800.5, 4900.8, 
              5500.2, 5800.5, 5700.8, 5200.2, 4500.5, 3200.8, 1500.2, 500.5, 100.8, 
              0.0, 0.0, 0.0, 0.0],
    "GAS": [15320.5, 14800.2, 14500.8, 14200.2, 14100.5, 14300.8, 14800.2, 15300.5, 
            15800.8, 16200.2, 16500.5, 16800.8, 17000.2, 17200.5, 17100.8, 16900.2, 
            16700.5, 16500.8, 16300.2, 16100.5, 15900.8, 15700.2, 15500.5, 15320.8],
    "COAL": [6500.5, 6400.3, 6300.8, 6200.2, 6100.5, 6000.8, 6100.2, 6200.5, 6300.8, 
             6400.2, 6500.5, 6600.8, 6700.2, 6800.5, 6900.8, 6800.2, 6700.5, 6600.8, 
             6500.2, 6400.5, 6300.8, 6200.2, 6100.5, 6000.8],
    "NUCLEAR": [5100.5, 5100.3, 5100.8, 5100.2, 5100.5, 5100.8, 5100.2, 5100.5, 5100.8, 
                5100.2, 5100.5, 5100.8, 5100.2, 5100.5, 5100.8, 5100.2, 5100.5, 5100.8, 
                5100.2, 5100.5, 5100.8, 5100.2, 5100.5, 5100.8]
}


def generate_generation_value(fuel_type: str, hour_of_day: int, volatility_factor: float = 0.1) -> float:
    """
    Generates a realistic generation value for a specific fuel type and hour of day.
    
    Args:
        fuel_type: Fuel type for generation (WIND, SOLAR, GAS, COAL, NUCLEAR)
        hour_of_day: Hour of day (0-23)
        volatility_factor: Factor to control random variation (0.0-1.0)
        
    Returns:
        Generated generation value in MW
    """
    if fuel_type not in SAMPLE_GENERATION_PATTERNS:
        raise ValueError(f"Unknown fuel type: {fuel_type}")
    
    if not 0 <= hour_of_day <= 23:
        raise ValueError(f"Hour of day must be between 0 and 23, got {hour_of_day}")
    
    # Get base generation pattern for this fuel type
    base_pattern = SAMPLE_GENERATION_PATTERNS[fuel_type]
    
    # Get base generation for this hour
    base_generation = base_pattern[hour_of_day]
    
    # Add some random variation based on volatility_factor
    variation = np.random.normal(0, base_generation * volatility_factor)
    generation = max(0.0, base_generation + variation)  # Ensure non-negative
    
    return generation


def create_mock_generation_forecast_data(
    start_time: datetime,
    hours: Optional[int] = None,
    fuel_types: Optional[List[str]] = None,
    region: str = SAMPLE_REGION
) -> pd.DataFrame:
    """
    Creates a DataFrame with mock generation forecast data for testing.
    
    Args:
        start_time: Start time for the forecast data
        hours: Number of hours to generate data for, defaults to FORECAST_HORIZON_HOURS
        fuel_types: List of fuel types to include, defaults to SAMPLE_FUEL_TYPES
        region: Region for the forecast data, defaults to SAMPLE_REGION
        
    Returns:
        DataFrame with mock generation forecast data
    """
    # Ensure start_time is in CST timezone
    start_time = localize_to_cst(start_time)
    
    # Use default values if not provided
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    
    if fuel_types is None:
        fuel_types = SAMPLE_FUEL_TYPES
    
    # Create date range
    date_range = generate_forecast_date_range(start_time, hours)
    
    # Create rows for the DataFrame
    rows = []
    for timestamp in date_range:
        hour_of_day = timestamp.hour
        
        for fuel_type in fuel_types:
            # Generate realistic generation value
            generation_mw = generate_generation_value(fuel_type, hour_of_day)
            
            # Create row
            row = {
                'timestamp': timestamp,
                'fuel_type': fuel_type,
                'generation_mw': generation_mw,
                'region': region
            }
            rows.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(rows)
    
    # Ensure correct data types
    df = df.astype({
        'timestamp': 'datetime64[ns, US/Central]',
        'fuel_type': 'string',
        'generation_mw': 'float64',
        'region': 'string'
    })
    
    return df


def create_mock_generation_forecast_models(
    start_time: datetime,
    hours: Optional[int] = None,
    fuel_types: Optional[List[str]] = None,
    region: str = SAMPLE_REGION
) -> List[GenerationForecast]:
    """
    Creates a list of GenerationForecast model instances for testing.
    
    Args:
        start_time: Start time for the forecast models
        hours: Number of hours to generate models for, defaults to FORECAST_HORIZON_HOURS
        fuel_types: List of fuel types to include, defaults to SAMPLE_FUEL_TYPES
        region: Region for the forecast models, defaults to SAMPLE_REGION
        
    Returns:
        List of GenerationForecast model instances
    """
    # Ensure start_time is in CST timezone
    start_time = localize_to_cst(start_time)
    
    # Use default values if not provided
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    
    if fuel_types is None:
        fuel_types = SAMPLE_FUEL_TYPES
    
    # Create date range
    date_range = generate_forecast_date_range(start_time, hours)
    
    # Create models
    models = []
    for timestamp in date_range:
        hour_of_day = timestamp.hour
        
        for fuel_type in fuel_types:
            # Generate realistic generation value
            generation_mw = generate_generation_value(fuel_type, hour_of_day)
            
            # Create model instance
            model = GenerationForecast(
                timestamp=timestamp.to_pydatetime(),
                fuel_type=fuel_type,
                generation_mw=generation_mw,
                region=region
            )
            models.append(model)
    
    return models


def create_pivoted_generation_forecast_data(
    start_time: datetime,
    hours: Optional[int] = None,
    fuel_types: Optional[List[str]] = None,
    region: str = SAMPLE_REGION
) -> pd.DataFrame:
    """
    Creates a DataFrame with generation forecast data pivoted by fuel type.
    
    Args:
        start_time: Start time for the forecast data
        hours: Number of hours to generate data for, defaults to FORECAST_HORIZON_HOURS
        fuel_types: List of fuel types to include, defaults to SAMPLE_FUEL_TYPES
        region: Region for the forecast data, defaults to SAMPLE_REGION
        
    Returns:
        DataFrame with generation forecast data pivoted by fuel type
    """
    # Create mock generation forecast data
    df = create_mock_generation_forecast_data(start_time, hours, fuel_types, region)
    
    # Pivot the DataFrame to have fuel types as columns
    pivoted_df = df.pivot(index='timestamp', columns='fuel_type', values='generation_mw')
    
    # Prefix column names with 'generation_'
    pivoted_df = pivoted_df.add_prefix('generation_')
    
    return pivoted_df


def create_incomplete_generation_forecast_data(
    start_time: datetime,
    hours: Optional[int] = None,
    fuel_types: Optional[List[str]] = None,
    region: str = SAMPLE_REGION,
    missing_indices: Optional[List[int]] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with incomplete generation forecast data (missing hours or fuel types).
    
    Args:
        start_time: Start time for the forecast data
        hours: Number of hours to generate data for, defaults to FORECAST_HORIZON_HOURS
        fuel_types: List of fuel types to include, defaults to SAMPLE_FUEL_TYPES
        region: Region for the forecast data, defaults to SAMPLE_REGION
        missing_indices: List of indices to remove from the DataFrame
        
    Returns:
        DataFrame with incomplete generation forecast data
    """
    # Create complete data first
    df = create_mock_generation_forecast_data(start_time, hours, fuel_types, region)
    
    # If missing_indices is provided, remove those rows
    if missing_indices:
        # Ensure indices are valid
        max_index = len(df) - 1
        valid_indices = [i for i in missing_indices if 0 <= i <= max_index]
        
        if valid_indices:
            # Drop the specified rows
            df = df.drop(df.index[valid_indices])
    
    return df


def create_invalid_generation_forecast_data(
    start_time: datetime,
    hours: Optional[int] = None,
    fuel_types: Optional[List[str]] = None,
    region: str = SAMPLE_REGION,
    invalid_indices: Optional[Dict[int, Dict[str, Any]]] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with invalid generation forecast data (negative or extreme values).
    
    Args:
        start_time: Start time for the forecast data
        hours: Number of hours to generate data for, defaults to FORECAST_HORIZON_HOURS
        fuel_types: List of fuel types to include, defaults to SAMPLE_FUEL_TYPES
        region: Region for the forecast data, defaults to SAMPLE_REGION
        invalid_indices: Dictionary mapping indices to dictionaries of invalid values
                         Example: {0: {'generation_mw': -100.0}, 5: {'fuel_type': 'INVALID'}}
        
    Returns:
        DataFrame with invalid generation forecast data
    """
    # Create complete data first
    df = create_mock_generation_forecast_data(start_time, hours, fuel_types, region)
    
    # If invalid_indices is provided, modify those rows
    if invalid_indices:
        # Make a copy to avoid warnings
        df = df.copy()
        
        # Ensure indices are valid
        max_index = len(df) - 1
        
        for index, changes in invalid_indices.items():
            if 0 <= index <= max_index:
                # Apply the specified changes to this row
                for column, value in changes.items():
                    if column in df.columns:
                        df.loc[index, column] = value
    
    return df


def create_mock_api_response(
    start_time: datetime,
    hours: Optional[int] = None,
    fuel_types: Optional[List[str]] = None,
    region: str = SAMPLE_REGION
) -> Dict[str, Any]:
    """
    Creates a mock API response dictionary for generation forecast data.
    
    Args:
        start_time: Start time for the forecast data
        hours: Number of hours to generate data for, defaults to FORECAST_HORIZON_HOURS
        fuel_types: List of fuel types to include, defaults to SAMPLE_FUEL_TYPES
        region: Region for the forecast data, defaults to SAMPLE_REGION
        
    Returns:
        Dictionary mimicking the API response format
    """
    # Create mock generation forecast data
    df = create_mock_generation_forecast_data(start_time, hours, fuel_types, region)
    
    # Convert DataFrame to records format and then to a dictionary
    forecasts = df.to_dict('records')
    
    # Create the API response structure
    response = {
        'status': 'success',
        'data': {
            'forecasts': forecasts,
            'metadata': {
                'start_time': start_time.isoformat(),
                'hours': hours if hours is not None else FORECAST_HORIZON_HOURS,
                'fuel_types': fuel_types if fuel_types is not None else SAMPLE_FUEL_TYPES,
                'region': region
            }
        },
        'timestamp': datetime.now(TIMEZONE).isoformat()
    }
    
    return response


def create_invalid_api_response(error_type: str = 'missing_data') -> Dict[str, Any]:
    """
    Creates an invalid mock API response for testing error handling.
    
    Args:
        error_type: Type of error to simulate:
                   - 'missing_data': Missing required data fields
                   - 'wrong_format': Data in incorrect format
                   - 'empty_response': Empty response body
                   - 'server_error': Server error status
                   - 'invalid_fuel_type': Unknown fuel type
        
    Returns:
        Dictionary with invalid API response structure
    """
    # Base for error responses
    error_responses = {
        'missing_data': {
            'status': 'success',
            'data': {
                'forecasts': [{'timestamp': '2023-01-01T00:00:00-06:00'}]  # Missing required fields
            }
        },
        'wrong_format': {
            'status': 'success',
            'data': {
                'forecasts': [
                    {
                        'timestamp': 'invalid-date-format',
                        'fuel_type': 'WIND',
                        'generation_mw': 'not-a-number',
                        'region': SAMPLE_REGION
                    }
                ]
            }
        },
        'empty_response': {},
        'server_error': {
            'status': 'error',
            'error': {
                'code': 500,
                'message': 'Internal server error'
            }
        },
        'invalid_fuel_type': {
            'status': 'success',
            'data': {
                'forecasts': [
                    {
                        'timestamp': '2023-01-01T00:00:00-06:00',
                        'fuel_type': 'UNKNOWN_FUEL',
                        'generation_mw': 1000.0,
                        'region': SAMPLE_REGION
                    }
                ]
            }
        }
    }
    
    # Return the appropriate error response
    if error_type in error_responses:
        return error_responses[error_type]
    else:
        raise ValueError(f"Unknown error type: {error_type}")


class MockGenerationForecastClient:
    """
    Mock client for generation forecast API to use in tests.
    """
    
    def __init__(self, responses: Optional[Dict[str, Any]] = None, error: Optional[Exception] = None):
        """
        Initialize the mock client with predefined responses.
        
        Args:
            responses: Dictionary mapping parameter keys to response objects
            error: Exception to raise when fetch methods are called
        """
        self._responses = responses or {}
        self._error = error
    
    def fetch_data(self, start_date: datetime, end_date: datetime, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Mock implementation of fetch_data that returns predefined responses or raises errors.
        
        Args:
            start_date: Start date for the data request
            end_date: End date for the data request
            params: Additional parameters for the request
            
        Returns:
            Predefined response for the given parameters
            
        Raises:
            Exception: The exception provided during initialization
        """
        if self._error:
            raise self._error
        
        # Create a key from the parameters
        params_str = f"{start_date.isoformat()}_{end_date.isoformat()}_{str(params)}"
        
        # Return the predefined response if available
        if params_str in self._responses:
            return self._responses[params_str]
        
        # Otherwise, create a mock response
        hours = (end_date - start_date).total_seconds() // 3600
        return create_mock_generation_forecast_data(start_date, int(hours))
    
    def get_by_fuel_type(self, start_date: datetime, end_date: datetime, fuel_types: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Mock implementation of get_by_fuel_type that returns predefined responses.
        
        Args:
            start_date: Start date for the data request
            end_date: End date for the data request
            fuel_types: List of fuel types to include
            
        Returns:
            Predefined pivoted response for the given parameters
            
        Raises:
            Exception: The exception provided during initialization
        """
        if self._error:
            raise self._error
        
        # Create a key from the parameters
        params_str = f"{start_date.isoformat()}_{end_date.isoformat()}_{str(fuel_types)}"
        
        # Return the predefined response if available
        if params_str in self._responses:
            return self._responses[params_str]
        
        # Otherwise, create a mock pivoted response
        hours = (end_date - start_date).total_seconds() // 3600
        return create_pivoted_generation_forecast_data(start_date, int(hours), fuel_types)
    
    def add_response(self, params: Dict[str, Any], response: pd.DataFrame) -> None:
        """
        Adds a new mock response for specific parameters.
        
        Args:
            params: Parameters that will trigger this response
            response: Response to return
        """
        # Convert params to a string key
        params_str = str(params)
        
        # Store the response
        self._responses[params_str] = response
    
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
    
    def get_available_fuel_types(self, reference_date: datetime) -> List[str]:
        """
        Mock implementation of get_available_fuel_types.
        
        Args:
            reference_date: Reference date for availability check
            
        Returns:
            List of available fuel types
            
        Raises:
            Exception: The exception provided during initialization
        """
        if self._error:
            raise self._error
            
        return SAMPLE_FUEL_TYPES