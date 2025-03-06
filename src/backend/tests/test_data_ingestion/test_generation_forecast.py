"""
Unit tests for the generation forecast functionality in the data ingestion module.
Tests the fetching, validation, and processing of generation forecast data from external sources.
"""

# External imports
import pytest  # version: 7.0.0+
from unittest.mock import MagicMock, patch
import pandas as pd  # version: 2.0.0+
import numpy as np  # version: 1.24.0+
from datetime import datetime, timedelta  # standard library

# Internal imports
from ...data_ingestion.generation_forecast import (
    fetch_generation_forecast,
    get_generation_forecast_by_fuel_type,
    get_fuel_type_list,
    calculate_total_generation,
    GenerationForecastClient
)
from ...data_ingestion.exceptions import (
    APIConnectionError,
    APIResponseError,
    DataValidationError,
    MissingDataError
)
from ...data_ingestion.data_validator import validate_generation_forecast_data
from ...data_ingestion.data_transformer import normalize_generation_forecast_data, pivot_generation_data
from ...models.data_models import GenerationForecast
from ...data_ingestion.api_client import APIClient


def create_mock_generation_data(start_date, end_date, fuel_types):
    """
    Creates mock generation forecast data for testing.
    
    Args:
        start_date (datetime): Start date for the test data
        end_date (datetime): End date for the test data
        fuel_types (list): List of fuel types to include
        
    Returns:
        pd.DataFrame: Mock generation forecast DataFrame
    """
    # Create a date range with hourly frequency
    date_range = pd.date_range(start=start_date, end=end_date, freq='H')
    
    # Create a list of dictionaries for each timestamp and fuel type
    data = []
    for timestamp in date_range:
        for fuel_type in fuel_types:
            # Generate realistic generation values based on fuel type
            if fuel_type == 'wind':
                # Wind generation varies by time of day
                hour = timestamp.hour
                generation_mw = np.random.normal(5000, 1000) * (1 + 0.2 * np.sin(hour / 24 * 2 * np.pi))
            elif fuel_type == 'solar':
                # Solar generation depends on daylight hours
                hour = timestamp.hour
                if 6 <= hour <= 18:  # Daylight hours
                    generation_mw = np.random.normal(3000, 500) * np.sin((hour - 6) / 12 * np.pi)
                else:
                    generation_mw = 0
            elif fuel_type == 'gas':
                # Gas generation is relatively stable
                generation_mw = np.random.normal(15000, 2000)
            elif fuel_type == 'coal':
                # Coal generation is stable but can vary
                generation_mw = np.random.normal(10000, 1500)
            elif fuel_type == 'nuclear':
                # Nuclear generation is very stable
                generation_mw = np.random.normal(8000, 500)
            else:
                # Default generation for other fuel types
                generation_mw = np.random.normal(2000, 500)
            
            # Ensure generation_mw is non-negative
            generation_mw = max(0, generation_mw)
            
            data.append({
                'timestamp': timestamp,
                'fuel_type': fuel_type,
                'generation_mw': generation_mw,
                'region': 'ERCOT'  # Default region
            })
    
    # Convert the list of dictionaries to a pandas DataFrame
    return pd.DataFrame(data)


class TestGenerationForecast:
    """Test cases for the generation forecast functions."""
    
    def setup_method(self, method):
        """Set up test fixtures before each test method."""
        # Set up test dates
        self.start_date = datetime.now() - timedelta(days=1)
        self.end_date = datetime.now() + timedelta(days=1)
        
        # Set up list of fuel types
        self.fuel_types = ['wind', 'solar', 'gas', 'coal', 'nuclear']
        
        # Create mock generation forecast data
        self.mock_data = create_mock_generation_data(
            self.start_date, 
            self.end_date, 
            self.fuel_types
        )
        
        # Set up mock API client
        self.mock_api_client = MagicMock()
    
    def test_fetch_generation_forecast_success(self):
        """Test successful fetching of generation forecast data."""
        # Mock the fetch_data function to return mock data
        with patch('...data_ingestion.generation_forecast.fetch_data') as mock_fetch:
            mock_fetch.return_value = {'data': self.mock_data.to_dict('records')}
            
            # Mock the validate_generation_forecast_data function
            with patch('...data_ingestion.generation_forecast.validate_generation_forecast_data') as mock_validate:
                mock_validate.return_value = MagicMock(is_valid=True)
                
                # Mock the normalize_generation_forecast_data function
                with patch('...data_ingestion.generation_forecast.normalize_generation_forecast_data') as mock_normalize:
                    mock_normalize.return_value = self.mock_data
                    
                    # Call the function under test
                    result = fetch_generation_forecast(self.start_date, self.end_date)
                    
                    # Assert that the mock functions were called
                    mock_fetch.assert_called_once()
                    mock_validate.assert_called_once()
                    mock_normalize.assert_called_once()
                    
                    # Assert that the result is as expected
                    assert isinstance(result, pd.DataFrame)
                    assert result.equals(self.mock_data)
                    assert 'timestamp' in result.columns
                    assert 'fuel_type' in result.columns
                    assert 'generation_mw' in result.columns
                    assert 'region' in result.columns
    
    def test_fetch_generation_forecast_api_error(self):
        """Test handling of API errors when fetching generation forecast data."""
        # Test APIConnectionError
        with patch('...data_ingestion.generation_forecast.fetch_data') as mock_fetch:
            mock_fetch.side_effect = APIConnectionError("test_endpoint", "generation_forecast", Exception("Connection error"))
            
            with pytest.raises(APIConnectionError):
                fetch_generation_forecast(self.start_date, self.end_date)
        
        # Test APIResponseError
        with patch('...data_ingestion.generation_forecast.fetch_data') as mock_fetch:
            mock_fetch.side_effect = APIResponseError("test_endpoint", 500, {"error": "Server error"})
            
            with pytest.raises(APIResponseError):
                fetch_generation_forecast(self.start_date, self.end_date)
    
    def test_fetch_generation_forecast_validation_error(self):
        """Test handling of validation errors in generation forecast data."""
        # Mock the fetch_data function to return mock data
        with patch('...data_ingestion.generation_forecast.fetch_data') as mock_fetch:
            mock_fetch.return_value = {'data': self.mock_data.to_dict('records')}
            
            # Mock the validate_generation_forecast_data function to return validation failure
            with patch('...data_ingestion.generation_forecast.validate_generation_forecast_data') as mock_validate:
                mock_validate.return_value = MagicMock(
                    is_valid=False,
                    errors={'validation_error': ['Data validation failed']},
                    format_errors=lambda: 'Data validation failed'
                )
                
                with pytest.raises(DataValidationError):
                    fetch_generation_forecast(self.start_date, self.end_date)
    
    def test_get_generation_forecast_by_fuel_type(self):
        """Test getting generation forecast data organized by fuel type."""
        # Mock the fetch_generation_forecast function to return mock data
        with patch('...data_ingestion.generation_forecast.fetch_generation_forecast') as mock_fetch:
            mock_fetch.return_value = self.mock_data
            
            # Mock the pivot_generation_data function
            with patch('...data_ingestion.generation_forecast.pivot_generation_data') as mock_pivot:
                # Create a pivoted dataframe with columns for each fuel type
                pivoted_data = pd.pivot_table(
                    self.mock_data, 
                    index='timestamp', 
                    columns='fuel_type', 
                    values='generation_mw',
                    aggfunc='sum'
                ).reset_index()
                mock_pivot.return_value = pivoted_data
                
                # Call the function under test
                result = get_generation_forecast_by_fuel_type(self.start_date, self.end_date)
                
                # Assert that the mock functions were called
                mock_fetch.assert_called_once_with(self.start_date, self.end_date)
                mock_pivot.assert_called_once()
                
                # Assert that the result is as expected
                assert isinstance(result, pd.DataFrame)
                assert 'timestamp' in result.columns
                for fuel_type in self.fuel_types:
                    assert fuel_type in result.columns
                assert len(result) == len(pd.date_range(start=self.start_date, end=self.end_date, freq='H'))
    
    def test_get_generation_forecast_by_fuel_type_filtered(self):
        """Test getting generation forecast data filtered to specific fuel types."""
        # Mock the fetch_generation_forecast function to return mock data
        with patch('...data_ingestion.generation_forecast.fetch_generation_forecast') as mock_fetch:
            mock_fetch.return_value = self.mock_data
            
            # Mock the pivot_generation_data function
            with patch('...data_ingestion.generation_forecast.pivot_generation_data') as mock_pivot:
                # Create a pivoted dataframe with columns for each fuel type
                filtered_data = self.mock_data[self.mock_data['fuel_type'].isin(['wind', 'solar'])]
                pivoted_data = pd.pivot_table(
                    filtered_data, 
                    index='timestamp', 
                    columns='fuel_type', 
                    values='generation_mw',
                    aggfunc='sum'
                ).reset_index()
                mock_pivot.return_value = pivoted_data
                
                # Call the function under test with filtered fuel types
                filter_types = ['wind', 'solar']
                result = get_generation_forecast_by_fuel_type(self.start_date, self.end_date, filter_types)
                
                # Assert that the mock functions were called
                mock_fetch.assert_called_once_with(self.start_date, self.end_date)
                mock_pivot.assert_called_once()
                
                # Assert that the result is as expected
                assert isinstance(result, pd.DataFrame)
                assert 'timestamp' in result.columns
                assert 'wind' in result.columns
                assert 'solar' in result.columns
                assert 'gas' not in result.columns
                assert 'coal' not in result.columns
                assert 'nuclear' not in result.columns
    
    def test_get_fuel_type_list(self):
        """Test getting a list of available fuel types."""
        # Create a test dataframe with multiple fuel types
        test_df = pd.DataFrame({
            'timestamp': [datetime.now()] * 5,
            'fuel_type': ['wind', 'solar', 'gas', 'coal', 'nuclear'],
            'generation_mw': [1000, 2000, 3000, 4000, 5000],
            'region': ['ERCOT'] * 5
        })
        
        # Call the function under test
        result = get_fuel_type_list(test_df)
        
        # Assert that the result is as expected
        assert isinstance(result, list)
        assert len(result) == 5
        assert 'wind' in result
        assert 'solar' in result
        assert 'gas' in result
        assert 'coal' in result
        assert 'nuclear' in result
        assert result == sorted(result)  # Should be alphabetically sorted
    
    def test_get_fuel_type_list_empty_df(self):
        """Test getting fuel type list with empty DataFrame."""
        # Create an empty dataframe
        empty_df = pd.DataFrame()
        
        # Call the function under test
        result = get_fuel_type_list(empty_df)
        
        # Assert that the result is an empty list
        assert isinstance(result, list)
        assert len(result) == 0
    
    def test_calculate_total_generation(self):
        """Test calculating total generation across all fuel types."""
        # Create a test dataframe with multiple fuel types and timestamps
        test_df = pd.DataFrame({
            'timestamp': [
                datetime(2023, 1, 1, 0, 0),
                datetime(2023, 1, 1, 0, 0),
                datetime(2023, 1, 1, 1, 0),
                datetime(2023, 1, 1, 1, 0)
            ],
            'fuel_type': ['wind', 'solar', 'wind', 'solar'],
            'generation_mw': [1000, 2000, 3000, 4000],
            'region': ['ERCOT'] * 4
        })
        
        # Call the function under test
        result = calculate_total_generation(test_df)
        
        # Assert that the result is as expected
        assert isinstance(result, pd.DataFrame)
        assert 'timestamp' in result.columns
        assert 'total_generation_mw' in result.columns
        assert len(result) == 2  # Two unique timestamps
        
        # Check calculated totals
        expected_totals = {
            pd.Timestamp('2023-01-01 00:00:00'): 3000,  # 1000 + 2000
            pd.Timestamp('2023-01-01 01:00:00'): 7000   # 3000 + 4000
        }
        for _, row in result.iterrows():
            assert row['total_generation_mw'] == expected_totals[row['timestamp']]
    
    def test_calculate_total_generation_empty_df(self):
        """Test calculating total generation with empty DataFrame."""
        # Create an empty dataframe
        empty_df = pd.DataFrame()
        
        # Call the function under test
        result = calculate_total_generation(empty_df)
        
        # Assert that the result is an empty dataframe
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestGenerationForecastClient:
    """Test cases for the GenerationForecastClient class."""
    
    def setup_method(self, method):
        """Set up test fixtures before each test method."""
        # Set up test dates
        self.start_date = datetime.now() - timedelta(days=1)
        self.end_date = datetime.now() + timedelta(days=1)
        
        # Set up list of fuel types
        self.fuel_types = ['wind', 'solar', 'gas', 'coal', 'nuclear']
        
        # Create mock generation forecast data
        self.mock_data = create_mock_generation_data(
            self.start_date, 
            self.end_date, 
            self.fuel_types
        )
        
        # Set up mock API client
        self.mock_api_client = MagicMock(spec=APIClient)
        
        # Create client instance with mocked dependencies
        with patch('...data_ingestion.generation_forecast.APIClient', return_value=self.mock_api_client):
            self.client = GenerationForecastClient()
    
    def test_fetch_data(self):
        """Test fetching data through the client."""
        # Mock the API client's get_data method
        self.mock_api_client.get_data.return_value = {'data': self.mock_data.to_dict('records')}
        
        # Mock the module-level fetch_generation_forecast function
        with patch('...data_ingestion.generation_forecast.fetch_generation_forecast') as mock_fetch:
            mock_fetch.return_value = self.mock_data
            
            # Call the client's fetch_data method
            result = self.client.fetch_data(self.start_date, self.end_date)
            
            # Assert that the module-level function was called
            mock_fetch.assert_called_once_with(self.start_date, self.end_date, None)
            
            # Assert that the result is as expected
            assert isinstance(result, pd.DataFrame)
            assert result.equals(self.mock_data)
    
    def test_get_by_fuel_type(self):
        """Test getting data by fuel type through the client."""
        # Mock the client's fetch_data method
        with patch.object(self.client, 'fetch_data', return_value=self.mock_data):
            # Mock the module-level get_generation_forecast_by_fuel_type function
            with patch('...data_ingestion.generation_forecast.get_generation_forecast_by_fuel_type') as mock_get:
                # Create a pivoted dataframe
                pivoted_data = pd.pivot_table(
                    self.mock_data, 
                    index='timestamp', 
                    columns='fuel_type', 
                    values='generation_mw',
                    aggfunc='sum'
                ).reset_index()
                mock_get.return_value = pivoted_data
                
                # Call the client's get_by_fuel_type method
                result = self.client.get_by_fuel_type(self.start_date, self.end_date, self.fuel_types)
                
                # Assert that the module-level function was called
                mock_get.assert_called_once_with(self.start_date, self.end_date, self.fuel_types)
                
                # Assert that the result is as expected
                assert isinstance(result, pd.DataFrame)
                assert 'timestamp' in result.columns
                for fuel_type in self.fuel_types:
                    assert fuel_type in result.columns
    
    def test_get_available_fuel_types(self):
        """Test getting available fuel types through the client."""
        # Mock the client's fetch_data method
        with patch.object(self.client, 'fetch_data', return_value=self.mock_data):
            # Mock the module-level get_fuel_type_list function
            with patch('...data_ingestion.generation_forecast.get_fuel_type_list') as mock_get:
                mock_get.return_value = self.fuel_types
                
                # Call the client's get_available_fuel_types method
                reference_date = datetime.now()
                result = self.client.get_available_fuel_types(reference_date)
                
                # Assert that the fetch_data method was called with expected date range
                expected_start = reference_date - timedelta(hours=12)
                expected_end = reference_date + timedelta(hours=12)
                self.client.fetch_data.assert_called_once_with(expected_start, expected_end)
                
                # Assert that the get_fuel_type_list function was called
                mock_get.assert_called_once_with(self.mock_data)
                
                # Assert that the result is as expected
                assert result == self.fuel_types
    
    def test_get_total_generation(self):
        """Test getting total generation through the client."""
        # Mock the client's fetch_data method
        with patch.object(self.client, 'fetch_data', return_value=self.mock_data):
            # Mock the module-level calculate_total_generation function
            with patch('...data_ingestion.generation_forecast.calculate_total_generation') as mock_calc:
                # Create expected total generation dataframe
                total_gen = pd.DataFrame({
                    'timestamp': pd.date_range(self.start_date, self.end_date, freq='H'),
                    'total_generation_mw': np.random.uniform(30000, 40000, len(pd.date_range(self.start_date, self.end_date, freq='H')))
                })
                mock_calc.return_value = total_gen
                
                # Call the client's get_total_generation method
                result = self.client.get_total_generation(self.start_date, self.end_date)
                
                # Assert that the fetch_data method was called
                self.client.fetch_data.assert_called_once_with(self.start_date, self.end_date)
                
                # Assert that the calculate_total_generation function was called
                mock_calc.assert_called_once_with(self.mock_data)
                
                # Assert that the result is as expected
                assert isinstance(result, pd.DataFrame)
                assert 'timestamp' in result.columns
                assert 'total_generation_mw' in result.columns
                assert result.equals(total_gen)
    
    def test_error_handling(self):
        """Test error handling in the client."""
        # Test handling of API connection error
        with patch.object(self.client, 'fetch_data', side_effect=APIConnectionError("test_endpoint", "generation_forecast", Exception("Connection error"))):
            with pytest.raises(APIConnectionError):
                self.client.get_by_fuel_type(self.start_date, self.end_date)
        
        # Test handling of API response error
        with patch.object(self.client, 'fetch_data', side_effect=APIResponseError("test_endpoint", 500, {"error": "Server error"})):
            with pytest.raises(APIResponseError):
                self.client.get_total_generation(self.start_date, self.end_date)
        
        # Test handling of validation error
        with patch.object(self.client, 'fetch_data', side_effect=DataValidationError("generation_forecast", ["validation error"])):
            with pytest.raises(DataValidationError):
                self.client.get_by_fuel_type(self.start_date, self.end_date)
        
        # Test empty dataframe handling in get_total_generation
        with patch.object(self.client, 'fetch_data', return_value=pd.DataFrame()):
            result = self.client.get_total_generation(self.start_date, self.end_date)
            assert isinstance(result, pd.DataFrame)
            assert result.empty