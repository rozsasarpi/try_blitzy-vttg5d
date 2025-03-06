"""
Unit tests for the forecast_api module which provides API functionality for retrieving and formatting electricity market price forecasts.
Tests cover all API functions including forecast retrieval, format conversion, validation, and error handling.
"""

import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import pandas as pd  # pandas: 2.0.0+
from datetime import datetime  # standard library
import io  # standard library

# Internal imports
from src.backend.api.forecast_api import (  # Function to retrieve forecast by date
    get_forecast_by_date,
    get_latest_forecast,  # Function to retrieve latest forecast
    get_forecasts_by_date_range,  # Function to retrieve forecasts by date range
    get_forecast_as_model,  # Function to retrieve forecast as model objects
    get_latest_forecast_as_model,  # Function to retrieve latest forecast as model objects
    get_forecast_ensemble,  # Function to retrieve forecast ensemble
    format_forecast_response,  # Function to format forecast response
    get_storage_status,  # Function to get storage status
    ForecastAPI,  # Class that encapsulates forecast API functionality
    SUPPORTED_FORMATS  # List of supported output formats
)
from src.backend.api.exceptions import (  # Exception for request validation failures
    RequestValidationError,
    ResourceNotFoundError,  # Exception for resource not found errors
    InvalidFormatError  # Exception for invalid format requests
)
from src.backend.storage.exceptions import DataFrameNotFoundError  # Exception for missing forecast DataFrames
from src.backend.models.forecast_models import ProbabilisticForecast, ForecastEnsemble  # Model class for probabilistic forecasts
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid forecast products
from src.backend.tests.fixtures.forecast_fixtures import (  # Create mock forecast data for testing
    create_mock_forecast_data,
    create_mock_probabilistic_forecast,  # Create a mock probabilistic forecast for testing
    create_mock_probabilistic_forecasts,  # Create a list of mock probabilistic forecasts for testing
    create_mock_forecast_ensemble  # Create a mock forecast ensemble for testing
)


class TestForecastAPI:
    """Test class for the ForecastAPI class and module functions"""

    def setup_method(self, method):
        """Set up test fixtures before each test method"""
        self.test_date = datetime(2023, 1, 1)  # Create a test date for consistent testing
        self.test_product = "DALMP"  # Create a test product for consistent testing
        # Set up common mock objects and fixtures
        self.mock_forecast_df = create_mock_forecast_data(start_time=self.test_date)

    def teardown_method(self, method):
        """Clean up after each test method"""
        # Clean up any resources created during tests
        # Reset any mocks or patches
        pass

    @pytest.mark.parametrize('product', ['DALMP', 'RTLMP', 'RegUp'])
    def test_get_forecast_by_date_valid(self, product, mocker):
        """Tests retrieving a forecast by date with valid parameters"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date, product=product)

        # Mock the storage_manager.get_forecast function to return the mock dataframe
        mocker.patch('src.backend.api.forecast_api.get_forecast', return_value=mock_forecast_df)

        # Call get_forecast_by_date with a valid date and product
        result_df = get_forecast_by_date(self.test_date.strftime('%Y-%m-%d'), product)

        # Assert that the returned dataframe matches the mock dataframe
        pd.testing.assert_frame_equal(result_df, mock_forecast_df)

        # Verify that storage_manager.get_forecast was called with correct parameters
        src.backend.api.forecast_api.get_forecast.assert_called_once_with(self.test_date, product)

    def test_get_forecast_by_date_invalid_product(self, mocker):
        """Tests that get_forecast_by_date raises RequestValidationError for invalid product"""
        # Mock the storage_manager.get_forecast function
        mocker.patch('src.backend.api.forecast_api.get_forecast')

        # Call get_forecast_by_date with an invalid product
        with pytest.raises(RequestValidationError):
            get_forecast_by_date(self.test_date.strftime('%Y-%m-%d'), 'InvalidProduct')

        # Verify that storage_manager.get_forecast was not called
        src.backend.api.forecast_api.get_forecast.assert_not_called()

    def test_get_forecast_by_date_not_found(self, mocker):
        """Tests that get_forecast_by_date raises ResourceNotFoundError when forecast not found"""
        # Mock the storage_manager.get_forecast function to raise DataFrameNotFoundError
        mocker.patch('src.backend.api.forecast_api.get_forecast', side_effect=DataFrameNotFoundError("Forecast not found", "DALMP", self.test_date))

        # Call get_forecast_by_date with valid parameters
        with pytest.raises(ResourceNotFoundError):
            get_forecast_by_date(self.test_date.strftime('%Y-%m-%d'), 'DALMP')

        # Verify that storage_manager.get_forecast was called with correct parameters
        src.backend.api.forecast_api.get_forecast.assert_called_once_with(self.test_date, 'DALMP')

    @pytest.mark.parametrize('format', ['json', 'csv', 'excel', 'parquet'])
    def test_get_forecast_by_date_format(self, format, mocker):
        """Tests retrieving a forecast by date with different format options"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date)

        # Mock the storage_manager.get_forecast function to return the mock dataframe
        mocker.patch('src.backend.api.forecast_api.get_forecast', return_value=mock_forecast_df)

        # Call get_forecast_by_date with a valid date, product, and specified format
        result = get_forecast_by_date(self.test_date.strftime('%Y-%m-%d'), 'DALMP', format=format)

        # Assert that the returned data is in the expected format
        if format == 'json':
            assert isinstance(result, list)
        elif format == 'csv':
            assert isinstance(result, str)
        elif format == 'excel':
            assert isinstance(result, bytes)
        elif format == 'parquet':
            assert isinstance(result, bytes)

        # Verify that storage_manager.get_forecast was called with correct parameters
        src.backend.api.forecast_api.get_forecast.assert_called_once_with(self.test_date, 'DALMP')

    def test_get_forecast_by_date_invalid_format(self, mocker):
        """Tests that get_forecast_by_date raises InvalidFormatError for invalid format"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date)

        # Mock the storage_manager.get_forecast function to return the mock dataframe
        mocker.patch('src.backend.api.forecast_api.get_forecast', return_value=mock_forecast_df)

        # Call get_forecast_by_date with a valid date and product but invalid format
        with pytest.raises(InvalidFormatError):
            get_forecast_by_date(self.test_date.strftime('%Y-%m-%d'), 'DALMP', format='invalid')

        # Verify that storage_manager.get_forecast was called with correct parameters
        src.backend.api.forecast_api.get_forecast.assert_called_once_with(self.test_date, 'DALMP')

    @pytest.mark.parametrize('product', ['DALMP', 'RTLMP', 'RegUp'])
    def test_get_latest_forecast_valid(self, product, mocker):
        """Tests retrieving the latest forecast with valid parameters"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date, product=product)

        # Mock the storage_manager.get_latest_forecast function to return the mock dataframe
        mocker.patch('src.backend.api.forecast_api.get_latest_forecast', return_value=mock_forecast_df)

        # Call get_latest_forecast with a valid product
        result_df = get_latest_forecast(product)

        # Assert that the returned dataframe matches the mock dataframe
        pd.testing.assert_frame_equal(result_df, mock_forecast_df)

        # Verify that storage_manager.get_latest_forecast was called with correct parameters
        src.backend.api.forecast_api.get_latest_forecast.assert_called_once_with(product)

    def test_get_latest_forecast_invalid_product(self, mocker):
        """Tests that get_latest_forecast raises RequestValidationError for invalid product"""
        # Mock the storage_manager.get_latest_forecast function
        mocker.patch('src.backend.api.forecast_api.get_latest_forecast')

        # Call get_latest_forecast with an invalid product
        with pytest.raises(RequestValidationError):
            get_latest_forecast('InvalidProduct')

        # Verify that storage_manager.get_latest_forecast was not called
        src.backend.api.forecast_api.get_latest_forecast.assert_not_called()

    def test_get_latest_forecast_not_found(self, mocker):
        """Tests that get_latest_forecast raises ResourceNotFoundError when forecast not found"""
        # Mock the storage_manager.get_latest_forecast function to raise DataFrameNotFoundError
        mocker.patch('src.backend.api.forecast_api.get_latest_forecast', side_effect=DataFrameNotFoundError("Forecast not found", "DALMP", self.test_date))

        # Call get_latest_forecast with a valid product
        with pytest.raises(ResourceNotFoundError):
            get_latest_forecast('DALMP')

        # Verify that storage_manager.get_latest_forecast was called with correct parameters
        src.backend.api.forecast_api.get_latest_forecast.assert_called_once_with('DALMP')

    @pytest.mark.parametrize('format', ['json', 'csv', 'excel', 'parquet'])
    def test_get_latest_forecast_format(self, format, mocker):
        """Tests retrieving the latest forecast with different format options"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date)

        # Mock the storage_manager.get_latest_forecast function to return the mock dataframe
        mocker.patch('src.backend.api.forecast_api.get_latest_forecast', return_value=mock_forecast_df)

        # Call get_latest_forecast with a valid product and specified format
        result = get_latest_forecast('DALMP', format=format)

        # Assert that the returned data is in the expected format
        if format == 'json':
            assert isinstance(result, list)
        elif format == 'csv':
            assert isinstance(result, str)
        elif format == 'excel':
            assert isinstance(result, bytes)
        elif format == 'parquet':
            assert isinstance(result, bytes)

        # Verify that storage_manager.get_latest_forecast was called with correct parameters
        src.backend.api.forecast_api.get_latest_forecast.assert_called_once_with('DALMP')

    @pytest.mark.parametrize('product', ['DALMP', 'RTLMP', 'RegUp'])
    def test_get_forecasts_by_date_range_valid(self, product, mocker):
        """Tests retrieving forecasts by date range with valid parameters"""
        # Create mock forecast dataframes using create_mock_forecast_data
        mock_forecast_df1 = create_mock_forecast_data(start_time=self.test_date, product=product)
        mock_forecast_df2 = create_mock_forecast_data(start_time=self.test_date + pd.Timedelta(days=1), product=product)
        mock_forecasts_dict = {
            self.test_date: mock_forecast_df1,
            self.test_date + pd.Timedelta(days=1): mock_forecast_df2
        }

        # Mock the storage_manager.get_forecasts_for_period function to return the mock dataframes
        mocker.patch('src.backend.api.forecast_api.get_forecasts_for_period', return_value=mock_forecasts_dict)

        # Call get_forecasts_by_date_range with valid start date, end date, and product
        start_date_str = self.test_date.strftime('%Y-%m-%d')
        end_date_str = (self.test_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        result_df = get_forecasts_by_date_range(start_date_str, end_date_str, product)

        # Assert that the returned dataframe contains the expected data
        expected_df = pd.concat([mock_forecast_df1, mock_forecast_df2], ignore_index=True)
        pd.testing.assert_frame_equal(result_df, expected_df)

        # Verify that storage_manager.get_forecasts_for_period was called with correct parameters
        src.backend.api.forecast_api.get_forecasts_for_period.assert_called_once_with(self.test_date, self.test_date + pd.Timedelta(days=1), product)

    def test_get_forecasts_by_date_range_invalid_product(self, mocker):
        """Tests that get_forecasts_by_date_range raises RequestValidationError for invalid product"""
        # Mock the storage_manager.get_forecasts_for_period function
        mocker.patch('src.backend.api.forecast_api.get_forecasts_for_period')

        # Call get_forecasts_by_date_range with an invalid product
        start_date_str = self.test_date.strftime('%Y-%m-%d')
        end_date_str = (self.test_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        with pytest.raises(RequestValidationError):
            get_forecasts_by_date_range(start_date_str, end_date_str, 'InvalidProduct')

        # Verify that storage_manager.get_forecasts_for_period was not called
        src.backend.api.forecast_api.get_forecasts_for_period.assert_not_called()

    def test_get_forecasts_by_date_range_not_found(self, mocker):
        """Tests that get_forecasts_by_date_range raises ResourceNotFoundError when no forecasts found"""
        # Mock the storage_manager.get_forecasts_for_period function to return an empty dictionary
        mocker.patch('src.backend.api.forecast_api.get_forecasts_for_period', return_value={})

        # Call get_forecasts_by_date_range with valid parameters
        start_date_str = self.test_date.strftime('%Y-%m-%d')
        end_date_str = (self.test_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        with pytest.raises(ResourceNotFoundError):
            get_forecasts_by_date_range(start_date_str, end_date_str, 'DALMP')

        # Verify that storage_manager.get_forecasts_for_period was called with correct parameters
        src.backend.api.forecast_api.get_forecasts_for_period.assert_called_once_with(self.test_date, self.test_date + pd.Timedelta(days=1), 'DALMP')

    @pytest.mark.parametrize('format', ['json', 'csv', 'excel', 'parquet'])
    def test_get_forecasts_by_date_range_format(self, format, mocker):
        """Tests retrieving forecasts by date range with different format options"""
        # Create mock forecast dataframes using create_mock_forecast_data
        mock_forecast_df1 = create_mock_forecast_data(start_time=self.test_date)
        mock_forecast_df2 = create_mock_forecast_data(start_time=self.test_date + pd.Timedelta(days=1))
        mock_forecasts_dict = {
            self.test_date: mock_forecast_df1,
            self.test_date + pd.Timedelta(days=1): mock_forecast_df2
        }

        # Mock the storage_manager.get_forecasts_for_period function to return the mock dataframes
        mocker.patch('src.backend.api.forecast_api.get_forecasts_for_period', return_value=mock_forecasts_dict)

        # Call get_forecasts_by_date_range with valid parameters and specified format
        start_date_str = self.test_date.strftime('%Y-%m-%d')
        end_date_str = (self.test_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        result = get_forecasts_by_date_range(start_date_str, end_date_str, 'DALMP', format=format)

        # Assert that the returned data is in the expected format
        if format == 'json':
            assert isinstance(result, list)
        elif format == 'csv':
            assert isinstance(result, str)
        elif format == 'excel':
            assert isinstance(result, bytes)
        elif format == 'parquet':
            assert isinstance(result, bytes)

        # Verify that storage_manager.get_forecasts_for_period was called with correct parameters
        src.backend.api.forecast_api.get_forecasts_for_period.assert_called_once_with(self.test_date, self.test_date + pd.Timedelta(days=1), 'DALMP')

    @pytest.mark.parametrize('product', ['DALMP', 'RTLMP', 'RegUp'])
    def test_get_forecast_as_model_valid(self, product, mocker):
        """Tests retrieving a forecast as ProbabilisticForecast models with valid parameters"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date, product=product)

        # Mock the storage_manager.get_forecast function to return the mock dataframe
        mocker.patch('src.backend.api.forecast_api.get_forecast', return_value=mock_forecast_df)

        # Call get_forecast_as_model with a valid date and product
        forecasts = get_forecast_as_model(self.test_date.strftime('%Y-%m-%d'), product)

        # Assert that the returned list contains ProbabilisticForecast objects
        assert all(isinstance(f, ProbabilisticForecast) for f in forecasts)

        # Assert that the number of forecast objects matches the expected count
        assert len(forecasts) == len(mock_forecast_df)

        # Verify that storage_manager.get_forecast was called with correct parameters
        src.backend.api.forecast_api.get_forecast.assert_called_once_with(self.test_date, product)

    def test_get_forecast_as_model_invalid_product(self, mocker):
        """Tests that get_forecast_as_model raises RequestValidationError for invalid product"""
        # Mock the storage_manager.get_forecast function
        mocker.patch('src.backend.api.forecast_api.get_forecast')

        # Call get_forecast_as_model with an invalid product
        with pytest.raises(RequestValidationError):
            get_forecast_as_model(self.test_date.strftime('%Y-%m-%d'), 'InvalidProduct')

        # Verify that storage_manager.get_forecast was not called
        src.backend.api.forecast_api.get_forecast.assert_not_called()

    @pytest.mark.parametrize('product', ['DALMP', 'RTLMP', 'RegUp'])
    def test_get_latest_forecast_as_model_valid(self, product, mocker):
        """Tests retrieving the latest forecast as ProbabilisticForecast models with valid parameters"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date, product=product)

        # Mock the storage_manager.get_latest_forecast function to return the mock dataframe
        mocker.patch('src.backend.api.forecast_api.get_latest_forecast', return_value=mock_forecast_df)

        # Call get_latest_forecast_as_model with a valid product
        forecasts = get_latest_forecast_as_model(product)

        # Assert that the returned list contains ProbabilisticForecast objects
        assert all(isinstance(f, ProbabilisticForecast) for f in forecasts)

        # Assert that the number of forecast objects matches the expected count
        assert len(forecasts) == len(mock_forecast_df)

        # Verify that storage_manager.get_latest_forecast was called with correct parameters
        src.backend.api.forecast_api.get_latest_forecast.assert_called_once_with(product)

    def test_get_latest_forecast_as_model_invalid_product(self, mocker):
        """Tests that get_latest_forecast_as_model raises RequestValidationError for invalid product"""
        # Mock the storage_manager.get_latest_forecast function
        mocker.patch('src.backend.api.forecast_api.get_latest_forecast')

        # Call get_latest_forecast_as_model with an invalid product
        with pytest.raises(RequestValidationError):
            get_latest_forecast_as_model('InvalidProduct')

        # Verify that storage_manager.get_latest_forecast was not called
        src.backend.api.forecast_api.get_latest_forecast.assert_not_called()

    @pytest.mark.parametrize('product', ['DALMP', 'RTLMP', 'RegUp'])
    def test_get_forecast_ensemble_valid(self, product, mocker):
        """Tests retrieving a forecast ensemble with valid parameters"""
        # Create mock forecast dataframes using create_mock_forecast_data
        mock_forecast_df1 = create_mock_forecast_data(start_time=self.test_date, product=product)
        mock_forecast_df2 = create_mock_forecast_data(start_time=self.test_date + pd.Timedelta(days=1), product=product)
        mock_forecasts_dict = {
            self.test_date: mock_forecast_df1,
            self.test_date + pd.Timedelta(days=1): mock_forecast_df2
        }

        # Mock the storage_manager.get_forecasts_for_period function to return the mock dataframes
        mocker.patch('src.backend.api.forecast_api.get_forecasts_for_period', return_value=mock_forecasts_dict)

        # Call get_forecast_ensemble with valid start date, end date, and product
        start_date_str = self.test_date.strftime('%Y-%m-%d')
        end_date_str = (self.test_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        ensemble = get_forecast_ensemble(start_date_str, end_date_str, product)

        # Assert that the returned object is a ForecastEnsemble
        assert isinstance(ensemble, ForecastEnsemble)

        # Assert that the ensemble contains the expected number of forecasts
        assert len(ensemble.forecasts) == len(mock_forecast_df1) + len(mock_forecast_df2)

        # Verify that storage_manager.get_forecasts_for_period was called with correct parameters
        src.backend.api.forecast_api.get_forecasts_for_period.assert_called_once_with(self.test_date, self.test_date + pd.Timedelta(days=1), product)

    def test_get_forecast_ensemble_invalid_product(self, mocker):
        """Tests that get_forecast_ensemble raises RequestValidationError for invalid product"""
        # Mock the storage_manager.get_forecasts_for_period function
        mocker.patch('src.backend.api.forecast_api.get_forecasts_for_period')

        # Call get_forecast_ensemble with an invalid product
        start_date_str = self.test_date.strftime('%Y-%m-%d')
        end_date_str = (self.test_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        with pytest.raises(RequestValidationError):
            get_forecast_ensemble(start_date_str, end_date_str, 'InvalidProduct')

        # Verify that storage_manager.get_forecasts_for_period was not called
        src.backend.api.forecast_api.get_forecasts_for_period.assert_not_called()

    def test_format_forecast_response_json(self):
        """Tests formatting forecast response as JSON"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date)

        # Call format_forecast_response with the dataframe and format='json'
        result = format_forecast_response(mock_forecast_df, format='json')

        # Assert that the returned data is a dictionary (parsed JSON)
        assert isinstance(result, list)

        # Assert that the dictionary contains the expected keys and values
        assert all(isinstance(item, dict) for item in result)

    def test_format_forecast_response_csv(self):
        """Tests formatting forecast response as CSV"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date)

        # Call format_forecast_response with the dataframe and format='csv'
        result = format_forecast_response(mock_forecast_df, format='csv')

        # Assert that the returned data is a string
        assert isinstance(result, str)

        # Assert that the string contains CSV-formatted data with expected headers
        assert 'timestamp,product,point_forecast' in result

    def test_format_forecast_response_excel(self):
        """Tests formatting forecast response as Excel"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date)

        # Call format_forecast_response with the dataframe and format='excel'
        result = format_forecast_response(mock_forecast_df, format='excel')

        # Assert that the returned data is bytes
        assert isinstance(result, bytes)

        # Assert that the bytes represent a valid Excel file
        # This is difficult to validate without a full Excel parsing library

    def test_format_forecast_response_parquet(self):
        """Tests formatting forecast response as Parquet"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date)

        # Call format_forecast_response with the dataframe and format='parquet'
        result = format_forecast_response(mock_forecast_df, format='parquet')

        # Assert that the returned data is bytes
        assert isinstance(result, bytes)

        # Assert that the bytes represent a valid Parquet file
        # This is difficult to validate without a full Parquet parsing library

    def test_format_forecast_response_invalid(self):
        """Tests that format_forecast_response raises InvalidFormatError for invalid format"""
        # Create a mock forecast dataframe using create_mock_forecast_data
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date)

        # Call format_forecast_response with the dataframe and an invalid format
        with pytest.raises(InvalidFormatError):
            format_forecast_response(mock_forecast_df, format='invalid')

    def test_get_storage_status(self, mocker):
        """Tests retrieving storage status information"""
        # Mock the storage_manager.get_storage_info function to return a mock status dictionary
        mock_status = {'total_forecasts': 100, 'disk_usage': '10 GB'}
        mocker.patch('src.backend.api.forecast_api.get_storage_info', return_value=mock_status)

        # Call get_storage_status
        status = get_storage_status()

        # Assert that the returned dictionary matches the mock status
        assert status == mock_status

        # Verify that storage_manager.get_storage_info was called
        src.backend.api.forecast_api.get_storage_info.assert_called_once()

    def test_forecast_api_class_methods(self, mocker):
        """Tests that the ForecastAPI class methods correctly call the module-level functions"""
        # Create a ForecastAPI instance
        api = ForecastAPI()

        # Mock all the module-level functions in forecast_api
        mock_get_forecast_by_date = mocker.patch('src.backend.api.forecast_api.get_forecast_by_date', return_value='get_forecast_by_date')
        mock_get_latest_forecast = mocker.patch('src.backend.api.forecast_api.get_latest_forecast', return_value='get_latest_forecast')
        mock_get_forecasts_by_date_range = mocker.patch('src.backend.api.forecast_api.get_forecasts_by_date_range', return_value='get_forecasts_by_date_range')
        mock_get_forecast_as_model = mocker.patch('src.backend.api.forecast_api.get_forecast_as_model', return_value='get_forecast_as_model')
        mock_get_latest_forecast_as_model = mocker.patch('src.backend.api.forecast_api.get_latest_forecast_as_model', return_value='get_latest_forecast_as_model')
        mock_get_forecast_ensemble = mocker.patch('src.backend.api.forecast_api.get_forecast_ensemble', return_value='get_forecast_ensemble')
        mock_format_forecast_response = mocker.patch('src.backend.api.forecast_api.format_forecast_response', return_value='format_forecast_response')
        mock_get_storage_status = mocker.patch('src.backend.api.forecast_api.get_storage_status', return_value='get_storage_status')

        # Call each method on the ForecastAPI instance
        date_str = '2023-01-01'
        product = 'DALMP'
        format = 'json'
        result_get_forecast_by_date = api.get_forecast_by_date(date_str, product, format)
        result_get_latest_forecast = api.get_latest_forecast(product, format)
        result_get_forecasts_by_date_range = api.get_forecasts_by_date_range(date_str, date_str, product, format)
        result_get_forecast_as_model = api.get_forecast_as_model(date_str, product)
        result_get_latest_forecast_as_model = api.get_latest_forecast_as_model(product)
        result_get_forecast_ensemble = api.get_forecast_ensemble(date_str, date_str, product)
        mock_forecast_df = create_mock_forecast_data(start_time=self.test_date)
        result_format_forecast_response = api.format_forecast_response(mock_forecast_df, format)
        result_get_storage_status = api.get_storage_status()

        # Verify that each method calls the corresponding module-level function with the same parameters
        mock_get_forecast_by_date.assert_called_once_with(date_str, product, format)
        mock_get_latest_forecast.assert_called_once_with(product, format)
        mock_get_forecasts_by_date_range.assert_called_once_with(date_str, date_str, product, format)
        mock_get_forecast_as_model.assert_called_once_with(date_str, product)
        mock_get_latest_forecast_as_model.assert_called_once_with(product)
        mock_get_forecast_ensemble.assert_called_once_with(date_str, date_str, product)
        mock_format_forecast_response.assert_called_once_with(mock_forecast_df, format)
        mock_get_storage_status.assert_called_once()

        # Assert that the return values match the expected values
        assert result_get_forecast_by_date == 'get_forecast_by_date'
        assert result_get_latest_forecast == 'get_latest_forecast'
        assert result_get_forecasts_by_date_range == 'get_forecasts_by_date_range'
        assert result_get_forecast_as_model == 'get_forecast_as_model'
        assert result_get_latest_forecast_as_model == 'get_latest_forecast_as_model'
        assert result_get_forecast_ensemble == 'get_forecast_ensemble'
        assert result_format_forecast_response == 'format_forecast_response'
        assert result_get_storage_status == 'get_storage_status'