"""Unit tests for the forecast client module that retrieves electricity market price forecasts from the backend API.
Tests the client's ability to fetch forecast data by date, date range, or latest available, with appropriate error handling and request formatting."""
import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import requests  # version ^2.28.0
import pandas as pd  # version 2.0.0+
from datetime import date, datetime  # standard library
import io  # standard library

from src.web.data.forecast_client import ForecastClient, get_forecast_by_date, get_latest_forecast, get_forecasts_by_date_range, validate_product, parse_response  # Client for retrieving forecast data from the backend API
from src.web.config.settings import API_BASE_URL, FORECAST_API_TIMEOUT  # Base URL for the forecast API
from src.web.config.product_config import PRODUCTS  # List of valid electricity market products
from src.web.utils.url_helpers import build_api_url, build_forecast_api_url  # Construct API URLs
from src.web.tests.fixtures.forecast_fixtures import create_sample_forecast_dataframe  # Create sample forecast dataframe for testing

TEST_API_URL = "http://test-api.example.com/api"
TEST_TIMEOUT = 10


def test_forecast_client_initialization():
    """Tests that the ForecastClient initializes correctly with default and custom parameters"""
    # Create a client with default parameters
    client_default = ForecastClient()
    assert client_default.base_url == API_BASE_URL
    assert client_default.timeout == FORECAST_API_TIMEOUT

    # Create a client with custom parameters
    client_custom = ForecastClient(base_url=TEST_API_URL, timeout=TEST_TIMEOUT)
    assert client_custom.base_url == TEST_API_URL
    assert client_custom.timeout == TEST_TIMEOUT


def test_validate_product_valid():
    """Tests that validate_product accepts valid products"""
    for product in PRODUCTS:
        validate_product(product)
        assert validate_product(product) is True


def test_validate_product_invalid():
    """Tests that validate_product rejects invalid products"""
    with pytest.raises(ValueError) as excinfo:
        validate_product("InvalidProduct")
    assert "Invalid product: InvalidProduct" in str(excinfo.value)


def test_get_forecast_by_date():
    """Tests that get_forecast_by_date correctly retrieves and processes forecast data"""
    # Create a mock response with sample forecast data
    sample_df = create_sample_forecast_dataframe()
    mock_response_data = sample_df.to_dict(orient="records")

    with unittest.mock.patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data

        # Call get_forecast_by_date with valid parameters
        product = "DALMP"
        date_str = "2023-08-01"
        forecast_df = get_forecast_by_date(product, date_str)

        # Verify that the correct URL was constructed
        expected_url = build_forecast_api_url(product, start_date=date_str, end_date=date_str)
        mock_get.assert_called_once_with(expected_url, timeout=FORECAST_API_TIMEOUT)

        # Verify that the returned dataframe has the expected structure
        assert isinstance(forecast_df, pd.DataFrame)
        assert not forecast_df.empty
        assert "timestamp" in forecast_df.columns
        assert "product" in forecast_df.columns

        # Verify that the function handles datetime objects correctly
        date_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        forecast_df_dt = get_forecast_by_date(product, date_dt)
        assert isinstance(forecast_df_dt, pd.DataFrame)
        assert not forecast_df_dt.empty


def test_get_forecast_by_date_with_client():
    """Tests that ForecastClient.get_forecast_by_date correctly retrieves and processes forecast data"""
    # Create a ForecastClient instance
    client = ForecastClient(base_url=TEST_API_URL, timeout=TEST_TIMEOUT)

    # Create a mock response with sample forecast data
    sample_df = create_sample_forecast_dataframe()
    mock_response_data = sample_df.to_dict(orient="records")

    with unittest.mock.patch.object(client.session, "get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data

        # Call client.get_forecast_by_date with valid parameters
        product = "DALMP"
        date_str = "2023-08-01"
        forecast_df = client.get_forecast_by_date(product, date_str)

        # Verify that the correct URL was constructed
        expected_url = build_forecast_api_url(product, start_date=date_str, end_date=date_str, base_url=TEST_API_URL)
        mock_get.assert_called_once_with(expected_url, timeout=TEST_TIMEOUT)

        # Verify that the returned dataframe has the expected structure
        assert isinstance(forecast_df, pd.DataFrame)
        assert not forecast_df.empty
        assert "timestamp" in forecast_df.columns
        assert "product" in forecast_df.columns

        # Verify that the function handles datetime objects correctly
        date_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        forecast_df_dt = client.get_forecast_by_date(product, date_dt)
        assert isinstance(forecast_df_dt, pd.DataFrame)
        assert not forecast_df_dt.empty


def test_get_latest_forecast():
    """Tests that get_latest_forecast correctly retrieves and processes the latest forecast data"""
    # Create a mock response with sample forecast data
    sample_df = create_sample_forecast_dataframe()
    mock_response_data = sample_df.to_dict(orient="records")

    with unittest.mock.patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data

        # Call get_latest_forecast with a valid product
        product = "DALMP"
        forecast_df = get_latest_forecast(product)

        # Verify that the correct URL was constructed
        expected_url = build_api_url(f"forecasts/{product}/latest")
        mock_get.assert_called_once_with(expected_url, timeout=FORECAST_API_TIMEOUT)

        # Verify that the returned dataframe has the expected structure
        assert isinstance(forecast_df, pd.DataFrame)
        assert not forecast_df.empty
        assert "timestamp" in forecast_df.columns
        assert "product" in forecast_df.columns


def test_get_latest_forecast_with_client():
    """Tests that ForecastClient.get_latest_forecast correctly retrieves and processes the latest forecast data"""
    # Create a ForecastClient instance
    client = ForecastClient(base_url=TEST_API_URL, timeout=TEST_TIMEOUT)

    # Create a mock response with sample forecast data
    sample_df = create_sample_forecast_dataframe()
    mock_response_data = sample_df.to_dict(orient="records")

    with unittest.mock.patch.object(client.session, "get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data

        # Call client.get_latest_forecast with a valid product
        product = "DALMP"
        forecast_df = client.get_latest_forecast(product)

        # Verify that the correct URL was constructed
        expected_url = build_api_url(f"forecasts/{product}/latest", base_url=TEST_API_URL)
        mock_get.assert_called_once_with(expected_url, timeout=TEST_TIMEOUT)

        # Verify that the returned dataframe has the expected structure
        assert isinstance(forecast_df, pd.DataFrame)
        assert not forecast_df.empty
        assert "timestamp" in forecast_df.columns
        assert "product" in forecast_df.columns


def test_get_forecasts_by_date_range():
    """Tests that get_forecasts_by_date_range correctly retrieves and processes forecast data for a date range"""
    # Create a mock response with sample forecast data
    sample_df = create_sample_forecast_dataframe()
    mock_response_data = sample_df.to_dict(orient="records")

    with unittest.mock.patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data

        # Call get_forecasts_by_date_range with valid parameters
        product = "DALMP"
        start_date_str = "2023-08-01"
        end_date_str = "2023-08-03"
        forecast_df = get_forecasts_by_date_range(product, start_date_str, end_date_str)

        # Verify that the correct URL was constructed with start_date and end_date
        expected_url = build_forecast_api_url(product, start_date=start_date_str, end_date=end_date_str)
        mock_get.assert_called_once_with(expected_url, timeout=FORECAST_API_TIMEOUT)

        # Verify that the returned dataframe has the expected structure
        assert isinstance(forecast_df, pd.DataFrame)
        assert not forecast_df.empty
        assert "timestamp" in forecast_df.columns
        assert "product" in forecast_df.columns

        # Verify that the function handles datetime objects correctly
        start_date_dt = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date_dt = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        forecast_df_dt = get_forecasts_by_date_range(product, start_date_dt, end_date_dt)
        assert isinstance(forecast_df_dt, pd.DataFrame)
        assert not forecast_df_dt.empty


def test_get_forecasts_by_date_range_with_client():
    """Tests that ForecastClient.get_forecasts_by_date_range correctly retrieves and processes forecast data for a date range"""
    # Create a ForecastClient instance
    client = ForecastClient(base_url=TEST_API_URL, timeout=TEST_TIMEOUT)

    # Create a mock response with sample forecast data
    sample_df = create_sample_forecast_dataframe()
    mock_response_data = sample_df.to_dict(orient="records")

    with unittest.mock.patch.object(client.session, "get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data

        # Call client.get_forecasts_by_date_range with valid parameters
        product = "DALMP"
        start_date_str = "2023-08-01"
        end_date_str = "2023-08-03"
        forecast_df = client.get_forecasts_by_date_range(product, start_date_str, end_date_str)

        # Verify that the correct URL was constructed with start_date and end_date
        expected_url = build_forecast_api_url(product, start_date=start_date_str, end_date=end_date_str, base_url=TEST_API_URL)
        mock_get.assert_called_once_with(expected_url, timeout=TEST_TIMEOUT)

        # Verify that the returned dataframe has the expected structure
        assert isinstance(forecast_df, pd.DataFrame)
        assert not forecast_df.empty
        assert "timestamp" in forecast_df.columns
        assert "product" in forecast_df.columns

        # Verify that the function handles datetime objects correctly
        start_date_dt = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date_dt = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        forecast_df_dt = client.get_forecasts_by_date_range(product, start_date_dt, end_date_dt)
        assert isinstance(forecast_df_dt, pd.DataFrame)
        assert not forecast_df_dt.empty


def test_parse_response_json():
    """Tests that parse_response correctly processes JSON response data"""
    # Create a mock response with JSON data
    sample_df = create_sample_forecast_dataframe()
    mock_response_data = sample_df.to_dict(orient="records")

    with unittest.mock.patch("requests.Response") as MockResponse:
        mock_response = MockResponse()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data

        # Call parse_response with the mock response and format='json'
        forecast_df = parse_response(mock_response, format='json')

        # Verify that the returned dataframe has the expected structure
        assert isinstance(forecast_df, pd.DataFrame)
        assert not forecast_df.empty
        assert "timestamp" in forecast_df.columns
        assert "product" in forecast_df.columns

        # Verify that the JSON data was correctly converted to a dataframe
        assert len(forecast_df) == len(mock_response_data)


def test_parse_response_csv():
    """Tests that parse_response correctly processes CSV response data"""
    # Create a mock response with CSV data
    sample_df = create_sample_forecast_dataframe()
    mock_response_data = sample_df.to_csv(index=False)

    with unittest.mock.patch("requests.Response") as MockResponse:
        mock_response = MockResponse()
        mock_response.status_code = 200
        mock_response.text = mock_response_data

        # Call parse_response with the mock response and format='csv'
        forecast_df = parse_response(mock_response, format='csv')

        # Verify that the returned dataframe has the expected structure
        assert isinstance(forecast_df, pd.DataFrame)
        assert not forecast_df.empty
        assert "timestamp" in forecast_df.columns
        assert "product" in forecast_df.columns

        # Verify that the CSV data was correctly converted to a dataframe
        assert len(forecast_df) == len(sample_df)


def test_parse_response_error():
    """Tests that parse_response correctly handles error responses"""
    with unittest.mock.patch("requests.Response") as MockResponse:
        mock_response = MockResponse()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"

        # Call parse_response with the mock response
        with pytest.raises(Exception) as excinfo:
            parse_response(mock_response)

        # Verify that an exception is raised
        assert "API request failed with status code: 400" in str(excinfo.value)
        # Verify that the error message includes the status code
        assert "Bad Request" in str(excinfo.value)


def test_check_api_health():
    """Tests that check_api_health correctly determines API availability"""
    # Create a ForecastClient instance
    client = ForecastClient(base_url=TEST_API_URL, timeout=TEST_TIMEOUT)

    with unittest.mock.patch.object(client.session, "get") as mock_get:
        # Create a mock response with 200 status code
        mock_get.return_value.status_code = 200
        # Call client.check_api_health()
        assert client.check_api_health() is True

        # Create a mock response with error status code
        mock_get.return_value.status_code = 500
        # Call client.check_api_health()
        assert client.check_api_health() is False

        # Mock the session.get method to raise exception
        mock_get.side_effect = requests.ConnectionError("Connection error")
        # Call client.check_api_health()
        assert client.check_api_health() is False


def test_client_close():
    """Tests that the ForecastClient.close method correctly closes the session"""
    # Create a ForecastClient instance
    client = ForecastClient(base_url=TEST_API_URL, timeout=TEST_TIMEOUT)

    # Mock the session.close method
    with unittest.mock.patch.object(client.session, "close") as mock_close:
        # Call client.close()
        client.close()

        # Verify that session.close was called
        mock_close.assert_called_once()


def test_error_handling_connection_error():
    """Tests that the client correctly handles connection errors"""
    # Create a ForecastClient instance
    client = ForecastClient(base_url=TEST_API_URL, timeout=TEST_TIMEOUT)

    # Patch the client's session.get to raise requests.ConnectionError
    with unittest.mock.patch.object(client.session, "get", side_effect=requests.ConnectionError("Connection error")):
        # Call client.get_forecast_by_date with valid parameters
        with pytest.raises(requests.ConnectionError) as excinfo:
            client.get_forecast_by_date("DALMP", "2023-01-01")

        # Verify that an appropriate exception is raised
        assert "Connection error" in str(excinfo.value)
        # Verify that the error message mentions connection issues
        assert "Connection error" in str(excinfo.value)


def test_error_handling_timeout():
    """Tests that the client correctly handles timeout errors"""
    # Create a ForecastClient instance
    client = ForecastClient(base_url=TEST_API_URL, timeout=TEST_TIMEOUT)

    # Patch the client's session.get to raise requests.Timeout
    with unittest.mock.patch.object(client.session, "get", side_effect=requests.Timeout("Timeout error")):
        # Call client.get_forecast_by_date with valid parameters
        with pytest.raises(requests.Timeout) as excinfo:
            client.get_forecast_by_date("DALMP", "2023-01-01")

        # Verify that an appropriate exception is raised
        assert "Timeout error" in str(excinfo.value)
        # Verify that the error message mentions timeout
        assert "Timeout error" in str(excinfo.value)


def test_error_handling_invalid_json():
    """Tests that the client correctly handles invalid JSON responses"""
    # Create a ForecastClient instance
    client = ForecastClient(base_url=TEST_API_URL, timeout=TEST_TIMEOUT)

    # Create a mock response with invalid JSON data
    with unittest.mock.patch.object(client.session, "get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.side_effect = ValueError("Invalid JSON")

        # Call client.get_forecast_by_date with valid parameters
        with pytest.raises(Exception) as excinfo:
            client.get_forecast_by_date("DALMP", "2023-01-01")

        # Verify that an appropriate exception is raised
        assert "Invalid JSON" in str(excinfo.value)
        # Verify that the error message mentions JSON parsing
        assert "Invalid JSON" in str(excinfo.value)


class TestForecastClient:
    """Test class for the ForecastClient"""

    def setup_method(self, method):
        """Set up method called before each test"""
        # Create a ForecastClient instance for testing
        self.client = ForecastClient(base_url=TEST_API_URL, timeout=TEST_TIMEOUT)
        # Set up common test data
        self.product = "DALMP"
        self.date_str = "2023-08-01"
        # Create sample forecast dataframe for testing
        self.sample_df = create_sample_forecast_dataframe()

    def teardown_method(self, method):
        """Tear down method called after each test"""
        # Close the client session
        self.client.close()
        # Clean up any test resources
        pass

    def test_initialization(self):
        """Test that the client initializes correctly"""
        # Verify that the client's base_url is set correctly
        assert self.client.base_url == TEST_API_URL
        # Verify that the client's timeout is set correctly
        assert self.client.timeout == TEST_TIMEOUT
        # Verify that the client's session is initialized
        assert isinstance(self.client.session, requests.Session)

    def test_get_forecast_by_date(self):
        """Test that get_forecast_by_date works correctly"""
        # Mock the session.get method to return sample data
        with unittest.mock.patch.object(self.client.session, "get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = self.sample_df.to_dict(orient="records")

            # Call get_forecast_by_date with test parameters
            forecast_df = self.client.get_forecast_by_date(self.product, self.date_str)

            # Verify that the correct URL was constructed
            expected_url = build_forecast_api_url(self.product, start_date=self.date_str, end_date=self.date_str, base_url=TEST_API_URL)
            mock_get.assert_called_once_with(expected_url, timeout=TEST_TIMEOUT)

            # Verify that the returned dataframe is correct
            assert isinstance(forecast_df, pd.DataFrame)
            assert not forecast_df.empty
            assert "timestamp" in forecast_df.columns
            assert "product" in forecast_df.columns

    def test_get_latest_forecast(self):
        """Test that get_latest_forecast works correctly"""
        # Mock the session.get method to return sample data
        with unittest.mock.patch.object(self.client.session, "get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = self.sample_df.to_dict(orient="records")

            # Call get_latest_forecast with test parameters
            forecast_df = self.client.get_latest_forecast(self.product)

            # Verify that the correct URL was constructed
            expected_url = build_api_url(f"forecasts/{self.product}/latest", base_url=TEST_API_URL)
            mock_get.assert_called_once_with(expected_url, timeout=TEST_TIMEOUT)

            # Verify that the returned dataframe is correct
            assert isinstance(forecast_df, pd.DataFrame)
            assert not forecast_df.empty
            assert "timestamp" in forecast_df.columns
            assert "product" in forecast_df.columns

    def test_get_forecasts_by_date_range(self):
        """Test that get_forecasts_by_date_range works correctly"""
        # Mock the session.get method to return sample data
        with unittest.mock.patch.object(self.client.session, "get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = self.sample_df.to_dict(orient="records")

            # Call get_forecasts_by_date_range with test parameters
            start_date = "2023-08-01"
            end_date = "2023-08-03"
            forecast_df = self.client.get_forecasts_by_date_range(self.product, start_date, end_date)

            # Verify that the correct URL was constructed
            expected_url = build_forecast_api_url(self.product, start_date=start_date, end_date=end_date, base_url=TEST_API_URL)
            mock_get.assert_called_once_with(expected_url, timeout=TEST_TIMEOUT)

            # Verify that the returned dataframe is correct
            assert isinstance(forecast_df, pd.DataFrame)
            assert not forecast_df.empty
            assert "timestamp" in forecast_df.columns
            assert "product" in forecast_df.columns

    def test_parse_response(self):
        """Test that parse_response works correctly for different formats"""
        # Create mock responses for different formats (JSON, CSV, etc.)
        json_data = self.sample_df.to_dict(orient="records")
        csv_data = self.sample_df.to_csv(index=False)

        with unittest.mock.patch("requests.Response") as MockResponse:
            # JSON response
            mock_response_json = MockResponse()
            mock_response_json.status_code = 200
            mock_response_json.json.return_value = json_data
            json_df = parse_response(mock_response_json, format="json")
            assert isinstance(json_df, pd.DataFrame)

            # CSV response
            mock_response_csv = MockResponse()
            mock_response_csv.status_code = 200
            mock_response_csv.text = csv_data
            csv_df = parse_response(mock_response_csv, format="csv")
            assert isinstance(csv_df, pd.DataFrame)

            # Call parse_response with each mock response
            # Verify that the correct parsing logic is applied
            # Verify that the returned dataframes are correct
            assert len(json_df) == len(self.sample_df)
            assert len(csv_df) == len(self.sample_df)

    def test_check_api_health(self):
        """Test that check_api_health correctly determines API availability"""
        with unittest.mock.patch.object(self.client.session, "get") as mock_get:
            # Mock the session.get method to return success response
            mock_get.return_value.status_code = 200
            # Call check_api_health and verify it returns True
            assert self.client.check_api_health() is True

            # Mock the session.get method to return error response
            mock_get.return_value.status_code = 500
            # Call check_api_health and verify it returns False
            assert self.client.check_api_health() is False

            # Mock the session.get method to raise exception
            mock_get.side_effect = requests.ConnectionError("Connection error")
            # Call check_api_health and verify it returns False
            assert self.client.check_api_health() is False

    def test_error_handling(self):
        """Test that the client correctly handles various error conditions"""
        # Test handling of connection errors
        with unittest.mock.patch.object(self.client.session, "get", side_effect=requests.ConnectionError("Connection error")):
            with pytest.raises(requests.ConnectionError):
                self.client.get_forecast_by_date("DALMP", "2023-01-01")

        # Test handling of timeout errors
        with unittest.mock.patch.object(self.client.session, "get", side_effect=requests.Timeout("Timeout error")):
            with pytest.raises(requests.Timeout):
                self.client.get_forecast_by_date("DALMP", "2023-01-01")

        # Test handling of invalid responses
        with unittest.mock.patch.object(self.client.session, "get") as mock_get:
            mock_get.return_value.status_code = 400
            mock_get.return_value.text = "Bad Request"
            with pytest.raises(Exception):
                self.client.get_forecast_by_date("DALMP", "2023-01-01")

        # Test handling of server errors
        with unittest.mock.patch.object(self.client.session, "get") as mock_get:
            mock_get.return_value.status_code = 500
            mock_get.return_value.text = "Internal Server Error"
            with pytest.raises(Exception):
                self.client.get_forecast_by_date("DALMP", "2023-01-01")

        # Verify that appropriate exceptions are raised
        pass