import pytest
from unittest.mock import patch, MagicMock
import requests
from datetime import datetime
import json
import time

# Import the code to be tested
from ...data_ingestion.api_client import APIClient, fetch_data, get_api_config, make_request, handle_response
from ...data_ingestion.exceptions import APIConnectionError, APIResponseError
from ...config.settings import DATA_SOURCES


class MockResponse:
    """Mock Response class for testing API responses"""
    
    def __init__(self, status_code=200, json_data=None, raise_json_error=False):
        """Initialize mock response with test data"""
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = json.dumps(json_data) if json_data else ""
        self._raise_json_error = raise_json_error
        
    def json(self):
        """Mock json method that returns stored JSON data or raises ValueError"""
        if self._raise_json_error:
            raise ValueError("Invalid JSON")
        return self._json_data


def test_get_api_config_valid_source():
    """Tests that get_api_config returns correct configuration for valid source"""
    # Call get_api_config with a valid source name from DATA_SOURCES
    source_name = "load_forecast"  # Assuming this key exists in DATA_SOURCES
    config = get_api_config(source_name)
    
    # Assert that the returned configuration matches the expected values
    assert config == DATA_SOURCES[source_name]
    # Verify URL and API key are correctly retrieved
    assert "url" in config
    assert "api_key" in config


def test_get_api_config_invalid_source():
    """Tests that get_api_config raises ValueError for invalid source"""
    # Use pytest.raises to verify ValueError is raised
    with pytest.raises(ValueError) as exc_info:
        # Call get_api_config with an invalid source name
        get_api_config("invalid_source")
    
    # Verify the error message contains available sources
    error_msg = str(exc_info.value)
    assert "Unknown data source" in error_msg
    assert "Available sources" in error_msg
    for source in DATA_SOURCES.keys():
        assert source in error_msg


def test_make_request_success():
    """Tests successful API request with make_request function"""
    # Create mock Response object with success status code
    mock_response = MockResponse(status_code=200, json_data={"data": "test"})
    
    # Mock requests.request to return the mock response
    with patch('requests.request', return_value=mock_response) as mock_request:
        # Call make_request with test parameters
        url = "http://test.com/api"
        params = {"param1": "value1"}
        headers = {"Authorization": "Bearer test_token"}
        response = make_request(method="GET", url=url, params=params, headers=headers)
        
        # Assert that the returned response matches the mock response
        assert response == mock_response
        
        # Verify requests.request was called with correct parameters
        mock_request.assert_called_once_with(
            method="GET",
            url=url,
            params=params,
            headers=headers,
            json=None,
            timeout=30  # Default timeout
        )


def test_make_request_retry_success():
    """Tests successful retry after initial connection failure"""
    # Create mock Response object with success status code
    mock_response = MockResponse(status_code=200, json_data={"data": "test"})
    
    # Mock requests.request to raise ConnectionError on first call, then return mock response
    with patch('requests.request') as mock_request:
        mock_request.side_effect = [requests.ConnectionError("Connection refused"), mock_response]
        
        # Mock time.sleep to avoid actual delays during test
        with patch('time.sleep') as mock_sleep:
            # Call make_request with test parameters
            url = "http://test.com/api"
            response = make_request(method="GET", url=url, max_retries=3)
            
            # Assert that the returned response matches the mock response
            assert response == mock_response
            
            # Verify requests.request was called twice
            assert mock_request.call_count == 2
            
            # Verify time.sleep was called with correct backoff time
            mock_sleep.assert_called_once_with(1.5)  # Default backoff factor


def test_make_request_max_retries_exceeded():
    """Tests that APIConnectionError is raised when max retries are exceeded"""
    # Mock requests.request to always raise ConnectionError
    with patch('requests.request', side_effect=requests.ConnectionError("Connection refused")) as mock_request:
        # Mock time.sleep to avoid actual delays during test
        with patch('time.sleep') as mock_sleep:
            # Use pytest.raises to verify APIConnectionError is raised
            with pytest.raises(APIConnectionError) as exc_info:
                # Call make_request with test parameters and small max_retries value
                url = "http://test.com/api"
                make_request(method="GET", url=url, max_retries=2)
            
            # Verify requests.request was called exactly max_retries + 1 times
            assert mock_request.call_count == 3  # Initial call + 2 retries
            
            # Verify time.sleep was called with correct backoff times
            assert mock_sleep.call_count == 2
            mock_sleep.assert_has_calls([
                patch.call(1.5),  # First retry
                patch.call(1.5 ** 2)  # Second retry with exponential backoff
            ])


def test_handle_response_success():
    """Tests successful handling of API response"""
    # Create mock Response object with success status code and JSON content
    json_data = {"result": "success", "data": [1, 2, 3]}
    mock_response = MockResponse(status_code=200, json_data=json_data)
    
    # Call handle_response with mock response
    result = handle_response(mock_response, "test_source")
    
    # Assert that the returned data matches the expected JSON content
    assert result == json_data


def test_handle_response_invalid_json():
    """Tests handling of response with invalid JSON"""
    # Create mock Response object with success status code but invalid JSON content
    mock_response = MockResponse(status_code=200, json_data={}, raise_json_error=True)
    
    # Use pytest.raises to verify APIResponseError is raised
    with pytest.raises(APIResponseError) as exc_info:
        # Call handle_response with mock response
        handle_response(mock_response, "test_source")
    
    # Verify error message mentions JSON parsing error
    error_msg = str(exc_info.value)
    assert "Invalid JSON" in error_msg or "JSON" in error_msg


def test_handle_response_error_status():
    """Tests handling of response with error status code"""
    # Create mock Response object with error status code (e.g., 404, 500)
    error_data = {"error": "Not found", "code": 404}
    mock_response = MockResponse(status_code=404, json_data=error_data)
    
    # Use pytest.raises to verify APIResponseError is raised
    with pytest.raises(APIResponseError) as exc_info:
        # Call handle_response with mock response
        handle_response(mock_response, "test_source")
    
    # Verify error message contains status code
    error_msg = str(exc_info.value)
    assert "404" in error_msg


def test_fetch_data():
    """Tests fetch_data function with mocked dependencies"""
    # Mock get_api_config to return test configuration
    test_config = {"url": "http://test.com/api", "api_key": "test_key"}
    
    # Patch get_api_config in the api_client module
    with patch('...data_ingestion.api_client.get_api_config', return_value=test_config) as mock_get_config:
        # Mock get function to return mock response
        mock_response = MockResponse(status_code=200, json_data={"data": "test_data"})
        with patch('...data_ingestion.api_client.get', return_value=mock_response) as mock_get:
            # Mock handle_response to return test data
            test_data = {"result": "success", "items": [1, 2, 3]}
            with patch('...data_ingestion.api_client.handle_response', return_value=test_data) as mock_handle:
                # Create test date range parameters
                start_date = datetime(2023, 1, 1)
                end_date = datetime(2023, 1, 7)
                
                # Call fetch_data with test parameters
                result = fetch_data("test_source", start_date=start_date, end_date=end_date)
                
                # Assert that the returned data matches expected test data
                assert result == test_data
                
                # Verify get_api_config was called with correct source name
                mock_get_config.assert_called_once_with("test_source")
                
                # Verify get was called with correct URL and parameters
                expected_params = {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                }
                mock_get.assert_called_once()
                call_args = mock_get.call_args[1]
                assert call_args["url"] == test_config["url"]
                assert call_args["params"] == expected_params
                assert call_args["headers"] == {"Authorization": f"Bearer {test_config['api_key']}"}
                
                # Verify handle_response was called with mock response
                mock_handle.assert_called_once_with(mock_response, "test_source")


def test_api_client_init():
    """Tests initialization of APIClient class"""
    # Mock get_api_config to return test configuration
    test_config = {"url": "http://test.com/api", "api_key": "test_key"}
    with patch('...data_ingestion.api_client.get_api_config', return_value=test_config) as mock_get_config:
        # Create APIClient instance with test source name
        client = APIClient("test_source")
        
        # Assert that client's _source_name attribute matches test source name
        assert client._source_name == "test_source"
        
        # Assert that client's _config attribute matches test configuration
        assert client._config == test_config
        
        # Verify get_api_config was called with correct source name
        mock_get_config.assert_called_once_with("test_source")


def test_api_client_get_data():
    """Tests get_data method of APIClient class"""
    # Mock fetch_data to return test data
    test_data = {"result": "success", "items": [1, 2, 3]}
    with patch('...data_ingestion.api_client.fetch_data', return_value=test_data) as mock_fetch:
        # Create APIClient instance with test source name
        client = APIClient("test_source")
        
        # Create test date range and additional parameters
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 7)
        additional_params = {"param1": "value1"}
        
        # Call client.get_data with test parameters
        result = client.get_data(start_date, end_date, additional_params)
        
        # Assert that the returned data matches expected test data
        assert result == test_data
        
        # Verify fetch_data was called with correct parameters
        mock_fetch.assert_called_once_with(
            "test_source",
            params=additional_params,
            start_date=start_date,
            end_date=end_date
        )


def test_api_client_get_latest_data():
    """Tests get_latest_data method of APIClient class"""
    # Mock get function to return mock response
    mock_response = MockResponse(status_code=200, json_data={"data": "latest_data"})
    with patch('...data_ingestion.api_client.get', return_value=mock_response) as mock_get:
        # Mock handle_response to return test data
        test_data = {"result": "success", "latest": True}
        with patch('...data_ingestion.api_client.handle_response', return_value=test_data) as mock_handle:
            # Create APIClient instance with test source name
            client = APIClient("test_source")
            client._config = {"url": "http://test.com/api", "api_key": "test_key"}
            
            # Call client.get_latest_data with test parameters
            params = {"param1": "value1"}
            result = client.get_latest_data(params)
            
            # Assert that the returned data matches expected test data
            assert result == test_data
            
            # Verify get was called with correct URL and parameters including latest=true flag
            expected_params = {"param1": "value1", "latest": True}
            mock_get.assert_called_once()
            call_args = mock_get.call_args[1]
            assert call_args["url"] == client._config["url"]
            assert call_args["params"] == expected_params
            assert call_args["headers"] == {"Authorization": f"Bearer {client._config['api_key']}"}
            
            # Verify handle_response was called with mock response
            mock_handle.assert_called_once_with(mock_response, "test_source")


def test_api_client_submit_data():
    """Tests submit_data method of APIClient class"""
    # Mock post function to return mock response
    mock_response = MockResponse(status_code=201, json_data={"status": "created"})
    with patch('...data_ingestion.api_client.post', return_value=mock_response) as mock_post:
        # Mock handle_response to return test response data
        test_response = {"id": 123, "status": "created"}
        with patch('...data_ingestion.api_client.handle_response', return_value=test_response) as mock_handle:
            # Create APIClient instance with test source name
            client = APIClient("test_source")
            client._config = {"url": "http://test.com/api", "api_key": "test_key"}
            
            # Create test data and parameters for submission
            test_data = {"name": "Test", "value": 42}
            params = {"action": "create"}
            
            # Call client.submit_data with test parameters
            result = client.submit_data(test_data, params)
            
            # Assert that the returned data matches expected response data
            assert result == test_response
            
            # Verify post was called with correct URL, data, and parameters
            mock_post.assert_called_once()
            call_args = mock_post.call_args[1]
            assert call_args["url"] == client._config["url"]
            assert call_args["params"] == params
            assert call_args["headers"] == {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {client._config['api_key']}"
            }
            assert call_args["json_data"] == test_data
            
            # Verify handle_response was called with mock response
            mock_handle.assert_called_once_with(mock_response, "test_source")