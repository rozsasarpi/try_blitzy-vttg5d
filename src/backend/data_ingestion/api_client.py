"""
Generic API client for the Electricity Market Price Forecasting System.

This module provides a unified interface for making API calls to external data sources
with proper error handling, retry logic, and logging. It is used by the data ingestion
component to reliably retrieve load forecasts, historical prices, and generation forecasts.
"""

import requests  # version: 2.28.2
import time
from typing import Dict, Any, Optional, Union
import json
from datetime import datetime  # standard library

# Internal imports
from ..config.settings import DATA_SOURCES
from .exceptions import APIConnectionError, APIResponseError
from ..utils.logging_utils import ComponentLogger

# Global variables
logger = ComponentLogger('api_client', {'component': 'data_ingestion'})
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1.5
DEFAULT_TIMEOUT = 30  # seconds


def get_api_config(source_name: str) -> Dict[str, Any]:
    """
    Retrieves API configuration for a specific data source.
    
    Args:
        source_name: Name of the data source to get configuration for
        
    Returns:
        API configuration with URL and API key
        
    Raises:
        ValueError: If the source name is not found in the configuration
    """
    if source_name in DATA_SOURCES:
        return DATA_SOURCES[source_name]
    else:
        available_sources = list(DATA_SOURCES.keys())
        raise ValueError(f"Unknown data source: {source_name}. Available sources: {available_sources}")


def make_request(
    method: str,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT,
    max_retries: int = MAX_RETRIES,
    backoff_factor: float = RETRY_BACKOFF_FACTOR
) -> requests.Response:
    """
    Makes an HTTP request to an API endpoint with retry logic.
    
    Args:
        method: HTTP method (GET, POST, etc.)
        url: API endpoint URL
        params: Query parameters
        headers: HTTP headers
        json_data: JSON data for POST requests
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        backoff_factor: Factor for exponential backoff between retries
        
    Returns:
        Response from the API
        
    Raises:
        APIConnectionError: If connection fails after all retries
    """
    retry_count = 0
    request_params = {
        'url': url,
        'method': method,
        'params': params or {},
        'json_data': json_data
    }
    
    logger.log_start("API request", request_params)
    start_time = time.time()
    
    while retry_count <= max_retries:
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                json=json_data,
                timeout=timeout
            )
            
            logger.log_completion("API request", start_time, {
                'status_code': response.status_code,
                'url': url,
                'method': method
            })
            
            return response
            
        except (requests.ConnectionError, requests.Timeout) as e:
            retry_count += 1
            
            if retry_count <= max_retries:
                # Calculate backoff time using exponential backoff
                backoff_time = backoff_factor ** (retry_count - 1)
                
                logger.with_context({
                    'retry_count': retry_count,
                    'max_retries': max_retries,
                    'backoff_time': backoff_time
                }).adapter.warning(
                    f"API request failed, retrying in {backoff_time:.2f} seconds "
                    f"(attempt {retry_count}/{max_retries}): {str(e)}"
                )
                
                time.sleep(backoff_time)
            else:
                logger.log_failure("API request", start_time, e, {
                    'url': url,
                    'method': method,
                    'max_retries_exceeded': True
                })
                
                raise APIConnectionError(url, source_name='unknown', original_exception=e)
                
        except Exception as e:
            logger.log_failure("API request", start_time, e, {
                'url': url,
                'method': method
            })
            
            raise


def get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = DEFAULT_TIMEOUT
) -> requests.Response:
    """
    Makes a GET request to an API endpoint.
    
    Args:
        url: API endpoint URL
        params: Query parameters
        headers: HTTP headers
        timeout: Request timeout in seconds
        
    Returns:
        Response from the API
    """
    return make_request(
        method='GET',
        url=url,
        params=params,
        headers=headers,
        timeout=timeout
    )


def post(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT
) -> requests.Response:
    """
    Makes a POST request to an API endpoint.
    
    Args:
        url: API endpoint URL
        params: Query parameters
        headers: HTTP headers
        json_data: JSON data for the request body
        timeout: Request timeout in seconds
        
    Returns:
        Response from the API
    """
    return make_request(
        method='POST',
        url=url,
        params=params,
        headers=headers,
        json_data=json_data,
        timeout=timeout
    )


def handle_response(response: requests.Response, source_name: str) -> Dict[str, Any]:
    """
    Handles an API response, checking for errors and parsing JSON.
    
    Args:
        response: Response from the API
        source_name: Name of the data source
        
    Returns:
        Parsed JSON response data
        
    Raises:
        APIResponseError: If the response indicates an error or cannot be parsed
    """
    if 200 <= response.status_code < 300:
        try:
            return response.json()
        except json.JSONDecodeError as e:
            logger.adapter.error(f"Failed to parse JSON response from {source_name}: {str(e)}")
            raise APIResponseError(
                api_endpoint=response.url,
                status_code=response.status_code,
                response_data={'error': 'Invalid JSON response'}
            )
    else:
        # Try to get error details from response
        error_data = None
        try:
            error_data = response.json()
        except:
            error_data = {'raw_text': response.text[:500] + ('...' if len(response.text) > 500 else '')}
            
        logger.adapter.error(
            f"API error from {source_name}: Status {response.status_code}, "
            f"URL: {response.url}, Error: {error_data}"
        )
        
        raise APIResponseError(
            api_endpoint=response.url,
            status_code=response.status_code,
            response_data=error_data
        )


def fetch_data(
    source_name: str,
    params: Optional[Dict[str, Any]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Fetches data from a specific data source API.
    
    Args:
        source_name: Name of the data source
        params: Additional query parameters
        start_date: Start date for the data range
        end_date: End date for the data range
        
    Returns:
        Parsed data from the API
        
    Raises:
        ValueError: If the source name is invalid
        APIConnectionError: If connection to the API fails
        APIResponseError: If the API returns an error response
    """
    api_config = get_api_config(source_name)
    url = api_config['url']
    api_key = api_config.get('api_key')
    
    # Prepare request parameters
    request_params = params or {}
    
    # Add date range if provided
    if start_date:
        request_params['start_date'] = start_date.isoformat()
    if end_date:
        request_params['end_date'] = end_date.isoformat()
    
    # Prepare headers with API key if provided
    headers = {}
    if api_key:
        headers['Authorization'] = f"Bearer {api_key}"
    
    # Make the request
    response = get(url, params=request_params, headers=headers)
    
    # Handle the response
    return handle_response(response, source_name)


class APIClient:
    """
    Client for interacting with external data source APIs.
    
    This class provides a simple interface for fetching data from external APIs
    with proper error handling and logging. It is used by the data ingestion component
    to retrieve load forecasts, historical prices, and generation forecasts.
    """
    
    def __init__(self, source_name: str):
        """
        Initializes an API client for a specific data source.
        
        Args:
            source_name: Name of the data source to connect to
            
        Raises:
            ValueError: If the source name is invalid
        """
        self._source_name = source_name
        self._config = get_api_config(source_name)
        self._logger = ComponentLogger('api_client', {
            'component': 'data_ingestion',
            'source': source_name
        })
    
    def get_data(
        self,
        start_date: datetime,
        end_date: datetime,
        additional_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Fetches data from the API with date range parameters.
        
        Args:
            start_date: Start date for the data range
            end_date: End date for the data range
            additional_params: Additional query parameters
            
        Returns:
            Parsed data from the API
            
        Raises:
            APIConnectionError: If connection to the API fails
            APIResponseError: If the API returns an error response
        """
        params = additional_params or {}
        return fetch_data(
            self._source_name,
            params=params,
            start_date=start_date,
            end_date=end_date
        )
    
    def get_latest_data(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Fetches the latest available data from the API.
        
        Args:
            params: Additional query parameters
            
        Returns:
            Latest data from the API
            
        Raises:
            APIConnectionError: If connection to the API fails
            APIResponseError: If the API returns an error response
        """
        request_params = params or {}
        request_params['latest'] = True
        
        url = self._config['url']
        api_key = self._config.get('api_key')
        
        # Prepare headers with API key if provided
        headers = {}
        if api_key:
            headers['Authorization'] = f"Bearer {api_key}"
        
        # Make the request
        response = get(url, params=request_params, headers=headers)
        
        # Handle the response
        return handle_response(response, self._source_name)
    
    def submit_data(
        self,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Submits data to the API via POST request.
        
        Args:
            data: Data to submit
            params: Additional query parameters
            
        Returns:
            Response data from the API
            
        Raises:
            APIConnectionError: If connection to the API fails
            APIResponseError: If the API returns an error response
        """
        url = self._config['url']
        api_key = self._config.get('api_key')
        
        # Prepare headers with API key if provided
        headers = {
            'Content-Type': 'application/json'
        }
        if api_key:
            headers['Authorization'] = f"Bearer {api_key}"
        
        # Make the request
        response = post(url, params=params, headers=headers, json_data=data)
        
        # Handle the response
        return handle_response(response, self._source_name)