"""
Utility module providing functions for URL manipulation, construction, and validation for the 
Electricity Market Price Forecasting System's web visualization interface. Handles API URL 
construction, query parameter management, and dashboard navigation URL generation.
"""

import re
import logging
import urllib.parse
from typing import Optional, Dict, Any, Union

from ..config.settings import API_BASE_URL

# Set up logger
logger = logging.getLogger(__name__)

# Regular expression for validating URLs
URL_REGEX = re.compile(r'^(?:http|https)://[^\s/$.?#].[^\s]*$')


def build_api_url(endpoint: str) -> str:
    """
    Constructs a complete API URL by joining the base URL with an endpoint.
    
    Args:
        endpoint: The API endpoint path
        
    Returns:
        Complete API URL
    """
    # Remove leading slash from endpoint if present
    if endpoint.startswith('/'):
        endpoint = endpoint[1:]
    
    # Join base URL with endpoint
    url = urllib.parse.urljoin(API_BASE_URL, endpoint)
    logger.debug(f"Built API URL: {url}")
    
    return url


def add_query_params(url: str, params: Dict[str, Any], replace_existing: bool = False) -> str:
    """
    Adds query parameters to a URL.
    
    Args:
        url: The base URL
        params: Dictionary of query parameters to add
        replace_existing: If True, replaces existing parameters with the same names
                         If False, merges with existing parameters
                         
    Returns:
        URL with added query parameters
    """
    if not params:
        return url
    
    # Parse the URL
    parsed_url = urllib.parse.urlparse(url)
    
    # Get existing query parameters
    existing_params = urllib.parse.parse_qs(parsed_url.query)
    
    # Create a new dictionary for the final parameters
    final_params = {}
    
    if replace_existing:
        # Start with existing parameters that aren't being replaced
        for key, value in existing_params.items():
            if key not in params:
                final_params[key] = value
                
        # Add all new parameters
        for key, value in params.items():
            final_params[key] = value
    else:
        # Start with all existing parameters
        final_params.update(existing_params)
        
        # Add new parameters, merging with existing ones if needed
        for key, value in params.items():
            if key in final_params:
                # Convert both to lists if they aren't already
                existing_value = final_params[key]
                if not isinstance(existing_value, list):
                    existing_value = [existing_value]
                
                new_value = value
                if not isinstance(new_value, list):
                    new_value = [new_value]
                
                # Combine the lists
                final_params[key] = existing_value + new_value
            else:
                final_params[key] = value
    
    # Encode the query string
    query_string = urllib.parse.urlencode(final_params, doseq=True)
    
    # Reconstruct the URL
    new_url = urllib.parse.urlunparse((
        parsed_url.scheme,
        parsed_url.netloc,
        parsed_url.path,
        parsed_url.params,
        query_string,
        parsed_url.fragment
    ))
    
    logger.debug(f"Added query parameters to URL: {new_url}")
    return new_url


def get_query_params(url: str) -> Dict[str, Union[str, list]]:
    """
    Extracts query parameters from a URL.
    
    Args:
        url: The URL to extract parameters from
        
    Returns:
        Dictionary of query parameters
    """
    # Parse the URL
    parsed_url = urllib.parse.urlparse(url)
    
    # Extract query parameters
    params = urllib.parse.parse_qs(parsed_url.query)
    
    # Convert list values to single values if list has only one item
    for key, value in params.items():
        if isinstance(value, list) and len(value) == 1:
            params[key] = value[0]
    
    return params


def build_forecast_api_url(
    product: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    additional_params: Optional[Dict[str, Any]] = None
) -> str:
    """
    Constructs a URL for the forecast API with appropriate parameters.
    
    Args:
        product: The price product (e.g., 'DALMP', 'RTLMP')
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format
        additional_params: Optional additional query parameters
        
    Returns:
        Complete forecast API URL with parameters
    """
    # Construct the base endpoint for forecasts
    endpoint = "forecasts"
    
    # Add product to the endpoint path if provided
    if product:
        endpoint = f"{endpoint}/{product}"
    
    # Build the base API URL
    url = build_api_url(endpoint)
    
    # Prepare query parameters
    params = {'format': 'json'}
    
    # Add date parameters if provided
    if start_date:
        params['start_date'] = start_date
    if end_date:
        params['end_date'] = end_date
    
    # Add additional parameters if provided
    if additional_params:
        params.update(additional_params)
    
    # Add parameters to URL
    final_url = add_query_params(url, params)
    
    logger.debug(f"Built forecast API URL: {final_url}")
    return final_url


def build_dashboard_url(page: str, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Constructs a URL for dashboard navigation.
    
    Args:
        page: The dashboard page to navigate to
        params: Optional query parameters
        
    Returns:
        Dashboard URL for the specified page with parameters
    """
    # Base dashboard URL (assumes running on the same server)
    base_url = "/"
    
    # Add page to the path if provided
    if page and page != 'index':
        base_url = f"{base_url}{page}"
        # Add trailing slash if not present
        if not base_url.endswith('/'):
            base_url = f"{base_url}/"
    
    # Add query parameters if provided
    if params:
        base_url = add_query_params(base_url, params)
    
    logger.debug(f"Built dashboard URL: {base_url}")
    return base_url


def is_valid_url(url: str) -> bool:
    """
    Validates if a string is a properly formatted URL.
    
    Args:
        url: The URL string to validate
        
    Returns:
        True if URL is valid, False otherwise
    """
    if not url:
        return False
    
    return bool(URL_REGEX.match(url))