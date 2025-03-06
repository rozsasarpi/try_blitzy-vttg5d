"""
Unit tests for the URL helper functions used in the Electricity Market Price Forecasting System's
web visualization interface.
"""

import pytest
from unittest.mock import patch
import urllib.parse

from ../../utils.url_helpers import (
    build_api_url, 
    add_query_params, 
    get_query_params, 
    build_forecast_api_url,
    build_dashboard_url,
    is_valid_url
)
from ../../config.settings import API_BASE_URL


def test_build_api_url():
    """Tests that build_api_url correctly joins the base URL with an endpoint"""
    # Test with simple endpoint
    endpoint = "forecasts"
    expected = f"{API_BASE_URL}/forecasts"
    assert build_api_url(endpoint) == expected
    
    # Test with leading slash in endpoint
    endpoint = "/forecasts"
    expected = f"{API_BASE_URL}/forecasts"
    assert build_api_url(endpoint) == expected
    
    # Test with complex path endpoint
    endpoint = "forecasts/DALMP/hourly"
    expected = f"{API_BASE_URL}/forecasts/DALMP/hourly"
    assert build_api_url(endpoint) == expected


def test_add_query_params():
    """Tests that add_query_params correctly adds query parameters to a URL"""
    # Test adding parameters to URL without existing params
    base_url = "http://example.com/api/forecasts"
    params = {"product": "DALMP", "start_date": "2023-06-01"}
    result = add_query_params(base_url, params)
    assert "product=DALMP" in result
    assert "start_date=2023-06-01" in result
    
    # Test merging with existing params (replace_existing=False)
    url_with_params = "http://example.com/api/forecasts?format=json"
    params = {"product": "DALMP", "start_date": "2023-06-01"}
    result = add_query_params(url_with_params, params, replace_existing=False)
    assert "format=json" in result
    assert "product=DALMP" in result
    assert "start_date=2023-06-01" in result
    
    # Test replacing existing params (replace_existing=True)
    url_with_params = "http://example.com/api/forecasts?product=RTLMP&format=json"
    params = {"product": "DALMP", "start_date": "2023-06-01"}
    result = add_query_params(url_with_params, params, replace_existing=True)
    assert "format=json" in result
    assert "product=DALMP" in result
    assert "start_date=2023-06-01" in result
    assert "product=RTLMP" not in result


def test_get_query_params():
    """Tests that get_query_params correctly extracts query parameters from a URL"""
    # Test URL with simple params
    url = "http://example.com/api/forecasts?product=DALMP&start_date=2023-06-01"
    params = get_query_params(url)
    assert params["product"] == "DALMP"
    assert params["start_date"] == "2023-06-01"
    
    # Test URL with multiple values for the same param
    url = "http://example.com/api/forecasts?product=DALMP&product=RTLMP"
    params = get_query_params(url)
    assert isinstance(params["product"], list)
    assert "DALMP" in params["product"]
    assert "RTLMP" in params["product"]
    
    # Test URL without params
    url = "http://example.com/api/forecasts"
    params = get_query_params(url)
    assert params == {}


def test_build_forecast_api_url():
    """Tests that build_forecast_api_url correctly constructs forecast API URLs"""
    # Test with product only
    product = "DALMP"
    url = build_forecast_api_url(product)
    assert f"{API_BASE_URL}/forecasts/{product}" in url
    assert "format=json" in url
    
    # Test with product, start_date, and end_date
    product = "RTLMP"
    start_date = "2023-06-01"
    end_date = "2023-06-03"
    url = build_forecast_api_url(product, start_date, end_date)
    assert f"{API_BASE_URL}/forecasts/{product}" in url
    assert "start_date=2023-06-01" in url
    assert "end_date=2023-06-03" in url
    
    # Test with additional parameters
    product = "RegUp"
    additional_params = {"resolution": "hourly", "include_samples": "true"}
    url = build_forecast_api_url(product, additional_params=additional_params)
    assert f"{API_BASE_URL}/forecasts/{product}" in url
    assert "resolution=hourly" in url
    assert "include_samples=true" in url


def test_build_dashboard_url():
    """Tests that build_dashboard_url correctly constructs dashboard navigation URLs"""
    # Test with page name
    page = "forecast"
    url = build_dashboard_url(page)
    assert url == "/forecast/"
    
    # Test with page name and parameters
    page = "comparison"
    params = {"products": "DALMP,RTLMP", "date": "2023-06-01"}
    url = build_dashboard_url(page, params)
    assert url.startswith("/comparison/")
    assert "products=DALMP%2CRTLMP" in url  # URL-encoded comma
    assert "date=2023-06-01" in url
    
    # Test without page name
    url = build_dashboard_url("")
    assert url == "/"


def test_is_valid_url():
    """Tests that is_valid_url correctly validates URL formats"""
    # Test valid HTTP URL
    assert is_valid_url("http://example.com") is True
    
    # Test valid HTTPS URL
    assert is_valid_url("https://example.com/path?query=value") is True
    
    # Test invalid URL (missing protocol)
    assert is_valid_url("example.com") is False
    
    # Test invalid URL (malformed)
    assert is_valid_url("http://") is False
    
    # Test None
    assert is_valid_url(None) is False
    
    # Test empty string
    assert is_valid_url("") is False


def test_add_query_params_with_special_characters():
    """Tests that add_query_params correctly handles special characters in parameters"""
    # Test URL-encoding of special characters
    base_url = "http://example.com/api/forecasts"
    params = {
        "name": "Test Name with spaces",
        "path": "/path/with/slashes",
        "special": "!@#$%^&*()"
    }
    
    url = add_query_params(base_url, params)
    
    # Verify parameters are correctly URL-encoded
    assert "name=Test+Name+with+spaces" in url or "name=Test%20Name%20with%20spaces" in url
    assert "path=%2Fpath%2Fwith%2Fslashes" in url
    
    # Parse the URL to verify parameters can be correctly decoded
    parsed_params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    assert parsed_params["name"][0] == "Test Name with spaces"
    assert parsed_params["path"][0] == "/path/with/slashes"
    assert parsed_params["special"][0] == "!@#$%^&*()"


@patch('src.web.utils.url_helpers.API_BASE_URL', 'http://test-api.example.com')
def test_build_api_url_with_mock():
    """Tests build_api_url with a mocked API_BASE_URL"""
    endpoint = "forecasts"
    expected = "http://test-api.example.com/forecasts"
    assert build_api_url(endpoint) == expected