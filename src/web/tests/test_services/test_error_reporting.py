import pytest
from unittest.mock import MagicMock, patch
import requests
import json
from datetime import datetime

from ...services.error_reporting import (
    ErrorReportingService, report_error, get_error, clear_error, 
    clear_all_errors, get_error_count, get_error_summary
)
from ...utils.error_handlers import format_exception, ERROR_TYPES

def test_error_reporting_service_init():
    """Tests the initialization of the ErrorReportingService class"""
    # Create a basic instance with defaults
    service = ErrorReportingService()
    assert service.error_registry == {}
    assert service.logger is not None
    assert service.external_reporting_enabled is False
    assert service.external_reporting_url is None
    
    # Create an instance with custom parameters
    service = ErrorReportingService(
        external_reporting_url="https://example.com/errors", 
        enable_external_reporting=True
    )
    assert service.external_reporting_enabled is True
    assert service.external_reporting_url == "https://example.com/errors"

def test_report_error():
    """Tests the report_error function"""
    # Clear any existing errors
    clear_all_errors()
    
    # Create a test exception
    test_exception = ValueError("Test error message")
    test_context = "test_function"
    
    # Report the error
    error_id = report_error(test_exception, test_context)
    
    # Verify an ID was returned
    assert error_id is not None
    assert isinstance(error_id, str)
    
    # Verify the error was stored
    error_details = get_error(error_id)
    assert error_details is not None
    assert error_details["type"] == "ValueError"
    assert error_details["message"] == "Test error message"
    assert error_details["context"] == test_context
    assert "timestamp" in error_details
    assert "formatted_error" in error_details

def test_report_error_with_traceback():
    """Tests the report_error function with traceback included"""
    # Clear any existing errors
    clear_all_errors()
    
    # Create a test exception
    test_exception = ValueError("Test error message")
    test_context = "test_function"
    
    # Report the error with traceback
    error_id = report_error(test_exception, test_context, include_traceback=True)
    
    # Verify the error was stored with traceback
    error_details = get_error(error_id)
    assert error_details is not None
    assert "traceback" in error_details
    assert isinstance(error_details["traceback"], str)

def test_get_error():
    """Tests the get_error function"""
    # Clear any existing errors
    clear_all_errors()
    
    # Create a test exception
    test_exception = ValueError("Test error message")
    test_context = "test_function"
    
    # Report the error
    error_id = report_error(test_exception, test_context)
    
    # Get the error
    error_details = get_error(error_id)
    assert error_details is not None
    assert error_details["type"] == "ValueError"
    assert error_details["message"] == "Test error message"
    assert error_details["context"] == test_context
    
    # Test with non-existent error ID
    non_existent_error = get_error("non-existent-id")
    assert non_existent_error is None

def test_clear_error():
    """Tests the clear_error function"""
    # Clear any existing errors
    clear_all_errors()
    
    # Create a test exception
    test_exception = ValueError("Test error message")
    test_context = "test_function"
    
    # Report the error
    error_id = report_error(test_exception, test_context)
    
    # Clear the error
    result = clear_error(error_id)
    assert result is True
    
    # Verify the error was cleared
    error_details = get_error(error_id)
    assert error_details is None
    
    # Test clearing non-existent error
    result = clear_error("non-existent-id")
    assert result is False

def test_clear_all_errors():
    """Tests the clear_all_errors function"""
    # Clear any existing errors
    clear_all_errors()
    
    # Create multiple test exceptions
    for i in range(3):
        test_exception = ValueError(f"Test error message {i}")
        test_context = f"test_function_{i}"
        report_error(test_exception, test_context)
    
    # Verify errors were created
    assert get_error_count() == 3
    
    # Clear all errors
    cleared_count = clear_all_errors()
    assert cleared_count == 3
    
    # Verify all errors were cleared
    assert get_error_count() == 0

def test_get_error_count():
    """Tests the get_error_count function"""
    # Clear any existing errors
    clear_all_errors()
    
    # Verify count is 0 initially
    assert get_error_count() == 0
    
    # Create multiple test exceptions
    for i in range(3):
        test_exception = ValueError(f"Test error message {i}")
        test_context = f"test_function_{i}"
        report_error(test_exception, test_context)
    
    # Verify count is correct
    assert get_error_count() == 3
    
    # Clear one error
    error_id = get_error_summary()[0]["id"]
    clear_error(error_id)
    
    # Verify count was decremented
    assert get_error_count() == 2

def test_get_error_summary():
    """Tests the get_error_summary function"""
    # Clear any existing errors
    clear_all_errors()
    
    # Create multiple test exceptions with different contexts
    for i in range(3):
        test_exception = ValueError(f"Test error message {i}")
        test_context = f"test_function_{i}"
        report_error(test_exception, test_context)
    
    # Get error summary
    summary = get_error_summary()
    
    # Verify summary structure
    assert len(summary) == 3
    for item in summary:
        assert "id" in item
        assert "type" in item
        assert "message" in item
        assert "timestamp" in item
    
    # Verify sorting by timestamp (newest first)
    timestamps = [item["timestamp"] for item in summary]
    assert timestamps[0] >= timestamps[1] >= timestamps[2]

def test_error_reporting_service_report_error():
    """Tests the ErrorReportingService.report_error method"""
    # Create a service instance
    service = ErrorReportingService()
    
    # Create a test exception
    test_exception = ValueError("Test error message")
    test_context = "test_function"
    
    # Report the error
    error_id = service.report_error(test_exception, test_context)
    
    # Verify an ID was returned
    assert error_id is not None
    assert isinstance(error_id, str)
    
    # Verify the error was stored
    error_details = service.get_error(error_id)
    assert error_details is not None
    assert error_details["type"] == "ValueError"
    assert error_details["message"] == "Test error message"
    assert error_details["context"] == test_context
    assert "timestamp" in error_details
    assert "formatted_error" in error_details

def test_error_reporting_service_get_error():
    """Tests the ErrorReportingService.get_error method"""
    # Create a service instance
    service = ErrorReportingService()
    
    # Create a test exception
    test_exception = ValueError("Test error message")
    test_context = "test_function"
    
    # Report the error
    error_id = service.report_error(test_exception, test_context)
    
    # Get the error
    error_details = service.get_error(error_id)
    assert error_details is not None
    assert error_details["type"] == "ValueError"
    assert error_details["message"] == "Test error message"
    assert error_details["context"] == test_context
    
    # Test with non-existent error ID
    non_existent_error = service.get_error("non-existent-id")
    assert non_existent_error is None

def test_error_reporting_service_clear_error():
    """Tests the ErrorReportingService.clear_error method"""
    # Create a service instance
    service = ErrorReportingService()
    
    # Create a test exception
    test_exception = ValueError("Test error message")
    test_context = "test_function"
    
    # Report the error
    error_id = service.report_error(test_exception, test_context)
    
    # Clear the error
    result = service.clear_error(error_id)
    assert result is True
    
    # Verify the error was cleared
    error_details = service.get_error(error_id)
    assert error_details is None
    
    # Test clearing non-existent error
    result = service.clear_error("non-existent-id")
    assert result is False

def test_error_reporting_service_clear_all_errors():
    """Tests the ErrorReportingService.clear_all_errors method"""
    # Create a service instance
    service = ErrorReportingService()
    
    # Create multiple test exceptions
    for i in range(3):
        test_exception = ValueError(f"Test error message {i}")
        test_context = f"test_function_{i}"
        service.report_error(test_exception, test_context)
    
    # Verify errors were created
    assert service.get_error_count() == 3
    
    # Clear all errors
    cleared_count = service.clear_all_errors()
    assert cleared_count == 3
    
    # Verify all errors were cleared
    assert service.get_error_count() == 0

def test_error_reporting_service_get_error_count():
    """Tests the ErrorReportingService.get_error_count method"""
    # Create a service instance
    service = ErrorReportingService()
    
    # Verify count is 0 initially
    assert service.get_error_count() == 0
    
    # Create multiple test exceptions
    for i in range(3):
        test_exception = ValueError(f"Test error message {i}")
        test_context = f"test_function_{i}"
        service.report_error(test_exception, test_context)
    
    # Verify count is correct
    assert service.get_error_count() == 3
    
    # Clear one error
    error_id = service.get_error_summary()[0]["id"]
    service.clear_error(error_id)
    
    # Verify count was decremented
    assert service.get_error_count() == 2

def test_error_reporting_service_get_error_summary():
    """Tests the ErrorReportingService.get_error_summary method"""
    # Create a service instance
    service = ErrorReportingService()
    
    # Create multiple test exceptions with different contexts
    for i in range(3):
        test_exception = ValueError(f"Test error message {i}")
        test_context = f"test_function_{i}"
        service.report_error(test_exception, test_context)
    
    # Get error summary
    summary = service.get_error_summary()
    
    # Verify summary structure
    assert len(summary) == 3
    for item in summary:
        assert "id" in item
        assert "type" in item
        assert "message" in item
        assert "timestamp" in item
    
    # Verify sorting by timestamp (newest first)
    timestamps = [item["timestamp"] for item in summary]
    assert timestamps[0] >= timestamps[1] >= timestamps[2]

def test_error_reporting_service_enable_external_reporting():
    """Tests the ErrorReportingService.enable_external_reporting method"""
    # Create a service instance
    service = ErrorReportingService()
    
    # Verify external reporting is disabled initially
    assert service.external_reporting_enabled is False
    
    # Enable external reporting
    service.enable_external_reporting()
    assert service.external_reporting_enabled is True
    assert service.external_reporting_url is None
    
    # Enable with URL
    test_url = "https://example.com/errors"
    service.enable_external_reporting(test_url)
    assert service.external_reporting_enabled is True
    assert service.external_reporting_url == test_url

def test_error_reporting_service_disable_external_reporting():
    """Tests the ErrorReportingService.disable_external_reporting method"""
    # Create a service instance with external reporting enabled
    service = ErrorReportingService(
        external_reporting_url="https://example.com/errors", 
        enable_external_reporting=True
    )
    
    # Verify external reporting is enabled initially
    assert service.external_reporting_enabled is True
    
    # Disable external reporting
    service.disable_external_reporting()
    assert service.external_reporting_enabled is False
    assert service.external_reporting_url == "https://example.com/errors"  # URL should remain unchanged

def test_error_reporting_service_send_to_external_service():
    """Tests the ErrorReportingService.send_to_external_service method"""
    # Create a service instance with external reporting enabled
    test_url = "https://example.com/errors"
    service = ErrorReportingService(
        external_reporting_url=test_url, 
        enable_external_reporting=True
    )
    
    # Create a test exception
    test_exception = ValueError("Test error message")
    test_context = "test_function"
    
    # Report the error
    error_id = service.report_error(test_exception, test_context)
    error_details = service.get_error(error_id)
    
    # Mock the requests.post method
    with patch("requests.post") as mock_post:
        # Configure the mock to return a successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        # Call the method under test
        result = service.send_to_external_service(error_id, error_details)
        
        # Verify the method returned True for successful sending
        assert result is True
        
        # Verify requests.post was called with the correct URL and payload
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == test_url
        assert "json" in kwargs
        assert kwargs["json"]["error_id"] == error_id
        assert kwargs["json"]["details"] == error_details
    
    # Test with requests.post raising an exception
    with patch("requests.post") as mock_post:
        # Configure the mock to raise an exception
        mock_post.side_effect = Exception("Connection error")
        
        # Call the method under test
        result = service.send_to_external_service(error_id, error_details)
        
        # Verify the method returned False when an exception occurs
        assert result is False
    
    # Create a service with external_reporting_enabled=False
    service = ErrorReportingService()
    result = service.send_to_external_service(error_id, error_details)
    assert result is False

@patch("src.web.services.error_reporting.MAX_ERROR_HISTORY", 5)
def test_max_error_history_limit():
    """Tests that the error registry respects the maximum history limit"""
    # Create a service instance
    service = ErrorReportingService()
    
    # Create more test exceptions than the MAX_ERROR_HISTORY limit
    for i in range(10):
        test_exception = ValueError(f"Test error message {i}")
        test_context = f"test_function_{i}"
        service.report_error(test_exception, test_context)
    
    # Verify that only MAX_ERROR_HISTORY errors are kept
    assert service.get_error_count() == 5
    
    # Verify that the oldest errors are removed
    summary = service.get_error_summary()
    messages = [item["message"] for item in summary]
    
    # We should have the 5 most recent messages (5-9)
    for i in range(5, 10):
        assert f"Test error message {i}" in messages
    
    # And the oldest messages (0-4) should be gone
    for i in range(0, 5):
        assert f"Test error message {i}" not in messages

def test_error_reporting_with_different_exception_types():
    """Tests error reporting with different types of exceptions"""
    # Create a service instance
    service = ErrorReportingService()
    
    # Create various exception types
    exceptions = [
        ValueError("Value error message"),
        TypeError("Type error message"),
        KeyError("Key error message"),
        IndexError("Index error message"),
        AttributeError("Attribute error message")
    ]
    
    error_ids = []
    for i, exception in enumerate(exceptions):
        error_id = service.report_error(exception, f"test_context_{i}")
        error_ids.append(error_id)
    
    # Verify all errors are stored correctly
    for i, (error_id, exception) in enumerate(zip(error_ids, exceptions)):
        error_details = service.get_error(error_id)
        assert error_details is not None
        assert error_details["type"] == exception.__class__.__name__
        assert error_details["message"] == str(exception)
        assert error_details["context"] == f"test_context_{i}"