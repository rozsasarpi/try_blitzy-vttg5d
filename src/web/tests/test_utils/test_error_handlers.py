"""
Unit tests for the error_handlers utility module which provides error handling functions and classes for the Electricity Market Price Forecasting System's Dash-based visualization interface. Tests ensure proper error handling, error message creation, exception formatting, and fallback data detection.
"""
import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
from unittest.mock import MagicMock

import dash  # dash: 2.9.0+
import dash_html_components as html  # dash_html_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+

from src.web.utils import error_handlers  # Import error handling functions and classes for testing
from src.web.config.settings import DEBUG  # Import debug flag to test different error verbosity levels
from src.web.config.themes import get_status_color  # Import function to get appropriate color for error and fallback indicators
from src.web.tests.fixtures.callback_fixtures import mock_callback_context, MockCallbackContext, sample_forecast_data  # Import fixture to create mock dash callback context

TEST_ERROR_MESSAGE = "An error occurred during testing"
TEST_ERROR_TYPE = "test_error"
TEST_ERROR_DETAILS = "Detailed error information for testing"
TEST_CALLBACK_NAME = "test_callback"


def test_format_exception():
    """Tests that format_exception correctly formats an exception into a readable string"""
    # Create a test exception (ValueError with a specific message)
    test_exception = ValueError("Test exception message")

    # Call format_exception with the test exception
    formatted_exception = error_handlers.format_exception(test_exception)

    # Assert that the result contains the exception type name
    assert "ValueError" in formatted_exception

    # Assert that the result contains the exception message
    assert "Test exception message" in formatted_exception

    # Assert that the format is 'ExceptionType: message'
    assert "ValueError: Test exception message" == formatted_exception


def test_create_error_message():
    """Tests that create_error_message creates a properly formatted error message component"""
    # Call create_error_message with test message, error type, and no details
    error_message_component = error_handlers.create_error_message(message=TEST_ERROR_MESSAGE, error_type=TEST_ERROR_TYPE)

    # Assert that the result is an html.Div component
    assert isinstance(error_message_component, html.Div)

    # Assert that the component contains the error message
    assert TEST_ERROR_MESSAGE in str(error_message_component)

    # Assert that the component uses the correct color from get_status_color
    expected_color = get_status_color("error", "light")
    assert expected_color in str(error_message_component)

    # Call create_error_message with details and show_details=True
    error_message_with_details = error_handlers.create_error_message(message=TEST_ERROR_MESSAGE, error_type=TEST_ERROR_TYPE, details=TEST_ERROR_DETAILS, show_details=True)

    # Assert that the component includes a collapsible details section
    assert "<summary>Technical Details</summary>" in str(error_message_with_details)

    # Call create_error_message with details but show_details=False
    error_message_without_details = error_handlers.create_error_message(message=TEST_ERROR_MESSAGE, error_type=TEST_ERROR_TYPE, details=TEST_ERROR_DETAILS, show_details=False)

    # Assert that the component does not include a details section
    assert "<summary>Technical Details</summary>" not in str(error_message_without_details)


@pytest.mark.parametrize('debug_mode', [True, False])
def test_handle_callback_error(debug_mode):
    """Tests that handle_callback_error correctly processes callback errors"""
    # Create a test exception
    test_exception = ValueError("Test callback error")

    # Mock dash.callback_context using mock_callback_context
    with unittest.mock.patch('src.web.utils.error_handlers.dash.callback_context', new_callable=MagicMock) as mock_ctx:
        mock_ctx.return_value.triggered = [{'prop_id': 'test-component.value'}]

        # Call handle_callback_error with the test exception and callback name
        error_info = error_handlers.handle_callback_error(error=test_exception, callback_name=TEST_CALLBACK_NAME)

        # Assert that the result is a dictionary with error information
        assert isinstance(error_info, dict)

        # Assert that the dictionary contains message, type, and details keys
        assert "message" in error_info
        assert "type" in error_info
        assert "details" in error_info

        # If debug_mode is True, assert that details includes traceback information
        if debug_mode:
            assert "Traceback" in error_info["details"]

        # If debug_mode is False, assert that details is more limited
        else:
            assert "Traceback" not in error_info["details"]


def test_handle_data_loading_error():
    """Tests that handle_data_loading_error creates appropriate error components for data loading errors"""
    # Create a test connection error exception
    connection_error = ConnectionError("Test connection error")

    # Call handle_data_loading_error with the connection error and context
    error_component_connection = error_handlers.handle_data_loading_error(error=connection_error, context="Test Data Loading")

    # Assert that the result is an html.Div component
    assert isinstance(error_component_connection, html.Div)

    # Assert that the component mentions connection issues
    assert "Could not connect" in str(error_component_connection)

    # Create a test timeout error exception
    timeout_error = TimeoutError("Test timeout error")

    # Call handle_data_loading_error with the timeout error and context
    error_component_timeout = error_handlers.handle_data_loading_error(error=timeout_error, context="Test Data Loading")

    # Assert that the component mentions timeout issues
    assert "timeout" in str(error_component_timeout)

    # Create a generic error exception
    generic_error = ValueError("Test generic error")

    # Call handle_data_loading_error with the generic error and context
    error_component_generic = error_handlers.handle_data_loading_error(error=generic_error, context="Test Data Loading")

    # Assert that the component contains a generic data loading error message
    assert "An error occurred while loading forecast data" in str(error_component_generic)


def test_handle_visualization_error():
    """Tests that handle_visualization_error creates appropriate error components for visualization errors"""
    # Create a test exception
    test_exception = ValueError("Test visualization error")

    # Call handle_visualization_error with the exception and component name
    error_component = error_handlers.handle_visualization_error(error=test_exception, component_name="Test Component")

    # Assert that the result is an html.Div component
    assert isinstance(error_component, html.Div)

    # Assert that the component mentions the specific component that failed
    assert "Test Component" in str(error_component)

    # Assert that the component contains the visualization error type
    assert "Visualization Error" in str(error_component)


def test_is_fallback_data():
    """Tests that is_fallback_data correctly identifies fallback forecast data"""
    # Create test data with is_fallback=True
    fallback_data = {"is_fallback": True}

    # Call is_fallback_data with the test data
    result_true = error_handlers.is_fallback_data(data=fallback_data)

    # Assert that the result is True
    assert result_true is True

    # Create test data with is_fallback=False
    non_fallback_data = {"is_fallback": False}

    # Call is_fallback_data with the test data
    result_false = error_handlers.is_fallback_data(data=non_fallback_data)

    # Assert that the result is False
    assert result_false is False

    # Call is_fallback_data with None
    result_none = error_handlers.is_fallback_data(data=None)

    # Assert that the result is False
    assert result_none is False

    # Create test data without is_fallback key
    no_key_data = {}

    # Call is_fallback_data with the test data
    result_no_key = error_handlers.is_fallback_data(data=no_key_data)

    # Assert that the result is False
    assert result_no_key is False


def test_create_fallback_notice():
    """Tests that create_fallback_notice creates a properly formatted fallback notice component"""
    # Call create_fallback_notice
    fallback_notice = error_handlers.create_fallback_notice()

    # Assert that the result is an html.Div component
    assert isinstance(fallback_notice, html.Div)

    # Assert that the component contains the fallback message
    assert error_handlers.FALLBACK_MESSAGE in str(fallback_notice)

    # Assert that the component uses the warning color from get_status_color
    expected_color = get_status_color("warning", "light")
    assert expected_color in str(fallback_notice)


def test_error_handler_initialization():
    """Tests that ErrorHandler initializes correctly"""
    # Create an ErrorHandler instance
    error_handler = error_handlers.ErrorHandler()

    # Assert that the error_registry is initialized as an empty dictionary
    assert isinstance(error_handler.error_registry, dict)
    assert len(error_handler.error_registry) == 0

    # Assert that the logger is initialized correctly
    assert error_handler.logger is not None


def test_error_handler_register_error():
    """Tests that ErrorHandler.register_error correctly registers errors"""
    # Create an ErrorHandler instance
    error_handler = error_handlers.ErrorHandler()

    # Create a test exception
    test_exception = ValueError("Test error")

    # Call register_error with the test exception and context
    error_id = error_handler.register_error(error=test_exception, context="Test Context")

    # Assert that the returned error_id is a string
    assert isinstance(error_id, str)

    # Assert that the error_id exists in the error_registry
    assert error_id in error_handler.error_registry

    # Assert that the registered error contains timestamp, error details, and context
    registered_error = error_handler.error_registry[error_id]
    assert "timestamp" in registered_error
    assert "error_type" in registered_error
    assert "error_message" in registered_error
    assert "context" in registered_error


def test_error_handler_get_error_component():
    """Tests that ErrorHandler.get_error_component returns appropriate error components"""
    # Create an ErrorHandler instance
    error_handler = error_handlers.ErrorHandler()

    # Register a test error and get its error_id
    test_exception = ValueError("Test error")
    error_id = error_handler.register_error(error=test_exception, context="Test Context")

    # Call get_error_component with the error_id and with_details=False
    error_component_no_details = error_handler.get_error_component(error_id=error_id, with_details=False)

    # Assert that the result is an html.Div component
    assert isinstance(error_component_no_details, html.Div)

    # Assert that the component contains the error message but not detailed information
    assert "Test error" in str(error_component_no_details)
    assert "Traceback" not in str(error_component_no_details)

    # Call get_error_component with the error_id and with_details=True
    error_component_with_details = error_handler.get_error_component(error_id=error_id, with_details=True)

    # Assert that the component contains detailed error information
    assert "Traceback" in str(error_component_with_details)

    # Call get_error_component with a non-existent error_id
    error_component_non_existent = error_handler.get_error_component(error_id="non-existent-id")

    # Assert that the component contains a generic error message
    assert "An error occurred, but details are no longer available" in str(error_component_non_existent)


def test_error_handler_clear_error():
    """Tests that ErrorHandler.clear_error correctly removes errors from the registry"""
    # Create an ErrorHandler instance
    error_handler = error_handlers.ErrorHandler()

    # Register a test error and get its error_id
    test_exception = ValueError("Test error")
    error_id = error_handler.register_error(error=test_exception, context="Test Context")

    # Assert that the error exists in the error_registry
    assert error_id in error_handler.error_registry

    # Call clear_error with the error_id
    result = error_handler.clear_error(error_id=error_id)

    # Assert that the result is True
    assert result is True

    # Assert that the error no longer exists in the error_registry
    assert error_id not in error_handler.error_registry

    # Call clear_error with a non-existent error_id
    result_non_existent = error_handler.clear_error(error_id="non-existent-id")

    # Assert that the result is False
    assert result_non_existent is False


def test_error_types_dictionary():
    """Tests that ERROR_TYPES dictionary contains all required error types"""
    # Assert that ERROR_TYPES is a dictionary
    assert isinstance(error_handlers.ERROR_TYPES, dict)

    # Assert that ERROR_TYPES contains 'data_loading' key
    assert "data_loading" in error_handlers.ERROR_TYPES

    # Assert that ERROR_TYPES contains 'visualization' key
    assert "visualization" in error_handlers.ERROR_TYPES

    # Assert that ERROR_TYPES contains 'api' key
    assert "api" in error_handlers.ERROR_TYPES

    # Assert that ERROR_TYPES contains 'processing' key
    assert "processing" in error_handlers.ERROR_TYPES

    # Assert that ERROR_TYPES contains 'unknown' key
    assert "unknown" in error_handlers.ERROR_TYPES

    # Assert that all values are non-empty strings
    for value in error_handlers.ERROR_TYPES.values():
        assert isinstance(value, str)
        assert len(value) > 0


def test_integration_with_dash_callback():
    """Tests integration of error handlers with Dash callbacks"""
    # Create a mock Dash callback function that raises an exception
    def mock_callback():
        raise ValueError("Simulated callback error")

    # Create a mock callback context
    mock_ctx = MockCallbackContext(triggered_id='test-component.value')

    # Set up the test environment to use the mock callback context
    with unittest.mock.patch('src.web.utils.error_handlers.dash.callback_context', new_callable=MagicMock) as mock_dash_ctx:
        mock_dash_ctx.return_value = mock_ctx
        try:
            # Call the callback function and catch the exception
            mock_callback()
        except ValueError as e:
            # Pass the exception to handle_callback_error
            error_info = error_handlers.handle_callback_error(error=e, callback_name="mock_callback")

            # Assert that the error information contains the expected details
            assert "Simulated callback error" in error_info["message"]
            assert "mock_callback" in error_info["details"]

            # Use the error information to create an error message component
            error_component = error_handlers.create_error_message(message=error_info["message"], error_type=error_info["type"], details=error_info["details"], show_details=True)

            # Assert that the component contains the expected error information
            assert "Simulated callback error" in str(error_component)
            assert "mock_callback" in str(error_component)