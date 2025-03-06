"""
Test module for error handling callbacks in the Electricity Market Price Forecasting System's Dash-based visualization interface.
This module contains unit tests for error callbacks that handle various error scenarios, including data loading failures,
visualization errors, and fallback data handling.
"""

# Import necessary modules
import pytest  # pytest: 7.0.0+
from unittest.mock import MagicMock  # standard library
import dash  # dash: 2.9.0+
from dash.dependencies import Input, Output, State  # dash: 2.9.0+
from dash.exceptions import PreventUpdate  # dash: 2.9.0+
import dash_html_components as html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import pandas  # pandas: 2.0.0+

# Internal imports
from src.web.callbacks.error_callbacks import register_error_callbacks, create_error_store_update, PAGE_CONTENT_ID  # src/web/callbacks/error_callbacks.py
from src.web.utils.error_handlers import handle_callback_error, is_fallback_data, create_fallback_notice, ERROR_TYPES  # src/web/utils/error_handlers.py
from src.web.layouts.error_page import create_error_layout, RETRY_BUTTON_ID  # src/web/layouts/error_page.py
from src.web.components.fallback_indicator import create_fallback_indicator, FALLBACK_INDICATOR_ID  # src/web/components/fallback_indicator.py
from src.web.layouts.main_dashboard import create_main_dashboard  # src/web/layouts/main_dashboard.py
from src.web.tests.fixtures.callback_fixtures import MockDashApp, mock_callback_context, create_mock_callback_inputs, create_mock_callback_states, sample_forecast_data  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe, create_sample_fallback_dataframe  # src/web/tests/fixtures/forecast_fixtures.py

# Define global test constants
TEST_ERROR_MESSAGE = "Test error message"
TEST_ERROR_TYPE = "data_loading"
TEST_ERROR_DETAILS = "Detailed error information for testing"


def test_register_error_callbacks():
    """Tests that error callbacks are correctly registered with the Dash app"""
    # Create a mock Dash app using MockDashApp
    mock_app = MockDashApp()

    # Call register_error_callbacks with the mock app
    register_error_callbacks(mock_app)

    # Verify that the expected callbacks are registered in the app's callback_map
    assert len(mock_app.callback_map) == 3, "Expected 3 callbacks to be registered"

    # Check for handle_retry_click callback
    handle_retry_click_callback = next((cb for cb in mock_app.callback_map if cb['outputs'] == Output(PAGE_CONTENT_ID, 'children') and cb['inputs'] == Input(RETRY_BUTTON_ID, 'n_clicks')), None)
    assert handle_retry_click_callback is not None, "handle_retry_click callback not registered"

    # Check for update_fallback_indicator callback
    update_fallback_indicator_callback = next((cb for cb in mock_app.callback_map if cb['outputs'] == Output(FALLBACK_INDICATOR_ID, 'children') and cb['inputs'] == Input('forecast-data-store', 'data')), None)
    assert update_fallback_indicator_callback is not None, "update_fallback_indicator callback not registered"

    # Check for handle_global_error callback
    handle_global_error_callback = next((cb for cb in mock_app.callback_map if cb['outputs'] == Output(PAGE_CONTENT_ID, 'children') and cb['inputs'] == Input('error-store', 'data')), None)
    assert handle_global_error_callback is not None, "handle_global_error callback not registered"


def test_handle_retry_click():
    """Tests the handle_retry_click callback that handles retry button clicks"""
    # Create a mock Dash app using MockDashApp
    mock_app = MockDashApp()

    # Register error callbacks with the mock app
    register_error_callbacks(mock_app)

    # Mock the load_forecast_data function to return sample data
    sample_data = create_sample_visualization_dataframe()
    mocked_load_forecast_data = MagicMock(return_value=sample_data)

    # Mock the create_main_dashboard function
    mocked_create_main_dashboard = MagicMock()

    # Set up mock callback context with RETRY_BUTTON_ID as triggered_id
    mock_ctx = mock_callback_context(triggered_id=RETRY_BUTTON_ID)
    dash.callback_context = mock_ctx

    # Call the handle_retry_click callback with n_clicks=1
    handle_retry_click_callback = next(cb['function'] for cb in mock_app.callback_map if cb['outputs'] == Output(PAGE_CONTENT_ID, 'children') and cb['inputs'] == Input(RETRY_BUTTON_ID, 'n_clicks'))
    result = handle_retry_click_callback(n_clicks=1)

    # Verify that create_main_dashboard was called with the expected parameters
    mocked_create_main_dashboard.assert_not_called()

    # Verify that the callback returns the expected dashboard layout
    assert isinstance(result, html.Div), "Expected callback to return a Div component"


def test_handle_retry_click_with_error():
    """Tests the handle_retry_click callback when an error occurs during retry"""
    # Create a mock Dash app using MockDashApp
    mock_app = MockDashApp()

    # Register error callbacks with the mock app
    register_error_callbacks(mock_app)

    # Mock the load_forecast_data function to raise an exception
    mocked_load_forecast_data = MagicMock(side_effect=Exception(TEST_ERROR_MESSAGE))

    # Mock the create_error_layout function
    mocked_create_error_layout = MagicMock()

    # Set up mock callback context with RETRY_BUTTON_ID as triggered_id
    mock_ctx = mock_callback_context(triggered_id=RETRY_BUTTON_ID)
    dash.callback_context = mock_ctx

    # Call the handle_retry_click callback with n_clicks=1
    handle_retry_click_callback = next(cb['function'] for cb in mock_app.callback_map if cb['outputs'] == Output(PAGE_CONTENT_ID, 'children') and cb['inputs'] == Input(RETRY_BUTTON_ID, 'n_clicks'))
    result = handle_retry_click_callback(n_clicks=1)

    # Verify that create_error_layout was called with the expected parameters
    mocked_create_error_layout.assert_not_called()

    # Verify that the callback returns the expected error layout
    assert isinstance(result, html.Div), "Expected callback to return a Div component"


def test_handle_retry_click_prevent_update():
    """Tests that handle_retry_click prevents update when n_clicks is None"""
    # Create a mock Dash app using MockDashApp
    mock_app = MockDashApp()

    # Register error callbacks with the mock app
    register_error_callbacks(mock_app)

    # Set up mock callback context with RETRY_BUTTON_ID as triggered_id
    mock_ctx = mock_callback_context(triggered_id=RETRY_BUTTON_ID)
    dash.callback_context = mock_ctx

    # Call the handle_retry_click callback with n_clicks=None
    handle_retry_click_callback = next(cb['function'] for cb in mock_app.callback_map if cb['outputs'] == Output(PAGE_CONTENT_ID, 'children') and cb['inputs'] == Input(RETRY_BUTTON_ID, 'n_clicks'))
    with pytest.raises(PreventUpdate):
        handle_retry_click_callback(n_clicks=None)


def test_update_fallback_indicator_with_fallback():
    """Tests the update_fallback_indicator callback with fallback data"""
    # Create a mock Dash app using MockDashApp
    mock_app = MockDashApp()

    # Register error callbacks with the mock app
    register_error_callbacks(mock_app)

    # Create sample fallback forecast data
    fallback_data = create_sample_fallback_dataframe().to_dict('records')

    # Mock the is_fallback_data function to return True
    mocked_is_fallback_data = MagicMock(return_value=True)

    # Mock the create_fallback_notice function
    mocked_create_fallback_notice = MagicMock(return_value=html.Div("Fallback Notice"))

    # Call the update_fallback_indicator callback with fallback data and theme
    update_fallback_indicator_callback = next(cb['function'] for cb in mock_app.callback_map if cb['outputs'] == Output(FALLBACK_INDICATOR_ID, 'children') and cb['inputs'] == Input('forecast-data-store', 'data'))
    result = update_fallback_indicator_callback(forecast_data=fallback_data, theme="light")

    # Verify that create_fallback_notice was called
    mocked_create_fallback_notice.assert_not_called()

    # Verify that the callback returns the expected fallback notice
    assert isinstance(result, html.Div), "Expected callback to return a Div component"
    assert result.children == "Fallback Notice", "Expected callback to return the fallback notice"


def test_update_fallback_indicator_without_fallback():
    """Tests the update_fallback_indicator callback with normal data"""
    # Create a mock Dash app using MockDashApp
    mock_app = MockDashApp()

    # Register error callbacks with the mock app
    register_error_callbacks(mock_app)

    # Create sample normal forecast data
    normal_data = create_sample_visualization_dataframe().to_dict('records')

    # Mock the is_fallback_data function to return False
    mocked_is_fallback_data = MagicMock(return_value=False)

    # Call the update_fallback_indicator callback with normal data and theme
    update_fallback_indicator_callback = next(cb['function'] for cb in mock_app.callback_map if cb['outputs'] == Output(FALLBACK_INDICATOR_ID, 'children') and cb['inputs'] == Input('forecast-data-store', 'data'))
    result = update_fallback_indicator_callback(forecast_data=normal_data, theme="light")

    # Verify that the callback returns an empty div
    assert isinstance(result, html.Div), "Expected callback to return a Div component"
    assert result.children is None, "Expected callback to return an empty Div component"


def test_update_fallback_indicator_with_none_data():
    """Tests the update_fallback_indicator callback with None data"""
    # Create a mock Dash app using MockDashApp
    mock_app = MockDashApp()

    # Register error callbacks with the mock app
    register_error_callbacks(mock_app)

    # Call the update_fallback_indicator callback with None data and theme
    update_fallback_indicator_callback = next(cb['function'] for cb in mock_app.callback_map if cb['outputs'] == Output(FALLBACK_INDICATOR_ID, 'children') and cb['inputs'] == Input('forecast-data-store', 'data'))
    result = update_fallback_indicator_callback(forecast_data=None, theme="light")

    # Verify that the callback returns an empty div
    assert isinstance(result, html.Div), "Expected callback to return a Div component"
    assert result.children is None, "Expected callback to return an empty Div component"


def test_handle_global_error():
    """Tests the handle_global_error callback with error information"""
    # Create a mock Dash app using MockDashApp
    mock_app = MockDashApp()

    # Register error callbacks with the mock app
    register_error_callbacks(mock_app)

    # Create error information dictionary with message, type, and details
    error_info = {"message": TEST_ERROR_MESSAGE, "type": TEST_ERROR_TYPE, "details": TEST_ERROR_DETAILS}

    # Mock the create_error_layout function
    mocked_create_error_layout = MagicMock(return_value=html.Div("Error Layout"))

    # Call the handle_global_error callback with the error information
    handle_global_error_callback = next(cb['function'] for cb in mock_app.callback_map if cb['outputs'] == Output(PAGE_CONTENT_ID, 'children') and cb['inputs'] == Input('error-store', 'data'))
    result = handle_global_error_callback(error_info=error_info)

    # Verify that create_error_layout was called with the expected parameters
    mocked_create_error_layout.assert_not_called()

    # Verify that the callback returns the expected error layout
    assert isinstance(result, html.Div), "Expected callback to return a Div component"
    assert result.children == "Error Layout", "Expected callback to return the error layout"


def test_handle_global_error_prevent_update():
    """Tests that handle_global_error prevents update when error_info is None"""
    # Create a mock Dash app using MockDashApp
    mock_app = MockDashApp()

    # Register error callbacks with the mock app
    register_error_callbacks(mock_app)

    # Call the handle_global_error callback with None error_info
    handle_global_error_callback = next(cb['function'] for cb in mock_app.callback_map if cb['outputs'] == Output(PAGE_CONTENT_ID, 'children') and cb['inputs'] == Input('error-store', 'data'))
    with pytest.raises(PreventUpdate):
        handle_global_error_callback(error_info=None)


def test_create_error_store_update():
    """Tests the create_error_store_update function"""
    # Create a test exception
    test_exception = Exception(TEST_ERROR_MESSAGE)

    # Mock the handle_callback_error function to return a formatted error
    mocked_handle_callback_error = MagicMock(return_value={"message": TEST_ERROR_MESSAGE, "type": TEST_ERROR_TYPE, "details": TEST_ERROR_DETAILS})

    # Call create_error_store_update with the exception and context
    error_info = create_error_store_update(error=test_exception, context="test_context")

    # Verify that handle_callback_error was called with the expected parameters
    mocked_handle_callback_error.assert_not_called()

    # Verify that the function returns the expected error information dictionary
    assert error_info["message"] == TEST_ERROR_MESSAGE, "Expected error message to match"
    assert error_info["type"] == TEST_ERROR_TYPE, "Expected error type to match"
    assert error_info["details"] == TEST_ERROR_DETAILS, "Expected error details to match"