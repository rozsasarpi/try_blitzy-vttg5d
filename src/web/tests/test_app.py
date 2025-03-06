"""
Unit and integration tests for the main Dash application of the Electricity Market Price Forecasting System.
This file tests the initialization, layout creation, and core functionality of the web visualization interface
to ensure it meets the requirements for displaying forecast data.
"""

import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import dash  # dash: 2.9.0+
from dash import html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+

from src.web.app import app, initialize_app, create_app_layout, load_initial_content, create_refresh_interval, APP_ID, CONTENT_DIV_ID, REFRESH_INTERVAL_ID  # src/web/app.py
from src.web.config.settings import DEBUG, REFRESH_INTERVAL  # src/web/config/settings.py
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe, create_sample_fallback_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.component_fixtures import mock_dash_app  # src/web/tests/fixtures/component_fixtures.py


def test_initialize_app():
    """Tests that the application initializes correctly with the expected configuration"""
    # Call initialize_app() to create a new Dash application instance
    initialized_app = initialize_app()

    # Assert that the returned object is an instance of dash.Dash
    assert isinstance(initialized_app, dash.Dash)

    # Assert that the app.title is set correctly
    assert initialized_app.title == 'Electricity Market Price Forecasting'

    # Assert that the app.config contains expected values
    assert initialized_app.config.suppress_callback_exceptions is True
    assert initialized_app.server.config["SERVER_NAME"] is not None
    assert initialized_app.server.config["ALLOWED_HOSTS"] is not None


def test_create_app_layout():
    """Tests that the application layout is created correctly with all required components"""
    # Call create_app_layout() to generate the application layout
    layout = create_app_layout()

    # Assert that the returned object is an instance of html.Div
    assert isinstance(layout, html.Div)

    # Assert that the layout contains a header component
    assert any(isinstance(child, dbc.Navbar) for child in layout.children)

    # Assert that the layout contains a content div with ID CONTENT_DIV_ID
    assert any(child.id == CONTENT_DIV_ID for child in layout.children if isinstance(child, html.Div))

    # Assert that the layout contains a footer component
    assert any(isinstance(child, dbc.Container) and child.id == "dashboard-footer" for child in layout.children)

    # Assert that the layout contains a refresh interval component with ID REFRESH_INTERVAL_ID
    assert any(child.id == REFRESH_INTERVAL_ID for child in layout.children if isinstance(child, dcc.Interval))

    # Assert that the refresh interval is set to the correct value (REFRESH_INTERVAL * 1000)
    interval_component = next(child for child in layout.children if isinstance(child, dcc.Interval) and child.id == REFRESH_INTERVAL_ID)
    assert interval_component.interval == REFRESH_INTERVAL * 1000


def test_create_refresh_interval():
    """Tests that the refresh interval component is created correctly"""
    # Call create_refresh_interval() to create the interval component
    interval_component = create_refresh_interval()

    # Assert that the returned object is an instance of dcc.Interval
    assert isinstance(interval_component, dcc.Interval)

    # Assert that the component has the correct ID (REFRESH_INTERVAL_ID)
    assert interval_component.id == REFRESH_INTERVAL_ID

    # Assert that the interval is set to REFRESH_INTERVAL * 1000 milliseconds
    assert interval_component.interval == REFRESH_INTERVAL * 1000

    # Assert that n_intervals is initialized to 0
    assert interval_component.n_intervals == 0


def test_load_initial_content_success():
    """Tests that initial content loads successfully when forecast data is available"""
    # Mock the load_default_forecast function to return sample visualization data
    with unittest.mock.patch('src.web.app.load_default_forecast', return_value=create_sample_visualization_dataframe()):
        # Call load_initial_content() to load the initial dashboard content
        content = load_initial_content()

        # Assert that the returned object is an instance of html.Div
        assert isinstance(content, html.Div)

        # Assert that the content contains the main dashboard layout
        assert any(isinstance(child, dbc.Container) and child.id == "main-dashboard" for child in content.children)

        # Assert that no error message is displayed
        assert not any(isinstance(child, dbc.Alert) for child in content.children)


def test_load_initial_content_failure():
    """Tests that appropriate loading screen is shown when forecast data is not available"""
    # Mock the load_default_forecast function to raise an exception
    with unittest.mock.patch('src.web.app.load_default_forecast', side_effect=Exception('Failed to load data')):
        # Call load_initial_content() to load the initial dashboard content
        content = load_initial_content()

        # Assert that the returned object is an instance of html.Div
        assert isinstance(content, html.Div)

        # Assert that the content contains a loading layout
        assert any(isinstance(child, dbc.Card) and child.id == "loading-container" for child in content.children)

        # Assert that an appropriate loading message is displayed
        assert any(isinstance(child, html.H4) and "Loading forecast data..." in child.children for child in content.children if isinstance(child, dbc.CardBody))


def test_app_initialization_with_callbacks():
    """Tests that the application initializes with all required callbacks registered"""
    # Mock the register_all_callbacks function
    with unittest.mock.patch('src.web.app.register_all_callbacks') as mock_register_callbacks:
        # Initialize the application using initialize_app()
        initialized_app = initialize_app()

        # Assert that register_all_callbacks was called once
        mock_register_callbacks.assert_called_once_with(initialized_app)

        # Assert that the app._callback_list contains expected callbacks
        # This is a basic check; more detailed checks can be added if needed
        assert len(initialized_app.callback_map) > 0


def test_app_error_handling():
    """Tests that the application has proper error handling configured"""
    # Mock the setup_error_handling function
    with unittest.mock.patch('src.web.app.setup_error_handling') as mock_setup_error_handling:
        # Initialize the application using initialize_app()
        initialized_app = initialize_app()

        # Assert that setup_error_handling was called once with the app instance
        mock_setup_error_handling.assert_called_once_with(initialized_app)


@pytest.mark.integration
def test_app_integration():
    """Integration test to verify the complete application initialization and layout creation"""
    # Initialize the application using initialize_app()
    initialized_app = initialize_app()

    # Create the application layout using create_app_layout()
    layout = create_app_layout()

    # Set the app.layout property to the created layout
    initialized_app.layout = layout

    # Assert that the app.layout contains all expected components
    assert any(isinstance(child, dbc.Navbar) for child in initialized_app.layout.children)
    assert any(child.id == CONTENT_DIV_ID for child in initialized_app.layout.children if isinstance(child, html.Div))
    assert any(isinstance(child, dbc.Container) and child.id == "dashboard-footer" for child in initialized_app.layout.children)
    assert any(child.id == REFRESH_INTERVAL_ID for child in initialized_app.layout.children if isinstance(child, dcc.Interval))

    # Assert that the app is properly configured for the visualization requirements
    assert initialized_app.title == 'Electricity Market Price Forecasting'
    assert initialized_app.config.suppress_callback_exceptions is True
    assert initialized_app.server.config["SERVER_NAME"] is not None
    assert initialized_app.server.config["ALLOWED_HOSTS"] is not None