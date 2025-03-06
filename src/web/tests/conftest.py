"""
Pytest configuration file that defines fixtures for testing the web visualization components of the Electricity Market Price Forecasting System.
This file provides reusable test fixtures for Dash components, mock data, and testing utilities to facilitate unit and integration testing of the dashboard interface.
"""

import pytest  # pytest: 7.0.0+
import dash  # dash: 2.9.0+
from dash import html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+
import pandas  # pandas: 2.0.0+
import unittest.mock  # standard library

from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe, create_sample_fallback_dataframe, create_multi_product_forecast_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.component_fixtures import mock_component, mock_control_panel, mock_time_series, mock_distribution_plot, mock_forecast_table, mock_product_comparison, mock_export_panel, mock_dash_app, MockTimeSeriesComponent  # src/web/tests/fixtures/component_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_callback_context, mock_dashboard_state, sample_forecast_data, sample_multi_product_data, MockCallbackContext, MockDashApp  # src/web/tests/fixtures/callback_fixtures.py
from src.web.layouts.main_dashboard import create_main_dashboard, get_initial_dashboard_state  # src/web/layouts/main_dashboard.py

TEST_CONFIG = {"debug": True, "test_mode": True}


def pytest_configure(config: pytest.Config) -> None:
    """Configures pytest for the web visualization tests"""
    # Register custom markers for different test categories
    config.addinivalue_line("markers", "dash: Mark test as a Dash component test.")
    config.addinivalue_line("markers", "component: Mark test as a component test.")
    config.addinivalue_line("markers", "callback: Mark test as a callback test.")
    config.addinivalue_line("markers", "integration: Mark test as an integration test.")

    # Set up any global pytest configuration needed for web tests
    # For example, configure logging for tests
    logging.basicConfig(level=logging.INFO)


def pytest_collection_modifyitems(config: pytest.Config, items: list) -> None:
    """Modifies test collection to add markers or skip tests based on conditions"""
    # Add 'dash' marker to all tests in the web module
    for item in items:
        if "src/web" in str(item.nodeid):
            item.add_marker(pytest.mark.dash)

        # Add 'component' marker to component tests
        if "src/web/components" in str(item.nodeid):
            item.add_marker(pytest.mark.component)

        # Add 'callback' marker to callback tests
        if "src/web/callbacks" in str(item.nodeid):
            item.add_marker(pytest.mark.callback)

        # Add 'integration' marker to integration tests
        if "src/web/integration" in str(item.nodeid):
            item.add_marker(pytest.mark.integration)


@pytest.fixture
def app() -> dash.Dash:
    """Fixture that provides a test Dash application instance"""
    # Create a new Dash application with test configuration
    app = dash.Dash(__name__)
    app.config.update(TEST_CONFIG)

    # Configure the app for testing mode
    app.enable_dev_tools(debug=True, dev_tools_ui=True, dev_tools_props_check=True, dev_tools_serve_dev_bundles=True)

    # Return the configured app instance
    return app


@pytest.fixture
def test_client(app: dash.Dash) -> dash.testing.DashTestClient:
    """Fixture that provides a test client for the Dash application"""
    # Create a test client for the provided app
    test_client = app.test()

    # Configure the test client
    # For example, set a default user agent

    # Return the configured test client
    return test_client


@pytest.fixture
def mock_forecast_data() -> pandas.DataFrame:
    """Fixture that provides mock forecast data for testing"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    mock_data = create_sample_visualization_dataframe()

    # Return the mock forecast data
    return mock_data


@pytest.fixture
def mock_fallback_data() -> pandas.DataFrame:
    """Fixture that provides mock fallback forecast data for testing"""
    # Create sample fallback dataframe using create_sample_fallback_dataframe
    mock_data = create_sample_fallback_dataframe()

    # Return the mock fallback data
    return mock_data


@pytest.fixture
def mock_multi_product_data() -> pandas.DataFrame:
    """Fixture that provides mock multi-product forecast data for testing"""
    # Create multi-product forecast dataframe using create_multi_product_forecast_dataframe
    mock_data = create_multi_product_forecast_dataframe()

    # Return the mock multi-product data
    return mock_data


@pytest.fixture
def dashboard_layout(mock_forecast_data: pandas.DataFrame) -> dbc.Container:
    """Fixture that provides a test dashboard layout"""
    # Create main dashboard layout using create_main_dashboard
    dashboard = create_main_dashboard(mock_forecast_data.to_dict())

    # Pass mock_forecast_data to the dashboard

    # Return the dashboard layout
    return dashboard


@pytest.fixture
def dashboard_state() -> Dict:
    """Fixture that provides a test dashboard state"""
    # Get initial dashboard state using get_initial_dashboard_state
    state = get_initial_dashboard_state()

    # Return the dashboard state
    return state


@pytest.fixture
def mock_callback_tester(app: dash.Dash) -> 'MockCallbackTester':
    """Fixture that provides a utility for testing callbacks"""
    # Create a MockCallbackTester instance with the provided app
    callback_tester = MockCallbackTester(app)

    # Return the callback tester
    return callback_tester


class MockCallbackTester:
    """Utility class for testing Dash callbacks"""

    def __init__(self, app: dash.Dash):
        """Initializes the MockCallbackTester"""
        # Store app property
        self.app = app
        # Initialize empty registered_callbacks dictionary
        self.registered_callbacks = {}

    def register_callback(self, callback_function: Callable, outputs: List, inputs: List, states: List = None) -> None:
        """Registers a callback for testing"""
        # Register the callback with the app
        @self.app.callback(outputs, inputs, state=states)
        def callback(*args):
            return callback_function(*args)

        # Store callback information in registered_callbacks
        self.registered_callbacks[callback_function.__name__] = {
            'outputs': outputs,
            'inputs': inputs,
            'states': states,
            'function': callback_function
        }

    def simulate_callback(self, callback_id: str, inputs: Dict, states: Dict = None) -> Any:
        """Simulates a callback execution with provided inputs and states"""
        # Get callback function from registered_callbacks
        callback_info = self.registered_callbacks.get(callback_id)
        if not callback_info:
            raise ValueError(f"Callback with id '{callback_id}' not registered.")
        callback_function = callback_info['function']

        # Create mock callback context using mock_callback_context
        mock_ctx = mock_callback_context(triggered_id=callback_id, inputs=inputs, states=states)

        # Set up the callback environment
        with unittest.mock.patch('dash.callback_context', mock_ctx):
            # Execute the callback with inputs and states
            if states:
                input_values = [inputs.get(input.component_id, None) for input in callback_info['inputs']]
                state_values = [states.get(state.component_id, None) for state in callback_info['states']]
                callback_result = callback_function(*input_values, *state_values)
            else:
                input_values = [inputs.get(input.component_id, None) for input in callback_info['inputs']]
                callback_result = callback_function(*input_values)

            # Return the callback result
            return callback_result