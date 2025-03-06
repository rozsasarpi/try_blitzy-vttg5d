"""
Initialization file for the test_callbacks package in the web visualization component of the Electricity Market Price Forecasting System.
This file makes callback testing fixtures and utilities available throughout the callback test suite.
"""
import pathlib  # pathlib: standard library

from src.web.tests.fixtures.callback_fixtures import mock_callback_context  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_dashboard_state  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_product_selection_callback  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_date_range_callback  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_visualization_options_callback  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_refresh_callback  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_viewport_change_callback  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_export_callback  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import MockCallbackContext  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import MockDashApp  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import CALLBACK_IDS  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import create_mock_callback_inputs  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import create_mock_callback_states  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_callback_function  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import sample_forecast_data  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.callback_fixtures import sample_multi_product_data  # src/web/tests/fixtures/callback_fixtures.py

TEST_CALLBACKS_DIR = pathlib.Path(__file__).parent

__all__ = [
    "mock_callback_context",
    "mock_dashboard_state",
    "mock_product_selection_callback",
    "mock_date_range_callback",
    "mock_visualization_options_callback",
    "mock_refresh_callback",
    "mock_viewport_change_callback",
    "mock_export_callback",
    "MockCallbackContext",
    "MockDashApp",
    "CALLBACK_IDS",
    "create_mock_callback_inputs",
    "create_mock_callback_states",
    "mock_callback_function",
    "sample_forecast_data",
    "sample_multi_product_data",
    "TEST_CALLBACKS_DIR"
]