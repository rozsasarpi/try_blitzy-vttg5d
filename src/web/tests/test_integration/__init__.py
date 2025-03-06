"""
Initialization file for the integration test package in the web visualization component of the Electricity Market Price Forecasting System.
This file makes test fixtures and utilities available throughout the integration test suite and defines common test data and configurations.
"""

import pathlib  # pathlib: standard library
import datetime  # datetime: standard library

import pytest  # pytest: 7.0.0+

from ..fixtures.forecast_fixtures import (  # src/web/tests/fixtures/forecast_fixtures.py
    sample_forecast_data,
    sample_fallback_forecast_data,
    SAMPLE_PRODUCTS,
)
from ..fixtures.component_fixtures import (  # src/web/tests/fixtures/component_fixtures.py
    mock_dash_app,
    mock_control_panel,
    mock_time_series,
    mock_distribution_plot,
    mock_forecast_table,
    COMPONENT_IDS,
)
from ..fixtures.callback_fixtures import (  # src/web/tests/fixtures/callback_fixtures.py
    mock_callback_context,
    mock_dashboard_state,
    CALLBACK_IDS,
)
from . import conftest  # src/web/tests/conftest.py


INTEGRATION_TEST_DIR = pathlib.Path(__file__).parent
TEST_DATA_DIR = INTEGRATION_TEST_DIR / 'data'
TEST_DATE = datetime.datetime.now().date()
TEST_START_DATE = TEST_DATE
TEST_END_DATE = TEST_DATE + datetime.timedelta(days=3)


__all__ = [
    "sample_forecast_data",
    "sample_fallback_forecast_data",
    "SAMPLE_PRODUCTS",
    "mock_dash_app",
    "mock_control_panel",
    "mock_time_series",
    "mock_distribution_plot",
    "mock_forecast_table",
    "COMPONENT_IDS",
    "mock_callback_context",
    "mock_dashboard_state",
    "CALLBACK_IDS",
    "INTEGRATION_TEST_DIR",
    "TEST_DATA_DIR",
    "TEST_DATE",
    "TEST_START_DATE",
    "TEST_END_DATE",
]