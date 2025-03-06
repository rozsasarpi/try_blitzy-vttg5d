"""
Initialization file for the test_components package in the web visualization component test suite.
Makes the test_components directory a proper Python package and provides shared test utilities for testing UI components.
"""

import pathlib  # pathlib: standard library
from pathlib import Path

import pytest  # pytest: 7.0.0+

from src.web.tests.fixtures.forecast_fixtures import sample_forecast_data  # Import sample forecast data fixture for testing
from src.web.tests.fixtures.forecast_fixtures import sample_time_series_data  # Import sample time series data fixture for testing
from src.web.tests.fixtures.forecast_fixtures import sample_distribution_data  # Import sample data for distribution visualization testing
from src.web.tests.fixtures.forecast_fixtures import sample_product_comparison_data  # Import sample data for product comparison testing
from src.web.tests.fixtures.forecast_fixtures import sample_hourly_table_data  # Import sample data for hourly table display testing
from src.web.tests.fixtures.forecast_fixtures import sample_fallback_forecast_data  # Import sample fallback forecast data for testing
from src.web.tests.fixtures.component_fixtures import MockTimeSeriesComponent  # Import mock implementation of TimeSeriesComponent for testing
from src.web.tests.fixtures.component_fixtures import mock_component  # Import mock component generator for UI testing
from src.web.tests.fixtures.component_fixtures import mock_control_panel  # Import mock control panel component for testing
from src.web.tests.fixtures.component_fixtures import mock_time_series  # Import mock time series component for testing
from src.web.tests.fixtures.component_fixtures import mock_distribution_plot  # Import mock distribution plot component for testing
from src.web.tests.fixtures.component_fixtures import mock_forecast_table  # Import mock forecast table component for testing
from src.web.tests.fixtures.component_fixtures import mock_product_comparison  # Import mock product comparison component for testing
from src.web.tests.fixtures.component_fixtures import mock_export_panel  # Import mock export panel component for testing
from src.web.tests.fixtures.component_fixtures import mock_dash_app  # Import mock Dash application for component testing
from src.web.tests.fixtures.component_fixtures import COMPONENT_IDS  # Import component IDs for testing

COMPONENT_TEST_DIR = Path(__file__).parent

__all__ = [
    "sample_forecast_data",
    "sample_time_series_data",
    "sample_distribution_data",
    "sample_product_comparison_data",
    "sample_hourly_table_data",
    "sample_fallback_forecast_data",
    "MockTimeSeriesComponent",
    "mock_component",
    "mock_control_panel",
    "mock_time_series",
    "mock_distribution_plot",
    "mock_forecast_table",
    "mock_product_comparison",
    "mock_export_panel",
    "mock_dash_app",
    "COMPONENT_IDS",
    "COMPONENT_TEST_DIR"
]