"""
Initialization file for the test_services package in the web visualization component of the Electricity Market Price Forecasting System.
This file makes test utilities and fixtures available for testing service components such as authentication, API service, and error reporting.
"""
from pathlib import Path  # standard library

import pytest  # pytest: 7.0.0+

from ..fixtures.callback_fixtures import mock_callback_context  # src/web/tests/fixtures/callback_fixtures.py
from ..fixtures.forecast_fixtures import sample_forecast_data  # src/web/tests/fixtures/forecast_fixtures.py

SERVICE_TEST_DIR = Path(__file__).parent

__all__ = [
    "mock_callback_context",
    "sample_forecast_data",
    "SERVICE_TEST_DIR",
]