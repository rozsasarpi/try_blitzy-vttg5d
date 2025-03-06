"""
Initialization file for the test_utils package in the web visualization component of the Electricity Market Price Forecasting System.
This file makes common test fixtures and utilities available for testing the utility functions that support the Dash-based visualization interface.
"""

from pathlib import Path  # Standard library

import pytest  # pytest: 7.0.0+

from ..fixtures.forecast_fixtures import sample_forecast_data  # Internal import
from ..fixtures.forecast_fixtures import sample_time_series_data  # Internal import
from ..fixtures.forecast_fixtures import sample_distribution_data  # Internal import

TEST_UTILS_DIR = Path(__file__).parent  # Define the test utils directory path


@pytest.fixture(scope="session")
def sample_forecast_data():
    """
    Re-export sample forecast data fixture for utility function testing
    """
    return sample_forecast_data


@pytest.fixture(scope="session")
def sample_time_series_data():
    """
    Re-export sample time series data fixture for plot helper testing
    """
    return sample_time_series_data


@pytest.fixture(scope="session")
def sample_distribution_data():
    """
    Re-export sample distribution data fixture for plot helper testing
    """
    return sample_distribution_data