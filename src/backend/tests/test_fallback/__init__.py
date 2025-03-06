"""
Initialization module for the fallback mechanism test package. This module makes test fixtures and utilities available to all test modules in the package, facilitating testing of the fallback mechanism which provides previous day's forecasts when current forecast generation fails.
"""

import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import datetime  # standard library

from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data  # Create mock forecast data for testing fallback mechanism
from src.backend.tests.fixtures.forecast_fixtures import SAMPLE_PRODUCTS  # Sample product list for testing fallback mechanism


@pytest.fixture
def create_fallback_test_data(product: str, target_date: datetime.datetime) -> dict:
    """
    Creates common test data for fallback mechanism tests

    Args:
        product (str): The product for which to create test data
        target_date (datetime.datetime): The target date for which to create test data

    Returns:
        dict: Dictionary containing test data for fallback tests
    """
    # Create a source date one day before target_date
    source_date = target_date - datetime.timedelta(days=1)

    # Generate mock forecast data for the product and source_date
    mock_forecast = create_mock_forecast_data(product=product, start_time=source_date)

    # Return a dictionary with source_date, target_date, product, and mock_forecast
    return {
        "source_date": source_date,
        "target_date": target_date,
        "product": product,
        "mock_forecast": mock_forecast
    }


__all__ = [
    "create_fallback_test_data",
    "create_mock_forecast_data",
    "SAMPLE_PRODUCTS"
]