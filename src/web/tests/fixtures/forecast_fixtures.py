"""
Provides test fixtures for forecast data to be used in unit and integration tests
for the web visualization components of the Electricity Market Price Forecasting System.
This module contains functions to generate sample forecast dataframes in various formats,
including normal forecasts, fallback forecasts, and different data scenarios for testing
visualization components.
"""

import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
from typing import List, Optional, Dict
from datetime import datetime, timedelta  # standard library

from src.web.config.product_config import PRODUCTS, PRODUCT_DETAILS, can_be_negative  # List of valid electricity market products
from src.web.data.schema import WEB_VISUALIZATION_SCHEMA, prepare_dataframe_for_visualization  # Schema for web visualization dataframes
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data, create_mock_probabilistic_forecasts, BASE_PRICE_PATTERNS, VOLATILITY_FACTORS  # Create mock forecast data from backend fixtures

DEFAULT_PERCENTILES = [10, 90]
SAMPLE_COUNT = 100
FORECAST_HORIZON_HOURS = 72


def create_sample_forecast_dataframe(
    product: Optional[str] = None,
    start_time: Optional[datetime] = None,
    hours: Optional[int] = None,
    is_fallback: Optional[bool] = None
) -> pandas.DataFrame:
    """
    Creates a sample forecast dataframe for testing web visualization components
    """
    if product is None:
        product = 'DALMP'
    if start_time is None:
        start_time = datetime.now()
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    if is_fallback is None:
        is_fallback = False
    return create_mock_forecast_data(product=product, start_time=start_time, hours=hours, is_fallback=is_fallback)


def create_sample_visualization_dataframe(
    product: Optional[str] = None,
    start_time: Optional[datetime] = None,
    hours: Optional[int] = None,
    is_fallback: Optional[bool] = None,
    percentiles: Optional[List[int]] = None
) -> pandas.DataFrame:
    """
    Creates a sample forecast dataframe in the format needed for web visualization
    """
    if percentiles is None:
        percentiles = DEFAULT_PERCENTILES
    backend_df = create_sample_forecast_dataframe(product=product, start_time=start_time, hours=hours, is_fallback=is_fallback)
    visualization_df = prepare_dataframe_for_visualization(backend_df, percentiles=percentiles)
    return visualization_df


def create_multi_product_forecast_dataframe(
    products: Optional[List[str]] = None,
    start_time: Optional[datetime] = None,
    hours: Optional[int] = None,
    is_fallback: Optional[bool] = None
) -> pandas.DataFrame:
    """
    Creates a sample forecast dataframe with multiple products for testing comparison views
    """
    if products is None:
        products = ['DALMP', 'RTLMP']
    if start_time is None:
        start_time = datetime.now()
    if hours is None:
        hours = FORECAST_HORIZON_HOURS
    if is_fallback is None:
        is_fallback = False

    dataframes = []
    for product in products:
        df = create_sample_forecast_dataframe(product=product, start_time=start_time, hours=hours, is_fallback=is_fallback)
        dataframes.append(df)

    combined_df = pandas.concat(dataframes, ignore_index=True)
    return combined_df


def create_sample_fallback_dataframe(
    product: Optional[str] = None,
    start_time: Optional[datetime] = None,
    hours: Optional[int] = None
) -> pandas.DataFrame:
    """
    Creates a sample fallback forecast dataframe for testing fallback handling
    """
    return create_sample_forecast_dataframe(product=product, start_time=start_time, hours=hours, is_fallback=True)


def create_incomplete_visualization_dataframe(
    product: Optional[str] = None,
    start_time: Optional[datetime] = None,
    hours: Optional[int] = None,
    hours_to_remove: Optional[List[int]] = None
) -> pandas.DataFrame:
    """
    Creates a sample visualization dataframe with missing hours for testing error handling
    """
    complete_df = create_sample_visualization_dataframe(product=product, start_time=start_time, hours=hours)
    if hours_to_remove is None:
        num_to_remove = min(10, len(complete_df))  # Remove up to 10 hours or less if the DataFrame is smaller
        hours_to_remove = numpy.random.choice(complete_df.index, size=num_to_remove, replace=False)
    incomplete_df = complete_df.drop(hours_to_remove)
    return incomplete_df


def create_invalid_visualization_dataframe(
    product: Optional[str] = None,
    start_time: Optional[datetime] = None,
    hours: Optional[int] = None,
    invalid_columns: Optional[Dict] = None
) -> pandas.DataFrame:
    """
    Creates a sample visualization dataframe with invalid values for testing validation
    """
    valid_df = create_sample_visualization_dataframe(product=product, start_time=start_time, hours=hours)
    invalid_df = valid_df.copy()

    if invalid_columns is None:
        # Create default invalid data (e.g., negative prices for products that shouldn't have them)
        invalid_columns = {}
        if product and not can_be_negative(product):
            invalid_columns['point_forecast'] = -10.0  # Negative price
    
    for column, invalid_value in invalid_columns.items():
        if column in invalid_df.columns:
            invalid_df[column] = invalid_value
    
    return invalid_df


def create_forecast_with_extreme_values(
    product: Optional[str] = None,
    start_time: Optional[datetime] = None,
    hours: Optional[int] = None,
    extreme_factor: Optional[float] = None
) -> pandas.DataFrame:
    """
    Creates a sample forecast dataframe with extreme price values for testing visualization limits
    """
    normal_df = create_sample_visualization_dataframe(product=product, start_time=start_time, hours=hours)
    if extreme_factor is None:
        extreme_factor = 10.0  # Default extreme factor

    extreme_df = normal_df.copy()
    for col in ['point_forecast', 'lower_bound', 'upper_bound']:
        if col in extreme_df.columns:
            extreme_df[col] = extreme_df[col] * extreme_factor

    return extreme_df