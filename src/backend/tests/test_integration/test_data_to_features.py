"""
Integration tests for the data-to-features pipeline of the Electricity Market Price Forecasting System.
This module tests the integration between the data ingestion components and feature engineering components,
ensuring that raw data from external sources is correctly transformed into feature vectors suitable for
the forecasting models.
"""

import datetime
from typing import Dict

import pytest  # version: 7.0.0+
import pandas as pd  # version: 2.0.0+
import numpy  # version: 1.24.0+
from unittest import mock  # package_name: unittest, version: standard library

from src.backend.tests.fixtures.load_forecast_fixtures import create_mock_load_forecast_data  # Create mock load forecast data for tests
from src.backend.tests.fixtures.historical_prices_fixtures import create_mock_historical_price_data  # Create mock historical price data for tests
from src.backend.tests.fixtures.generation_forecast_fixtures import create_mock_generation_forecast_data  # Create mock generation forecast data for tests
from src.backend.tests.fixtures.feature_fixtures import create_mock_feature_input_data  # Create mock input data dictionary for feature creation
from src.backend.tests.fixtures.feature_fixtures import MockFeatureCreator  # Mock class for feature creation testing
from src.backend.data_ingestion import DataIngestionManager  # Manager class for coordinating data ingestion from all sources
from src.backend.data_ingestion.data_transformer import DataTransformer  # Class for transforming data into standardized formats
from src.backend.feature_engineering.base_features import BaseFeatureCreator  # Class for creating base features from raw input data
from src.backend.feature_engineering.derived_features import DerivedFeatureCreator  # Class for creating derived features from base features
from src.backend.feature_engineering.product_hour_features import ProductHourFeatureCreator  # Class for creating product/hour-specific features
from src.backend.feature_engineering.exceptions import FeatureEngineeringError  # Exception for feature engineering errors
from src.backend.data_ingestion.exceptions import DataIngestionError  # Exception for data ingestion errors
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid price products for validation
from src.backend.utils.date_utils import localize_to_cst  # Convert datetime to CST timezone


def setup_test_data(start_time: datetime.datetime) -> Dict[str, pd.DataFrame]:
    """
    Creates a complete set of test data for integration testing
    """
    # Create mock load forecast data
    load_forecast_data = create_mock_load_forecast_data(start_time)

    # Create mock historical price data
    historical_price_data = create_mock_historical_price_data(
        start_date=start_time - datetime.timedelta(days=1),
        end_date=start_time
    )

    # Create mock generation forecast data
    generation_forecast_data = create_mock_generation_forecast_data(start_time)

    # Combine all data into a dictionary
    test_data = {
        "load_forecast": load_forecast_data,
        "historical_prices": historical_price_data,
        "generation_forecast": generation_forecast_data,
    }

    return test_data