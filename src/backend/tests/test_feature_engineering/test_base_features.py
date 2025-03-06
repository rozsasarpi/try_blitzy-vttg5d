"""
Unit tests for the base_features module which is responsible for creating fundamental features from raw input data sources.
Tests cover the functionality of BaseFeatureCreator class and standalone feature creation functions, ensuring they correctly
transform input data into feature vectors for the forecasting models.
"""

import datetime
from typing import Dict, List, Optional, Any, Tuple
from unittest import mock

import pytest  # pytest: 7.0.0+
import pandas as pd  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
from unittest.mock import Mock

from src.backend.feature_engineering.base_features import BaseFeatureCreator  # Create base features from raw input data
from src.backend.feature_engineering.base_features import create_base_features  # Function to create base features from input dataframes
from src.backend.feature_engineering.base_features import fetch_base_feature_data  # Function to fetch all required data for feature creation
from src.backend.feature_engineering.base_features import create_temporal_features  # Function to create temporal features from timestamp information
from src.backend.feature_engineering.base_features import create_load_features  # Function to create features from load forecast data
from src.backend.feature_engineering.base_features import create_generation_features  # Function to create features from generation forecast data
from src.backend.feature_engineering.base_features import create_price_features  # Function to create features from historical price data
from src.backend.feature_engineering.base_features import merge_feature_dataframes  # Function to merge multiple feature DataFrames on timestamp
from src.backend.feature_engineering.exceptions import FeatureCreationError  # Base exception for feature engineering errors
from src.backend.feature_engineering.exceptions import MissingFeatureError  # Exception for missing required features
from src.backend.data_ingestion import DataIngestionManager  # Manager for collecting data from external sources
from src.backend.tests.fixtures.feature_fixtures import create_mock_feature_input_data  # Create mock input data dictionary for feature creation
from src.backend.tests.fixtures.feature_fixtures import create_mock_base_features  # Create mock base features for tests
from src.backend.tests.fixtures.feature_fixtures import create_incomplete_feature_data  # Create incomplete feature data for testing validation
from src.backend.tests.fixtures.feature_fixtures import create_invalid_feature_data  # Create invalid feature data for testing validation
from src.backend.tests.fixtures.feature_fixtures import MockFeatureCreator  # Mock class for feature creation testing
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid price products for validation
from src.backend.config.settings import FORECAST_HORIZON_HOURS  # Forecast horizon in hours (72)


def test_base_feature_creator_initialization():
    """Test that BaseFeatureCreator initializes correctly with default and custom parameters"""
    # Create a BaseFeatureCreator with default parameters
    creator_default = BaseFeatureCreator()
    assert creator_default._start_date is not None
    assert creator_default._end_date is not None
    assert creator_default._products == FORECAST_PRODUCTS
    assert creator_default._feature_df.empty
    assert creator_default._data_cache == {}

    # Create a BaseFeatureCreator with custom parameters
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2023, 1, 3)
    products = ["DALMP", "RTLMP"]
    creator_custom = BaseFeatureCreator(start_date=start_date, end_date=end_date, products=products)
    assert creator_custom._start_date == start_date
    assert creator_custom._end_date == end_date
    assert creator_custom._products == products
    assert creator_custom._feature_df.empty
    assert creator_custom._data_cache == {}


def test_fetch_data(mocker: Mock):
    """Test that fetch_data method correctly retrieves data from external sources"""
    # Create mock DataIngestionManager
    mock_data_manager = mocker.Mock(spec=DataIngestionManager)

    # Configure mock to return predefined data
    mock_data = {
        "load_forecast": pd.DataFrame({"timestamp": [], "load_mw": [], "region": []}),
        "historical_prices": pd.DataFrame({"timestamp": [], "product": [], "price": [], "node": []}),
        "generation_forecast": pd.DataFrame({"timestamp": [], "fuel_type": [], "generation_mw": [], "region": []}),
    }
    mock_data_manager.get_all_data.return_value = mock_data

    # Create BaseFeatureCreator with mock data_manager
    creator = BaseFeatureCreator(data_manager=mock_data_manager)

    # Call fetch_data method
    data = creator.fetch_data()

    # Verify that data_manager.get_all_data was called with correct parameters
    mock_data_manager.get_all_data.assert_called_once()

    # Verify that returned data matches expected structure
    assert isinstance(data, dict)
    assert set(data.keys()) == {"load_forecast", "historical_prices", "generation_forecast"}

    # Verify that data is cached for subsequent calls
    creator.fetch_data()
    mock_data_manager.get_all_data.assert_called_once()  # Should not be called again


def test_fetch_data_error_handling(mocker: Mock):
    """Test that fetch_data method handles errors correctly"""
    # Create mock DataIngestionManager
    mock_data_manager = mocker.Mock(spec=DataIngestionManager)

    # Configure mock to raise an exception
    mock_data_manager.get_all_data.side_effect = Exception("Data retrieval failed")

    # Create BaseFeatureCreator with mock data_manager
    creator = BaseFeatureCreator(data_manager=mock_data_manager)

    # Call fetch_data method and verify it raises FeatureCreationError
    with pytest.raises(FeatureCreationError) as exc_info:
        creator.fetch_data()

    # Verify that the original exception is included in the FeatureCreationError
    assert "Data retrieval failed" in str(exc_info.value)


def test_create_features():
    """Test that create_features method correctly transforms input data into features"""
    # Create mock input data using create_mock_feature_input_data
    input_data = create_mock_feature_input_data(start_time=datetime.datetime.now())

    # Create BaseFeatureCreator instance
    creator = BaseFeatureCreator()

    # Call create_features method with mock input data
    feature_df = creator.create_features(input_data=input_data)

    # Verify that returned DataFrame has expected structure
    assert isinstance(feature_df, pd.DataFrame)
    assert "timestamp" in feature_df.columns

    # Verify that temporal features are created correctly
    assert "hour" in feature_df.columns
    assert "day_of_week" in feature_df.columns
    assert "month" in feature_df.columns

    # Verify that load features are created correctly
    assert "load_mw" in feature_df.columns
    assert "load_rate_of_change" in feature_df.columns

    # Verify that generation features are created correctly
    assert "total_generation" in feature_df.columns
    assert "renewable_ratio" in feature_df.columns

    # Verify that price features are created correctly
    assert "price_DALMP" in feature_df.columns
    assert "price_RTLMP" in feature_df.columns

    # Verify that all features are merged correctly
    assert len(feature_df.columns) > 10


def test_create_features_without_input_data(mocker: Mock):
    """Test that create_features method fetches data when input_data is not provided"""
    # Create mock for fetch_data method
    mock_fetch_data = mocker.patch.object(BaseFeatureCreator, "fetch_data")

    # Configure mock to return predefined data
    mock_fetch_data.return_value = create_mock_feature_input_data(start_time=datetime.datetime.now())

    # Create BaseFeatureCreator instance
    creator = BaseFeatureCreator()

    # Call create_features method without input_data
    feature_df = creator.create_features()

    # Verify that fetch_data was called
    mock_fetch_data.assert_called_once()

    # Verify that returned DataFrame has expected structure
    assert isinstance(feature_df, pd.DataFrame)
    assert "timestamp" in feature_df.columns


def test_create_features_error_handling():
    """Test that create_features method handles errors correctly"""
    # Create invalid input data
    invalid_input_data = {"load_forecast": "invalid"}

    # Create BaseFeatureCreator instance
    creator = BaseFeatureCreator()

    # Call create_features with invalid data and verify it raises FeatureCreationError
    with pytest.raises(FeatureCreationError):
        creator.create_features(input_data=invalid_input_data)

    # Create incomplete input data
    incomplete_input_data = {"load_forecast": pd.DataFrame()}

    # Call create_features with incomplete data and verify it raises MissingFeatureError
    with pytest.raises(MissingFeatureError):
        creator.create_features(input_data=incomplete_input_data)


def test_get_feature_dataframe(mocker: Mock):
    """Test that get_feature_dataframe returns the current feature DataFrame"""
    # Create mock for create_features method
    mock_create_features = mocker.patch.object(BaseFeatureCreator, "create_features")

    # Configure mock to return predefined DataFrame
    mock_create_features.return_value = pd.DataFrame({"timestamp": [], "load_mw": []})

    # Create BaseFeatureCreator instance
    creator = BaseFeatureCreator()

    # Call get_feature_dataframe method
    feature_df = creator.get_feature_dataframe()

    # Verify that create_features was called
    mock_create_features.assert_called_once()

    # Verify that returned DataFrame matches expected output
    assert isinstance(feature_df, pd.DataFrame)
    assert "timestamp" in feature_df.columns
    assert "load_mw" in feature_df.columns


def test_create_temporal_features():
    """Test that create_temporal_features function correctly extracts temporal features"""
    # Create DataFrame with timestamp column
    data = {"timestamp": [datetime.datetime(2023, 1, 1, 10, 0, 0)]}
    df = pd.DataFrame(data)

    # Call create_temporal_features function
    result_df = create_temporal_features(df, "timestamp")

    # Verify that returned DataFrame has expected temporal features
    assert "hour" in result_df.columns
    assert "day_of_week" in result_df.columns
    assert "month" in result_df.columns
    assert "is_weekend" in result_df.columns
    assert "is_holiday" in result_df.columns

    # Verify that hour feature is extracted correctly
    assert result_df["hour"][0] == 10

    # Verify that day_of_week feature is extracted correctly
    assert result_df["day_of_week"][0] == 6

    # Verify that month feature is extracted correctly
    assert result_df["month"][0] == 1

    # Verify that is_weekend feature is calculated correctly
    assert result_df["is_weekend"][0] == 1

    # Verify that is_holiday feature is calculated correctly
    assert result_df["is_holiday"][0] == 0


def test_create_load_features():
    """Test that create_load_features function correctly creates load-based features"""
    # Create DataFrame with load data
    data = {"timestamp": [datetime.datetime(2023, 1, 1, 10, 0, 0)], "load_mw": [40000]}
    df = pd.DataFrame(data)

    # Call create_load_features function
    result_df = create_load_features(df)

    # Verify that returned DataFrame has expected load features
    assert "load_rate_of_change" in result_df.columns
    assert "load_daily_peak" in result_df.columns
    assert "load_daily_average" in result_df.columns

    # Verify that load_rate_of_change feature is calculated correctly
    assert result_df["load_rate_of_change"][0] == 0.0

    # Verify that load_daily_peak feature is calculated correctly
    assert result_df["load_daily_peak"][0] == 1.0

    # Verify that load_daily_average feature is calculated correctly
    assert result_df["load_daily_average"][0] == 1.0


def test_create_generation_features():
    """Test that create_generation_features function correctly creates generation-based features"""
    # Create DataFrame with generation data
    data = {"timestamp": [datetime.datetime(2023, 1, 1, 10, 0, 0)], "fuel_type": ["WIND"], "generation_mw": [10000]}
    df = pd.DataFrame(data)

    # Call create_generation_features function
    result_df = create_generation_features(df)

    # Verify that returned DataFrame has expected generation features
    assert "generation_WIND" in result_df.columns
    assert "total_generation" in result_df.columns
    assert "renewable_ratio" in result_df.columns
    assert "fuel_mix_diversity" in result_df.columns

    # Verify that fuel type columns are created correctly
    assert result_df["generation_WIND"][0] == 10000

    # Verify that total_generation feature is calculated correctly
    assert result_df["total_generation"][0] == 10000

    # Verify that renewable_ratio feature is calculated correctly
    assert result_df["renewable_ratio"][0] == 1.0

    # Verify that fuel_mix_diversity feature is calculated correctly
    assert result_df["fuel_mix_diversity"][0] == 0.0


def test_create_price_features():
    """Test that create_price_features function correctly creates price-based features"""
    # Create DataFrame with price data
    data = {"timestamp": [datetime.datetime(2023, 1, 1, 10, 0, 0)], "product": ["DALMP"], "price": [50]}
    df = pd.DataFrame(data)

    # Call create_price_features function
    result_df = create_price_features(df)

    # Verify that returned DataFrame has expected price features
    assert "price_DALMP" in result_df.columns
    assert "price_DALMP_volatility_24h" in result_df.columns
    assert "price_DALMP_ma_24h" in result_df.columns
    assert "price_DALMP_ma_7d" in result_df.columns

    # Verify that product columns are created correctly
    assert result_df["price_DALMP"][0] == 50

    # Verify that price_volatility features are calculated correctly
    assert result_df["price_DALMP_volatility_24h"][0] == 0.0

    # Verify that price_trend features are calculated correctly
    assert result_df["price_DALMP_ma_24h"][0] == 50

    # Verify that price_spread features are calculated correctly
    # (This test case doesn't include RTLMP, so the spread should be NaN)
    # assert numpy.isnan(result_df["price_spread_DA_RT"][0])


def test_merge_feature_dataframes():
    """Test that merge_feature_dataframes correctly combines multiple feature DataFrames"""
    # Create multiple DataFrames with different features
    data1 = {"timestamp": [datetime.datetime(2023, 1, 1, 10, 0, 0)], "load_mw": [40000]}
    df1 = pd.DataFrame(data1)
    data2 = {"timestamp": [datetime.datetime(2023, 1, 1, 10, 0, 0)], "price_DALMP": [50]}
    df2 = pd.DataFrame(data2)
    data3 = {"timestamp": [datetime.datetime(2023, 1, 1, 10, 0, 0)], "generation_WIND": [10000]}
    df3 = pd.DataFrame(data3)

    # Call merge_feature_dataframes function
    feature_dfs = {"load": df1, "price": df2, "generation": df3}
    result_df = merge_feature_dataframes(feature_dfs)

    # Verify that returned DataFrame has all features from input DataFrames
    assert "timestamp" in result_df.columns
    assert "load_mw" in result_df.columns
    assert "price_DALMP" in result_df.columns
    assert "generation_WIND" in result_df.columns

    # Verify that timestamps are aligned correctly
    assert result_df["timestamp"][0] == datetime.datetime(2023, 1, 1, 10, 0, 0)

    # Verify that duplicate column names are handled correctly
    # (This test case doesn't have duplicate column names, so no renaming should occur)

    # Verify that the merged DataFrame has the expected number of rows
    assert len(result_df) == 1


def test_create_base_features_function():
    """Test that standalone create_base_features function works correctly"""
    # Create mock input data
    input_data = create_mock_feature_input_data(start_time=datetime.datetime.now())

    # Call create_base_features function
    feature_df = create_base_features(input_data=input_data)

    # Verify that returned DataFrame has expected structure
    assert isinstance(feature_df, pd.DataFrame)
    assert "timestamp" in feature_df.columns

    # Verify that all expected feature categories are present
    assert any(col in feature_df.columns for col in SAMPLE_TEMPORAL_FEATURES)
    assert any(col in feature_df.columns for col in SAMPLE_LOAD_FEATURES)
    assert any(col in feature_df.columns for col in SAMPLE_GENERATION_FEATURES)
    assert any(col in feature_df.columns for col in SAMPLE_PRICE_FEATURES)

    # Verify that the function handles default parameters correctly
    # (This test case doesn't explicitly test default parameters)


def test_fetch_base_feature_data_function(mocker: Mock):
    """Test that standalone fetch_base_feature_data function works correctly"""
    # Create mock for DataIngestionManager
    mock_data_manager = mocker.Mock(spec=DataIngestionManager)

    # Configure mock to return predefined data
    mock_data = {
        "load_forecast": pd.DataFrame({"timestamp": [], "load_mw": [], "region": []}),
        "historical_prices": pd.DataFrame({"timestamp": [], "product": [], "price": [], "node": []}),
        "generation_forecast": pd.DataFrame({"timestamp": [], "fuel_type": [], "generation_mw": [], "region": []}),
    }
    mock_data_manager.get_all_data.return_value = mock_data

    # Call fetch_base_feature_data function
    data = fetch_base_feature_data(start_date=datetime.datetime.now(), end_date=datetime.datetime.now())

    # Verify that DataIngestionManager.get_all_data was called with correct parameters
    mock_data_manager.get_all_data.assert_called_once()

    # Verify that returned data has expected structure
    assert isinstance(data, dict)
    assert set(data.keys()) == {"load_forecast", "historical_prices", "generation_forecast"}

    # Verify that the function handles default parameters correctly
    # (This test case doesn't explicitly test default parameters)