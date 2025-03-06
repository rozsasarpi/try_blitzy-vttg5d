"""
Provides test fixtures for feature engineering components to be used in unit and integration tests
for the Electricity Market Price Forecasting System. These fixtures include mock feature data,
sample feature vectors, and utility functions to generate test data with various characteristics
for testing the feature engineering pipeline.
"""

import datetime
from typing import Dict, List, Optional, Any, Tuple

import pytest  # pytest: 7.0.0+
import pandas as pd  # pandas: 2.0.0+
import numpy as np  # numpy: 1.24.0+

from src.backend.feature_engineering.base_features import BaseFeatureCreator  # Create base features from raw input data
from src.backend.feature_engineering.derived_features import DerivedFeatureCreator  # Create derived features from base features
from src.backend.feature_engineering.lagged_features import LaggedFeatureGenerator  # Generate lagged features from time series data
from src.backend.feature_engineering.lagged_features import DEFAULT_LAG_PERIODS  # Default lag periods to use for feature generation
from src.backend.feature_engineering.feature_normalizer import FeatureNormalizer  # Normalize features for model input
from src.backend.feature_engineering.feature_selector import FeatureSelector  # Select relevant features for specific product/hour combinations
from src.backend.feature_engineering.product_hour_features import ProductHourFeatureCreator  # Create and manage product/hour-specific features
from src.backend.feature_engineering.exceptions import FeatureEngineeringError  # Base exception for feature engineering errors
from src.backend.tests.fixtures.load_forecast_fixtures import create_mock_load_forecast_data  # Create mock load forecast data for tests
from src.backend.tests.fixtures.historical_prices_fixtures import create_mock_api_response  # Create mock historical price data for tests
from src.backend.tests.fixtures.generation_forecast_fixtures import create_mock_generation_forecast_data  # Create mock generation forecast data for tests
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid price products for validation
from src.backend.config.settings import FORECAST_HORIZON_HOURS  # Number of hours in the forecast horizon (72)
from src.backend.utils.date_utils import localize_to_cst  # Convert datetime to CST timezone

SAMPLE_TEMPORAL_FEATURES = ["hour", "day_of_week", "month", "is_weekend", "is_holiday"]
SAMPLE_LOAD_FEATURES = ["load_mw", "load_rate_of_change", "load_daily_peak", "load_daily_average"]
SAMPLE_GENERATION_FEATURES = ["wind_generation", "solar_generation", "thermal_generation", "total_generation", "renewable_ratio", "fuel_mix_diversity"]
SAMPLE_PRICE_FEATURES = ["dalmp", "rtlmp", "regup", "regdown", "rrs", "nsrs", "price_volatility_24h", "price_trend_24h", "price_spread_dalmp_rtlmp"]


def create_mock_feature_data(
    start_time: datetime.datetime,
    hours: Optional[int] = None,
    feature_categories: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with mock feature data for testing.

    Args:
        start_time: Starting timestamp for the feature data.
        hours: Number of hours to generate (defaults to FORECAST_HORIZON_HOURS).
        feature_categories: List of feature categories to include (temporal, load, generation, price).

    Returns:
        DataFrame with mock feature data.
    """
    # Ensure start_time is in CST timezone
    start_time = localize_to_cst(start_time)

    # Use default hours if not provided
    if hours is None:
        hours = FORECAST_HORIZON_HOURS

    # Use all categories if not provided
    if feature_categories is None:
        feature_categories = ["temporal", "load", "generation", "price"]

    # Create date range
    date_range = pd.date_range(start=start_time, periods=hours, freq='H')

    # Create timestamp column
    data = {'timestamp': date_range}
    df = pd.DataFrame(data)

    # Add temporal features
    if 'temporal' in feature_categories:
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['month'] = df['timestamp'].dt.month
        df['is_weekend'] = (df['timestamp'].dt.dayofweek >= 5).astype(int)
        df['is_holiday'] = 0  # Placeholder
    
    # Add load features
    if 'load' in feature_categories:
        df['load_mw'] = numpy.random.rand(hours) * 50000
        df['load_rate_of_change'] = numpy.random.rand(hours)
        df['load_daily_peak'] = numpy.random.rand(hours)
        df['load_daily_average'] = numpy.random.rand(hours)

    # Add generation features
    if 'generation' in feature_categories:
        df['wind_generation'] = numpy.random.rand(hours) * 15000
        df['solar_generation'] = numpy.random.rand(hours) * 8000
        df['thermal_generation'] = numpy.random.rand(hours) * 20000
        df['total_generation'] = df['wind_generation'] + df['solar_generation'] + df['thermal_generation']
        df['renewable_ratio'] = df['wind_generation'] + df['solar_generation'] / df['total_generation']
        df['fuel_mix_diversity'] = numpy.random.rand(hours)

    # Add price features
    if 'price' in feature_categories:
        df['dalmp'] = numpy.random.rand(hours) * 50
        df['rtlmp'] = numpy.random.rand(hours) * 60
        df['regup'] = numpy.random.rand(hours) * 10
        df['regdown'] = numpy.random.rand(hours) * 8
        df['rrs'] = numpy.random.rand(hours) * 5
        df['nsrs'] = numpy.random.rand(hours) * 3
        df['price_volatility_24h'] = numpy.random.rand(hours) * 2
        df['price_trend_24h'] = numpy.random.rand(hours)
        df['price_spread_dalmp_rtlmp'] = numpy.random.rand(hours) * 10

    # Ensure correct column types
    df = df.astype({
        'timestamp': 'datetime64[ns, America/Chicago]',
        'hour': 'int64',
        'day_of_week': 'int64',
        'month': 'int64',
        'is_weekend': 'int64',
        'is_holiday': 'int64',
        'load_mw': 'float64',
        'load_rate_of_change': 'float64',
        'load_daily_peak': 'float64',
        'load_daily_average': 'float64',
        'wind_generation': 'float64',
        'solar_generation': 'float64',
        'thermal_generation': 'float64',
        'total_generation': 'float64',
        'renewable_ratio': 'float64',
        'fuel_mix_diversity': 'float64',
        'dalmp': 'float64',
        'rtlmp': 'float64',
        'regup': 'float64',
        'regdown': 'float64',
        'rrs': 'float64',
        'nsrs': 'float64',
        'price_volatility_24h': 'float64',
        'price_trend_24h': 'float64',
        'price_spread_dalmp_rtlmp': 'float64'
    })

    return df


def create_mock_base_features(
    start_time: datetime.datetime,
    hours: Optional[int] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with mock base features for testing.

    Args:
        start_time: Starting timestamp for the feature data.
        hours: Number of hours to generate (defaults to FORECAST_HORIZON_HOURS).

    Returns:
        DataFrame with mock base features.
    """
    return create_mock_feature_data(start_time, hours, feature_categories=['temporal', 'load', 'generation'])


def create_mock_derived_features(
    base_features_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with mock derived features for testing.

    Args:
        base_features_df: DataFrame containing base features.

    Returns:
        DataFrame with mock derived features.
    """
    if base_features_df is None:
        base_features_df = create_mock_base_features(start_time=datetime.datetime.now())
    
    # Create load/generation ratio feature
    base_features_df['load_generation_ratio'] = numpy.random.rand(len(base_features_df))
    
    # Create price spread features
    base_features_df['price_spread_DA_RT'] = numpy.random.rand(len(base_features_df))
    
    # Create volatility features
    base_features_df['volatility_DALMP_24h'] = numpy.random.rand(len(base_features_df))
    
    # Create renewable impact features
    base_features_df['renewable_impact'] = numpy.random.rand(len(base_features_df))
    
    # Create temporal interaction features
    base_features_df['hour_x_load'] = numpy.random.rand(len(base_features_df))
    
    return base_features_df


def create_mock_lagged_features(
    features_df: Optional[pd.DataFrame] = None,
    columns_to_lag: Optional[List[str]] = None,
    lag_periods: Optional[List[int]] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with mock lagged features for testing.

    Args:
        features_df: DataFrame containing base features.
        columns_to_lag: List of columns to create lagged features for.
        lag_periods: List of lag periods to use.

    Returns:
        DataFrame with mock lagged features.
    """
    if features_df is None:
        features_df = create_mock_base_features(start_time=datetime.datetime.now())
    if columns_to_lag is None:
        columns_to_lag = ['load_mw', 'wind_generation']
    if lag_periods is None:
        lag_periods = DEFAULT_LAG_PERIODS
    
    generator = LaggedFeatureGenerator(features_df, timestamp_column='timestamp')
    generator.add_feature_columns(columns_to_lag)
    return generator.generate_all_lagged_features()


def create_mock_normalized_features(
    features_df: Optional[pd.DataFrame] = None,
    method: str = 'standard',
    columns_to_normalize: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with mock normalized features for testing.

    Args:
        features_df: DataFrame containing base features.
        method: Normalization method ('standard', 'minmax', 'robust').
        columns_to_normalize: List of columns to normalize.

    Returns:
        DataFrame with mock normalized features.
    """
    if features_df is None:
        features_df = create_mock_base_features(start_time=datetime.datetime.now())
    if columns_to_normalize is None:
        columns_to_normalize = features_df.select_dtypes(include=numpy.number).columns.tolist()
    
    normalizer = FeatureNormalizer(method=method)
    return normalizer.fit_transform(features_df, columns_to_normalize)


def create_mock_product_hour_features(
    features_df: Optional[pd.DataFrame] = None,
    product: str = 'DALMP',
    hour: int = 12,
    normalize: bool = False
) -> pd.DataFrame:
    """
    Creates a DataFrame with mock product/hour-specific features for testing.

    Args:
        features_df: DataFrame containing base features.
        product: The price product identifier.
        hour: The target hour (0-23).
        normalize: Whether to normalize the features.

    Returns:
        DataFrame with mock product/hour-specific features.
    """
    if features_df is None:
        features_df = create_mock_base_features(start_time=datetime.datetime.now())
    
    creator = ProductHourFeatureCreator()
    return creator.create_features(product, hour)


def create_mock_feature_input_data(
    start_time: datetime.datetime,
    hours: Optional[int] = None
) -> Dict[str, pd.DataFrame]:
    """
    Creates a dictionary with mock input data for feature creation.

    Args:
        start_time: Starting timestamp for the feature data.
        hours: Number of hours to generate (defaults to FORECAST_HORIZON_HOURS).

    Returns:
        Dictionary with mock input data for feature creation.
    """
    # Ensure start_time is in CST timezone
    start_time = localize_to_cst(start_time)

    # Use default hours if not provided
    if hours is None:
        hours = FORECAST_HORIZON_HOURS

    # Create mock data
    load_forecast_data = create_mock_load_forecast_data(start_time, hours)
    historical_price_data = create_mock_api_response(start_time, hours)
    generation_forecast_data = create_mock_generation_forecast_data(start_time, hours)

    # Combine all data into a dictionary
    input_data = {
        'load_forecast': load_forecast_data,
        'historical_prices': historical_price_data,
        'generation_forecast': generation_forecast_data
    }

    return input_data


def create_incomplete_feature_data(
    features_df: Optional[pd.DataFrame] = None,
    columns_to_remove: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with incomplete feature data (missing columns).

    Args:
        features_df: DataFrame containing base features.
        columns_to_remove: List of columns to remove.

    Returns:
        DataFrame with incomplete feature data.
    """
    if features_df is None:
        features_df = create_mock_base_features(start_time=datetime.datetime.now())
    if columns_to_remove is None:
        columns_to_remove = ['load_mw', 'wind_generation']
    
    df = features_df.copy()
    df = df.drop(columns=columns_to_remove, errors='ignore')
    return df


def create_invalid_feature_data(
    features_df: Optional[pd.DataFrame] = None,
    invalid_columns: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with invalid feature data (wrong types or values).

    Args:
        features_df: DataFrame containing base features.
        invalid_columns: Dictionary of columns to make invalid.

    Returns:
        DataFrame with invalid feature data.
    """
    if features_df is None:
        features_df = create_mock_base_features(start_time=datetime.datetime.now())
    if invalid_columns is None:
        invalid_columns = {'load_mw': -100, 'hour': 'invalid'}
    
    df = features_df.copy()
    for column, value in invalid_columns.items():
        if column in df.columns:
            df[column] = value
    return df

class MockFeatureCreator:
    """Mock class for feature creation to use in tests."""

    def __init__(self, features_df: Optional[pd.DataFrame] = None, error: Optional[Exception] = None):
        """
        Initialize the mock feature creator with predefined features.

        Args:
            features_df: DataFrame to return when create_features is called.
            error: Exception to raise when create_features is called.
        """
        self._features_df = features_df if features_df is not None else pd.DataFrame()
        self._feature_cache: Dict = {}
        self._error = error

    def create_features(self, params: Optional[dict] = None) -> pd.DataFrame:
        """
        Mock implementation of create_features that returns predefined features or raises errors.

        Args:
            params (Optional[dict]): Parameters to use as cache key.

        Returns:
            Predefined features DataFrame.
        """
        if self._error:
            raise self._error

        if params:
            cache_key = str(params)
            if cache_key in self._feature_cache:
                return self._feature_cache[cache_key]
            else:
                return self._features_df

        return self._features_df

    def set_features(self, features_df: pd.DataFrame) -> None:
        """
        Sets the features DataFrame to be returned.

        Args:
            features_df: DataFrame to return.
        """
        self._features_df = features_df

    def set_error(self, error: Exception) -> None:
        """
        Sets an error to be raised on feature creation.

        Args:
            error: Exception to raise.
        """
        self._error = error

    def clear_error(self) -> None:
        """
        Clears any set error.
        """
        self._error = None

    def add_cached_features(self, params: dict, features_df: pd.DataFrame) -> None:
        """
        Adds cached features for specific parameters.

        Args:
            params (dict): Parameters to use as cache key.
            features_df (pd.DataFrame): Features DataFrame to cache.
        """
        cache_key = str(params)
        self._feature_cache[cache_key] = features_df

    def clear_cache(self) -> None:
        """
        Clears the feature cache.
        """
        self._feature_cache = {}

class MockFeatureSelector:
    """Mock class for feature selection to use in tests."""

    def __init__(self, selected_features: Optional[Dict] = None, error: Optional[Exception] = None):
        """
        Initialize the mock feature selector with predefined selections.

        Args:
            selected_features: Dictionary mapping product/hour to selected features.
            error: Exception to raise when select_features is called.
        """
        self._selected_features = selected_features if selected_features is not None else {}
        self._error = error

    def select_features(self, features_df: pd.DataFrame, product: str, hour: int) -> pd.DataFrame:
        """
        Mock implementation of select_features that returns predefined selections or raises errors.

        Args:
            features_df: DataFrame to select features from.
            product: The price product identifier.
            hour: The target hour (0-23).

        Returns:
            Selected features DataFrame.
        """
        if self._error:
            raise self._error

        key = f"{product}_{hour}"
        if key in self._selected_features:
            return self._selected_features[key]
        else:
            if features_df is None or features_df.empty:
                raise ValueError("Features DataFrame is empty or None")
            return features_df[['timestamp', 'load_mw', 'hour']]

    def add_selection(self, product: str, hour: int, selected_df: pd.DataFrame) -> None:
        """
        Adds a new mock selection for specific product/hour.

        Args:
            product: The price product identifier.
            hour: The target hour (0-23).
            selected_df: DataFrame with selected features.
        """
        key = f"{product}_{hour}"
        self._selected_features[key] = selected_df

    def set_error(self, error: Exception) -> None:
        """
        Sets an error to be raised on selection.

        Args:
            error: Exception to raise.
        """
        self._error = error

    def clear_error(self) -> None:
        """
        Clears any set error.
        """
        self._error = None