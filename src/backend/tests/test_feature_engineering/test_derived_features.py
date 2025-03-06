"""
Unit tests for the derived features module of the Electricity Market Price Forecasting System.
Tests the creation of derived features from base features, including load/generation ratio,
price spreads, volatility features, renewable impact features, and temporal interaction features.
"""

import pytest  # pytest: 7.3.1
import pandas as pd  # pandas: 2.0.0
import numpy as np  # numpy: 1.24.0
from datetime import datetime  # standard library

# Internal imports
from ...feature_engineering.derived_features import create_derived_features, validate_base_features
from ...feature_engineering.derived_features import create_load_generation_ratio
from ...feature_engineering.derived_features import create_price_spread_features
from ...feature_engineering.derived_features import create_volatility_features
from ...feature_engineering.derived_features import create_renewable_impact_features
from ...feature_engineering.derived_features import create_temporal_interaction_features
from ...feature_engineering.derived_features import DerivedFeatureCreator
from ...feature_engineering.exceptions import DerivedFeatureError, MissingFeatureError
from ..fixtures.feature_fixtures import create_mock_base_features, create_incomplete_feature_data
from ...config/settings import FORECAST_PRODUCTS  # List of valid price products for validation

# Define sample windows for volatility features
SAMPLE_WINDOWS = [24, 48, 168]

# Define sample features to interact with temporal features
SAMPLE_FEATURES_TO_INTERACT = ["load_mw", "total_generation", "renewable_ratio"]


def test_validate_base_features_with_valid_data():
    """Tests that validate_base_features returns True when all required features are present"""
    # Arrange
    base_features = create_mock_base_features(start_time=datetime.now())

    # Act
    result = validate_base_features(base_features)

    # Assert
    assert result is True


def test_validate_base_features_with_missing_features():
    """Tests that validate_base_features raises MissingFeatureError when required features are missing"""
    # Arrange
    incomplete_data = create_incomplete_feature_data(start_time=datetime.now())

    # Act & Assert
    with pytest.raises(MissingFeatureError) as exc_info:
        validate_base_features(incomplete_data)
    assert "Missing required base features" in str(exc_info.value)


def test_create_load_generation_ratio():
    """Tests the creation of load/generation ratio feature"""
    # Arrange
    base_features = create_mock_base_features(start_time=datetime.now())
    base_features['total_generation'] = base_features['wind_generation'] + base_features['solar_generation']

    # Act
    ratio_series = create_load_generation_ratio(base_features)

    # Assert
    assert isinstance(ratio_series, pd.Series)
    assert len(ratio_series) == len(base_features)
    assert (ratio_series >= 0).all()
    assert (ratio_series <= 10).all()


def test_create_price_spread_features():
    """Tests the creation of price spread features between different price products"""
    # Arrange
    base_features = create_mock_base_features(start_time=datetime.now())
    base_features['dalmp'] = np.random.rand(len(base_features)) * 50
    base_features['rtlmp'] = np.random.rand(len(base_features)) * 60
    base_features['regup'] = np.random.rand(len(base_features)) * 10
    base_features['regdown'] = np.random.rand(len(base_features)) * 8
    base_features['rrs'] = np.random.rand(len(base_features)) * 5
    base_features['nsrs'] = np.random.rand(len(base_features)) * 3
    base_features.columns = [col.lower() for col in base_features.columns]

    # Act
    spread_df = create_price_spread_features(base_features)

    # Assert
    assert isinstance(spread_df, pd.DataFrame)
    assert 'spread_dalmp_rtlmp' in spread_df.columns
    assert 'spread_dalmp_regup' in spread_df.columns
    assert 'spread_dalmp_regdown' in spread_df.columns
    assert 'spread_dalmp_rrs' in spread_df.columns
    assert 'spread_dalmp_nsrs' in spread_df.columns
    assert 'spread_regup_regdown' in spread_df.columns
    assert len(spread_df) == len(base_features)


def test_create_volatility_features():
    """Tests the creation of volatility features based on historical price variations"""
    # Arrange
    base_features = create_mock_base_features(start_time=datetime.now())
    base_features['dalmp'] = np.random.rand(len(base_features)) * 50
    base_features.columns = [col.lower() for col in base_features.columns]

    # Act
    volatility_df = create_volatility_features(base_features, SAMPLE_WINDOWS)

    # Assert
    assert isinstance(volatility_df, pd.DataFrame)
    assert 'volatility_dalmp_24h' in volatility_df.columns
    assert 'volatility_dalmp_48h' in volatility_df.columns
    assert 'volatility_dalmp_168h' in volatility_df.columns
    assert len(volatility_df) == len(base_features)


def test_create_renewable_impact_features():
    """Tests the creation of features that capture the impact of renewable generation on prices"""
    # Arrange
    base_features = create_mock_base_features(start_time=datetime.now())
    base_features['total_generation'] = base_features['wind_generation'] + base_features['solar_generation']

    # Act
    renewable_df = create_renewable_impact_features(base_features)

    # Assert
    assert isinstance(renewable_df, pd.DataFrame)
    assert 'total_renewable' in renewable_df.columns
    assert 'renewable_ratio' in renewable_df.columns
    assert 'renewable_to_load' in renewable_df.columns
    assert 'solar_hour_impact' in renewable_df.columns
    assert 'wind_hour_impact' in renewable_df.columns
    assert len(renewable_df) == len(base_features)


def test_create_temporal_interaction_features():
    """Tests the creation of interaction features between temporal and other features"""
    # Arrange
    base_features = create_mock_base_features(start_time=datetime.now())

    # Act
    interaction_df = create_temporal_interaction_features(base_features, SAMPLE_FEATURES_TO_INTERACT)

    # Assert
    assert isinstance(interaction_df, pd.DataFrame)
    assert 'hour_x_load_mw' in interaction_df.columns
    assert 'weekend_x_load_mw' in interaction_df.columns
    assert 'day_of_week_x_load_mw' in interaction_df.columns
    assert len(interaction_df) == len(base_features)


def test_create_derived_features_function():
    """Tests the main create_derived_features function that creates all derived features"""
    # Arrange
    base_features = create_mock_base_features(start_time=datetime.now())
    base_features['total_generation'] = base_features['wind_generation'] + base_features['solar_generation']
    base_features['dalmp'] = np.random.rand(len(base_features)) * 50
    base_features.columns = [col.lower() for col in base_features.columns]

    # Act
    derived_df = create_derived_features(base_features)

    # Assert
    assert isinstance(derived_df, pd.DataFrame)
    assert len(derived_df.columns) > len(base_features.columns)
    assert 'load_generation_ratio' in derived_df.columns
    assert 'spread_dalmp_rtlmp' in derived_df.columns
    assert 'volatility_dalmp_24h' in derived_df.columns
    assert 'total_renewable' in derived_df.columns
    assert 'hour_x_load_mw' in derived_df.columns
    assert len(derived_df) == len(base_features)


def test_create_derived_features_with_missing_features():
    """Tests that create_derived_features raises DerivedFeatureError when required features are missing"""
    # Arrange
    incomplete_data = create_incomplete_feature_data(start_time=datetime.now())

    # Act & Assert
    with pytest.raises(DerivedFeatureError) as exc_info:
        create_derived_features(incomplete_data)
    assert "Error in derived feature creation" in str(exc_info.value)


class TestDerivedFeatureCreator:
    """Test class for the DerivedFeatureCreator class"""

    def test_constructor(self):
        """Tests that the DerivedFeatureCreator constructor initializes properly"""
        # Arrange
        base_features = create_mock_base_features(start_time=datetime.now())

        # Act
        creator = DerivedFeatureCreator(base_features)

        # Assert
        assert isinstance(creator, DerivedFeatureCreator)
        assert creator._base_features_df is base_features

    def test_create_features(self):
        """Tests the create_features method of DerivedFeatureCreator"""
        # Arrange
        base_features = create_mock_base_features(start_time=datetime.now())
        base_features['total_generation'] = base_features['wind_generation'] + base_features['solar_generation']
        base_features['dalmp'] = np.random.rand(len(base_features)) * 50
        base_features.columns = [col.lower() for col in base_features.columns]
        creator = DerivedFeatureCreator(base_features)

        # Act
        derived_df = creator.create_features()

        # Assert
        assert isinstance(derived_df, pd.DataFrame)
        assert len(derived_df.columns) > 0
        assert 'load_generation_ratio' in derived_df.columns
        assert 'spread_dalmp_rtlmp' in derived_df.columns
        assert 'volatility_dalmp_24h' in derived_df.columns
        assert 'total_renewable' in derived_df.columns
        assert 'hour_x_load_mw' in derived_df.columns
        assert len(derived_df) == len(base_features)

    def test_get_feature_dataframe(self):
        """Tests the get_feature_dataframe method of DerivedFeatureCreator"""
        # Arrange
        base_features = create_mock_base_features(start_time=datetime.now())
        base_features['total_generation'] = base_features['wind_generation'] + base_features['solar_generation']
        base_features['dalmp'] = np.random.rand(len(base_features)) * 50
        base_features.columns = [col.lower() for col in base_features.columns]
        creator = DerivedFeatureCreator(base_features)
        creator.create_features()

        # Act
        feature_df = creator.get_feature_dataframe()

        # Assert
        assert isinstance(feature_df, pd.DataFrame)
        assert len(feature_df.columns) > 0
        assert len(feature_df) == len(base_features)

    def test_get_combined_features(self):
        """Tests the get_combined_features method of DerivedFeatureCreator"""
        # Arrange
        base_features = create_mock_base_features(start_time=datetime.now())
        base_features['total_generation'] = base_features['wind_generation'] + base_features['solar_generation']
        base_features['dalmp'] = np.random.rand(len(base_features)) * 50
        base_features.columns = [col.lower() for col in base_features.columns]
        creator = DerivedFeatureCreator(base_features)

        # Act
        combined_df = creator.get_combined_features()

        # Assert
        assert isinstance(combined_df, pd.DataFrame)
        assert len(combined_df.columns) > len(base_features.columns)
        assert 'load_generation_ratio' in combined_df.columns
        assert 'load_mw' in combined_df.columns
        assert 'spread_dalmp_rtlmp' in combined_df.columns
        assert len(combined_df) == len(base_features)
        assert 'timestamp' in combined_df.columns

    def test_error_handling(self):
        """Tests that DerivedFeatureCreator properly handles errors"""
        # Arrange
        incomplete_data = create_incomplete_feature_data(start_time=datetime.now())
        creator = DerivedFeatureCreator(incomplete_data)

        # Act & Assert
        with pytest.raises(DerivedFeatureError) as exc_info:
            creator.create_features()
        assert "Error in derived feature creation" in str(exc_info.value)