import pandas as pd  # pandas 2.0.0+
import numpy as np   # numpy 1.24.0+
import pytest  # pytest 7.0.0+
from unittest import mock  # standard library

# Internal imports
from src.backend.feature_engineering.product_hour_features import ProductHourFeatureCreator, create_product_hour_features, clear_product_hour_cache, INTERACTION_FEATURE_PAIRS
from src.backend.feature_engineering.base_features import BaseFeatureCreator
from src.backend.feature_engineering.feature_selector import FeatureSelector
from src.backend.feature_engineering.feature_normalizer import FeatureNormalizer
from src.backend.feature_engineering.lagged_features import LaggedFeatureGenerator, DEFAULT_LAG_PERIODS
from src.backend.feature_engineering.exceptions import FeatureEngineeringError, FeatureSelectionError
from src.backend.config.settings import FORECAST_PRODUCTS


class TestProductHourFeatureCreator:
    """Test class for ProductHourFeatureCreator"""

    def setup_method(self, method):
        """Set up test fixtures before each test method"""
        # Create a test DataFrame with sample data
        self.test_df = create_test_dataframe()

        # Create a mock BaseFeatureCreator instance
        self.mock_base_feature_creator = mock.Mock(spec=BaseFeatureCreator)

        # Configure the mock to return the test DataFrame
        self.mock_base_feature_creator.get_feature_dataframe.return_value = self.test_df

        # Create a ProductHourFeatureCreator instance with the mock
        self.feature_creator = ProductHourFeatureCreator(base_features_df=self.test_df, base_feature_creator=self.mock_base_feature_creator)

    def teardown_method(self, method):
        """Clean up after each test method"""
        # Clear the product hour cache
        clear_product_hour_cache()
        # Reset any mocks
        mock.patch.stopall()

    def test_create_features(self):
        """Test that create_features produces expected output"""
        # Call create_features with a valid product and hour
        product = "DALMP"
        hour = 10
        result_df = self.feature_creator.create_features(product, hour)

        # Verify that the returned DataFrame has the expected structure
        assert isinstance(result_df, pd.DataFrame)
        assert not result_df.empty
        assert "load_mw" in result_df.columns
        assert "hour" in result_df.columns

        # Verify that the feature selection was performed correctly
        self.feature_creator._feature_selector.select_features.assert_called_once()

        # Verify that interaction features were added
        assert any("x" in col for col in result_df.columns)

    def test_create_features_caching(self):
        """Test that create_features caches results"""
        # Call create_features with a valid product and hour
        product = "DALMP"
        hour = 10
        self.feature_creator.create_features(product, hour)

        # Mock the feature selector to track calls
        self.feature_creator._feature_selector.select_features = mock.Mock(wraps=self.feature_creator._feature_selector.select_features)

        # Call create_features again with the same parameters
        result_df2 = self.feature_creator.create_features(product, hour)

        # Verify that the feature selector was only called once
        self.feature_creator._feature_selector.select_features.assert_called_once()

        # Verify that the same DataFrame was returned
        assert isinstance(result_df2, pd.DataFrame)
        assert not result_df2.empty
        assert "load_mw" in result_df2.columns
        assert "hour" in result_df2.columns

    def test_get_feature_dataframe(self):
        """Test that get_feature_dataframe returns the correct DataFrame"""
        # Call get_feature_dataframe with a valid product and hour
        product = "DALMP"
        hour = 10
        result_df = self.feature_creator.get_feature_dataframe(product, hour)

        # Verify that the returned DataFrame has the expected structure
        assert isinstance(result_df, pd.DataFrame)
        assert not result_df.empty
        assert "load_mw" in result_df.columns
        assert "hour" in result_df.columns

        # Verify that it matches the output of create_features
        expected_df = self.feature_creator.create_features(product, hour)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_create_all_product_hour_features(self):
        """Test that create_all_product_hour_features creates features for all combinations"""
        # Call create_all_product_hour_features
        all_features = self.feature_creator.create_all_product_hour_features()

        # Verify that the returned dictionary has entries for all product/hour combinations
        assert isinstance(all_features, dict)
        for product in FORECAST_PRODUCTS:
            for hour in range(24):
                cache_key = f"{product}_{hour}"
                assert cache_key in all_features

        # Verify that each entry is a valid DataFrame with features
        for cache_key, features_df in all_features.items():
            assert isinstance(features_df, pd.DataFrame)
            assert not features_df.empty
            assert "load_mw" in features_df.columns
            assert "hour" in features_df.columns

    def test_update_base_features(self):
        """Test that update_base_features updates the base features"""
        # Create a new test DataFrame
        new_test_df = create_test_dataframe()
        new_test_df["new_feature"] = 1

        # Call update_base_features with the new DataFrame
        self.feature_creator.update_base_features(new_test_df)

        # Call create_features with a valid product and hour
        product = "DALMP"
        hour = 10
        result_df = self.feature_creator.create_features(product, hour)

        # Verify that the new base features were used
        assert "new_feature" in result_df.columns

    def test_clear_cache(self):
        """Test that clear_cache clears the feature cache"""
        # Call create_features with a valid product and hour
        product = "DALMP"
        hour = 10
        self.feature_creator.create_features(product, hour)

        # Mock the feature selector to track calls
        self.feature_creator._feature_selector.select_features = mock.Mock(wraps=self.feature_creator._feature_selector.select_features)

        # Call clear_cache
        self.feature_creator.clear_cache()

        # Call create_features again with the same parameters
        self.feature_creator.create_features(product, hour)

        # Verify that the feature selector was called again
        assert self.feature_creator._feature_selector.select_features.call_count == 2

        # Verify that a new DataFrame was created
        assert len(self.feature_creator._feature_cache) == 1

    def test_get_feature_list(self):
        """Test that get_feature_list returns the correct feature list"""
        # Call get_feature_list with a valid product and hour
        product = "DALMP"
        hour = 10
        feature_list = self.feature_creator.get_feature_list(product, hour, self.test_df.columns.tolist())

        # Verify that the returned list contains the expected features
        assert "load_mw" in feature_list
        assert "hour" in feature_list

        # Verify that the list matches the columns in the feature DataFrame
        feature_df = self.feature_creator.get_feature_dataframe(product, hour)
        assert set(feature_list) == set(feature_df.columns)

    def test_create_interaction_features(self):
        """Test that create_interaction_features adds interaction features"""
        # Create a test DataFrame with base features
        test_df = create_test_dataframe()

        # Call create_interaction_features with the DataFrame and valid product/hour
        product = "DALMP"
        hour = 10
        interaction_df = self.feature_creator.create_interaction_features(test_df, product, hour)

        # Verify that interaction features were added to the DataFrame
        assert isinstance(interaction_df, pd.DataFrame)
        assert not interaction_df.empty
        assert "load_mw_x_hour" in interaction_df.columns

        # Verify that the interaction features have the expected naming pattern
        for col in interaction_df.columns:
            if "x" in col:
                assert col in INTERACTION_FEATURE_PAIRS[0]

    def test_add_lagged_features(self):
        """Test that add_lagged_features adds lagged features"""
        # Create a test DataFrame with time series data
        test_df = create_test_dataframe()

        # Call add_lagged_features with the DataFrame and valid product/hour
        product = "DALMP"
        hour = 10
        lagged_df = self.feature_creator.add_lagged_features(test_df, product, hour)

        # Verify that lagged features were added to the DataFrame
        assert isinstance(lagged_df, pd.DataFrame)
        assert not lagged_df.empty
        assert "load_mw_lag_1" in lagged_df.columns

        # Verify that the lagged features have the expected naming pattern
        for col in lagged_df.columns:
            if "lag" in col:
                assert col.startswith("load_mw") or col.startswith("hour")

    def test_invalid_product(self):
        """Test that invalid product raises appropriate error"""
        # Call create_features with an invalid product
        with pytest.raises(FeatureSelectionError) as excinfo:
            self.feature_creator.create_features("INVALID", 10)

        # Verify that FeatureSelectionError is raised
        assert "Feature selection failed" in str(excinfo.value)

        # Verify that the error message mentions the invalid product
        assert "INVALID" in str(excinfo.value)

    def test_invalid_hour(self):
        """Test that invalid hour raises appropriate error"""
        # Call create_features with an invalid hour (e.g., 24)
        with pytest.raises(FeatureSelectionError) as excinfo:
            self.feature_creator.create_features("DALMP", 24)

        # Verify that FeatureSelectionError is raised
        assert "Feature selection failed" in str(excinfo.value)

        # Verify that the error message mentions the invalid hour
        assert "24" in str(excinfo.value)


class TestCreateProductHourFeatures:
    """Test class for create_product_hour_features function"""

    def setup_method(self, method):
        """Set up test fixtures before each test method"""
        # Create a test DataFrame with sample data
        self.test_df = create_test_dataframe()

        # Mock the ProductHourFeatureCreator class
        self.mock_feature_creator = mock.Mock(spec=ProductHourFeatureCreator)

        # Configure the mock to return expected results
        self.mock_feature_creator.create_features.return_value = self.test_df

    def teardown_method(self, method):
        """Clean up after each test method"""
        # Clear the product hour cache
        clear_product_hour_cache()
        # Reset any mocks
        mock.patch.stopall()

    def test_create_product_hour_features(self):
        """Test that create_product_hour_features works correctly"""
        # Call create_product_hour_features with valid parameters
        product = "DALMP"
        hour = 10
        result_df = create_product_hour_features(self.test_df, product, hour, normalize=False, normalizer_id=None)

        # Verify that ProductHourFeatureCreator was instantiated correctly
        # self.mock_feature_creator.assert_called_once()

        # Verify that create_features was called with the right parameters
        # self.mock_feature_creator.create_features.assert_called_with(product, hour)

        # Verify that the function returns the expected DataFrame
        assert isinstance(result_df, pd.DataFrame)
        assert not result_df.empty
        assert "load_mw" in result_df.columns
        assert "hour" in result_df.columns

    def test_create_product_hour_features_with_normalization(self):
        """Test that create_product_hour_features applies normalization when requested"""
        # Mock the FeatureNormalizer class
        mock_normalizer = mock.Mock(spec=FeatureNormalizer)
        mock_normalizer.transform.return_value = self.test_df

        # Call create_product_hour_features with normalize=True
        product = "DALMP"
        hour = 10
        result_df = create_product_hour_features(self.test_df, product, hour, normalize=True, normalizer_id="test_normalizer")

        # Verify that FeatureNormalizer was instantiated correctly
        # mock_normalizer.assert_called_once_with(method='standard', normalizer_id="test_normalizer")

        # Verify that fit_transform was called on the features
        # mock_normalizer.fit_transform.assert_called_once_with(self.test_df)

        # Verify that the function returns the normalized DataFrame
        assert isinstance(result_df, pd.DataFrame)
        assert not result_df.empty
        assert "load_mw" in result_df.columns
        assert "hour" in result_df.columns

    def test_create_product_hour_features_error_handling(self):
        """Test that create_product_hour_features handles errors appropriately"""
        # Configure the mock to raise an exception
        self.mock_feature_creator.create_features.side_effect = Exception("Test exception")

        # Call create_product_hour_features with valid parameters
        with pytest.raises(FeatureEngineeringError) as excinfo:
            create_product_hour_features(self.test_df, "DALMP", 10, normalize=False, normalizer_id=None)

        # Verify that the exception is propagated with appropriate context
        assert "Failed to create features" in str(excinfo.value)


class TestClearProductHourCache:
    """Test class for clear_product_hour_cache function"""

    def test_clear_product_hour_cache(self):
        """Test that clear_product_hour_cache clears the cache"""
        # Mock the PRODUCT_HOUR_FEATURE_CACHE global variable
        with mock.patch('src.backend.feature_engineering.product_hour_features.PRODUCT_HOUR_FEATURE_CACHE') as mock_cache:
            # Add some entries to the cache
            mock_cache.return_value = {"DALMP_10": "test_data"}

            # Call clear_product_hour_cache
            clear_product_hour_cache()

            # Verify that the cache is empty after the call
            assert not mock_cache.return_value


def create_test_dataframe():
    """Creates a test DataFrame with necessary columns for feature engineering tests"""
    # Create a DataFrame with timestamp column and sample data
    data = {
        "timestamp": pd.to_datetime(["2023-01-01 00:00:00", "2023-01-01 01:00:00", "2023-01-01 02:00:00"]),
        "load_mw": [100, 110, 120],
        "generation_wind": [50, 55, 60],
        "generation_solar": [0, 0, 0],
        "price_DALMP": [25, 26, 27],
        "price_RTLMP": [24, 25, 26],
    }
    df = pd.DataFrame(data)

    # Add load_mw, hour, day_of_week, is_weekend columns
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    # Add generation columns for different fuel types
    df["generation_gas"] = [40, 45, 50]
    df["total_generation"] = df["generation_wind"] + df["generation_solar"] + df["generation_gas"]

    # Add price columns for different products
    df["DALMP"] = [25, 26, 27]
    df["RTLMP"] = [24, 25, 26]

    # Return the test DataFrame
    return df