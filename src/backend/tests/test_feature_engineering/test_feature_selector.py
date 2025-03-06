# src/backend/tests/test_feature_engineering/test_feature_selector.py
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
import datetime  # standard library
import pytest  # pytest: 7.0.0+

from src.backend.feature_engineering.feature_selector import FeatureSelector  # Class under test for feature selection
from src.backend.feature_engineering.feature_selector import select_features_by_product_hour  # Function under test for feature selection
from src.backend.feature_engineering.feature_selector import get_hour_category  # Function under test for determining hour categories
from src.backend.feature_engineering.feature_selector import get_feature_list_for_product_hour  # Function under test for getting feature lists
from src.backend.feature_engineering.feature_selector import validate_features_exist  # Function under test for feature validation
from src.backend.feature_engineering.feature_selector import BASE_FEATURES  # List of base features for testing
from src.backend.feature_engineering.feature_selector import PRODUCT_SPECIFIC_FEATURES  # Dictionary of product-specific features for testing
from src.backend.feature_engineering.feature_selector import HOUR_SPECIFIC_FEATURES  # Dictionary of hour-specific features for testing
from src.backend.feature_engineering.exceptions import FeatureSelectionError  # Exception for feature selection failures
from src.backend.feature_engineering.exceptions import MissingFeatureError  # Exception for missing required features
from src.backend.tests.fixtures.feature_fixtures import create_mock_feature_data  # Create mock feature data for tests
from src.backend.tests.fixtures.feature_fixtures import create_incomplete_feature_data  # Create incomplete feature data for testing validation
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid price products for validation


@pytest.fixture
def feature_data():
    """Fixture to provide mock feature data for tests."""
    return create_mock_feature_data(datetime.datetime.now())


@pytest.fixture
def feature_selector():
    """Fixture to provide a FeatureSelector instance."""
    selector = FeatureSelector()
    yield selector
    selector.clear_cache()


def test_get_hour_category():
    """Tests the get_hour_category function for different hours"""
    # Test peak hours (7-22)
    assert "peak_hours" in get_hour_category(7)
    assert "peak_hours" in get_hour_category(12)
    assert "peak_hours" in get_hour_category(22)
    assert "peak_hours" not in get_hour_category(6)
    assert "peak_hours" not in get_hour_category(23)

    # Test off-peak hours (0-6, 23)
    assert "off_peak_hours" in get_hour_category(0)
    assert "off_peak_hours" in get_hour_category(6)
    assert "off_peak_hours" in get_hour_category(23)
    assert "off_peak_hours" not in get_hour_category(7)
    assert "off_peak_hours" not in get_hour_category(12)

    # Test solar hours (8-17)
    assert "solar_hours" in get_hour_category(8)
    assert "solar_hours" in get_hour_category(12)
    assert "solar_hours" in get_hour_category(17)
    assert "solar_hours" not in get_hour_category(7)
    assert "solar_hours" not in get_hour_category(18)

    # Test evening ramp hours (16-20)
    assert "evening_ramp" in get_hour_category(16)
    assert "evening_ramp" in get_hour_category(18)
    assert "evening_ramp" in get_hour_category(20)
    assert "evening_ramp" not in get_hour_category(15)
    assert "evening_ramp" not in get_hour_category(21)

    # Verify that hours can belong to multiple categories
    hour_16_categories = get_hour_category(16)
    assert "peak_hours" in hour_16_categories
    assert "solar_hours" in hour_16_categories
    assert "evening_ramp" in hour_16_categories


def test_get_feature_list_for_product_hour(feature_data):
    """Tests the get_feature_list_for_product_hour function"""
    available_features = feature_data.columns.tolist()

    # Test with DALMP product at noon (hour 12)
    dalmp_noon_features = get_feature_list_for_product_hour("DALMP", 12, available_features)
    assert "timestamp" in dalmp_noon_features
    assert "load_mw" in dalmp_noon_features
    assert "day_ahead_demand_forecast" in dalmp_noon_features
    assert "solar_generation" in dalmp_noon_features

    # Verify that base features are included
    for feature in BASE_FEATURES:
        assert feature in dalmp_noon_features

    # Verify that DALMP-specific features are included
    for feature in PRODUCT_SPECIFIC_FEATURES["DALMP"]:
        assert feature in dalmp_noon_features

    # Verify that solar hour features are included (hour 12 is in solar hours)
    for feature in HOUR_SPECIFIC_FEATURES["solar_hours"]:
        assert feature in dalmp_noon_features

    # Test with RegUp product at midnight (hour 0)
    regup_midnight_features = get_feature_list_for_product_hour("RegUp", 0, available_features)
    assert "timestamp" in regup_midnight_features
    assert "load_mw" in regup_midnight_features
    assert "regulation_requirement" in regup_midnight_features
    assert "off_peak_load_ratio" in regup_midnight_features

    # Verify that RegUp-specific features are included
    for feature in PRODUCT_SPECIFIC_FEATURES["RegUp"]:
        assert feature in regup_midnight_features

    # Verify that off-peak hour features are included (hour 0 is in off-peak hours)
    for feature in HOUR_SPECIFIC_FEATURES["off_peak_hours"]:
        assert feature in regup_midnight_features

    # Test with available_features parameter to filter features
    filtered_features = get_feature_list_for_product_hour("DALMP", 12, ["timestamp", "load_mw"])
    assert "timestamp" in filtered_features
    assert "load_mw" in filtered_features
    assert "day_ahead_demand_forecast" not in filtered_features


def test_validate_features_exist(feature_data):
    """Tests the validate_features_exist function"""
    # Create mock feature data with all required features
    required_features = ["timestamp", "load_mw", "hour"]
    mock_df = feature_data[required_features]

    # Verify that validation passes when all features exist
    assert validate_features_exist(mock_df, required_features)

    # Create incomplete feature data missing some required features
    incomplete_df = create_incomplete_feature_data(feature_data, ["load_mw"])
    missing_features = ["load_mw"]

    # Verify that MissingFeatureError is raised when features are missing
    with pytest.raises(MissingFeatureError) as excinfo:
        validate_features_exist(incomplete_df, required_features)
    assert "Missing required features" in str(excinfo.value)
    assert missing_features == excinfo.value.missing_features


def test_select_features_by_product_hour(feature_data):
    """Tests the select_features_by_product_hour function"""
    # Create mock feature data
    mock_df = feature_data

    # Test with valid product and hour
    selected_df = select_features_by_product_hour(mock_df, "DALMP", 12)
    assert isinstance(selected_df, pandas.DataFrame)
    assert "timestamp" in selected_df.columns
    assert "load_mw" in selected_df.columns

    # Verify that returned DataFrame contains expected features
    expected_features = BASE_FEATURES + PRODUCT_SPECIFIC_FEATURES["DALMP"] + HOUR_SPECIFIC_FEATURES["solar_hours"]
    for feature in expected_features:
        if feature in mock_df.columns:
            assert feature in selected_df.columns

    # Test with invalid product (not in FORECAST_PRODUCTS)
    with pytest.raises(ValueError) as excinfo:
        select_features_by_product_hour(mock_df, "InvalidProduct", 12)
    assert "Invalid product" in str(excinfo.value)

    # Verify that ValueError is raised for invalid product
    with pytest.raises(ValueError) as excinfo:
        select_features_by_product_hour(mock_df, "DALMP", 24)
    assert "Invalid hour" in str(excinfo.value)

    # Test with incomplete feature data
    incomplete_df = create_incomplete_feature_data(feature_data, ["load_mw"])
    with pytest.raises(MissingFeatureError) as excinfo:
        select_features_by_product_hour(incomplete_df, "DALMP", 12)
    assert "Missing required features" in str(excinfo.value)


def test_feature_selector_select_features(feature_selector, feature_data):
    """Tests the FeatureSelector.select_features method"""
    # Test with valid product and hour
    selected_df = feature_selector.select_features(feature_data, "DALMP", 12)
    assert isinstance(selected_df, pandas.DataFrame)
    assert "timestamp" in selected_df.columns
    assert "load_mw" in selected_df.columns

    # Verify that returned DataFrame contains expected features
    expected_features = BASE_FEATURES + PRODUCT_SPECIFIC_FEATURES["DALMP"] + HOUR_SPECIFIC_FEATURES["solar_hours"]
    for feature in expected_features:
        if feature in feature_data.columns:
            assert feature in selected_df.columns

    # Test with same product and hour again to verify caching
    selected_df_cached = feature_selector.select_features(feature_data, "DALMP", 12)
    assert selected_df.equals(selected_df_cached)

    # Verify that cached result is returned
    assert selected_df is not selected_df_cached

    # Test with different product and hour
    selected_df_different = feature_selector.select_features(feature_data, "RTLMP", 6)
    assert not selected_df.equals(selected_df_different)

    # Verify that different features are selected
    expected_features_different = BASE_FEATURES + PRODUCT_SPECIFIC_FEATURES["RTLMP"]
    for feature in expected_features_different:
        if feature in feature_data.columns:
            assert feature in selected_df_different.columns

    # Test with invalid product
    with pytest.raises(FeatureSelectionError) as excinfo:
        feature_selector.select_features(feature_data, "InvalidProduct", 12)
    assert "Invalid product" in str(excinfo.value)

    # Test with invalid hour
    with pytest.raises(FeatureSelectionError) as excinfo:
        feature_selector.select_features(feature_data, "DALMP", 24)
    assert "Invalid hour" in str(excinfo.value)


def test_feature_selector_get_feature_list(feature_selector, feature_data):
    """Tests the FeatureSelector.get_feature_list method"""
    # Get list of available features from feature_data
    available_features = feature_data.columns.tolist()

    # Test with DALMP product at noon (hour 12)
    dalmp_noon_features = feature_selector.get_feature_list("DALMP", 12, available_features)
    assert "timestamp" in dalmp_noon_features
    assert "load_mw" in dalmp_noon_features
    assert "day_ahead_demand_forecast" in dalmp_noon_features
    assert "solar_generation" in dalmp_noon_features

    # Verify that returned list contains expected features
    expected_features = BASE_FEATURES + PRODUCT_SPECIFIC_FEATURES["DALMP"] + HOUR_SPECIFIC_FEATURES["solar_hours"]
    for feature in expected_features:
        if feature in feature_data.columns:
            assert feature in dalmp_noon_features

    # Test with same product and hour again to verify caching
    dalmp_noon_features_cached = feature_selector.get_feature_list("DALMP", 12, available_features)
    assert dalmp_noon_features == dalmp_noon_features_cached

    # Verify that cached result is returned
    assert dalmp_noon_features is not dalmp_noon_features_cached

    # Test with different product and hour
    rtlmp_six_features = feature_selector.get_feature_list("RTLMP", 6, available_features)
    assert dalmp_noon_features != rtlmp_six_features

    # Verify that different features are returned
    expected_features_rtlmp = BASE_FEATURES + PRODUCT_SPECIFIC_FEATURES["RTLMP"]
    for feature in expected_features_rtlmp:
        if feature in feature_data.columns:
            assert feature in rtlmp_six_features

    # Test with filtered available_features
    filtered_features = feature_selector.get_feature_list("DALMP", 12, ["timestamp", "load_mw"])
    assert "timestamp" in filtered_features
    assert "load_mw" in filtered_features
    assert "day_ahead_demand_forecast" not in filtered_features


def test_feature_selector_clear_cache(feature_selector, feature_data):
    """Tests the FeatureSelector.clear_cache method"""
    # Select features for a product/hour combination to populate cache
    feature_selector.select_features(feature_data, "DALMP", 12)
    cache_key = "DALMP_12"
    assert cache_key in feature_selector._feature_cache

    # Verify that result is cached by selecting again
    feature_selector.select_features(feature_data, "DALMP", 12)
    assert cache_key in feature_selector._feature_cache

    # Clear the cache
    feature_selector.clear_cache()
    assert cache_key not in feature_selector._feature_cache

    # Verify that cache is cleared by checking that selection is recomputed
    feature_selector.select_features(feature_data, "DALMP", 12)
    assert cache_key in feature_selector._feature_cache


def test_feature_selector_update_feature_dataframe(feature_selector, feature_data):
    """Tests the FeatureSelector.update_feature_dataframe method"""
    # Create new features to add
    new_features = {"new_feature_1": numpy.random.rand(len(feature_data))}

    # Update the feature DataFrame
    updated_df = feature_selector.update_feature_dataframe(feature_data, new_features)

    # Verify that new features are added to the DataFrame
    assert "new_feature_1" in updated_df.columns

    # Verify that original DataFrame is not modified (copy is returned)
    assert "new_feature_1" not in feature_data.columns

    # Verify that cache is cleared by checking that selection is recomputed
    feature_selector.select_features(feature_data, "DALMP", 12)
    cache_key = "DALMP_12"
    assert cache_key in feature_selector._feature_cache
    feature_selector.update_feature_dataframe(feature_data, new_features)
    assert cache_key not in feature_selector._feature_cache


def test_feature_selector_add_interaction_features(feature_selector, feature_data):
    """Tests the FeatureSelector.add_interaction_features method"""
    # Define feature pairs for interaction
    feature_pairs = [("load_mw", "hour")]

    # Add interaction features to the DataFrame
    interaction_df = feature_selector.add_interaction_features(feature_data, feature_pairs)

    # Verify that interaction features are added with correct naming pattern
    assert "load_mw_x_hour" in interaction_df.columns

    # Verify that interaction values are correct (product of source features)
    assert numpy.allclose(interaction_df["load_mw_x_hour"], feature_data["load_mw"] * feature_data["hour"])

    # Verify that original DataFrame is not modified (copy is returned)
    assert "load_mw_x_hour" not in feature_data.columns

    # Verify that cache is cleared by checking that selection is recomputed
    feature_selector.select_features(feature_data, "DALMP", 12)
    cache_key = "DALMP_12"
    assert cache_key in feature_selector._feature_cache
    feature_selector.add_interaction_features(feature_data, feature_pairs)
    assert cache_key not in feature_selector._feature_cache


def test_error_handling(feature_selector):
    """Tests error handling in feature selection"""
    # Test with None as feature DataFrame
    with pytest.raises(TypeError):
        feature_selector.select_features(None, "DALMP", 12)

    # Test with empty DataFrame
    with pytest.raises(MissingFeatureError):
        feature_selector.select_features(pandas.DataFrame(), "DALMP", 12)

    # Test with incomplete feature data
    incomplete_df = create_incomplete_feature_data(create_mock_feature_data(datetime.datetime.now()), ["load_mw"])
    with pytest.raises(MissingFeatureError) as excinfo:
        feature_selector.select_features(incomplete_df, "DALMP", 12)
    assert "Missing required features" in str(excinfo.value)


@pytest.mark.parametrize('product,hour', [
    ('DALMP', 0),
    ('DALMP', 12),
    ('RTLMP', 6),
    ('RTLMP', 18),
    ('RegUp', 3),
    ('RegUp', 15),
    ('RegDown', 9),
    ('RegDown', 21),
    ('RRS', 7),
    ('NSRS', 22)
])
def test_parametrized_product_hour_combinations(product, hour, feature_selector, feature_data):
    """Tests feature selection with various product/hour combinations"""
    # Select features for the given product/hour combination
    selected_df = feature_selector.select_features(feature_data, product, hour)

    # Verify that selection contains expected base features
    for feature in BASE_FEATURES:
        if feature in feature_data.columns:
            assert feature in selected_df.columns

    # Verify that selection contains expected product-specific features
    if product in PRODUCT_SPECIFIC_FEATURES:
        for feature in PRODUCT_SPECIFIC_FEATURES[product]:
            if feature in feature_data.columns:
                assert feature in selected_df.columns

    # Verify that selection contains expected hour-specific features
    for category in get_hour_category(hour):
        if category in HOUR_SPECIFIC_FEATURES:
            for feature in HOUR_SPECIFIC_FEATURES[category]:
                if feature in feature_data.columns:
                    assert feature in selected_df.columns

    # Verify that selection does not contain irrelevant features
    irrelevant_features = [
        "some_other_feature",
        "another_irrelevant_feature"
    ]
    for feature in irrelevant_features:
        assert feature not in selected_df.columns