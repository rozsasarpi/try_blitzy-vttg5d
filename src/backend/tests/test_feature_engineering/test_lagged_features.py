# src/backend/tests/test_feature_engineering/test_lagged_features.py
"""Unit tests for the lagged features module in the feature engineering component
of the Electricity Market Price Forecasting System. Tests the functionality to
create time-lagged variables from historical data, which are essential for
capturing temporal patterns in electricity prices.
"""

import pytest  # pytest: 7.0.0+
import pandas as pd  # pandas 2.0.0+
import numpy  # numpy 1.24.0+

from src.backend.feature_engineering.lagged_features import LaggedFeatureGenerator  # Class for generating lagged features from time series data
from src.backend.feature_engineering.lagged_features import generate_lagged_features  # Function to create lagged features for any columns
from src.backend.feature_engineering.lagged_features import DEFAULT_LAG_PERIODS  # Default lag periods to use for feature generation
from src.backend.feature_engineering.lagged_features import create_lag_column_name  # Function to create standardized column names for lagged features
from src.backend.feature_engineering.lagged_features import get_lag_feature_names  # Function to get all lag feature column names
from src.backend.feature_engineering.exceptions import LaggedFeatureError  # Exception for lagged feature creation failures
from src.backend.tests.fixtures.feature_fixtures import create_mock_feature_data  # Create mock feature data for testing
from src.backend.tests.fixtures.feature_fixtures import create_mock_lagged_features  # Create mock lagged features for testing


def test_create_lag_column_name():
    """Tests the create_lag_column_name function for correct naming format"""
    # Test with a simple column name and lag period
    column_name = "load_mw"
    lag_period = 24
    expected_name = "load_mw_lag_24"
    assert create_lag_column_name(column_name, lag_period) == expected_name, "Verify the format follows '{column_name}_lag_{lag_period}'"

    # Test with different column names and lag periods
    column_name = "price"
    lag_period = 7
    expected_name = "price_lag_7"
    assert create_lag_column_name(column_name, lag_period) == expected_name, "Verify consistent naming convention"


def test_get_lag_feature_names():
    """Tests the get_lag_feature_names function for generating all lag feature names"""
    # Define test columns and lag periods
    columns = ["load_mw", "price"]
    lag_periods = [24, 48]

    # Call get_lag_feature_names with the test data
    lag_feature_names = get_lag_feature_names(columns, lag_periods)

    # Verify all expected lag feature names are generated
    expected_names = ["load_mw_lag_24", "load_mw_lag_48", "price_lag_24", "price_lag_48"]
    assert set(lag_feature_names) == set(expected_names), "Verify all expected lag feature names are generated"

    # Verify the correct number of names is generated (columns × lag_periods)
    assert len(lag_feature_names) == len(columns) * len(lag_periods), "Verify the correct number of names is generated (columns × lag_periods)"

    # Verify each name follows the correct format
    for name in lag_feature_names:
        assert "_lag_" in name, "Verify each name follows the correct format"


def test_lagged_feature_generator_init():
    """Tests the initialization of the LaggedFeatureGenerator class"""
    # Create a sample DataFrame with timestamp column
    data = {'timestamp': pd.to_datetime(['2023-01-01 00:00:00', '2023-01-01 01:00:00', '2023-01-01 02:00:00'])}
    df = pd.DataFrame(data)

    # Initialize LaggedFeatureGenerator with the DataFrame
    generator = LaggedFeatureGenerator(df, timestamp_column='timestamp')

    # Verify the generator stores the DataFrame correctly
    assert generator._df.equals(df), "Verify the generator stores the DataFrame correctly"

    # Verify the timestamp column is set correctly
    assert generator._timestamp_column == 'timestamp', "Verify the timestamp column is set correctly"

    # Verify default lag periods are used when not specified
    assert generator._lag_periods == DEFAULT_LAG_PERIODS, "Verify default lag periods are used when not specified"

    # Initialize with custom lag periods and verify they are set correctly
    custom_lag_periods = [1, 2, 3]
    generator = LaggedFeatureGenerator(df, timestamp_column='timestamp', lag_periods=custom_lag_periods)
    assert generator._lag_periods == custom_lag_periods, "Initialize with custom lag periods and verify they are set correctly"


def test_lagged_feature_generator_add_feature_columns():
    """Tests adding feature columns to the LaggedFeatureGenerator"""
    # Create a sample DataFrame with multiple columns
    data = {'timestamp': pd.to_datetime(['2023-01-01 00:00:00', '2023-01-01 01:00:00']), 'load_mw': [100, 110], 'price': [20, 22]}
    df = pd.DataFrame(data)

    # Initialize LaggedFeatureGenerator with the DataFrame
    generator = LaggedFeatureGenerator(df, timestamp_column='timestamp')

    # Add a subset of columns as feature columns
    generator.add_feature_columns(['load_mw', 'price'])

    # Verify the feature columns are stored correctly
    assert generator._feature_columns == ['load_mw', 'price'], "Verify the feature columns are stored correctly"

    # Add more columns and verify they are appended to the existing list
    generator.add_feature_columns(['new_column'])
    assert generator._feature_columns == ['load_mw', 'price', 'new_column'], "Add more columns and verify they are appended to the existing list"

    # Test adding a non-existent column and verify it raises an error
    with pytest.raises(LaggedFeatureError):
        generator.add_feature_columns(['non_existent_column']), "Test adding a non-existent column and verify it raises an error"


def test_lagged_feature_generator_set_lag_periods():
    """Tests setting custom lag periods in the LaggedFeatureGenerator"""
    # Create a sample DataFrame
    data = {'timestamp': pd.to_datetime(['2023-01-01 00:00:00', '2023-01-01 01:00:00']), 'load_mw': [100, 110]}
    df = pd.DataFrame(data)

    # Initialize LaggedFeatureGenerator with default lag periods
    generator = LaggedFeatureGenerator(df, timestamp_column='timestamp')

    # Set custom lag periods and verify they replace the defaults
    custom_lag_periods = [1, 2, 3]
    generator.set_lag_periods(custom_lag_periods)
    assert generator._lag_periods == custom_lag_periods, "Set custom lag periods and verify they replace the defaults"

    # Test with invalid lag periods (non-integers) and verify it raises an error
    with pytest.raises(LaggedFeatureError):
        generator.set_lag_periods([1.5, 2.5]), "Test with invalid lag periods (non-integers) and verify it raises an error"

    # Test with empty lag periods list and verify it raises an error
    with pytest.raises(LaggedFeatureError):
        generator.set_lag_periods([]), "Test with empty lag periods list and verify it raises an error"


def test_generate_category_lagged_features():
    """Tests generating lagged features for a specific column"""
    # Create a sample DataFrame with time series data
    data = {'timestamp': pd.to_datetime(['2023-01-01 00:00:00', '2023-01-01 01:00:00', '2023-01-01 02:00:00']), 'load_mw': [100, 110, 120]}
    df = pd.DataFrame(data)

    # Initialize LaggedFeatureGenerator with the DataFrame
    generator = LaggedFeatureGenerator(df, timestamp_column='timestamp', lag_periods=[1, 2])

    # Add a specific column for lagging
    generator.add_feature_columns(['load_mw'])

    # Generate lagged features for that column
    generator.generate_category_lagged_features('load_mw')

    # Verify lagged columns are created with correct naming
    assert 'load_mw_lag_1' in generator._df.columns, "Verify lagged columns are created with correct naming"
    assert 'load_mw_lag_2' in generator._df.columns, "Verify lagged columns are created with correct naming"

    # Verify lagged values are correctly shifted from original data
    assert generator._df['load_mw_lag_1'].tolist() == [numpy.nan, 100.0, 110.0], "Verify lagged values are correctly shifted from original data"
    assert generator._df['load_mw_lag_2'].tolist() == [numpy.nan, numpy.nan, 100.0], "Verify lagged values are correctly shifted from original data"

    # Test with a non-existent column and verify it raises an error
    with pytest.raises(LaggedFeatureError):
        generator.generate_category_lagged_features('non_existent_column'), "Test with a non-existent column and verify it raises an error"


def test_generate_all_lagged_features():
    """Tests generating lagged features for all specified columns"""
    # Create a sample DataFrame with multiple columns
    data = {'timestamp': pd.to_datetime(['2023-01-01 00:00:00', '2023-01-01 01:00:00', '2023-01-01 02:00:00']), 'load_mw': [100, 110, 120], 'price': [20, 22, 24]}
    df = pd.DataFrame(data)

    # Initialize LaggedFeatureGenerator with the DataFrame
    generator = LaggedFeatureGenerator(df, timestamp_column='timestamp', lag_periods=[1, 2])

    # Add multiple columns for lagging
    generator.add_feature_columns(['load_mw', 'price'])

    # Generate lagged features for all columns
    result_df = generator.generate_all_lagged_features()

    # Verify lagged columns are created for all specified columns
    assert 'load_mw_lag_1' in result_df.columns, "Verify lagged columns are created for all specified columns"
    assert 'load_mw_lag_2' in result_df.columns, "Verify lagged columns are created for all specified columns"
    assert 'price_lag_1' in result_df.columns, "Verify lagged columns are created for all specified columns"
    assert 'price_lag_2' in result_df.columns, "Verify lagged columns are created for all specified columns"

    # Verify the correct number of new columns is created (columns × lag_periods)
    assert len(result_df.columns) == len(df.columns) + 4, "Verify the correct number of new columns is created (columns × lag_periods)"

    # Test without adding feature columns first and verify it raises an error
    generator = LaggedFeatureGenerator(df, timestamp_column='timestamp', lag_periods=[1, 2])
    with pytest.raises(LaggedFeatureError):
        generator.generate_all_lagged_features(), "Test without adding feature columns first and verify it raises an error"


def test_get_feature_names():
    """Tests getting the names of all generated lagged feature columns"""
    # Create a sample DataFrame
    data = {'timestamp': pd.to_datetime(['2023-01-01 00:00:00', '2023-01-01 01:00:00']), 'load_mw': [100, 110], 'price': [20, 22]}
    df = pd.DataFrame(data)

    # Initialize LaggedFeatureGenerator with the DataFrame
    generator = LaggedFeatureGenerator(df, timestamp_column='timestamp', lag_periods=[1, 2])

    # Add feature columns and set lag periods
    generator.add_feature_columns(['load_mw', 'price'])

    # Call get_feature_names method
    feature_names = generator.get_feature_names()

    # Verify all expected feature names are returned
    expected_names = ['load_mw_lag_1', 'load_mw_lag_2', 'price_lag_1', 'price_lag_2']
    assert set(feature_names) == set(expected_names), "Verify all expected feature names are returned"

    # Verify the correct number of names is returned (columns × lag_periods)
    assert len(feature_names) == 4, "Verify the correct number of names is returned (columns × lag_periods)"

    # Verify each name follows the correct format
    for name in feature_names:
        assert "_lag_" in name, "Verify each name follows the correct format"


def test_generate_lagged_features_function():
    """Tests the generate_lagged_features function for creating lagged features"""
    # Create a sample DataFrame with time series data
    data = {'timestamp': pd.to_datetime(['2023-01-01 00:00:00', '2023-01-01 01:00:00', '2023-01-01 02:00:00']), 'load_mw': [100, 110, 120], 'price': [20, 22, 24]}
    df = pd.DataFrame(data)

    # Call generate_lagged_features with specific columns and lag periods
    result_df = generate_lagged_features(df, columns=['load_mw', 'price'], lag_periods=[1, 2])

    # Verify the returned DataFrame contains the original columns
    assert set(df.columns).issubset(result_df.columns), "Verify the returned DataFrame contains the original columns"

    # Verify lagged columns are created with correct naming
    assert 'load_mw_lag_1' in result_df.columns, "Verify lagged columns are created with correct naming"
    assert 'load_mw_lag_2' in result_df.columns, "Verify lagged columns are created with correct naming"
    assert 'price_lag_1' in result_df.columns, "Verify lagged columns are created with correct naming"
    assert 'price_lag_2' in result_df.columns, "Verify lagged columns are created with correct naming"

    # Verify lagged values are correctly shifted from original data
    assert result_df['load_mw_lag_1'].tolist() == [numpy.nan, 100.0, 110.0], "Verify lagged values are correctly shifted from original data"
    assert result_df['load_mw_lag_2'].tolist() == [numpy.nan, numpy.nan, 100.0], "Verify lagged values are correctly shifted from original data"

    # Test with default lag periods by not specifying lag_periods parameter
    result_df = generate_lagged_features(df, columns=['load_mw'])
    assert 'load_mw_lag_24' in result_df.columns, "Test with default lag periods by not specifying lag_periods parameter"

    # Test with missing required columns and verify it raises an error
    with pytest.raises(LaggedFeatureError):
        generate_lagged_features(df, columns=['missing_column']), "Test with missing required columns and verify it raises an error"


def test_generate_lagged_features_with_missing_timestamp():
    """Tests error handling when timestamp column is missing"""
    # Create a sample DataFrame without a timestamp column
    data = {'load_mw': [100, 110, 120], 'price': [20, 22, 24]}
    df = pd.DataFrame(data)

    # Call generate_lagged_features with this DataFrame
    with pytest.raises(LaggedFeatureError) as exc_info:
        generate_lagged_features(df, columns=['load_mw'], timestamp_column='timestamp')

    # Verify it raises a LaggedFeatureError with appropriate message
    assert "Input dataframe missing required columns" in str(exc_info.value), "Verify it raises a LaggedFeatureError with appropriate message"


def test_generate_lagged_features_with_unsorted_data():
    """Tests that lagged features are correctly generated with unsorted data"""
    # Create a sample DataFrame with unsorted timestamp data
    data = {'timestamp': pd.to_datetime(['2023-01-01 01:00:00', '2023-01-01 00:00:00', '2023-01-01 02:00:00']), 'load_mw': [110, 100, 120]}
    df = pd.DataFrame(data)

    # Call generate_lagged_features with this DataFrame
    result_df = generate_lagged_features(df, columns=['load_mw'], lag_periods=[1])

    # Verify the function sorts the data before generating lags
    assert result_df['load_mw_lag_1'].tolist() == [numpy.nan, 110.0, 100.0], "Verify lagged values are correctly calculated based on sorted timestamps"


def test_generate_lagged_features_with_custom_lag_periods():
    """Tests generating lagged features with custom lag periods"""
    # Create a sample DataFrame with time series data
    data = {'timestamp': pd.to_datetime(['2023-01-01 00:00:00', '2023-01-01 01:00:00', '2023-01-01 02:00:00']), 'load_mw': [100, 110, 120]}
    df = pd.DataFrame(data)

    # Define custom lag periods different from defaults
    custom_lag_periods = [3, 6]

    # Call generate_lagged_features with custom lag periods
    result_df = generate_lagged_features(df, columns=['load_mw'], lag_periods=custom_lag_periods)

    # Verify only the specified lag periods are created
    assert 'load_mw_lag_3' in result_df.columns, "Verify only the specified lag periods are created"
    assert 'load_mw_lag_6' in result_df.columns, "Verify only the specified lag periods are created"
    assert 'load_mw_lag_1' not in result_df.columns, "Verify only the specified lag periods are created"

    # Verify lagged values are correctly shifted for each custom lag period
    assert result_df['load_mw_lag_3'].tolist() == [numpy.nan, numpy.nan, numpy.nan], "Verify lagged values are correctly shifted for each custom lag period"
    assert result_df['load_mw_lag_6'].tolist() == [numpy.nan, numpy.nan, numpy.nan], "Verify lagged values are correctly shifted for each custom lag period"


def test_lagged_feature_error_handling():
    """Tests error handling in lagged feature generation"""
    # Test with invalid DataFrame (None) and verify it raises appropriate error
    with pytest.raises(LaggedFeatureError):
        generate_lagged_features(None, columns=['load_mw']), "Test with invalid DataFrame (None) and verify it raises appropriate error"

    # Test with empty DataFrame and verify it raises appropriate error
    with pytest.raises(LaggedFeatureError):
        generate_lagged_features(pd.DataFrame(), columns=['load_mw']), "Test with empty DataFrame and verify it raises appropriate error"

    # Test with invalid columns list and verify it raises appropriate error
    data = {'timestamp': pd.to_datetime(['2023-01-01 00:00:00']), 'load_mw': [100]}
    df = pd.DataFrame(data)
    with pytest.raises(LaggedFeatureError):
        generate_lagged_features(df, columns=None), "Test with invalid columns list and verify it raises appropriate error"

    # Test with invalid lag periods and verify it raises appropriate error
    with pytest.raises(LaggedFeatureError):
        generate_lagged_features(df, columns=['load_mw'], lag_periods="invalid"), "Test with invalid lag periods and verify it raises appropriate error"


def test_integration_with_mock_data():
    """Integration test using mock feature data"""
    # Create mock feature data using create_mock_feature_data
    start_time = datetime.datetime(2023, 1, 1)
    mock_data = create_mock_feature_data(start_time)

    # Generate lagged features for price and load columns
    lagged_df = generate_lagged_features(mock_data, columns=['dalmp', 'load_mw'], lag_periods=[24, 48])

    # Verify lagged features are correctly generated
    assert 'dalmp_lag_24' in lagged_df.columns, "Verify lagged features are correctly generated"
    assert 'load_mw_lag_48' in lagged_df.columns, "Verify lagged features are correctly generated"

    # Verify integration with other feature engineering components
    assert 'hour' in lagged_df.columns, "Verify integration with other feature engineering components"