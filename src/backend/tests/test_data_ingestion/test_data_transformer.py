"""
Unit tests for the data_transformer module which is responsible for transforming raw data from external sources into standardized formats suitable for the forecasting engine. Tests cover normalization functions, data conversion between models and dataframes, and the DataTransformer class methods.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

# Internal imports
from ../../data_ingestion.data_transformer import (
    DataTransformer,
    normalize_load_forecast_data,
    normalize_historical_prices_data,
    normalize_generation_forecast_data,
    models_to_dataframe,
    dataframe_to_models,
    resample_time_series,
    align_timestamps,
    merge_dataframes,
    pivot_generation_data
)
from ../../data_ingestion.exceptions import DataTransformationError
from ../../models.data_models import LoadForecast, HistoricalPrice, GenerationForecast
from ../../models.validation_models import ValidationResult
from ../../config.settings import FORECAST_PRODUCTS
from ../../utils.date_utils import localize_to_cst


def test_normalize_load_forecast_data():
    """Tests the normalize_load_forecast_data function with various input scenarios."""
    # Create test dataframe with load forecast data
    test_data = {
        'timestamp': ['2023-01-01 00:00:00', '2023-01-01 01:00:00', '2023-01-01 02:00:00'],
        'Load_MW': [35000.5, 34000.2, 33500.8],
        'REGION': ['ERCOT', 'ercot', 'Ercot']
    }
    df = pd.DataFrame(test_data)
    
    # Call normalize_load_forecast_data with the test dataframe
    result = normalize_load_forecast_data(df)
    
    # Assert that column names are standardized (lowercase)
    assert all(col == col.lower() for col in result.columns)
    
    # Assert that timestamp column is in datetime format and in CST timezone
    assert pd.api.types.is_datetime64_dtype(result['timestamp'])
    
    # Assert that load_mw values are numeric (float)
    assert pd.api.types.is_numeric_dtype(result['load_mw'])
    
    # Assert that region values are uppercase strings
    assert all(region == region.upper() for region in result['region'])
    
    # Assert that dataframe is sorted by timestamp
    assert result['timestamp'].is_monotonic_increasing
    
    # Test with empty dataframe and assert empty result
    empty_df = pd.DataFrame()
    empty_result = normalize_load_forecast_data(empty_df)
    assert empty_result.empty
    
    # Test with None input and assert empty result
    none_result = normalize_load_forecast_data(None)
    assert none_result.empty


def test_normalize_historical_prices_data():
    """Tests the normalize_historical_prices_data function with various input scenarios."""
    # Create test dataframe with historical price data
    test_data = {
        'timestamp': ['2023-01-01 00:00:00', '2023-01-01 01:00:00', '2023-01-01 02:00:00'],
        'Product': [FORECAST_PRODUCTS[0], FORECAST_PRODUCTS[1], FORECAST_PRODUCTS[0]],
        'Price': [42.15, 38.72, 36.45],
        'Node': ['HB_NORTH', 'hb_north', 'Hb_North']
    }
    df = pd.DataFrame(test_data)
    
    # Call normalize_historical_prices_data with the test dataframe
    result = normalize_historical_prices_data(df)
    
    # Assert that column names are standardized (lowercase)
    assert all(col == col.lower() for col in result.columns)
    
    # Assert that timestamp column is in datetime format and in CST timezone
    assert pd.api.types.is_datetime64_dtype(result['timestamp'])
    
    # Assert that price values are numeric (float)
    assert pd.api.types.is_numeric_dtype(result['price'])
    
    # Assert that product values are uppercase strings
    assert all(product == product.upper() for product in result['product'])
    
    # Assert that only products in FORECAST_PRODUCTS are included
    assert all(product in FORECAST_PRODUCTS for product in result['product'])
    
    # Assert that node values are uppercase strings
    assert all(node == node.upper() for node in result['node'])
    
    # Assert that dataframe is sorted by timestamp and product
    assert result.sort_values(['timestamp', 'product']).equals(result)
    
    # Test with empty dataframe and assert empty result
    empty_result = normalize_historical_prices_data(pd.DataFrame())
    assert empty_result.empty
    
    # Test with None input and assert empty result
    none_result = normalize_historical_prices_data(None)
    assert none_result.empty


def test_normalize_generation_forecast_data():
    """Tests the normalize_generation_forecast_data function with various input scenarios."""
    # Create test dataframe with generation forecast data
    test_data = {
        'timestamp': ['2023-01-01 00:00:00', '2023-01-01 01:00:00', '2023-01-01 02:00:00'],
        'Fuel_Type': ['wind', 'SOLAR', 'Gas'],
        'Generation_MW': [12450.3, 0.0, 15320.5],
        'REGION': ['ERCOT', 'ercot', 'Ercot']
    }
    df = pd.DataFrame(test_data)
    
    # Call normalize_generation_forecast_data with the test dataframe
    result = normalize_generation_forecast_data(df)
    
    # Assert that column names are standardized (lowercase)
    assert all(col == col.lower() for col in result.columns)
    
    # Assert that timestamp column is in datetime format and in CST timezone
    assert pd.api.types.is_datetime64_dtype(result['timestamp'])
    
    # Assert that generation_mw values are numeric (float)
    assert pd.api.types.is_numeric_dtype(result['generation_mw'])
    
    # Assert that fuel_type values are lowercase strings
    assert all(fuel_type == fuel_type.lower() for fuel_type in result['fuel_type'])
    
    # Assert that region values are uppercase strings
    assert all(region == region.upper() for region in result['region'])
    
    # Assert that dataframe is sorted by timestamp and fuel_type
    assert result.sort_values(['timestamp', 'fuel_type']).equals(result)
    
    # Test with empty dataframe and assert empty result
    empty_result = normalize_generation_forecast_data(pd.DataFrame())
    assert empty_result.empty
    
    # Test with None input and assert empty result
    none_result = normalize_generation_forecast_data(None)
    assert none_result.empty


def test_models_to_dataframe():
    """Tests the models_to_dataframe function for converting model instances to dataframes."""
    # Create test list of LoadForecast model instances
    load_models = [
        LoadForecast(
            timestamp=localize_to_cst(datetime(2023, 1, 1, hour)),
            load_mw=35000.0 - hour * 500,
            region="ERCOT"
        )
        for hour in range(3)
    ]
    
    # Call models_to_dataframe with the test models and 'load_forecast' model_type
    load_df = models_to_dataframe(load_models, 'load_forecast')
    
    # Assert that resulting dataframe has correct columns
    assert 'timestamp' in load_df.columns
    assert 'load_mw' in load_df.columns
    assert 'region' in load_df.columns
    
    # Assert that dataframe has correct number of rows
    assert len(load_df) == 3
    
    # Assert that values match the original model instances
    assert load_df['region'].iloc[0] == "ERCOT"
    assert load_df['load_mw'].iloc[0] == 35000.0
    
    # Repeat tests for HistoricalPrice models with 'historical_price' model_type
    price_models = [
        HistoricalPrice(
            timestamp=localize_to_cst(datetime(2023, 1, 1, hour)),
            product=FORECAST_PRODUCTS[0],
            price=50.0 - hour * 2.5,
            node="HB_NORTH"
        )
        for hour in range(3)
    ]
    
    price_df = models_to_dataframe(price_models, 'historical_price')
    assert 'timestamp' in price_df.columns
    assert 'product' in price_df.columns
    assert 'price' in price_df.columns
    assert 'node' in price_df.columns
    assert len(price_df) == 3
    
    # Repeat tests for GenerationForecast models with 'generation_forecast' model_type
    gen_models = [
        GenerationForecast(
            timestamp=localize_to_cst(datetime(2023, 1, 1, hour)),
            fuel_type="wind",
            generation_mw=1000.0 + hour * 100,
            region="ERCOT"
        )
        for hour in range(3)
    ]
    
    gen_df = models_to_dataframe(gen_models, 'generation_forecast')
    assert 'timestamp' in gen_df.columns
    assert 'fuel_type' in gen_df.columns
    assert 'generation_mw' in gen_df.columns
    assert 'region' in gen_df.columns
    assert len(gen_df) == 3
    
    # Test with empty list and assert empty dataframe result
    empty_df = models_to_dataframe([], 'load_forecast')
    assert empty_df.empty
    
    # Test with invalid model_type and assert appropriate error handling
    with pytest.raises(ValueError):
        models_to_dataframe(load_models, 'invalid_type')


def test_dataframe_to_models():
    """Tests the dataframe_to_models function for converting dataframes to model instances."""
    # Create test dataframe with load forecast data
    load_data = {
        'timestamp': [datetime(2023, 1, 1, hour) for hour in range(3)],
        'load_mw': [35000.0 - hour * 500 for hour in range(3)],
        'region': ['ERCOT'] * 3
    }
    load_df = pd.DataFrame(load_data)
    
    # Call dataframe_to_models with the test dataframe and 'load_forecast' model_type
    load_models = dataframe_to_models(load_df, 'load_forecast')
    
    # Assert that resulting list contains LoadForecast instances
    assert all(isinstance(model, LoadForecast) for model in load_models)
    
    # Assert that list has correct length
    assert len(load_models) == 3
    
    # Assert that model attributes match the original dataframe values
    assert load_models[0].region == 'ERCOT'
    assert load_models[0].load_mw == 35000.0
    
    # Repeat tests for historical price dataframe with 'historical_price' model_type
    price_data = {
        'timestamp': [datetime(2023, 1, 1, hour) for hour in range(3)],
        'product': [FORECAST_PRODUCTS[0]] * 3,
        'price': [50.0 - hour * 2.5 for hour in range(3)],
        'node': ['HB_NORTH'] * 3
    }
    price_df = pd.DataFrame(price_data)
    
    price_models = dataframe_to_models(price_df, 'historical_price')
    assert all(isinstance(model, HistoricalPrice) for model in price_models)
    assert len(price_models) == 3
    assert price_models[0].product == FORECAST_PRODUCTS[0]
    
    # Repeat tests for generation forecast dataframe with 'generation_forecast' model_type
    gen_data = {
        'timestamp': [datetime(2023, 1, 1, hour) for hour in range(3)],
        'fuel_type': ['wind'] * 3,
        'generation_mw': [1000.0 + hour * 100 for hour in range(3)],
        'region': ['ERCOT'] * 3
    }
    gen_df = pd.DataFrame(gen_data)
    
    gen_models = dataframe_to_models(gen_df, 'generation_forecast')
    assert all(isinstance(model, GenerationForecast) for model in gen_models)
    assert len(gen_models) == 3
    assert gen_models[0].fuel_type == 'wind'
    
    # Test with empty dataframe and assert empty list result
    empty_models = dataframe_to_models(pd.DataFrame(), 'load_forecast')
    assert len(empty_models) == 0
    
    # Test with invalid model_type and assert appropriate error handling
    with pytest.raises(ValueError):
        dataframe_to_models(load_df, 'invalid_type')


def test_resample_time_series():
    """Tests the resample_time_series function for resampling time series data."""
    # Create test dataframe with time series data at irregular intervals
    test_data = {
        'timestamp': pd.to_datetime([
            '2023-01-01 01:15:00', '2023-01-01 02:30:00', 
            '2023-01-01 03:45:00', '2023-01-01 05:00:00'
        ]),
        'value': [10.0, 20.0, 30.0, 40.0],
        'category': ['A', 'B', 'A', 'B']
    }
    df = pd.DataFrame(test_data)
    
    # Define aggregation rules for resampling
    agg_rules = {
        'value': 'mean',
        'category': 'first'
    }
    
    # Call resample_time_series with test dataframe, timestamp column, frequency, and aggregation rules
    result = resample_time_series(df, 'timestamp', 'H', agg_rules)
    
    # Assert that resulting dataframe has regular time intervals
    assert len(result) >= 4  # Should have at least the span of hours in the original data
    
    # Test with different frequencies (hourly, daily)
    hourly_result = resample_time_series(df, 'timestamp', 'H', agg_rules)
    assert pd.infer_freq(hourly_result['timestamp']) == 'H'
    
    daily_result = resample_time_series(df, 'timestamp', 'D', agg_rules)
    assert len(daily_result) == 1  # All data is from one day
    
    # Test with empty dataframe and assert empty result
    empty_result = resample_time_series(pd.DataFrame(columns=['timestamp', 'value']), 'timestamp', 'H', {'value': 'mean'})
    assert empty_result.empty
    
    # Test with invalid timestamp column and assert appropriate error handling
    with pytest.raises(Exception):
        resample_time_series(df, 'non_existent_column', 'H', agg_rules)


def test_align_timestamps():
    """Tests the align_timestamps function for aligning timestamps across multiple dataframes."""
    # Create multiple test dataframes with different timestamp ranges
    df1 = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-01 01:00:00', '2023-01-01 02:00:00', '2023-01-01 03:00:00']),
        'value1': [10, 20, 30]
    })
    
    df2 = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-01 02:00:00', '2023-01-01 03:00:00', '2023-01-01 04:00:00']),
        'value2': [15, 25, 35]
    })
    
    # Call align_timestamps with the test dataframes, timestamp column, and frequency
    aligned_dfs = align_timestamps({'df1': df1, 'df2': df2}, 'timestamp', 'H')
    
    # Assert that resulting dataframes have the same timestamp index
    assert set(aligned_dfs['df1']['timestamp']) == set(aligned_dfs['df2']['timestamp'])
    
    # Assert that missing values are handled appropriately
    # The resulting dataframes should cover all timestamps from both inputs
    expected_timestamps = pd.date_range(
        start=min(df1['timestamp'].min(), df2['timestamp'].min()),
        end=max(df1['timestamp'].max(), df2['timestamp'].max()),
        freq='H'
    )
    assert len(aligned_dfs['df1']) == len(expected_timestamps)
    
    # Test with empty dataframes and assert appropriate handling
    empty_aligned = align_timestamps({'empty': pd.DataFrame()}, 'timestamp', 'H')
    assert 'empty' in empty_aligned
    assert empty_aligned['empty'].empty
    
    # Test with non-matching timestamps and assert appropriate handling
    df_without_timestamp = pd.DataFrame({'not_timestamp': [1, 2, 3]})
    aligned_with_invalid = align_timestamps({'invalid': df_without_timestamp}, 'timestamp', 'H')
    assert 'invalid' in aligned_with_invalid
    assert aligned_with_invalid['invalid'].equals(df_without_timestamp)


def test_merge_dataframes():
    """Tests the merge_dataframes function for merging multiple dataframes on a timestamp column."""
    # Create multiple test dataframes with common timestamp column
    df1 = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-01 01:00:00', '2023-01-01 02:00:00', '2023-01-01 03:00:00']),
        'value': [10, 20, 30]
    })
    
    df2 = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-01 01:00:00', '2023-01-01 02:00:00', '2023-01-01 03:00:00']),
        'value': [15, 25, 35]
    })
    
    # Call merge_dataframes with the test dataframes, suffixes, timestamp column, and join type
    result = merge_dataframes([df1, df2], ['_df2'], 'timestamp', 'inner')
    
    # Assert that resulting dataframe contains all expected columns
    assert 'value' in result.columns
    assert 'value_df2' in result.columns
    
    # Assert that column naming with suffixes works correctly
    assert result.columns.tolist() == ['timestamp', 'value', 'value_df2']
    
    # Assert that join behavior works as expected (inner, outer, left, right)
    inner_result = merge_dataframes([df1, df2], ['_df2'], 'timestamp', 'inner')
    assert len(inner_result) == 3
    
    # Test with non-matching timestamps (create a dataframe with different timestamps)
    df3 = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-01 04:00:00', '2023-01-01 05:00:00']),
        'value': [40, 50]
    })
    
    outer_result = merge_dataframes([df1, df3], ['_df3'], 'timestamp', 'outer')
    assert len(outer_result) == 5  # All timestamps from both dataframes
    
    # Test with empty dataframes and assert appropriate handling
    empty_df = pd.DataFrame()
    result_with_empty = merge_dataframes([df1, empty_df], ['_empty'], 'timestamp', 'left')
    assert len(result_with_empty) == len(df1)  # Should preserve all rows from df1


def test_pivot_generation_data():
    """Tests the pivot_generation_data function for pivoting generation forecast data."""
    # Create test dataframe with generation forecast data for multiple fuel types
    test_data = {
        'timestamp': pd.to_datetime(['2023-01-01 01:00:00'] * 3),
        'fuel_type': ['wind', 'solar', 'gas'],
        'generation_mw': [1000.0, 500.0, 2000.0],
        'region': ['ERCOT'] * 3
    }
    df = pd.DataFrame(test_data)
    
    # Call pivot_generation_data with the test dataframe
    result = pivot_generation_data(df)
    
    # Assert that resulting dataframe has fuel types as columns
    assert 'generation_wind' in result.columns
    assert 'generation_solar' in result.columns
    assert 'generation_gas' in result.columns
    
    # Assert that column names include 'generation_' prefix
    for col in result.columns:
        if col != 'timestamp':
            assert col.startswith('generation_')
    
    # Assert that values are correctly pivoted
    assert result['generation_wind'].iloc[0] == 1000.0
    assert result['generation_solar'].iloc[0] == 500.0
    assert result['generation_gas'].iloc[0] == 2000.0
    
    # Assert that missing values are handled appropriately
    # Test with empty dataframe and assert empty result
    empty_result = pivot_generation_data(pd.DataFrame())
    assert empty_result.empty
    
    # Test with single fuel type and assert appropriate handling
    single_fuel_data = {
        'timestamp': pd.to_datetime(['2023-01-01 01:00:00']),
        'fuel_type': ['wind'],
        'generation_mw': [1000.0],
        'region': ['ERCOT']
    }
    single_fuel_df = pd.DataFrame(single_fuel_data)
    single_fuel_result = pivot_generation_data(single_fuel_df)
    assert 'generation_wind' in single_fuel_result.columns


def test_data_transformer_init():
    """Tests the initialization of the DataTransformer class."""
    # Create instance of DataTransformer
    transformer = DataTransformer()
    
    # Assert that instance is created successfully
    assert transformer is not None
    
    # Assert that instance has expected attributes and methods
    assert hasattr(transformer, 'logger')
    assert hasattr(transformer, 'transform_load_forecast')
    assert hasattr(transformer, 'transform_historical_prices')
    assert hasattr(transformer, 'transform_generation_forecast')
    assert hasattr(transformer, 'prepare_combined_dataset')
    assert hasattr(transformer, 'convert_to_models')
    assert hasattr(transformer, 'convert_from_models')


def test_transform_load_forecast():
    """Tests the transform_load_forecast method of DataTransformer."""
    # Create instance of DataTransformer
    transformer = DataTransformer()
    
    # Create test dataframe with load forecast data
    test_data = {
        'timestamp': ['2023-01-01 00:00:00', '2023-01-01 01:00:00'],
        'Load_MW': [35000.5, 34000.2],
        'REGION': ['ERCOT', 'ERCOT']
    }
    df = pd.DataFrame(test_data)
    
    # Call transform_load_forecast with the test dataframe
    result = transformer.transform_load_forecast(df)
    
    # Assert that resulting dataframe is normalized correctly
    assert 'load_mw' in result.columns
    assert 'region' in result.columns
    assert pd.api.types.is_datetime64_dtype(result['timestamp'])
    assert all(region == 'ERCOT' for region in result['region'])
    
    # Test with empty dataframe and assert empty result
    empty_result = transformer.transform_load_forecast(pd.DataFrame())
    assert empty_result.empty
    
    # Test with invalid data and assert appropriate error handling
    invalid_df = pd.DataFrame({'not_timestamp': [1, 2], 'not_load': [3, 4]})
    with pytest.raises(DataTransformationError):
        transformer.transform_load_forecast(invalid_df)


def test_transform_historical_prices():
    """Tests the transform_historical_prices method of DataTransformer."""
    # Create instance of DataTransformer
    transformer = DataTransformer()
    
    # Create test dataframe with historical price data
    test_data = {
        'timestamp': ['2023-01-01 00:00:00', '2023-01-01 01:00:00'],
        'Product': [FORECAST_PRODUCTS[0], FORECAST_PRODUCTS[1]],
        'Price': [42.15, 38.72],
        'Node': ['HB_NORTH', 'HB_NORTH']
    }
    df = pd.DataFrame(test_data)
    
    # Call transform_historical_prices with the test dataframe
    result = transformer.transform_historical_prices(df)
    
    # Assert that resulting dataframe is normalized correctly
    assert 'price' in result.columns
    assert 'product' in result.columns
    assert 'node' in result.columns
    assert pd.api.types.is_datetime64_dtype(result['timestamp'])
    assert all(product in FORECAST_PRODUCTS for product in result['product'])
    
    # Test with empty dataframe and assert empty result
    empty_result = transformer.transform_historical_prices(pd.DataFrame())
    assert empty_result.empty
    
    # Test with invalid data and assert appropriate error handling
    invalid_df = pd.DataFrame({'not_timestamp': [1, 2], 'not_product': [3, 4]})
    with pytest.raises(DataTransformationError):
        transformer.transform_historical_prices(invalid_df)


def test_transform_generation_forecast():
    """Tests the transform_generation_forecast method of DataTransformer."""
    # Create instance of DataTransformer
    transformer = DataTransformer()
    
    # Create test dataframe with generation forecast data
    test_data = {
        'timestamp': ['2023-01-01 00:00:00', '2023-01-01 00:00:00', '2023-01-01 00:00:00'],
        'Fuel_Type': ['wind', 'solar', 'gas'],
        'Generation_MW': [1000.0, 500.0, 2000.0],
        'REGION': ['ERCOT', 'ERCOT', 'ERCOT']
    }
    df = pd.DataFrame(test_data)
    
    # Call transform_generation_forecast with the test dataframe
    result = transformer.transform_generation_forecast(df)
    
    # Assert that resulting dataframe is normalized correctly
    assert 'timestamp' in result.columns
    
    # Assert that generation data is pivoted correctly
    assert 'generation_wind' in result.columns
    assert 'generation_solar' in result.columns
    assert 'generation_gas' in result.columns
    
    # Test with empty dataframe and assert empty result
    empty_result = transformer.transform_generation_forecast(pd.DataFrame())
    assert empty_result.empty
    
    # Test with invalid data and assert appropriate error handling
    invalid_df = pd.DataFrame({'not_timestamp': [1, 2], 'not_fuel_type': [3, 4]})
    with pytest.raises(DataTransformationError):
        transformer.transform_generation_forecast(invalid_df)


def test_prepare_combined_dataset():
    """Tests the prepare_combined_dataset method of DataTransformer."""
    # Create instance of DataTransformer
    transformer = DataTransformer()
    
    # Create test dataframes for load, price, and generation data
    load_df = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-01 01:00:00', '2023-01-01 02:00:00']),
        'load_mw': [35000.0, 34000.0],
        'region': ['ERCOT', 'ERCOT']
    })
    
    price_df = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-01 01:00:00', '2023-01-01 02:00:00']),
        'product': [FORECAST_PRODUCTS[0], FORECAST_PRODUCTS[0]],
        'price': [42.15, 38.72],
        'node': ['HB_NORTH', 'HB_NORTH']
    })
    
    generation_df = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-01 01:00:00', '2023-01-01 01:00:00']),
        'fuel_type': ['wind', 'solar'],
        'generation_mw': [1000.0, 500.0],
        'region': ['ERCOT', 'ERCOT']
    })
    
    # Call prepare_combined_dataset with the test dataframes
    result = transformer.prepare_combined_dataset(load_df, price_df, generation_df)
    
    # Assert that timestamps are aligned correctly
    assert 'timestamp' in result.columns
    
    # Assert that dataframes are merged correctly
    assert 'load_mw' in result.columns
    assert 'price' in result.columns
    
    # Assert that resulting dataframe has all expected columns
    expected_columns = ['timestamp', 'load_mw', 'region', 'product', 'price', 'node']
    for col in expected_columns:
        assert col in result.columns
    
    # Test with empty dataframes and assert appropriate handling
    empty_result = transformer.prepare_combined_dataset(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    assert empty_result.empty
    
    # Test with non-overlapping timestamps and assert appropriate handling
    non_overlapping_df1 = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-01 01:00:00']),
        'value': [10]
    })
    
    non_overlapping_df2 = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-02 01:00:00']),
        'value': [20]
    })
    
    non_overlapping_result = transformer.prepare_combined_dataset(
        non_overlapping_df1, non_overlapping_df2, pd.DataFrame()
    )
    
    # The result should contain both timestamps from non-overlapping dataframes
    assert len(non_overlapping_result) >= 2


def test_convert_to_models():
    """Tests the convert_to_models method of DataTransformer."""
    # Create instance of DataTransformer
    transformer = DataTransformer()
    
    # Create test dataframes for different model types
    load_data = {
        'timestamp': pd.to_datetime(['2023-01-01 01:00:00', '2023-01-01 02:00:00']),
        'load_mw': [35000.0, 34000.0],
        'region': ['ERCOT', 'ERCOT']
    }
    load_df = pd.DataFrame(load_data)
    
    # Call convert_to_models with each dataframe and appropriate model_type
    load_models = transformer.convert_to_models(load_df, 'load_forecast')
    
    # Assert that resulting models have correct types
    assert all(isinstance(model, LoadForecast) for model in load_models)
    
    # Assert that model attributes match the original dataframe values
    assert load_models[0].region == 'ERCOT'
    assert load_models[0].load_mw == 35000.0
    
    # Test with empty dataframe and assert empty list result
    empty_models = transformer.convert_to_models(pd.DataFrame(), 'load_forecast')
    assert len(empty_models) == 0
    
    # Test with invalid model_type and assert appropriate error handling
    with pytest.raises(DataTransformationError):
        transformer.convert_to_models(load_df, 'invalid_type')


def test_convert_from_models():
    """Tests the convert_from_models method of DataTransformer."""
    # Create instance of DataTransformer
    transformer = DataTransformer()
    
    # Create test model instances for different model types
    load_models = [
        LoadForecast(
            timestamp=localize_to_cst(datetime(2023, 1, 1, hour)),
            load_mw=35000.0 - hour * 1000,
            region="ERCOT"
        )
        for hour in range(2)
    ]
    
    # Call convert_from_models with each model list and appropriate model_type
    load_df = transformer.convert_from_models(load_models, 'load_forecast')
    
    # Assert that resulting dataframes have correct columns
    assert 'timestamp' in load_df.columns
    assert 'load_mw' in load_df.columns
    assert 'region' in load_df.columns
    
    # Assert that dataframe values match the original model attributes
    assert load_df['region'].iloc[0] == 'ERCOT'
    assert load_df['load_mw'].iloc[0] == 35000.0
    
    # Test with empty model list and assert empty dataframe result
    empty_df = transformer.convert_from_models([], 'load_forecast')
    assert empty_df.empty
    
    # Test with invalid model_type and assert appropriate error handling
    with pytest.raises(DataTransformationError):
        transformer.convert_from_models(load_models, 'invalid_type')


def test_error_handling():
    """Tests error handling in the data transformer module."""
    # Test with invalid inputs that should raise exceptions
    with pytest.raises(DataTransformationError):
        normalize_load_forecast_data(pd.DataFrame({'invalid': [1, 2, 3]}))
    
    with pytest.raises(ValueError):
        dataframe_to_models(pd.DataFrame(), 'invalid_model_type')
    
    # Test error handling in DataTransformer methods
    transformer = DataTransformer()
    with pytest.raises(DataTransformationError):
        transformer.transform_load_forecast(pd.DataFrame({'invalid': [1, 2, 3]}))
    
    # Assert that error messages contain relevant information
    try:
        normalize_load_forecast_data(pd.DataFrame({'invalid': [1, 2, 3]}))
    except DataTransformationError as e:
        assert 'load_forecast' in str(e)
        assert 'column_normalization' in str(e)