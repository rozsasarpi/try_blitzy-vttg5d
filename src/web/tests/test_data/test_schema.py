"""
Unit tests for the schema validation and transformation functions in the web visualization interface.
This module tests the functionality of the schema.py module, ensuring that forecast data is properly
validated, transformed, and prepared for visualization.
"""

import pytest  # pytest: 7.0.0+
import pandas as pd  # pandas: 2.0.0+
import numpy as np  # numpy: 1.24.0+
from datetime import datetime  # standard library
import pandera as pa  # pandera: 0.16.0+

from src.web.data.schema import (  # src/web/data/schema.py
    validate_forecast_dataframe,
    prepare_dataframe_for_visualization,
    extract_samples_from_dataframe,
    get_sample_columns,
    convert_to_price_forecast_models,
    add_unit_information,
    validate_price_ranges,
    get_schema_info,
    WEB_VISUALIZATION_SCHEMA,
    DEFAULT_PERCENTILES
)
from src.web.tests.fixtures.forecast_fixtures import (  # src/web/tests/fixtures/forecast_fixtures.py
    create_sample_forecast_dataframe,
    create_sample_visualization_dataframe,
    create_invalid_visualization_dataframe
)
from src.web.config.product_config import PRODUCTS, can_be_negative  # src/web/config/product_config.py
from src.backend.models.data_models import PriceForecast, SAMPLE_COLUMN_PREFIX  # src/backend/models/data_models.py


def test_web_visualization_schema_structure():
    """Tests that the web visualization schema has the expected structure"""
    # Get schema information using get_schema_info()
    schema_info = get_schema_info()

    # Assert that the schema has the expected columns (timestamp, product, point_forecast, lower_bound, upper_bound, is_fallback)
    expected_columns = ["timestamp", "product", "point_forecast", "lower_bound", "upper_bound", "is_fallback"]
    assert set(schema_info["columns"].keys()) == set(expected_columns)

    # Assert that the column types are correct
    assert schema_info["columns"]["timestamp"]["type"] == "datetime64[ns]"
    assert schema_info["columns"]["product"]["type"] == "str"
    assert schema_info["columns"]["point_forecast"]["type"] == "float64"
    assert schema_info["columns"]["lower_bound"]["type"] == "float64"
    assert schema_info["columns"]["upper_bound"]["type"] == "float64"
    assert schema_info["columns"]["is_fallback"]["type"] == "bool"

    # Assert that the product column has a check for valid products
    assert "checks" in schema_info["columns"]["product"]
    product_check = schema_info["columns"]["product"]["checks"][0]
    assert "isin" in product_check, f"Expected 'isin' in product check, but got {product_check}"


def test_validate_forecast_dataframe_valid():
    """Tests that a valid visualization dataframe passes validation"""
    # Create a sample visualization dataframe using create_sample_visualization_dataframe()
    sample_df = create_sample_visualization_dataframe()

    # Call validate_forecast_dataframe() with the sample dataframe
    is_valid, errors = validate_forecast_dataframe(sample_df)

    # Assert that the validation result is True
    assert is_valid is True

    # Assert that there are no validation errors
    assert not errors


def test_validate_forecast_dataframe_invalid():
    """Tests that an invalid visualization dataframe fails validation"""
    # Create an invalid visualization dataframe using create_invalid_visualization_dataframe()
    invalid_df = create_invalid_visualization_dataframe()

    # Call validate_forecast_dataframe() with the invalid dataframe
    is_valid, errors = validate_forecast_dataframe(invalid_df)

    # Assert that the validation result is False
    assert is_valid is False

    # Assert that there are validation errors in the result
    assert errors


def test_prepare_dataframe_for_visualization():
    """Tests the transformation of a backend forecast dataframe to visualization format"""
    # Create a sample forecast dataframe in backend format using create_sample_forecast_dataframe()
    backend_df = create_sample_forecast_dataframe()

    # Call prepare_dataframe_for_visualization() with the sample dataframe
    viz_df = prepare_dataframe_for_visualization(backend_df)

    # Assert that the resulting dataframe has the expected columns (timestamp, product, point_forecast, lower_bound, upper_bound, is_fallback)
    expected_columns = ["timestamp", "product", "point_forecast", "lower_bound", "upper_bound", "is_fallback"]
    assert set(viz_df.columns) == set(expected_columns)

    # Assert that the resulting dataframe has the correct number of rows
    assert len(viz_df) == len(backend_df)

    # Assert that lower_bound <= point_forecast <= upper_bound for all rows
    assert (viz_df["lower_bound"] <= viz_df["point_forecast"]).all()
    assert (viz_df["point_forecast"] <= viz_df["upper_bound"]).all()

    # Validate the resulting dataframe against WEB_VISUALIZATION_SCHEMA
    is_valid, errors = validate_forecast_dataframe(viz_df)
    assert is_valid is True
    assert not errors


def test_prepare_dataframe_for_visualization_custom_percentiles():
    """Tests the transformation with custom percentiles"""
    # Create a sample forecast dataframe in backend format
    backend_df = create_sample_forecast_dataframe()

    # Define custom percentiles [5, 95]
    custom_percentiles = [5, 95]

    # Call prepare_dataframe_for_visualization() with the sample dataframe and custom percentiles
    viz_df = prepare_dataframe_for_visualization(backend_df, percentiles=custom_percentiles)

    # Assert that the resulting dataframe has lower_bound and upper_bound columns
    assert "lower_bound" in viz_df.columns
    assert "upper_bound" in viz_df.columns

    # Verify that the bounds correspond to the custom percentiles
    sample_columns = get_sample_columns(backend_df)
    lower_values = np.percentile(backend_df[sample_columns].values, custom_percentiles[0], axis=1)
    upper_values = np.percentile(backend_df[sample_columns].values, custom_percentiles[1], axis=1)

    np.testing.assert_allclose(viz_df["lower_bound"].values, lower_values, rtol=1e-5)
    np.testing.assert_allclose(viz_df["upper_bound"].values, upper_values, rtol=1e-5)


def test_extract_samples_from_dataframe():
    """Tests the extraction of percentile values from sample columns"""
    # Create a sample forecast dataframe with known sample values
    data = {'timestamp': [datetime(2023, 1, 1)],
            'product': ['DALMP'],
            'point_forecast': [50.0],
            'sample_001': [40.0],
            'sample_002': [50.0],
            'sample_003': [60.0]}
    sample_df = pd.DataFrame(data)

    # Define percentiles to extract [10, 50, 90]
    percentiles = [10, 50, 90]

    # Call extract_samples_from_dataframe() with the sample dataframe and percentiles
    result_df = extract_samples_from_dataframe(sample_df, percentiles=percentiles)

    # Assert that the resulting dataframe has percentile columns for each specified percentile
    assert "percentile_10" in result_df.columns
    assert "percentile_50" in result_df.columns
    assert "percentile_90" in result_df.columns

    # Verify that the percentile values are correctly calculated from the samples
    assert result_df["percentile_10"][0] == 42.0  # 10th percentile of [40, 50, 60]
    assert result_df["percentile_50"][0] == 50.0  # 50th percentile of [40, 50, 60]
    assert result_df["percentile_90"][0] == 58.0  # 90th percentile of [40, 50, 60]


def test_get_sample_columns():
    """Tests the identification of sample columns in a dataframe"""
    # Create a sample forecast dataframe with known sample columns
    data = {'timestamp': [datetime(2023, 1, 1)],
            'product': ['DALMP'],
            'point_forecast': [50.0],
            'sample_001': [40.0],
            'sample_002': [50.0],
            'sample_003': [60.0],
            'other_column': [100]}
    sample_df = pd.DataFrame(data)

    # Call get_sample_columns() with the sample dataframe
    sample_columns = get_sample_columns(sample_df)

    # Assert that all sample columns are correctly identified
    assert "sample_001" in sample_columns
    assert "sample_002" in sample_columns
    assert "sample_003" in sample_columns

    # Assert that non-sample columns are not included in the result
    assert "timestamp" not in sample_columns
    assert "product" not in sample_columns
    assert "point_forecast" not in sample_columns
    assert "other_column" not in sample_columns


def test_convert_to_price_forecast_models():
    """Tests the conversion of a dataframe to PriceForecast models"""
    # Create a sample forecast dataframe
    data = {'timestamp': [datetime(2023, 1, 1)],
            'product': ['DALMP'],
            'point_forecast': [50.0],
            'sample_001': [40.0],
            'sample_002': [50.0],
            'sample_003': [60.0],
            'generation_timestamp': [datetime(2023, 1, 1, 12, 0, 0)],
            'is_fallback': [False]}
    sample_df = pd.DataFrame(data)

    # Call convert_to_price_forecast_models() with the sample dataframe
    forecast_models = convert_to_price_forecast_models(sample_df)

    # Assert that the result is a list of PriceForecast instances
    assert isinstance(forecast_models, list)
    assert all(isinstance(model, PriceForecast) for model in forecast_models)

    # Assert that the list has the same length as the dataframe
    assert len(forecast_models) == len(sample_df)

    # Verify that the model attributes match the dataframe values
    model = forecast_models[0]
    assert model.timestamp == sample_df["timestamp"][0]
    assert model.product == sample_df["product"][0]
    assert model.point_forecast == sample_df["point_forecast"][0]
    assert model.samples == [40.0, 50.0, 60.0]
    assert model.generation_timestamp == sample_df["generation_timestamp"][0]
    assert model.is_fallback == sample_df["is_fallback"][0]


def test_add_unit_information():
    """Tests adding unit information to a visualization dataframe"""
    # Create a sample visualization dataframe
    data = {'timestamp': [datetime(2023, 1, 1)],
            'product': ['DALMP'],
            'point_forecast': [50.0],
            'lower_bound': [40.0],
            'upper_bound': [60.0],
            'is_fallback': [False]}
    sample_df = pd.DataFrame(data)

    # Call add_unit_information() with the sample dataframe
    result_df = add_unit_information(sample_df)

    # Assert that the resulting dataframe has a 'unit' column
    assert "unit" in result_df.columns

    # Verify that the unit values are correct for each product
    assert result_df["unit"][0] == "$/MWh"  # Unit for DALMP


def test_validate_price_ranges_valid():
    """Tests validation of valid price ranges"""
    # Create a sample visualization dataframe with valid price ranges
    data = {'timestamp': [datetime(2023, 1, 1)],
            'product': ['DALMP'],
            'point_forecast': [50.0],
            'lower_bound': [40.0],
            'upper_bound': [60.0],
            'is_fallback': [False]}
    sample_df = pd.DataFrame(data)

    # Call validate_price_ranges() with the sample dataframe
    is_valid, errors = validate_price_ranges(sample_df)

    # Assert that the validation result is True
    assert is_valid is True

    # Assert that there are no validation errors
    assert not errors


def test_validate_price_ranges_invalid():
    """Tests validation of invalid price ranges"""
    # Create a sample visualization dataframe
    data = {'timestamp': [datetime(2023, 1, 1)],
            'product': ['RegUp'],
            'point_forecast': [-10.0],
            'lower_bound': [-20.0],
            'upper_bound': [0.0],
            'is_fallback': [False]}
    sample_df = pd.DataFrame(data)

    # Modify the dataframe to have invalid price ranges (e.g., negative prices for products that can't be negative)
    # Call validate_price_ranges() with the modified dataframe
    is_valid, errors = validate_price_ranges(sample_df)

    # Assert that the validation result is False
    assert is_valid is False

    # Assert that there are validation errors in the result
    assert errors


def test_validate_price_ranges_bounds_check():
    """Tests validation of price bounds relationships"""
    # Create a sample visualization dataframe
    data = {'timestamp': [datetime(2023, 1, 1)],
            'product': ['DALMP'],
            'point_forecast': [50.0],
            'lower_bound': [60.0],
            'upper_bound': [70.0],
            'is_fallback': [False]}
    sample_df = pd.DataFrame(data)

    # Modify the dataframe to have invalid bounds (e.g., lower_bound > point_forecast)
    # Call validate_price_ranges() with the modified dataframe
    is_valid, errors = validate_price_ranges(sample_df)

    # Assert that the validation result is False
    assert is_valid is False

    # Assert that there are validation errors about bounds relationships
    assert errors