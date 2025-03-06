# src/backend/tests/test_models/test_data_models.py
"""
Unit tests for the data model classes defined in src/backend/models/data_models.py.
Tests the functionality of BaseDataModel, LoadForecast, HistoricalPrice, GenerationForecast,
PriceForecast, and utility functions like create_sample_columns and create_empty_forecast_dataframe.
"""

import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
from datetime import datetime  # standard library

from src.backend.models.data_models import (
    BaseDataModel,
    LoadForecast,
    HistoricalPrice,
    GenerationForecast,
    PriceForecast,
    create_sample_columns,
    create_empty_forecast_dataframe,
)
from src.backend.config.settings import FORECAST_PRODUCTS, PROBABILISTIC_SAMPLE_COUNT
from src.backend.tests.fixtures.load_forecast_fixtures import create_mock_load_forecast_models
from src.backend.tests.fixtures.historical_prices_fixtures import create_mock_historical_price_models
from src.backend.tests.fixtures.forecast_fixtures import create_mock_price_samples


def test_base_data_model_to_dict():
    """Tests the to_dict method of BaseDataModel"""
    # Create a simple subclass of BaseDataModel with test fields
    @dataclasses.dataclass
    class TestModel(BaseDataModel):
        field1: str
        field2: int
        field3: datetime

    # Create an instance of the test model
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    test_model = TestModel(field1="test", field2=123, field3=test_datetime)

    # Call to_dict() on the instance
    result = test_model.to_dict()

    # Assert that the returned dictionary contains all expected fields
    assert "field1" in result
    assert "field2" in result
    assert "field3" in result

    # Assert that datetime fields are properly converted to ISO format strings
    assert result["field3"] == test_datetime.isoformat()


def test_base_data_model_from_dict():
    """Tests the from_dict method of BaseDataModel"""
    # Create a simple subclass of BaseDataModel with test fields
    @dataclasses.dataclass
    class TestModel(BaseDataModel):
        field1: str
        field2: int
        field3: datetime

    # Create a dictionary with test data including ISO format datetime strings
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    test_data = {"field1": "test", "field2": 123, "field3": test_datetime.isoformat()}

    # Call from_dict() on the test model class with the dictionary
    test_model = TestModel.from_dict(test_data)

    # Assert that the returned instance has all expected fields
    assert test_model.field1 == "test"
    assert test_model.field2 == 123

    # Assert that ISO format strings are properly converted to datetime objects
    assert test_model.field3 == test_datetime


def test_load_forecast_initialization():
    """Tests the initialization of LoadForecast model"""
    # Create a LoadForecast instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    load_forecast = LoadForecast(timestamp=test_datetime, load_mw=1000.0, region="ERCOT")

    # Assert that all fields are correctly initialized
    assert load_forecast.timestamp == test_datetime
    assert load_forecast.load_mw == 1000.0
    assert load_forecast.region == "ERCOT"

    # Assert that the model validates correctly
    assert load_forecast.timestamp == test_datetime
    assert load_forecast.load_mw == 1000.0
    assert load_forecast.region == "ERCOT"


def test_load_forecast_to_dataframe_row():
    """Tests the to_dataframe_row method of LoadForecast"""
    # Create a LoadForecast instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    load_forecast = LoadForecast(timestamp=test_datetime, load_mw=1000.0, region="ERCOT")

    # Call to_dataframe_row() on the instance
    row = load_forecast.to_dataframe_row()

    # Assert that the returned dictionary contains all expected fields
    assert "timestamp" in row
    assert "load_mw" in row
    assert "region" in row

    # Assert that the field values match the original model
    assert row["timestamp"] == test_datetime
    assert row["load_mw"] == 1000.0
    assert row["region"] == "ERCOT"


def test_historical_price_initialization():
    """Tests the initialization of HistoricalPrice model"""
    # Create a HistoricalPrice instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    historical_price = HistoricalPrice(
        timestamp=test_datetime, product="DALMP", price=50.0, node="HB_NORTH"
    )

    # Assert that all fields are correctly initialized
    assert historical_price.timestamp == test_datetime
    assert historical_price.product == "DALMP"
    assert historical_price.price == 50.0
    assert historical_price.node == "HB_NORTH"

    # Assert that the model validates correctly
    assert historical_price.timestamp == test_datetime
    assert historical_price.product == "DALMP"
    assert historical_price.price == 50.0
    assert historical_price.node == "HB_NORTH"


def test_historical_price_to_dataframe_row():
    """Tests the to_dataframe_row method of HistoricalPrice"""
    # Create a HistoricalPrice instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    historical_price = HistoricalPrice(
        timestamp=test_datetime, product="DALMP", price=50.0, node="HB_NORTH"
    )

    # Call to_dataframe_row() on the instance
    row = historical_price.to_dataframe_row()

    # Assert that the returned dictionary contains all expected fields
    assert "timestamp" in row
    assert "product" in row
    assert "price" in row
    assert "node" in row

    # Assert that the field values match the original model
    assert row["timestamp"] == test_datetime
    assert row["product"] == "DALMP"
    assert row["price"] == 50.0
    assert row["node"] == "HB_NORTH"


def test_generation_forecast_initialization():
    """Tests the initialization of GenerationForecast model"""
    # Create a GenerationForecast instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    generation_forecast = GenerationForecast(
        timestamp=test_datetime, fuel_type="WIND", generation_mw=1000.0, region="ERCOT"
    )

    # Assert that all fields are correctly initialized
    assert generation_forecast.timestamp == test_datetime
    assert generation_forecast.fuel_type == "WIND"
    assert generation_forecast.generation_mw == 1000.0
    assert generation_forecast.region == "ERCOT"

    # Assert that the model validates correctly
    assert generation_forecast.timestamp == test_datetime
    assert generation_forecast.fuel_type == "WIND"
    assert generation_forecast.generation_mw == 1000.0
    assert generation_forecast.region == "ERCOT"


def test_generation_forecast_to_dataframe_row():
    """Tests the to_dataframe_row method of GenerationForecast"""
    # Create a GenerationForecast instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    generation_forecast = GenerationForecast(
        timestamp=test_datetime, fuel_type="WIND", generation_mw=1000.0, region="ERCOT"
    )

    # Call to_dataframe_row() on the instance
    row = generation_forecast.to_dataframe_row()

    # Assert that the returned dictionary contains all expected fields
    assert "timestamp" in row
    assert "fuel_type" in row
    assert "generation_mw" in row
    assert "region" in row

    # Assert that the field values match the original model
    assert row["timestamp"] == test_datetime
    assert row["fuel_type"] == "WIND"
    assert row["generation_mw"] == 1000.0
    assert row["region"] == "ERCOT"


def test_price_forecast_initialization():
    """Tests the initialization of PriceForecast model"""
    # Create a PriceForecast instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    price_samples = create_mock_price_samples()
    price_forecast = PriceForecast(
        timestamp=test_datetime,
        product="DALMP",
        point_forecast=50.0,
        samples=price_samples,
        generation_timestamp=test_datetime,
        is_fallback=False,
    )

    # Assert that all fields are correctly initialized
    assert price_forecast.timestamp == test_datetime
    assert price_forecast.product == "DALMP"
    assert price_forecast.point_forecast == 50.0
    assert price_forecast.samples == price_samples
    assert price_forecast.generation_timestamp == test_datetime
    assert price_forecast.is_fallback == False

    # Assert that the model validates correctly
    assert price_forecast.timestamp == test_datetime
    assert price_forecast.product == "DALMP"
    assert price_forecast.point_forecast == 50.0
    assert price_forecast.samples == price_samples
    assert price_forecast.generation_timestamp == test_datetime
    assert price_forecast.is_fallback == False


def test_price_forecast_initialization_with_invalid_product():
    """Tests that PriceForecast initialization fails with invalid product"""
    # Attempt to create a PriceForecast with an invalid product name
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    price_samples = create_mock_price_samples()
    with pytest.raises(ValueError) as excinfo:
        PriceForecast(
            timestamp=test_datetime,
            product="INVALID",
            point_forecast=50.0,
            samples=price_samples,
            generation_timestamp=test_datetime,
            is_fallback=False,
        )

    # Assert that a ValueError is raised
    assert "Invalid product" in str(excinfo.value)

    # Assert that the error message mentions invalid product
    assert "Must be one of" in str(excinfo.value)


def test_price_forecast_initialization_with_invalid_samples_length():
    """Tests that PriceForecast initialization fails with wrong number of samples"""
    # Attempt to create a PriceForecast with too few or too many samples
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    with pytest.raises(ValueError) as excinfo:
        PriceForecast(
            timestamp=test_datetime,
            product="DALMP",
            point_forecast=50.0,
            samples=[1.0] * (PROBABILISTIC_SAMPLE_COUNT + 1),
            generation_timestamp=test_datetime,
            is_fallback=False,
        )

    # Assert that a ValueError is raised
    assert "Sample count" in str(excinfo.value)

    # Assert that the error message mentions sample count
    assert "does not match required count" in str(excinfo.value)


def test_price_forecast_to_dataframe_row():
    """Tests the to_dataframe_row method of PriceForecast"""
    # Create a PriceForecast instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    price_samples = create_mock_price_samples()
    price_forecast = PriceForecast(
        timestamp=test_datetime,
        product="DALMP",
        point_forecast=50.0,
        samples=price_samples,
        generation_timestamp=test_datetime,
        is_fallback=False,
    )

    # Call to_dataframe_row() on the instance
    row = price_forecast.to_dataframe_row()

    # Assert that the returned dictionary contains all expected fields
    assert "timestamp" in row
    assert "product" in row
    assert "point_forecast" in row
    assert "generation_timestamp" in row
    assert "is_fallback" in row

    # Assert that sample columns are correctly named and populated
    sample_columns = create_sample_columns(len(price_samples))
    for i, sample in enumerate(price_samples):
        assert sample_columns[i] in row
        assert row[sample_columns[i]] == sample

    # Assert that the field values match the original model
    assert row["timestamp"] == test_datetime
    assert row["product"] == "DALMP"
    assert row["point_forecast"] == 50.0
    assert row["generation_timestamp"] == test_datetime
    assert row["is_fallback"] == False


def test_price_forecast_from_dataframe_row():
    """Tests the from_dataframe_row method of PriceForecast"""
    # Create a PriceForecast instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    price_samples = create_mock_price_samples()
    price_forecast = PriceForecast(
        timestamp=test_datetime,
        product="DALMP",
        point_forecast=50.0,
        samples=price_samples,
        generation_timestamp=test_datetime,
        is_fallback=False,
    )

    # Convert the instance to a dataframe row
    row_dict = price_forecast.to_dataframe_row()

    # Create a pandas Series from the row dictionary
    row = pandas.Series(row_dict)

    # Call from_dataframe_row() with the Series
    new_price_forecast = PriceForecast.from_dataframe_row(row)

    # Assert that the returned PriceForecast instance has all expected fields
    assert new_price_forecast.timestamp == test_datetime
    assert new_price_forecast.product == "DALMP"
    assert new_price_forecast.point_forecast == 50.0
    assert new_price_forecast.generation_timestamp == test_datetime
    assert new_price_forecast.is_fallback == False

    # Assert that the samples are correctly extracted from sample columns
    assert new_price_forecast.samples == price_samples

    # Assert that the field values match the original Series
    assert new_price_forecast.timestamp == row["timestamp"]
    assert new_price_forecast.product == row["product"]
    assert new_price_forecast.point_forecast == row["point_forecast"]
    assert new_price_forecast.generation_timestamp == row["generation_timestamp"]
    assert new_price_forecast.is_fallback == row["is_fallback"]


def test_price_forecast_get_percentile():
    """Tests the get_percentile method of PriceForecast"""
    # Create a PriceForecast instance with known sample values
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    price_samples = [10.0, 20.0, 30.0, 40.0, 50.0]
    price_forecast = PriceForecast(
        timestamp=test_datetime,
        product="DALMP",
        point_forecast=30.0,
        samples=price_samples,
        generation_timestamp=test_datetime,
        is_fallback=False,
    )

    # Call get_percentile() with various percentile values (0.1, 0.5, 0.9)
    percentile_10 = price_forecast.get_percentile(0.1)
    percentile_50 = price_forecast.get_percentile(0.5)
    percentile_90 = price_forecast.get_percentile(0.9)

    # Assert that the returned percentile values match expected values
    assert percentile_10 == 12.0
    assert percentile_50 == 30.0
    assert percentile_90 == 46.0

    # Test edge cases (0.0, 1.0)
    assert price_forecast.get_percentile(0.0) == 10.0
    assert price_forecast.get_percentile(1.0) == 50.0


def test_price_forecast_get_percentile_invalid_input():
    """Tests that get_percentile fails with invalid percentile values"""
    # Create a PriceForecast instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    price_samples = create_mock_price_samples()
    price_forecast = PriceForecast(
        timestamp=test_datetime,
        product="DALMP",
        point_forecast=50.0,
        samples=price_samples,
        generation_timestamp=test_datetime,
        is_fallback=False,
    )

    # Attempt to call get_percentile() with invalid values (negative, >1)
    with pytest.raises(ValueError) as excinfo:
        price_forecast.get_percentile(-0.1)

    # Assert that a ValueError is raised
    assert "Percentile must be between 0 and 1" in str(excinfo.value)

    # Assert that the error message mentions valid percentile range
    with pytest.raises(ValueError) as excinfo:
        price_forecast.get_percentile(1.1)

    # Assert that a ValueError is raised
    assert "Percentile must be between 0 and 1" in str(excinfo.value)


def test_create_sample_columns():
    """Tests the create_sample_columns utility function"""
    # Call create_sample_columns() with various count values
    columns_5 = create_sample_columns(5)
    columns_10 = create_sample_columns(10)
    columns_100 = create_sample_columns(100)

    # Assert that the returned list has the correct length
    assert len(columns_5) == 5
    assert len(columns_10) == 10
    assert len(columns_100) == 100

    # Assert that column names follow the expected format (sample_XXX)
    assert columns_5[0] == "sample_001"
    assert columns_10[5] == "sample_006"
    assert columns_100[99] == "sample_100"

    # Assert that column names are zero-padded correctly
    assert columns_5[0] == "sample_001"
    assert columns_5[4] == "sample_005"
    assert columns_10[9] == "sample_010"
    assert columns_100[0] == "sample_001"
    assert columns_100[9] == "sample_010"
    assert columns_100[99] == "sample_100"


def test_create_empty_forecast_dataframe():
    """Tests the create_empty_forecast_dataframe utility function"""
    # Call create_empty_forecast_dataframe()
    df = create_empty_forecast_dataframe()

    # Assert that the returned DataFrame has the expected columns
    expected_columns = [
        "timestamp",
        "product",
        "point_forecast",
        "generation_timestamp",
        "is_fallback",
    ]
    sample_columns = create_sample_columns(PROBABILISTIC_SAMPLE_COUNT)
    expected_columns.extend(sample_columns)
    assert list(df.columns) == expected_columns

    # Assert that the DataFrame has the correct column types
    assert df["timestamp"].dtype == "datetime64[ns]"
    assert df["product"].dtype == "string"
    assert df["point_forecast"].dtype == "float64"
    assert df["generation_timestamp"].dtype == "datetime64[ns]"
    assert df["is_fallback"].dtype == "bool"
    for col in sample_columns:
        assert df[col].dtype == "float64"

    # Assert that the DataFrame has 0 rows
    assert len(df) == 0

    # Assert that sample columns match those from create_sample_columns
    assert sample_columns == create_sample_columns(PROBABILISTIC_SAMPLE_COUNT)


@pytest.mark.parametrize(
    "model_class,test_data",
    [
        (
            LoadForecast,
            {
                "timestamp": datetime(2023, 1, 1, 0, 0, 0),
                "load_mw": 1000.0,
                "region": "ERCOT",
            },
        ),
        (
            HistoricalPrice,
            {
                "timestamp": datetime(2023, 1, 1, 0, 0, 0),
                "product": "DALMP",
                "price": 50.0,
                "node": "HB_NORTH",
            },
        ),
        (
            GenerationForecast,
            {
                "timestamp": datetime(2023, 1, 1, 0, 0, 0),
                "fuel_type": "WIND",
                "generation_mw": 1000.0,
                "region": "ERCOT",
            },
        ),
        (
            PriceForecast,
            {
                "timestamp": datetime(2023, 1, 1, 0, 0, 0),
                "product": "DALMP",
                "point_forecast": 50.0,
                "samples": create_mock_price_samples(),
                "generation_timestamp": datetime(2023, 1, 1, 0, 0, 0),
                "is_fallback": False,
            },
        ),
    ],
)
def test_model_roundtrip_conversion(model_class, test_data):
    """Tests roundtrip conversion of models (to_dict -> from_dict)"""
    # Create an instance of the model class with test data
    model_instance = model_class(**test_data)

    # Convert the instance to a dictionary using to_dict()
    model_dict = model_instance.to_dict()

    # Create a new instance from the dictionary using from_dict()
    new_model_instance = model_class.from_dict(model_dict)

    # Assert that the new instance has the same field values as the original
    assert new_model_instance == model_instance


def test_dataframe_roundtrip_conversion():
    """Tests roundtrip conversion of PriceForecast (to_dataframe_row -> from_dataframe_row)"""
    # Create a PriceForecast instance with test data
    test_datetime = datetime(2023, 1, 1, 0, 0, 0)
    price_samples = create_mock_price_samples()
    price_forecast = PriceForecast(
        timestamp=test_datetime,
        product="DALMP",
        point_forecast=50.0,
        samples=price_samples,
        generation_timestamp=test_datetime,
        is_fallback=False,
    )

    # Convert the instance to a dataframe row using to_dataframe_row()
    row_dict = price_forecast.to_dataframe_row()

    # Create a pandas Series from the row dictionary
    row = pandas.Series(row_dict)

    # Create a new instance from the Series using from_dataframe_row()
    new_price_forecast = PriceForecast.from_dataframe_row(row)

    # Assert that the new instance has the same field values as the original
    assert new_price_forecast == price_forecast