"""
Unit tests for validation models in the Electricity Market Price Forecasting System.
This module tests the functionality of validation classes and functions defined in
the validation_models.py module, ensuring proper validation of data structures,
including pandas DataFrames and data model instances, against defined schemas
and validation rules.
"""

import pytest  # pytest: 7.0.0+
import pandas as pd  # pandas: 2.0.0+
import numpy as np  # numpy: 1.24.0+
import pandera as pa  # pandera: 0.16.0+
from datetime import datetime  # standard library

from src.backend.models.validation_models import ValidationResult  # Class representing the result of a validation operation
from src.backend.models.validation_models import DataFrameValidator  # Class for validating pandas DataFrames against schemas
from src.backend.models.validation_models import ModelValidator  # Class for validating data model instances
from src.backend.models.validation_models import validate_dataframe  # Function to validate a pandas DataFrame against a pandera schema
from src.backend.models.validation_models import validate_model  # Function to validate a data model instance against validation rules
from src.backend.models.validation_models import format_validation_errors  # Function to format validation errors into a human-readable string
from src.backend.models.data_models import BaseDataModel  # Base class for all data models
from src.backend.models.data_models import LoadForecast  # Data model for load forecast information
from src.backend.models.data_models import HistoricalPrice  # Data model for historical price information
from src.backend.models.data_models import GenerationForecast  # Data model for generation forecast information
from src.backend.models.data_models import PriceForecast  # Data model for price forecast information
from src.backend.config.settings import FORECAST_PRODUCTS  # List of valid price products for validation
from src.backend.config.schema_config import FORECAST_OUTPUT_SCHEMA  # Schema for validating forecast output data
from src.backend.config.schema_config import LOAD_FORECAST_SCHEMA  # Schema for validating load forecast data
from src.backend.config.schema_config import HISTORICAL_PRICE_SCHEMA  # Schema for validating historical price data
from src.backend.config.schema_config import GENERATION_FORECAST_SCHEMA  # Schema for validating generation forecast data
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_data  # Create mock forecast data for testing
from src.backend.tests.fixtures.load_forecast_fixtures import create_mock_load_forecast_data  # Create mock load forecast data for testing
from src.backend.tests.fixtures.historical_prices_fixtures import create_mock_historical_price_data  # Create mock historical price data for testing
from src.backend.tests.fixtures.generation_forecast_fixtures import create_mock_generation_forecast_data  # Create mock generation forecast data for testing


class TestValidationResult:
    """Test class for ValidationResult"""
    def __init__(self):
        """Initialize TestValidationResult"""
        pass

    def test_initialization(self):
        """Test initialization of ValidationResult"""
        # Create ValidationResult with default parameters
        result1 = ValidationResult(is_valid=True)
        assert result1.is_valid is True
        assert result1.errors == {}
        assert result1.validation_time is not None

        # Create ValidationResult with custom parameters
        errors = {"category1": ["message1", "message2"]}
        validation_time = datetime(2023, 1, 1)
        result2 = ValidationResult(is_valid=False, errors=errors, validation_time=validation_time)
        assert result2.is_valid is False
        assert result2.errors == errors
        assert result2.validation_time == validation_time

    def test_to_dict(self):
        """Test to_dict method of ValidationResult"""
        # Create ValidationResult with known values
        errors = {"category1": ["message1", "message2"]}
        validation_time = datetime(2023, 1, 1, 12, 0, 0)
        result = ValidationResult(is_valid=False, errors=errors, validation_time=validation_time)

        # Call to_dict method
        result_dict = result.to_dict()

        # Assert dictionary contains expected keys and values
        assert "is_valid" in result_dict
        assert "errors" in result_dict
        assert "validation_time" in result_dict
        assert result_dict["is_valid"] is False
        assert result_dict["errors"] == errors
        assert result_dict["validation_time"] == "2023-01-01T12:00:00"

    def test_format_errors(self):
        """Test format_errors method of ValidationResult"""
        # Create ValidationResult with no errors
        result1 = ValidationResult(is_valid=True)
        assert result1.format_errors() == "Validation successful, no errors."

        # Create ValidationResult with errors
        errors = {"category1": ["message1", "message2"], "category2": ["message3"]}
        result2 = ValidationResult(is_valid=False, errors=errors)
        formatted_errors = result2.format_errors()
        assert "category1" in formatted_errors
        assert "message1" in formatted_errors
        assert "message2" in formatted_errors
        assert "category2" in formatted_errors
        assert "message3" in formatted_errors

    def test_add_error(self):
        """Test add_error method of ValidationResult"""
        # Create ValidationResult with no errors
        result = ValidationResult(is_valid=True)

        # Call add_error with category and message
        result.add_error("category1", "message1")
        assert result.is_valid is False
        assert "category1" in result.errors
        assert "message1" in result.errors["category1"]

        # Add another error to same category
        result.add_error("category1", "message2")
        assert "message2" in result.errors["category1"]

        # Add error to a new category
        result.add_error("category2", "message3")
        assert "category2" in result.errors
        assert "message3" in result.errors["category2"]

    def test_merge(self):
        """Test merge method of ValidationResult"""
        # Create two ValidationResult objects
        result1 = ValidationResult(is_valid=True)
        result2 = ValidationResult(is_valid=False, errors={"category1": ["message1"]})

        # Call merge method
        result1.merge(result2)

        # Assert is_valid is logical AND of both results
        assert result1.is_valid is False

        # Assert errors contains all errors from both results
        assert "category1" in result1.errors
        assert "message1" in result1.errors["category1"]

        # Assert validation_time is most recent of both
        assert result1.validation_time >= result2.validation_time

class TestDataFrameValidator:
    """Test class for DataFrameValidator"""
    def __init__(self):
        """Initialize TestDataFrameValidator"""
        pass

    def test_validate_forecast_output(self):
        """Test validate_forecast_output method"""
        # Create DataFrameValidator instance
        validator = DataFrameValidator()

        # Create valid forecast DataFrame
        valid_df = create_mock_forecast_data(start_time=datetime(2023, 1, 1))

        # Call validate_forecast_output
        result = validator.validate_forecast_output(valid_df)

        # Assert result is valid
        assert result.is_valid is True

        # Create invalid forecast DataFrame
        invalid_df = create_mock_forecast_data(start_time=datetime(2023, 1, 1))
        invalid_df['point_forecast'] = 'invalid'

        # Call validate_forecast_output
        result = validator.validate_forecast_output(invalid_df)

        # Assert result is invalid with appropriate errors
        assert result.is_valid is False
        assert 'point_forecast' in result.errors

    def test_validate_load_forecast(self):
        """Test validate_load_forecast method"""
        # Create DataFrameValidator instance
        validator = DataFrameValidator()

        # Create valid load forecast DataFrame
        valid_df = create_mock_load_forecast_data(start_time=datetime(2023, 1, 1))

        # Call validate_load_forecast
        result = validator.validate_load_forecast(valid_df)

        # Assert result is valid
        assert result.is_valid is True

        # Create invalid load forecast DataFrame
        invalid_df = create_mock_load_forecast_data(start_time=datetime(2023, 1, 1))
        invalid_df['load_mw'] = -100

        # Call validate_load_forecast
        result = validator.validate_load_forecast(invalid_df)

        # Assert result is invalid with appropriate errors
        assert result.is_valid is False
        assert 'load_mw' in result.errors

    def test_validate_historical_prices(self):
        """Test validate_historical_prices method"""
        # Create DataFrameValidator instance
        validator = DataFrameValidator()

        # Create valid historical prices DataFrame
        valid_df = create_mock_historical_price_data(start_date=datetime(2023, 1, 1), end_date=datetime(2023, 1, 2))

        # Call validate_historical_prices
        result = validator.validate_historical_prices(valid_df)

        # Assert result is valid
        assert result.is_valid is True

        # Create invalid historical prices DataFrame
        invalid_df = create_mock_historical_price_data(start_date=datetime(2023, 1, 1), end_date=datetime(2023, 1, 2))
        invalid_df['price'] = 'invalid'

        # Call validate_historical_prices
        result = validator.validate_historical_prices(invalid_df)

        # Assert result is invalid with appropriate errors
        assert result.is_valid is False
        assert 'price' in result.errors

    def test_validate_generation_forecast(self):
        """Test validate_generation_forecast method"""
        # Create DataFrameValidator instance
        validator = DataFrameValidator()

        # Create valid generation forecast DataFrame
        valid_df = create_mock_generation_forecast_data(start_time=datetime(2023, 1, 1))

        # Call validate_generation_forecast
        result = validator.validate_generation_forecast(valid_df)

        # Assert result is valid
        assert result.is_valid is True

        # Create invalid generation forecast DataFrame
        invalid_df = create_mock_generation_forecast_data(start_time=datetime(2023, 1, 1))
        invalid_df['generation_mw'] = -100

        # Call validate_generation_forecast
        result = validator.validate_generation_forecast(invalid_df)

        # Assert result is invalid with appropriate errors
        assert result.is_valid is False
        assert 'generation_mw' in result.errors

class TestModelValidator:
    """Test class for ModelValidator"""
    def __init__(self):
        """Initialize TestModelValidator"""
        pass

    def test_validate_load_forecast(self):
        """Test validate_load_forecast method"""
        # Create ModelValidator instance
        validator = ModelValidator()

        # Create valid LoadForecast model
        valid_model = LoadForecast(timestamp=datetime(2023, 1, 1), load_mw=1000, region='ERCOT')

        # Call validate_load_forecast
        result = validator.validate_load_forecast(valid_model)

        # Assert result is valid
        assert result.is_valid is True

        # Create invalid LoadForecast model
        invalid_model = LoadForecast(timestamp=datetime(2023, 1, 1), load_mw=-100, region='')

        # Call validate_load_forecast
        result = validator.validate_load_forecast(invalid_model)

        # Assert result is invalid with appropriate errors
        assert result.is_valid is False
        assert 'load_mw' in result.errors
        assert 'region' in result.errors

    def test_validate_historical_price(self):
        """Test validate_historical_price method"""
        # Create ModelValidator instance
        validator = ModelValidator()

        # Create valid HistoricalPrice model
        valid_model = HistoricalPrice(timestamp=datetime(2023, 1, 1), product='DALMP', price=100, node='HB_NORTH')

        # Call validate_historical_price
        result = validator.validate_historical_price(valid_model)

        # Assert result is valid
        assert result.is_valid is True

        # Create invalid HistoricalPrice model
        invalid_model = HistoricalPrice(timestamp=datetime(2023, 1, 1), product='invalid', price=100, node='')

        # Call validate_historical_price
        result = validator.validate_historical_price(invalid_model)

        # Assert result is invalid with appropriate errors
        assert result.is_valid is False
        assert 'product' in result.errors
        assert 'node' in result.errors

    def test_validate_generation_forecast(self):
        """Test validate_generation_forecast method"""
        # Create ModelValidator instance
        validator = ModelValidator()

        # Create valid GenerationForecast model
        valid_model = GenerationForecast(timestamp=datetime(2023, 1, 1), fuel_type='WIND', generation_mw=1000, region='ERCOT')

        # Call validate_generation_forecast
        result = validator.validate_generation_forecast(valid_model)

        # Assert result is valid
        assert result.is_valid is True

        # Create invalid GenerationForecast model
        invalid_model = GenerationForecast(timestamp=datetime(2023, 1, 1), fuel_type='', generation_mw=-100, region='ERCOT')

        # Call validate_generation_forecast
        result = validator.validate_generation_forecast(invalid_model)

        # Assert result is invalid with appropriate errors
        assert result.is_valid is False
        assert 'fuel_type' in result.errors
        assert 'generation_mw' in result.errors

    def test_validate_price_forecast(self):
        """Test validate_price_forecast method"""
        # Create ModelValidator instance
        validator = ModelValidator()

        # Create valid PriceForecast model
        valid_model = PriceForecast(timestamp=datetime(2023, 1, 1), product='DALMP', point_forecast=100, samples=[100] * 100, generation_timestamp=datetime(2023, 1, 1))

        # Call validate_price_forecast
        result = validator.validate_price_forecast(valid_model)

        # Assert result is valid
        assert result.is_valid is True

        # Create invalid PriceForecast model
        invalid_model = PriceForecast(timestamp=datetime(2023, 1, 1), product='invalid', point_forecast=100, samples=[100] * 50, generation_timestamp=datetime(2023, 1, 1))

        # Call validate_price_forecast
        result = validator.validate_price_forecast(invalid_model)

        # Assert result is invalid with appropriate errors
        assert result.is_valid is False
        assert 'product' in result.errors
        assert 'samples' in result.errors

    def test_validate_common_fields(self):
        """Tests the ModelValidator.validate_common_fields method"""
        # Create a ModelValidator instance
        validator = ModelValidator()

        # Create a valid BaseDataModel instance
        class ValidModel(BaseDataModel):
            timestamp: datetime

        valid_model = ValidModel(timestamp=datetime(2023, 1, 1))

        # Call validate_common_fields with the model
        errors = validator.validate_common_fields(valid_model)

        # Assert that the returned errors dictionary is empty
        assert not errors

        # Create a model with missing timestamp
        class InvalidModel(BaseDataModel):
            pass

        invalid_model = InvalidModel()

        # Call validate_common_fields with the invalid model
        errors = validator.validate_common_fields(invalid_model)

        # Assert that errors contains appropriate error messages
        assert 'timestamp' in errors
        assert "Timestamp is required" in errors['timestamp']

class TestValidationFunctions:
    """Test class for validation utility functions"""
    def __init__(self):
        """Initialize TestValidationFunctions"""
        pass

    def test_validate_dataframe(self):
        """Test validate_dataframe function"""
        # Create valid DataFrame
        valid_df = create_mock_forecast_data(start_time=datetime(2023, 1, 1))

        # Call validate_dataframe with appropriate schema
        result = validate_dataframe(valid_df, FORECAST_OUTPUT_SCHEMA)

        # Assert result is valid
        assert result.is_valid is True

        # Create invalid DataFrame
        invalid_df = create_mock_forecast_data(start_time=datetime(2023, 1, 1))
        invalid_df['point_forecast'] = 'invalid'

        # Call validate_dataframe
        result = validate_dataframe(invalid_df, FORECAST_OUTPUT_SCHEMA)

        # Assert result is invalid with appropriate errors
        assert result.is_valid is False
        assert 'point_forecast' in result.errors

    def test_validate_model(self):
        """Test validate_model function"""
        # Create valid instances of different model types
        valid_load_forecast = LoadForecast(timestamp=datetime(2023, 1, 1), load_mw=1000, region='ERCOT')
        valid_historical_price = HistoricalPrice(timestamp=datetime(2023, 1, 1), product='DALMP', price=100, node='HB_NORTH')
        valid_generation_forecast = GenerationForecast(timestamp=datetime(2023, 1, 1), fuel_type='WIND', generation_mw=1000, region='ERCOT')
        valid_price_forecast = PriceForecast(timestamp=datetime(2023, 1, 1), product='DALMP', point_forecast=100, samples=[100] * 100, generation_timestamp=datetime(2023, 1, 1))

        # Call validate_model with each model
        result_load_forecast = validate_model(valid_load_forecast)
        result_historical_price = validate_model(valid_historical_price)
        result_generation_forecast = validate_model(valid_generation_forecast)
        result_price_forecast = validate_model(valid_price_forecast)

        # Assert that the returned ValidationResult has is_valid=True for valid models
        assert result_load_forecast.is_valid is True
        assert result_historical_price.is_valid is True
        assert result_generation_forecast.is_valid is True
        assert result_price_forecast.is_valid is True

        # Create invalid instances of different model types
        invalid_load_forecast = LoadForecast(timestamp=datetime(2023, 1, 1), load_mw=-100, region='')
        invalid_historical_price = HistoricalPrice(timestamp=datetime(2023, 1, 1), product='invalid', price=100, node='')
        invalid_generation_forecast = GenerationForecast(timestamp=datetime(2023, 1, 1), fuel_type='', generation_mw=-100, region='ERCOT')
        invalid_price_forecast = PriceForecast(timestamp=datetime(2023, 1, 1), product='invalid', point_forecast=100, samples=[100] * 50, generation_timestamp=datetime(2023, 1, 1))

        # Call validate_model with each invalid model
        result_invalid_load_forecast = validate_model(invalid_load_forecast)
        result_invalid_historical_price = validate_model(invalid_historical_price)
        result_invalid_generation_forecast = validate_model(invalid_generation_forecast)
        result_invalid_price_forecast = validate_model(invalid_price_forecast)

        # Assert that the returned ValidationResult has is_valid=False for invalid models
        assert result_invalid_load_forecast.is_valid is False
        assert result_invalid_historical_price.is_valid is False
        assert result_invalid_generation_forecast.is_valid is False
        assert result_invalid_price_forecast.is_valid is False

        # Assert that errors contains appropriate error messages
        assert 'load_mw' in result_invalid_load_forecast.errors
        assert 'product' in result_invalid_historical_price.errors
        assert 'fuel_type' in result_invalid_generation_forecast.errors
        assert 'product' in result_invalid_price_forecast.errors

    def test_format_validation_errors(self):
        """Test format_validation_errors function"""
        # Create errors dictionary with various error types
        errors = {
            "category1": ["message1", "message2"],
            "category2": ["message3"]
        }

        # Call format_validation_errors
        formatted_string = format_validation_errors(errors)

        # Assert formatted string contains all errors
        assert "category1" in formatted_string
        assert "message1" in formatted_string
        assert "message2" in formatted_string
        assert "category2" in formatted_string
        assert "message3" in formatted_string

        # Test with empty dictionary
        empty_errors = {}
        empty_string = format_validation_errors(empty_errors)
        assert empty_string == "No validation errors."