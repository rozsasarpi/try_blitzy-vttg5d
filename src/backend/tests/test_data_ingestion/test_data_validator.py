"""
Unit tests for the data_validator module in the data ingestion pipeline of the 
Electricity Market Price Forecasting System. This test file validates the 
functionality of data validation components, ensuring they correctly identify
valid, invalid, and incomplete data across different data sources.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Import the components to test
from ../../data_ingestion.data_validator import (
    DataValidator,
    validate_load_forecast_data,
    validate_historical_prices_data,
    validate_generation_forecast_data,
    check_timestamp_coverage,
    validate_data_consistency
)

# Import exceptions and models
from ../../data_ingestion.exceptions import DataValidationError, MissingDataError
from ../../models.validation_models import ValidationResult
from ../../models.data_models import LoadForecast, HistoricalPrice, GenerationForecast

# Import constants and utility functions
from ../../config.settings import FORECAST_PRODUCTS
from ../../utils.date_utils import localize_to_cst

# Import test fixtures
from ..fixtures.load_forecast_fixtures import (
    create_mock_load_forecast_data,
    create_incomplete_load_forecast_data,
    create_invalid_load_forecast_data,
    create_mock_load_forecast_models
)
from ..fixtures.historical_prices_fixtures import (
    create_mock_historical_price_data,
    create_incomplete_historical_price_data,
    create_invalid_historical_price_data,
    create_mock_historical_price_models
)
from ..fixtures.generation_forecast_fixtures import (
    create_mock_generation_forecast_data,
    create_incomplete_generation_forecast_data,
    create_invalid_generation_forecast_data,
    create_mock_generation_forecast_models
)


# Tests for individual validation functions
def test_validate_load_forecast_data_valid():
    """Tests that validate_load_forecast_data correctly validates valid load forecast data"""
    # Create valid mock load forecast data
    df = create_mock_load_forecast_data(
        start_time=datetime.now(),
        hours=24
    )
    
    # Call the validation function
    result = validate_load_forecast_data(df)
    
    # Assert validation successful
    assert result.is_valid
    assert not result.errors  # Should be empty dictionary


def test_validate_load_forecast_data_missing_data():
    """Tests that validate_load_forecast_data correctly handles missing data"""
    with pytest.raises(MissingDataError):
        validate_load_forecast_data(None)
        

def test_validate_load_forecast_data_missing_columns():
    """Tests that validate_load_forecast_data correctly identifies missing columns"""
    # Create valid mock data and then remove a column
    df = create_mock_load_forecast_data(
        start_time=datetime.now(),
        hours=24
    )
    
    # Remove a required column
    df_invalid = df.drop(columns=['load_mw'])
    
    with pytest.raises(DataValidationError):
        validate_load_forecast_data(df_invalid)
        

def test_validate_load_forecast_data_invalid_values():
    """Tests that validate_load_forecast_data correctly identifies invalid values"""
    # Create invalid load forecast data with negative load values
    df = create_invalid_load_forecast_data(
        start_time=datetime.now(),
        hours=24,
        invalid_indices={0: {'load_mw': -100.0}}
    )
    
    # Validate the data
    result = validate_load_forecast_data(df)
    
    # Assert validation failure
    assert not result.is_valid
    assert 'range_error' in result.errors


def test_validate_historical_prices_data_valid():
    """Tests that validate_historical_prices_data correctly validates valid historical price data"""
    # Create valid mock historical price data
    df = create_mock_historical_price_data(
        start_date=datetime.now() - timedelta(days=7),
        end_date=datetime.now()
    )
    
    # Call the validation function
    result = validate_historical_prices_data(df)
    
    # Assert validation successful
    assert result.is_valid
    assert not result.errors  # Should be empty dictionary


def test_validate_historical_prices_data_missing_data():
    """Tests that validate_historical_prices_data correctly handles missing data"""
    with pytest.raises(MissingDataError):
        validate_historical_prices_data(None)
        

def test_validate_historical_prices_data_missing_columns():
    """Tests that validate_historical_prices_data correctly identifies missing columns"""
    # Create valid mock data and then remove a column
    df = create_mock_historical_price_data(
        start_date=datetime.now() - timedelta(days=7),
        end_date=datetime.now()
    )
    
    # Remove a required column
    df_invalid = df.drop(columns=['product'])
    
    with pytest.raises(DataValidationError):
        validate_historical_prices_data(df_invalid)
        

def test_validate_historical_prices_data_invalid_product():
    """Tests that validate_historical_prices_data correctly identifies invalid product values"""
    # Create invalid historical price data with invalid product values
    df = create_invalid_historical_price_data(
        start_date=datetime.now() - timedelta(days=7),
        end_date=datetime.now(),
        invalid_type='invalid_product'
    )
    
    # Validate the data
    result = validate_historical_prices_data(df)
    
    # Assert validation failure
    assert not result.is_valid
    assert 'product_error' in result.errors


def test_validate_generation_forecast_data_valid():
    """Tests that validate_generation_forecast_data correctly validates valid generation forecast data"""
    # Create valid mock generation forecast data
    df = create_mock_generation_forecast_data(
        start_time=datetime.now(),
        hours=24
    )
    
    # Call the validation function
    result = validate_generation_forecast_data(df)
    
    # Assert validation successful
    assert result.is_valid
    assert not result.errors  # Should be empty dictionary


def test_validate_generation_forecast_data_missing_data():
    """Tests that validate_generation_forecast_data correctly handles missing data"""
    with pytest.raises(MissingDataError):
        validate_generation_forecast_data(None)
        

def test_validate_generation_forecast_data_missing_columns():
    """Tests that validate_generation_forecast_data correctly identifies missing columns"""
    # Create valid mock data and then remove a column
    df = create_mock_generation_forecast_data(
        start_time=datetime.now(),
        hours=24
    )
    
    # Remove a required column
    df_invalid = df.drop(columns=['generation_mw'])
    
    with pytest.raises(DataValidationError):
        validate_generation_forecast_data(df_invalid)
        

def test_validate_generation_forecast_data_invalid_values():
    """Tests that validate_generation_forecast_data correctly identifies invalid values"""
    # Create invalid generation forecast data with negative generation values
    df = create_invalid_generation_forecast_data(
        start_time=datetime.now(),
        hours=24,
        invalid_indices={0: {'generation_mw': -100.0}}
    )
    
    # Validate the data
    result = validate_generation_forecast_data(df)
    
    # Assert validation failure
    assert not result.is_valid
    assert 'range_error' in result.errors


def test_check_timestamp_coverage_complete():
    """Tests that check_timestamp_coverage correctly identifies complete time coverage"""
    # Create mock data with timestamps covering the required range
    now = datetime.now()
    start_time = localize_to_cst(now)
    end_time = localize_to_cst(now + timedelta(hours=24))
    
    # Create data with timestamps from start_time to end_time + 6 hours (to ensure coverage)
    df = create_mock_load_forecast_data(
        start_time=start_time - timedelta(hours=1),  # Start 1 hour earlier
        hours=26  # Cover 26 hours to ensure coverage of the 24-hour range
    )
    
    # Check timestamp coverage
    result = check_timestamp_coverage(df, start_time, end_time)
    
    # Assert validation successful
    assert result.is_valid


def test_check_timestamp_coverage_incomplete():
    """Tests that check_timestamp_coverage correctly identifies incomplete time coverage"""
    # Create mock data with timestamps not covering the required range
    now = datetime.now()
    start_time = localize_to_cst(now)
    end_time = localize_to_cst(now + timedelta(hours=24))
    
    # Create data that doesn't cover the full range
    df = create_mock_load_forecast_data(
        start_time=start_time + timedelta(hours=2),  # Start 2 hours later (missing coverage)
        hours=20  # Only cover 20 hours (missing coverage at the end)
    )
    
    # Check timestamp coverage
    result = check_timestamp_coverage(df, start_time, end_time)
    
    # Assert validation failure
    assert not result.is_valid
    assert 'coverage_error' in result.errors


def test_validate_data_consistency_consistent():
    """Tests that validate_data_consistency correctly identifies consistent data across sources"""
    # Create mock data for all three sources with matching timestamps
    now = datetime.now()
    start_time = localize_to_cst(now)
    hours = 24
    
    # Create load forecast data
    load_df = create_mock_load_forecast_data(
        start_time=start_time,
        hours=hours
    )
    
    # Create historical price data with matching timestamps
    price_df = create_mock_historical_price_data(
        start_date=start_time,
        end_date=start_time + timedelta(hours=hours-1)
    )
    
    # Create generation forecast data with matching timestamps
    gen_df = create_mock_generation_forecast_data(
        start_time=start_time,
        hours=hours
    )
    
    # Validate consistency
    result = validate_data_consistency(load_df, price_df, gen_df)
    
    # Assert validation successful
    assert result.is_valid


def test_validate_data_consistency_inconsistent():
    """Tests that validate_data_consistency correctly identifies inconsistent data across sources"""
    # Create mock data for sources with non-matching timestamps
    now = datetime.now()
    start_time = localize_to_cst(now)
    
    # Create load forecast data
    load_df = create_mock_load_forecast_data(
        start_time=start_time,
        hours=24
    )
    
    # Create historical price data with different timestamps
    price_df = create_mock_historical_price_data(
        start_date=start_time - timedelta(hours=12),  # Start 12 hours earlier
        end_date=start_time  # End where load forecast starts
    )
    
    # Create generation forecast data with different timestamps
    gen_df = create_mock_generation_forecast_data(
        start_time=start_time + timedelta(hours=6),  # Start 6 hours later
        hours=24
    )
    
    # Validate consistency
    result = validate_data_consistency(load_df, price_df, gen_df)
    
    # Assert validation failure
    assert not result.is_valid
    assert 'consistency_error' in result.errors


# Tests for DataValidator class methods
def test_data_validator_validate_load_forecast():
    """Tests the DataValidator.validate_load_forecast method"""
    # Create validator instance
    validator = DataValidator()
    
    # Create valid mock data
    df = create_mock_load_forecast_data(
        start_time=datetime.now(),
        hours=24
    )
    
    # Validate the data
    result = validator.validate_load_forecast(df)
    
    # Assert validation successful
    assert result.is_valid


def test_data_validator_validate_historical_prices():
    """Tests the DataValidator.validate_historical_prices method"""
    # Create validator instance
    validator = DataValidator()
    
    # Create valid mock data
    df = create_mock_historical_price_data(
        start_date=datetime.now() - timedelta(days=7),
        end_date=datetime.now()
    )
    
    # Validate the data
    result = validator.validate_historical_prices(df)
    
    # Assert validation successful
    assert result.is_valid


def test_data_validator_validate_generation_forecast():
    """Tests the DataValidator.validate_generation_forecast method"""
    # Create validator instance
    validator = DataValidator()
    
    # Create valid mock data
    df = create_mock_generation_forecast_data(
        start_time=datetime.now(),
        hours=24
    )
    
    # Validate the data
    result = validator.validate_generation_forecast(df)
    
    # Assert validation successful
    assert result.is_valid


def test_data_validator_validate_data_sources():
    """Tests the DataValidator.validate_data_sources method"""
    # Create validator instance
    validator = DataValidator()
    
    # Create valid mock data for all three sources with matching timestamps
    now = datetime.now()
    start_time = localize_to_cst(now)
    hours = 24
    
    # Create load forecast data
    load_df = create_mock_load_forecast_data(
        start_time=start_time,
        hours=hours
    )
    
    # Create historical price data with matching timestamps
    price_df = create_mock_historical_price_data(
        start_date=start_time,
        end_date=start_time + timedelta(hours=hours-1)
    )
    
    # Create generation forecast data with matching timestamps
    gen_df = create_mock_generation_forecast_data(
        start_time=start_time,
        hours=hours
    )
    
    # Validate all data sources
    result = validator.validate_data_sources(load_df, price_df, gen_df)
    
    # Assert validation successful
    assert result.is_valid


def test_data_validator_validate_time_range_coverage():
    """Tests the DataValidator.validate_time_range_coverage method"""
    # Create validator instance
    validator = DataValidator()
    
    # Create mock data with timestamps covering the required range
    now = datetime.now()
    start_time = localize_to_cst(now)
    end_time = localize_to_cst(now + timedelta(hours=24))
    
    # Create data with timestamps from start_time to end_time + 6 hours (to ensure coverage)
    df = create_mock_load_forecast_data(
        start_time=start_time - timedelta(hours=1),  # Start 1 hour earlier
        hours=26  # Cover 26 hours to ensure coverage of the 24-hour range
    )
    
    # Check timestamp coverage
    result = validator.validate_time_range_coverage(df, start_time, end_time)
    
    # Assert validation successful
    assert result.is_valid


def test_data_validator_create_model_instances():
    """Tests the DataValidator.create_model_instances method"""
    # Create validator instance
    validator = DataValidator()
    
    # Create valid mock data
    df = create_mock_load_forecast_data(
        start_time=datetime.now(),
        hours=24
    )
    
    # Create model instances
    models = validator.create_model_instances(df, 'load_forecast')
    
    # Assert models created successfully
    assert len(models) == len(df)
    assert isinstance(models[0], LoadForecast)


def test_data_validator_error_handling():
    """Tests that DataValidator methods handle errors appropriately"""
    # Create validator instance
    validator = DataValidator()
    
    # Create a mock that will raise an exception
    with patch('../../data_ingestion.data_validator.validate_load_forecast_data') as mock_validate:
        mock_validate.side_effect = Exception("Test exception")
        
        # Create valid mock data
        df = create_mock_load_forecast_data(
            start_time=datetime.now(),
            hours=24
        )
        
        # Call method that should handle the exception
        result = validator.validate_load_forecast(df)
        
        # Assert validation failure
        assert not result.is_valid
        assert 'validation_error' in result.errors


# Class-based tests for more structured testing
class TestDataValidator:
    """Test class for the DataValidator class and related functions"""
    
    def setUp(self):
        """Set up test fixtures before each test"""
        self.validator = DataValidator()
        
        # Set up common test data
        now = datetime.now()
        self.start_time = localize_to_cst(now)
        self.end_time = localize_to_cst(now + timedelta(hours=24))
    
    def test_validate_load_forecast_valid_data(self):
        """Test validation of valid load forecast data"""
        df = create_mock_load_forecast_data(
            start_time=self.start_time,
            hours=24
        )
        
        result = self.validator.validate_load_forecast(df)
        
        assert result.is_valid
        assert not result.errors
    
    def test_validate_load_forecast_invalid_data(self):
        """Test validation of invalid load forecast data"""
        df = create_invalid_load_forecast_data(
            start_time=self.start_time,
            hours=24,
            invalid_indices={0: {'load_mw': -100.0}}
        )
        
        result = self.validator.validate_load_forecast(df)
        
        assert not result.is_valid
        assert 'range_error' in result.errors
    
    def test_validate_historical_prices_valid_data(self):
        """Test validation of valid historical price data"""
        df = create_mock_historical_price_data(
            start_date=self.start_time - timedelta(days=7),
            end_date=self.start_time
        )
        
        result = self.validator.validate_historical_prices(df)
        
        assert result.is_valid
        assert not result.errors
    
    def test_validate_historical_prices_invalid_data(self):
        """Test validation of invalid historical price data"""
        df = create_invalid_historical_price_data(
            start_date=self.start_time - timedelta(days=7),
            end_date=self.start_time,
            invalid_type='invalid_product'
        )
        
        result = self.validator.validate_historical_prices(df)
        
        assert not result.is_valid
        assert 'product_error' in result.errors
    
    def test_validate_generation_forecast_valid_data(self):
        """Test validation of valid generation forecast data"""
        df = create_mock_generation_forecast_data(
            start_time=self.start_time,
            hours=24
        )
        
        result = self.validator.validate_generation_forecast(df)
        
        assert result.is_valid
        assert not result.errors
    
    def test_validate_generation_forecast_invalid_data(self):
        """Test validation of invalid generation forecast data"""
        df = create_invalid_generation_forecast_data(
            start_time=self.start_time,
            hours=24,
            invalid_indices={0: {'generation_mw': -100.0}}
        )
        
        result = self.validator.validate_generation_forecast(df)
        
        assert not result.is_valid
        assert 'range_error' in result.errors
    
    def test_validate_data_sources_all_valid(self):
        """Test validation of all data sources when all are valid"""
        # Create valid mock data for all sources
        load_df = create_mock_load_forecast_data(
            start_time=self.start_time,
            hours=24
        )
        
        price_df = create_mock_historical_price_data(
            start_date=self.start_time,
            end_date=self.start_time + timedelta(hours=23)
        )
        
        gen_df = create_mock_generation_forecast_data(
            start_time=self.start_time,
            hours=24
        )
        
        result = self.validator.validate_data_sources(load_df, price_df, gen_df)
        
        assert result.is_valid
        assert not result.errors
    
    def test_validate_data_sources_some_invalid(self):
        """Test validation of all data sources when some are invalid"""
        # Create valid mock data for load and generation
        load_df = create_mock_load_forecast_data(
            start_time=self.start_time,
            hours=24
        )
        
        # Create invalid historical price data
        price_df = create_invalid_historical_price_data(
            start_date=self.start_time,
            end_date=self.start_time + timedelta(hours=23),
            invalid_type='invalid_product'
        )
        
        gen_df = create_mock_generation_forecast_data(
            start_time=self.start_time,
            hours=24
        )
        
        result = self.validator.validate_data_sources(load_df, price_df, gen_df)
        
        assert not result.is_valid
        assert 'product_error' in result.errors
    
    def test_validate_time_range_coverage_complete(self):
        """Test validation of time range coverage when complete"""
        df = create_mock_load_forecast_data(
            start_time=self.start_time - timedelta(hours=1),
            hours=26
        )
        
        result = self.validator.validate_time_range_coverage(df, self.start_time, self.end_time)
        
        assert result.is_valid
        assert not result.errors
    
    def test_validate_time_range_coverage_incomplete(self):
        """Test validation of time range coverage when incomplete"""
        df = create_mock_load_forecast_data(
            start_time=self.start_time + timedelta(hours=2),
            hours=20
        )
        
        result = self.validator.validate_time_range_coverage(df, self.start_time, self.end_time)
        
        assert not result.is_valid
        assert 'coverage_error' in result.errors
    
    def test_create_model_instances_load_forecast(self):
        """Test creation of LoadForecast model instances"""
        df = create_mock_load_forecast_data(
            start_time=self.start_time,
            hours=24
        )
        
        models = self.validator.create_model_instances(df, 'load_forecast')
        
        assert len(models) == len(df)
        assert all(isinstance(model, LoadForecast) for model in models)
        assert models[0].timestamp == df.iloc[0]['timestamp']
        assert models[0].load_mw == df.iloc[0]['load_mw']
        assert models[0].region == df.iloc[0]['region']
    
    def test_create_model_instances_historical_price(self):
        """Test creation of HistoricalPrice model instances"""
        df = create_mock_historical_price_data(
            start_date=self.start_time,
            end_date=self.start_time + timedelta(hours=23)
        )
        
        models = self.validator.create_model_instances(df, 'historical_price')
        
        assert len(models) == len(df)
        assert all(isinstance(model, HistoricalPrice) for model in models)
        assert models[0].timestamp == df.iloc[0]['timestamp']
        assert models[0].product == df.iloc[0]['product']
        assert models[0].price == df.iloc[0]['price']
        assert models[0].node == df.iloc[0]['node']
    
    def test_create_model_instances_generation_forecast(self):
        """Test creation of GenerationForecast model instances"""
        df = create_mock_generation_forecast_data(
            start_time=self.start_time,
            hours=24
        )
        
        models = self.validator.create_model_instances(df, 'generation_forecast')
        
        assert len(models) == len(df)
        assert all(isinstance(model, GenerationForecast) for model in models)
        assert models[0].timestamp == df.iloc[0]['timestamp']
        assert models[0].fuel_type == df.iloc[0]['fuel_type']
        assert models[0].generation_mw == df.iloc[0]['generation_mw']
        assert models[0].region == df.iloc[0]['region']
    
    def test_create_model_instances_invalid_data(self):
        """Test creation of model instances with invalid data"""
        df = create_invalid_load_forecast_data(
            start_time=self.start_time,
            hours=24,
            invalid_indices={0: {'load_mw': -100.0}}
        )
        
        models = self.validator.create_model_instances(df, 'load_forecast')
        
        # Should return empty list for invalid data
        assert len(models) == 0
    
    def test_create_model_instances_invalid_type(self):
        """Test creation of model instances with invalid model type"""
        df = create_mock_load_forecast_data(
            start_time=self.start_time,
            hours=24
        )
        
        with pytest.raises(ValueError):
            self.validator.create_model_instances(df, 'invalid_type')