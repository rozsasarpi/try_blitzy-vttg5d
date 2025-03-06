"""
Implements data validation functionality for the data ingestion pipeline of the Electricity Market Price Forecasting System.
This module validates input data from external sources against defined schemas and validation rules to ensure data quality 
before further processing.
"""

# Standard library imports
from typing import List, Dict, Any, Optional
from datetime import datetime

# External imports
import pandas as pd  # version: 2.0.0+
import pandera as pa  # version: 0.16.0+

# Internal imports
from .exceptions import DataValidationError, MissingDataError
from ..config.schema_config import (
    LOAD_FORECAST_SCHEMA,
    HISTORICAL_PRICE_SCHEMA,
    GENERATION_FORECAST_SCHEMA
)
from ..config.settings import FORECAST_PRODUCTS
from ..models.data_models import (
    LoadForecast,
    HistoricalPrice,
    GenerationForecast
)
from ..models.validation_models import ValidationResult
from ..utils.validation_utils import (
    validate_dataframe,
    check_required_columns,
    check_value_ranges
)
from ..utils.logging_utils import get_logger

# Configure logger
logger = get_logger(__name__)

# Constants for required columns in each data source
REQUIRED_LOAD_FORECAST_COLUMNS = ["timestamp", "load_mw", "region"]
REQUIRED_HISTORICAL_PRICE_COLUMNS = ["timestamp", "product", "price", "node"]
REQUIRED_GENERATION_FORECAST_COLUMNS = ["timestamp", "fuel_type", "generation_mw", "region"]


def validate_load_forecast_data(df: pd.DataFrame) -> ValidationResult:
    """
    Validates load forecast data against schema and validation rules.
    
    Args:
        df: DataFrame containing load forecast data
        
    Returns:
        ValidationResult: Validation result with success status and errors
    """
    logger.info("Validating load forecast data")
    
    # Check if dataframe is None or empty
    if df is None or df.empty:
        logger.error("Load forecast data is missing or empty")
        raise MissingDataError("load_forecast", ["entire dataframe"])
    
    # Check for required columns
    missing_columns = [col for col in REQUIRED_LOAD_FORECAST_COLUMNS if col not in df.columns]
    if missing_columns:
        logger.error(f"Load forecast data missing required columns: {missing_columns}")
        raise DataValidationError("load_forecast", [f"Missing required columns: {missing_columns}"])
    
    # Validate against schema
    try:
        validity, schema_errors = validate_dataframe(df, LOAD_FORECAST_SCHEMA)
        
        # Initialize validation result
        errors = schema_errors.copy() if not validity else {}
        is_valid = validity
        
        # Check value ranges if schema validation passed
        if is_valid:
            # Check that load_mw is positive
            negative_loads = df[df["load_mw"] <= 0]
            if not negative_loads.empty:
                is_valid = False
                errors["range_error"] = [f"Load must be positive, found {len(negative_loads)} non-positive values"]
                logger.error(f"Range validation failed for load forecast data: {len(negative_loads)} non-positive values")
        
        return ValidationResult(is_valid=is_valid, errors=errors)
    except Exception as e:
        logger.error(f"Unexpected error during load forecast validation: {str(e)}")
        return ValidationResult(
            is_valid=False,
            errors={"validation_error": [f"Unexpected error during validation: {str(e)}"]}
        )


def validate_historical_prices_data(df: pd.DataFrame) -> ValidationResult:
    """
    Validates historical price data against schema and validation rules.
    
    Args:
        df: DataFrame containing historical price data
        
    Returns:
        ValidationResult: Validation result with success status and errors
    """
    logger.info("Validating historical price data")
    
    # Check if dataframe is None or empty
    if df is None or df.empty:
        logger.error("Historical price data is missing or empty")
        raise MissingDataError("historical_prices", ["entire dataframe"])
    
    # Check for required columns
    missing_columns = [col for col in REQUIRED_HISTORICAL_PRICE_COLUMNS if col not in df.columns]
    if missing_columns:
        logger.error(f"Historical price data missing required columns: {missing_columns}")
        raise DataValidationError("historical_prices", [f"Missing required columns: {missing_columns}"])
    
    # Validate against schema
    try:
        validity, schema_errors = validate_dataframe(df, HISTORICAL_PRICE_SCHEMA)
        
        # Initialize validation result
        errors = schema_errors.copy() if not validity else {}
        is_valid = validity
        
        # Check that product values are valid if schema validation passed
        if is_valid:
            invalid_products = set(df["product"].unique()) - set(FORECAST_PRODUCTS)
            if invalid_products:
                is_valid = False
                errors["product_error"] = [f"Invalid product values: {invalid_products}"]
                logger.error(f"Product validation failed for historical price data: {invalid_products}")
        
        return ValidationResult(is_valid=is_valid, errors=errors)
    except Exception as e:
        logger.error(f"Unexpected error during historical price validation: {str(e)}")
        return ValidationResult(
            is_valid=False,
            errors={"validation_error": [f"Unexpected error during validation: {str(e)}"]}
        )


def validate_generation_forecast_data(df: pd.DataFrame) -> ValidationResult:
    """
    Validates generation forecast data against schema and validation rules.
    
    Args:
        df: DataFrame containing generation forecast data
        
    Returns:
        ValidationResult: Validation result with success status and errors
    """
    logger.info("Validating generation forecast data")
    
    # Check if dataframe is None or empty
    if df is None or df.empty:
        logger.error("Generation forecast data is missing or empty")
        raise MissingDataError("generation_forecast", ["entire dataframe"])
    
    # Check for required columns
    missing_columns = [col for col in REQUIRED_GENERATION_FORECAST_COLUMNS if col not in df.columns]
    if missing_columns:
        logger.error(f"Generation forecast data missing required columns: {missing_columns}")
        raise DataValidationError("generation_forecast", [f"Missing required columns: {missing_columns}"])
    
    # Validate against schema
    try:
        validity, schema_errors = validate_dataframe(df, GENERATION_FORECAST_SCHEMA)
        
        # Initialize validation result
        errors = schema_errors.copy() if not validity else {}
        is_valid = validity
        
        # Check value ranges if schema validation passed
        if is_valid:
            # Check that generation_mw is non-negative
            negative_gen = df[df["generation_mw"] < 0]
            if not negative_gen.empty:
                is_valid = False
                errors["range_error"] = [f"Generation must be non-negative, found {len(negative_gen)} negative values"]
                logger.error(f"Range validation failed for generation forecast data: {len(negative_gen)} negative values")
        
        return ValidationResult(is_valid=is_valid, errors=errors)
    except Exception as e:
        logger.error(f"Unexpected error during generation forecast validation: {str(e)}")
        return ValidationResult(
            is_valid=False,
            errors={"validation_error": [f"Unexpected error during validation: {str(e)}"]}
        )


def check_timestamp_coverage(df: pd.DataFrame, start_time: datetime, end_time: datetime) -> ValidationResult:
    """
    Checks if the data covers the required time range.
    
    Args:
        df: DataFrame to check
        start_time: Start time of the required range
        end_time: End time of the required range
        
    Returns:
        ValidationResult: Validation result with coverage information
    """
    logger.info(f"Checking timestamp coverage from {start_time} to {end_time}")
    
    if df is None or df.empty:
        return ValidationResult(
            is_valid=False,
            errors={"coverage_error": ["DataFrame is empty or None"]}
        )
    
    # Extract min and max timestamps
    try:
        min_time = pd.Timestamp(df["timestamp"].min())
        max_time = pd.Timestamp(df["timestamp"].max())
        
        # Check coverage
        is_start_covered = min_time <= pd.Timestamp(start_time)
        is_end_covered = max_time >= pd.Timestamp(end_time)
        
        if is_start_covered and is_end_covered:
            return ValidationResult(is_valid=True)
        
        errors = {}
        
        if not is_start_covered:
            errors["coverage_error"] = [
                f"Data starts at {min_time}, which is after the required start time {start_time}"
            ]
        
        if not is_end_covered:
            if "coverage_error" not in errors:
                errors["coverage_error"] = []
            errors["coverage_error"].append(
                f"Data ends at {max_time}, which is before the required end time {end_time}"
            )
        
        return ValidationResult(is_valid=False, errors=errors)
        
    except Exception as e:
        logger.error(f"Error checking timestamp coverage: {str(e)}")
        return ValidationResult(
            is_valid=False,
            errors={"coverage_error": [f"Error checking timestamp coverage: {str(e)}"]}
        )


def validate_data_consistency(load_df: pd.DataFrame, price_df: pd.DataFrame, generation_df: pd.DataFrame) -> ValidationResult:
    """
    Validates consistency across different data sources.
    
    Args:
        load_df: Load forecast dataframe
        price_df: Historical price dataframe
        generation_df: Generation forecast dataframe
        
    Returns:
        ValidationResult: Validation result with consistency information
    """
    logger.info("Validating data consistency across sources")
    
    # Check if any dataframe is None or empty
    if load_df is None or load_df.empty or price_df is None or price_df.empty or generation_df is None or generation_df.empty:
        return ValidationResult(
            is_valid=False,
            errors={"consistency_error": ["One or more dataframes are empty or None"]}
        )
    
    try:
        # Extract unique timestamps from each dataframe
        load_timestamps = set(pd.to_datetime(load_df["timestamp"]))
        price_timestamps = set(pd.to_datetime(price_df["timestamp"]))
        generation_timestamps = set(pd.to_datetime(generation_df["timestamp"]))
        
        # Find timestamps common to all dataframes
        common_timestamps = load_timestamps.intersection(price_timestamps).intersection(generation_timestamps)
        
        # Calculate coverage percentage for each dataframe
        all_timestamps = load_timestamps.union(price_timestamps).union(generation_timestamps)
        
        if not all_timestamps:
            # No timestamps in any dataframe
            return ValidationResult(
                is_valid=False,
                errors={"consistency_error": ["No timestamps found in any dataframe"]}
            )
        
        load_coverage = len(load_timestamps) / len(all_timestamps) * 100
        price_coverage = len(price_timestamps) / len(all_timestamps) * 100
        generation_coverage = len(generation_timestamps) / len(all_timestamps) * 100
        
        # Check if coverage is sufficient (e.g., >90%)
        min_coverage = 90.0  # 90% coverage threshold
        is_valid = (
            load_coverage >= min_coverage and
            price_coverage >= min_coverage and
            generation_coverage >= min_coverage
        )
        
        if is_valid:
            return ValidationResult(is_valid=True)
        
        # If we have insufficient coverage, report the details
        errors = {}
        
        if load_coverage < min_coverage:
            errors["consistency_error"] = [
                f"Load forecast has insufficient coverage: {load_coverage:.1f}%"
            ]
        
        if price_coverage < min_coverage:
            if "consistency_error" not in errors:
                errors["consistency_error"] = []
            errors["consistency_error"].append(
                f"Historical prices have insufficient coverage: {price_coverage:.1f}%"
            )
        
        if generation_coverage < min_coverage:
            if "consistency_error" not in errors:
                errors["consistency_error"] = []
            errors["consistency_error"].append(
                f"Generation forecast has insufficient coverage: {generation_coverage:.1f}%"
            )
        
        return ValidationResult(is_valid=False, errors=errors)
        
    except Exception as e:
        logger.error(f"Error validating data consistency: {str(e)}")
        return ValidationResult(
            is_valid=False,
            errors={"consistency_error": [f"Error validating data consistency: {str(e)}"]}
        )


def convert_to_model_instances(df: pd.DataFrame, model_type: str) -> List:
    """
    Converts dataframe rows to data model instances.
    
    Args:
        df: DataFrame to convert
        model_type: Type of model to create ('load_forecast', 'historical_price', 'generation_forecast')
        
    Returns:
        List of data model instances
    """
    logger.info(f"Converting dataframe to {model_type} model instances")
    
    if df is None or df.empty:
        logger.warning(f"Empty dataframe provided for {model_type} conversion")
        return []
    
    # Determine model class based on type
    model_class = None
    if model_type == 'load_forecast':
        model_class = LoadForecast
    elif model_type == 'historical_price':
        model_class = HistoricalPrice
    elif model_type == 'generation_forecast':
        model_class = GenerationForecast
    else:
        logger.error(f"Unknown model type: {model_type}")
        raise ValueError(f"Unknown model type: {model_type}")
    
    # Convert rows to model instances
    instances = []
    for _, row in df.iterrows():
        try:
            # Create model instance from row
            instance = model_class(**row.to_dict())
            instances.append(instance)
        except Exception as e:
            logger.error(f"Error converting row to {model_type} instance: {str(e)}")
            # Continue with the next row rather than failing completely
            continue
    
    logger.info(f"Converted {len(instances)} rows to {model_type} model instances")
    return instances


class DataValidator:
    """
    Main class for validating data in the ingestion pipeline.
    
    Provides methods for validating different types of data and ensuring data quality.
    """
    
    def __init__(self):
        """
        Initializes the data validator.
        """
        self.logger = logger
        self.logger.info("Initializing DataValidator")
    
    def validate_load_forecast(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validates load forecast data.
        
        Args:
            df: DataFrame containing load forecast data
            
        Returns:
            ValidationResult: Validation result
        """
        self.logger.info("Starting load forecast validation")
        try:
            return validate_load_forecast_data(df)
        except Exception as e:
            self.logger.error(f"Error during load forecast validation: {str(e)}")
            return ValidationResult(
                is_valid=False,
                errors={"validation_error": [f"Error during load forecast validation: {str(e)}"]}
            )
    
    def validate_historical_prices(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validates historical price data.
        
        Args:
            df: DataFrame containing historical price data
            
        Returns:
            ValidationResult: Validation result
        """
        self.logger.info("Starting historical price validation")
        try:
            return validate_historical_prices_data(df)
        except Exception as e:
            self.logger.error(f"Error during historical price validation: {str(e)}")
            return ValidationResult(
                is_valid=False,
                errors={"validation_error": [f"Error during historical price validation: {str(e)}"]}
            )
    
    def validate_generation_forecast(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validates generation forecast data.
        
        Args:
            df: DataFrame containing generation forecast data
            
        Returns:
            ValidationResult: Validation result
        """
        self.logger.info("Starting generation forecast validation")
        try:
            return validate_generation_forecast_data(df)
        except Exception as e:
            self.logger.error(f"Error during generation forecast validation: {str(e)}")
            return ValidationResult(
                is_valid=False,
                errors={"validation_error": [f"Error during generation forecast validation: {str(e)}"]}
            )
    
    def validate_data_sources(self, load_df: pd.DataFrame, price_df: pd.DataFrame, generation_df: pd.DataFrame) -> ValidationResult:
        """
        Validates all data sources and their consistency.
        
        Args:
            load_df: Load forecast dataframe
            price_df: Historical price dataframe
            generation_df: Generation forecast dataframe
            
        Returns:
            ValidationResult: Combined validation result
        """
        self.logger.info("Starting validation for all data sources")
        
        # Validate each data source individually
        load_result = self.validate_load_forecast(load_df)
        price_result = self.validate_historical_prices(price_df)
        gen_result = self.validate_generation_forecast(generation_df)
        
        # Check consistency across data sources
        consistency_result = validate_data_consistency(load_df, price_df, generation_df)
        
        # A result is only valid if all component results are valid
        is_valid = (
            load_result.is_valid and
            price_result.is_valid and
            gen_result.is_valid and
            consistency_result.is_valid
        )
        
        # Merge errors from all results
        errors = {}
        
        for result, source_name in [
            (load_result, "load_forecast"),
            (price_result, "historical_prices"),
            (gen_result, "generation_forecast"),
            (consistency_result, "data_consistency")
        ]:
            for category, messages in result.errors.items():
                if category not in errors:
                    errors[category] = []
                # Prefix with source name for clarity
                prefixed_messages = [f"{source_name}: {msg}" for msg in messages]
                errors[category].extend(prefixed_messages)
        
        return ValidationResult(is_valid=is_valid, errors=errors)
    
    def validate_time_range_coverage(self, df: pd.DataFrame, start_time: datetime, end_time: datetime) -> ValidationResult:
        """
        Validates that data covers the required time range.
        
        Args:
            df: DataFrame to validate
            start_time: Start time of the required range
            end_time: End time of the required range
            
        Returns:
            ValidationResult: Validation result
        """
        self.logger.info(f"Starting time range validation from {start_time} to {end_time}")
        try:
            return check_timestamp_coverage(df, start_time, end_time)
        except Exception as e:
            self.logger.error(f"Error during time range validation: {str(e)}")
            return ValidationResult(
                is_valid=False,
                errors={"coverage_error": [f"Error during time range validation: {str(e)}"]}
            )
    
    def create_model_instances(self, df: pd.DataFrame, model_type: str) -> List:
        """
        Creates data model instances from validated dataframes.
        
        Args:
            df: DataFrame to convert
            model_type: Type of model to create ('load_forecast', 'historical_price', 'generation_forecast')
            
        Returns:
            List of model instances
        """
        self.logger.info(f"Creating model instances for {model_type}")
        
        # Validate the dataframe first based on model type
        validation_result = None
        if model_type == 'load_forecast':
            validation_result = self.validate_load_forecast(df)
        elif model_type == 'historical_price':
            validation_result = self.validate_historical_prices(df)
        elif model_type == 'generation_forecast':
            validation_result = self.validate_generation_forecast(df)
        else:
            self.logger.error(f"Unknown model type: {model_type}")
            return []
        
        # If validation passes, convert to model instances
        if validation_result.is_valid:
            return convert_to_model_instances(df, model_type)
        else:
            self.logger.error(f"Validation failed for {model_type}, cannot create model instances")
            self.logger.error(f"Validation errors: {validation_result.format_errors()}")
            return []