"""
Defines validation models and utilities for the Electricity Market Price Forecasting System.

This module provides classes and functions for validating data structures, including
pandas DataFrames and data model instances, against defined schemas and validation rules.
It serves as a central component for ensuring data quality throughout the forecasting pipeline.
"""

import dataclasses  # standard library
from datetime import datetime  # standard library
from typing import Dict, List, Any, Optional  # standard library
import logging  # standard library

import pandas as pd  # version: 2.0.0
import pandera as pa  # version: 0.16.0

from ..config.settings import FORECAST_PRODUCTS
from ..config.schema_config import (
    FORECAST_OUTPUT_SCHEMA,
    LOAD_FORECAST_SCHEMA,
    HISTORICAL_PRICE_SCHEMA,
    GENERATION_FORECAST_SCHEMA
)
from .data_models import BaseDataModel


# Configure logger
logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ValidationResult:
    """Class representing the result of a validation operation."""
    is_valid: bool
    errors: Optional[Dict[str, List[str]]] = None
    validation_time: Optional[datetime] = None
    
    def __post_init__(self):
        """Initialize defaults for optional fields."""
        if self.errors is None:
            self.errors = {}
        if self.validation_time is None:
            self.validation_time = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the validation result to a dictionary.
        
        Returns:
            Dictionary representation of the validation result
        """
        return {
            'is_valid': self.is_valid,
            'errors': self.errors,
            'validation_time': self.validation_time.isoformat()
        }
    
    def format_errors(self) -> str:
        """
        Formats the validation errors into a human-readable string.
        
        Returns:
            Formatted error message
        """
        if not self.errors:
            return "Validation successful, no errors."
        
        return format_validation_errors(self.errors)
    
    def add_error(self, category: str, message: str) -> None:
        """
        Adds an error to the validation result.
        
        Args:
            category: Category of the error
            message: Error message
        """
        self.is_valid = False
        if category not in self.errors:
            self.errors[category] = []
        self.errors[category].append(message)
    
    def merge(self, other: 'ValidationResult') -> 'ValidationResult':
        """
        Merges another validation result into this one.
        
        Args:
            other: Another validation result to merge
        
        Returns:
            Self, for method chaining
        """
        # Only valid if both are valid
        self.is_valid = self.is_valid and other.is_valid
        
        # Merge errors
        for category, messages in other.errors.items():
            if category not in self.errors:
                self.errors[category] = []
            self.errors[category].extend(messages)
        
        # Keep the most recent validation time
        if other.validation_time and (not self.validation_time or 
                                    other.validation_time > self.validation_time):
            self.validation_time = other.validation_time
        
        return self


def validate_dataframe(df: pd.DataFrame, schema: pa.DataFrameSchema) -> ValidationResult:
    """
    Validates a pandas DataFrame against a pandera schema.
    
    Args:
        df: DataFrame to validate
        schema: Pandera schema to validate against
    
    Returns:
        Validation result object with success status and errors
    """
    logger.debug(f"Validating DataFrame with schema: {type(schema).__name__}")
    
    try:
        # Validate DataFrame against schema
        schema.validate(df, lazy=True)
        return ValidationResult(is_valid=True)
    
    except pa.errors.SchemaError as e:
        # Extract error information
        errors = {}
        for error in e.failures:
            column = error.schema_context.get('column', 'DataFrame')
            if column not in errors:
                errors[column] = []
            errors[column].append(error.message)
        
        logger.warning(f"DataFrame validation failed: {format_validation_errors(errors)}")
        return ValidationResult(is_valid=False, errors=errors)


def validate_model(model: BaseDataModel) -> ValidationResult:
    """
    Validates a data model instance against validation rules.
    
    Args:
        model: Model instance to validate
    
    Returns:
        Validation result object with success status and errors
    """
    # Create a validator and validate based on model type
    validator = ModelValidator()
    
    # Determine the model type and call the appropriate validation method
    model_type = type(model).__name__
    
    if model_type == 'LoadForecast':
        return validator.validate_load_forecast(model)
    elif model_type == 'HistoricalPrice':
        return validator.validate_historical_price(model)
    elif model_type == 'GenerationForecast':
        return validator.validate_generation_forecast(model)
    elif model_type == 'PriceForecast':
        return validator.validate_price_forecast(model)
    else:
        result = ValidationResult(is_valid=False)
        result.add_error('validation', f"Unknown model type: {model_type}")
        return result


def format_validation_errors(errors: Dict[str, List[str]]) -> str:
    """
    Formats validation errors into a human-readable string.
    
    Args:
        errors: Dictionary of validation errors
    
    Returns:
        Formatted error message
    """
    if not errors:
        return "No validation errors."
    
    formatted_errors = []
    for category, messages in errors.items():
        formatted_messages = "; ".join(messages)
        formatted_errors.append(f"{category}: {formatted_messages}")
    
    return "\n".join(formatted_errors)


class DataFrameValidator:
    """Class for validating pandas DataFrames against schemas."""
    
    def __init__(self):
        """Initializes a DataFrameValidator instance."""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def validate_forecast_input(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validates forecast input data against schema.
        
        Args:
            df: DataFrame containing forecast input data
        
        Returns:
            Validation result
        """
        return validate_dataframe(df, HISTORICAL_PRICE_SCHEMA)
    
    def validate_forecast_output(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validates forecast output data against schema.
        
        Args:
            df: DataFrame containing forecast output data
        
        Returns:
            Validation result
        """
        return validate_dataframe(df, FORECAST_OUTPUT_SCHEMA)
    
    def validate_historical_prices(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validates historical price data against schema.
        
        Args:
            df: DataFrame containing historical price data
        
        Returns:
            Validation result
        """
        return validate_dataframe(df, HISTORICAL_PRICE_SCHEMA)
    
    def validate_load_forecast(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validates load forecast data against schema.
        
        Args:
            df: DataFrame containing load forecast data
        
        Returns:
            Validation result
        """
        return validate_dataframe(df, LOAD_FORECAST_SCHEMA)
    
    def validate_generation_forecast(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validates generation forecast data against schema.
        
        Args:
            df: DataFrame containing generation forecast data
        
        Returns:
            Validation result
        """
        return validate_dataframe(df, GENERATION_FORECAST_SCHEMA)


class ModelValidator:
    """Class for validating data model instances."""
    
    def __init__(self):
        """Initializes a ModelValidator instance."""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def validate_load_forecast(self, model) -> ValidationResult:
        """
        Validates a load forecast model.
        
        Args:
            model: LoadForecast model instance
        
        Returns:
            Validation result
        """
        errors = self.validate_common_fields(model)
        
        # Validate load_mw is positive
        if hasattr(model, 'load_mw') and model.load_mw <= 0:
            if 'load_mw' not in errors:
                errors['load_mw'] = []
            errors['load_mw'].append("Load must be positive")
        
        # Validate region is not empty
        if hasattr(model, 'region') and not model.region:
            if 'region' not in errors:
                errors['region'] = []
            errors['region'].append("Region cannot be empty")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    def validate_historical_price(self, model) -> ValidationResult:
        """
        Validates a historical price model.
        
        Args:
            model: HistoricalPrice model instance
        
        Returns:
            Validation result
        """
        errors = self.validate_common_fields(model)
        
        # Validate product is in FORECAST_PRODUCTS
        if hasattr(model, 'product') and model.product not in FORECAST_PRODUCTS:
            if 'product' not in errors:
                errors['product'] = []
            errors['product'].append(f"Product must be one of {FORECAST_PRODUCTS}")
        
        # Validate node is not empty
        if hasattr(model, 'node') and not model.node:
            if 'node' not in errors:
                errors['node'] = []
            errors['node'].append("Node cannot be empty")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    def validate_generation_forecast(self, model) -> ValidationResult:
        """
        Validates a generation forecast model.
        
        Args:
            model: GenerationForecast model instance
        
        Returns:
            Validation result
        """
        errors = self.validate_common_fields(model)
        
        # Validate generation_mw is non-negative
        if hasattr(model, 'generation_mw') and model.generation_mw < 0:
            if 'generation_mw' not in errors:
                errors['generation_mw'] = []
            errors['generation_mw'].append("Generation must be non-negative")
        
        # Validate fuel_type is not empty
        if hasattr(model, 'fuel_type') and not model.fuel_type:
            if 'fuel_type' not in errors:
                errors['fuel_type'] = []
            errors['fuel_type'].append("Fuel type cannot be empty")
        
        # Validate region is not empty
        if hasattr(model, 'region') and not model.region:
            if 'region' not in errors:
                errors['region'] = []
            errors['region'].append("Region cannot be empty")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    def validate_price_forecast(self, model) -> ValidationResult:
        """
        Validates a price forecast model.
        
        Args:
            model: PriceForecast model instance
        
        Returns:
            Validation result
        """
        from ..config.settings import PROBABILISTIC_SAMPLE_COUNT
        
        errors = self.validate_common_fields(model)
        
        # Validate product is in FORECAST_PRODUCTS
        if hasattr(model, 'product') and model.product not in FORECAST_PRODUCTS:
            if 'product' not in errors:
                errors['product'] = []
            errors['product'].append(f"Product must be one of {FORECAST_PRODUCTS}")
        
        # Validate samples list has correct length
        if hasattr(model, 'samples'):
            if len(model.samples) != PROBABILISTIC_SAMPLE_COUNT:
                if 'samples' not in errors:
                    errors['samples'] = []
                errors['samples'].append(
                    f"Sample count must be {PROBABILISTIC_SAMPLE_COUNT}, got {len(model.samples)}"
                )
        
        # Validate generation_timestamp is not None
        if hasattr(model, 'generation_timestamp') and model.generation_timestamp is None:
            if 'generation_timestamp' not in errors:
                errors['generation_timestamp'] = []
            errors['generation_timestamp'].append("Generation timestamp cannot be None")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    def validate_common_fields(self, model: BaseDataModel) -> Dict[str, List[str]]:
        """
        Validates fields common to all data models.
        
        Args:
            model: Data model instance
        
        Returns:
            Dictionary of validation errors
        """
        errors = {}
        
        # Validate timestamp field
        if not hasattr(model, 'timestamp') or model.timestamp is None:
            errors['timestamp'] = ["Timestamp is required"]
        elif not isinstance(model.timestamp, datetime):
            if 'timestamp' not in errors:
                errors['timestamp'] = []
            errors['timestamp'].append("Timestamp must be a datetime object")
        
        return errors