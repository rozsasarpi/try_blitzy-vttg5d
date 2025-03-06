"""
Initialization module for the forecast validation component of the Electricity Market Price Forecasting System.
This module exports the core validation functions and classes, providing a unified interface for
validating forecast data against quality standards, physical constraints, and consistency requirements.
"""

import pandas as pd  # version: 2.0.0
import datetime
from typing import Dict, List, Any

# Internal imports
from .exceptions import (
    ForecastValidationError,
    CompletenessValidationError,
    PlausibilityValidationError,
    ConsistencyValidationError,
    SchemaValidationError
)
from .validation_result import (
    ValidationCategory,
    ValidationResult,
    create_success_result,
    create_error_result,
    combine_validation_results,
    get_validation_error
)
from .schema_validator import (
    validate_forecast_schema,
    SchemaValidator
)
from .completeness_validator import (
    validate_forecast_completeness,
    CompletenessValidator
)
from .plausibility_validator import (
    validate_forecast_plausibility,
    PlausibilityValidator,
    PRODUCT_CONSTRAINTS
)
from .consistency_validator import (
    validate_forecast_consistency,
    ConsistencyValidator,
    PRODUCT_RELATIONSHIPS,
    TEMPORAL_SMOOTHNESS_THRESHOLD
)
from ..utils.logging_utils import get_logger

# Global variables
logger = get_logger(__name__)

def validate_forecast(forecast_df: pd.DataFrame, start_date: datetime.datetime) -> ValidationResult:
    """
    Comprehensive validation of a forecast dataframe against all validation criteria.
    
    Args:
        forecast_df: DataFrame containing forecast data to validate
        start_date: Start date for the forecast horizon
        
    Returns:
        ValidationResult: Combined validation result from all validation types
    """
    # Log start of forecast validation
    logger.info(f"Starting comprehensive forecast validation for forecast starting at {start_date}")
    
    # Initialize list to store validation results
    validation_results = []
    
    # Validate schema using validate_forecast_schema
    logger.info("Validating forecast schema")
    schema_result = validate_forecast_schema(forecast_df)
    validation_results.append(schema_result)
    
    # Validate completeness using validate_forecast_completeness
    logger.info("Validating forecast completeness")
    completeness_result = validate_forecast_completeness(forecast_df, start_date)
    validation_results.append(completeness_result)
    
    # Validate plausibility using validate_forecast_plausibility
    logger.info("Validating forecast plausibility")
    plausibility_result = validate_forecast_plausibility(forecast_df)
    validation_results.append(plausibility_result)
    
    # Validate consistency using validate_forecast_consistency
    logger.info("Validating forecast consistency")
    consistency_result = validate_forecast_consistency(forecast_df)
    validation_results.append(consistency_result)
    
    # Combine all validation results using combine_validation_results
    combined_result = combine_validation_results(validation_results)
    
    # Log validation outcome
    if combined_result.is_valid:
        logger.info("Forecast validation successful")
    else:
        logger.warning(f"Forecast validation failed with {len(combined_result.errors)} errors")
    
    # Return combined validation result
    return combined_result

def create_validator(validator_type: str, config: Dict) -> typing.Union[SchemaValidator, CompletenessValidator, PlausibilityValidator, ConsistencyValidator]:
    """
    Factory function to create a validator instance of the specified type.
    
    Args:
        validator_type: Type of validator to create ('schema', 'completeness', 'plausibility', 'consistency')
        config: Configuration dictionary for the validator
        
    Returns:
        Validator instance of the requested type
    """
    # Check validator_type parameter
    if validator_type == 'schema':
        # If 'schema', create and return SchemaValidator instance
        return SchemaValidator(**config)
    elif validator_type == 'completeness':
        # If 'completeness', create and return CompletenessValidator instance
        return CompletenessValidator(**config)
    elif validator_type == 'plausibility':
        # If 'plausibility', create and return PlausibilityValidator instance
        return PlausibilityValidator(**config)
    elif validator_type == 'consistency':
        # If 'consistency', create and return ConsistencyValidator instance
        return ConsistencyValidator(**config)
    else:
        # If unknown type, raise ValueError with supported types
        raise ValueError(f"Unknown validator type: {validator_type}. Supported types: 'schema', 'completeness', 'plausibility', 'consistency'")

class ForecastValidator:
    """
    Composite validator that applies all validation types to forecast data.
    """
    
    def __init__(
        self,
        schema_validator: SchemaValidator = None,
        completeness_validator: CompletenessValidator = None,
        plausibility_validator: PlausibilityValidator = None,
        consistency_validator: ConsistencyValidator = None
    ):
        """
        Initialize the forecast validator with optional custom validators.
        
        Args:
            schema_validator: Optional custom schema validator
            completeness_validator: Optional custom completeness validator
            plausibility_validator: Optional custom plausibility validator
            consistency_validator: Optional custom consistency validator
        """
        # Initialize _schema_validator with provided instance or create default
        self._schema_validator = schema_validator or SchemaValidator()
        # Initialize _completeness_validator with provided instance or create default
        self._completeness_validator = completeness_validator or CompletenessValidator()
        # Initialize _plausibility_validator with provided instance or create default
        self._plausibility_validator = plausibility_validator or PlausibilityValidator()
        # Initialize _consistency_validator with provided instance or create default
        self._consistency_validator = consistency_validator or ConsistencyValidator()
        # Log validator initialization
        logger.info("Initialized ForecastValidator with validators for schema, completeness, plausibility, and consistency")
    
    def validate(self, forecast_df: pd.DataFrame, start_date: datetime.datetime) -> ValidationResult:
        """
        Validate a forecast dataframe against all validation criteria.
        
        Args:
            forecast_df: DataFrame containing forecast data to validate
            start_date: Start date for the forecast horizon
            
        Returns:
            ValidationResult: Combined validation result
        """
        # Log start of validation
        logger.info(f"Starting validation with ForecastValidator for forecast starting at {start_date}")
        
        # Initialize list to store validation results
        validation_results = []
        
        # Validate schema using _schema_validator
        schema_result = self.validate_schema(forecast_df)
        validation_results.append(schema_result)
        
        # Validate completeness using _completeness_validator
        completeness_result = self.validate_completeness(forecast_df, start_date)
        validation_results.append(completeness_result)
        
        # Validate plausibility using _plausibility_validator
        plausibility_result = self.validate_plausibility(forecast_df)
        validation_results.append(plausibility_result)
        
        # Validate consistency using _consistency_validator
        consistency_result = self.validate_consistency(forecast_df)
        validation_results.append(consistency_result)
        
        # Combine all validation results
        combined_result = combine_validation_results(validation_results)
        
        # Log validation outcome
        if combined_result.is_valid:
            logger.info("Forecast validation successful")
        else:
            logger.warning(f"Forecast validation failed with {len(combined_result.errors)} errors")
        
        # Return combined validation result
        return combined_result
    
    def validate_schema(self, forecast_df: pd.DataFrame) -> ValidationResult:
        """
        Validate forecast schema only.
        
        Args:
            forecast_df: DataFrame containing forecast data to validate
            
        Returns:
            ValidationResult: Schema validation result
        """
        # Log start of schema validation
        logger.info("Starting schema validation")
        # Call validate method on _schema_validator
        result = self._schema_validator.validate(forecast_df)
        # Log validation outcome
        if result.is_valid:
            logger.info("Schema validation successful")
        else:
            logger.warning(f"Schema validation failed with {len(result.errors)} errors")
        # Return validation result
        return result
    
    def validate_completeness(self, forecast_df: pd.DataFrame, start_date: datetime.datetime) -> ValidationResult:
        """
        Validate forecast completeness only.
        
        Args:
            forecast_df: DataFrame containing forecast data to validate
            start_date: Start date for the forecast horizon
            
        Returns:
            ValidationResult: Completeness validation result
        """
        # Log start of completeness validation
        logger.info(f"Starting completeness validation for forecast starting at {start_date}")
        # Call validate method on _completeness_validator
        result = self._completeness_validator.validate(forecast_df, start_date)
        # Log validation outcome
        if result.is_valid:
            logger.info("Completeness validation successful")
        else:
            logger.warning(f"Completeness validation failed with {len(result.errors)} errors")
        # Return validation result
        return result
    
    def validate_plausibility(self, forecast_df: pd.DataFrame) -> ValidationResult:
        """
        Validate forecast plausibility only.
        
        Args:
            forecast_df: DataFrame containing forecast data to validate
            
        Returns:
            ValidationResult: Plausibility validation result
        """
        # Log start of plausibility validation
        logger.info("Starting plausibility validation")
        # Call validate method on _plausibility_validator
        result = self._plausibility_validator.validate(forecast_df)
        # Log validation outcome
        if result.is_valid:
            logger.info("Plausibility validation successful")
        else:
            logger.warning(f"Plausibility validation failed with {len(result.errors)} errors")
        # Return validation result
        return result
    
    def validate_consistency(self, forecast_df: pd.DataFrame) -> ValidationResult:
        """
        Validate forecast consistency only.
        
        Args:
            forecast_df: DataFrame containing forecast data to validate
            
        Returns:
            ValidationResult: Consistency validation result
        """
        # Log start of consistency validation
        logger.info("Starting consistency validation")
        # Call validate method on _consistency_validator
        result = self._consistency_validator.validate(forecast_df)
        # Log validation outcome
        if result.is_valid:
            logger.info("Consistency validation successful")
        else:
            logger.warning(f"Consistency validation failed with {len(result.errors)} errors")
        # Return validation result
        return result