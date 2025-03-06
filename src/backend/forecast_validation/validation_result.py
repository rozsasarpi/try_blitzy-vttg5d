"""
Defines the core validation result data structures and utilities for the forecast validation component.

This module provides a standardized way to represent, track, and report validation outcomes
across different validation types (completeness, plausibility, consistency, and schema).
"""

import enum
import typing
from dataclasses import dataclass

# Internal imports
from .exceptions import (
    ForecastValidationError,
    CompletenessValidationError,
    PlausibilityValidationError,
    ConsistencyValidationError,
    SchemaValidationError
)
from ..utils.logging_utils import get_logger

# Setup logger
logger = get_logger(__name__)


class ValidationCategory(enum.Enum):
    """Enumeration of validation categories for forecast validation."""
    COMPLETENESS = "completeness"
    PLAUSIBILITY = "plausibility"
    CONSISTENCY = "consistency"
    SCHEMA = "schema"
    GENERIC = "generic"


@dataclass
class ValidationResult:
    """Class representing the result of a forecast validation operation."""
    is_valid: bool
    category: ValidationCategory
    _errors: typing.Dict[str, typing.List[str]] = None

    def __post_init__(self):
        """Initialize errors dictionary if None."""
        if self._errors is None:
            self._errors = {}
        logger.debug(f"Created ValidationResult: is_valid={self.is_valid}, category={self.category}")

    @property
    def errors(self) -> typing.Dict[str, typing.List[str]]:
        """Dictionary of validation errors by category."""
        return self._errors

    def add_error(self, error_category: str, error_message: str) -> None:
        """
        Adds an error message to a specific error category.
        
        Args:
            error_category: Category of the error
            error_message: Detailed error message
        """
        # When adding an error, the result is no longer valid
        self.is_valid = False
        
        # Initialize the category if it doesn't exist
        if error_category not in self._errors:
            self._errors[error_category] = []
        
        # Add the error message
        self._errors[error_category].append(error_message)
        logger.debug(f"Added error to {self.category} validation: [{error_category}] {error_message}")

    def merge_errors(self, other_result: 'ValidationResult') -> None:
        """
        Merges errors from another validation result.
        
        Args:
            other_result: Another ValidationResult to merge errors from
        """
        if other_result.has_errors():
            # When merging errors from an invalid result, this result becomes invalid too
            self.is_valid = False
            
            # Merge the error dictionaries
            for category, messages in other_result.errors.items():
                if category not in self._errors:
                    self._errors[category] = []
                self._errors[category].extend(messages)
            
            logger.debug(f"Merged errors from another {other_result.category} validation result")

    def has_errors(self) -> bool:
        """
        Checks if the validation result has any errors.
        
        Returns:
            True if errors exist, False otherwise
        """
        return bool(self._errors)

    def get_error_count(self) -> int:
        """
        Returns the total number of error messages across all categories.
        
        Returns:
            Total number of error messages
        """
        return sum(len(messages) for messages in self._errors.values())

    def format_errors(self) -> str:
        """
        Formats all errors into a human-readable string.
        
        Returns:
            Formatted error message
        """
        if not self.has_errors():
            return "No validation errors"
        
        formatted_errors = []
        for category, messages in self._errors.items():
            formatted_category = f"{category}:"
            formatted_messages = [f"  - {msg}" for msg in messages]
            formatted_errors.append(formatted_category + "\n" + "\n".join(formatted_messages))
        
        return "\n".join(formatted_errors)


def create_success_result(category: ValidationCategory) -> ValidationResult:
    """
    Creates a successful validation result for a specific validation category.
    
    Args:
        category: The validation category
        
    Returns:
        A successful validation result with no errors
    """
    return ValidationResult(is_valid=True, category=category, _errors={})


def create_error_result(category: ValidationCategory, errors: typing.Dict[str, typing.List[str]]) -> ValidationResult:
    """
    Creates a validation result with errors for a specific validation category.
    
    Args:
        category: The validation category
        errors: Dictionary of error messages by category
        
    Returns:
        A validation result with errors
    """
    return ValidationResult(is_valid=False, category=category, _errors=errors)


def combine_validation_results(results: typing.List[ValidationResult]) -> ValidationResult:
    """
    Combines multiple validation results into a single result.
    
    Args:
        results: List of validation results to combine
        
    Returns:
        A combined validation result
    """
    if not results:
        return create_success_result(ValidationCategory.GENERIC)
    
    # Check if any result is invalid
    if any(not result.is_valid for result in results):
        # Create a combined result with is_valid=False
        combined_result = create_success_result(ValidationCategory.GENERIC)
        combined_result.is_valid = False
        
        # Merge all error dictionaries
        for result in results:
            if not result.is_valid:
                combined_result.merge_errors(result)
        
        return combined_result
    
    # If all results are valid, return a generic success result
    return create_success_result(ValidationCategory.GENERIC)


def get_validation_error(result: ValidationResult, message: str) -> ForecastValidationError:
    """
    Creates an appropriate validation error exception based on the validation result.
    
    Args:
        result: The validation result
        message: Error message for the exception
        
    Returns:
        An appropriate validation error exception
    """
    if result.is_valid:
        return None
    
    # Create the appropriate error type based on the category
    if result.category == ValidationCategory.COMPLETENESS:
        return CompletenessValidationError(message, result.errors)
    elif result.category == ValidationCategory.PLAUSIBILITY:
        return PlausibilityValidationError(message, result.errors)
    elif result.category == ValidationCategory.CONSISTENCY:
        return ConsistencyValidationError(message, result.errors)
    elif result.category == ValidationCategory.SCHEMA:
        return SchemaValidationError(message, result.errors)
    else:
        return ForecastValidationError(message, result.errors)