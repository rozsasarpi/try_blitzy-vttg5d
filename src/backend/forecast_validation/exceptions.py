import typing

class ForecastValidationError(Exception):
    """Base exception class for all forecast validation errors"""
    
    def __init__(self, message: str, errors: typing.Dict[str, typing.List[str]] = None):
        """
        Initializes the base forecast validation error
        
        Args:
            message: The error message
            errors: Dictionary of validation errors by category
        """
        super().__init__(message)
        self.errors = errors or {}
    
    def get_errors(self) -> typing.Dict[str, typing.List[str]]:
        """
        Returns the validation errors dictionary
        
        Returns:
            Dictionary of validation errors by category
        """
        return self.errors
    
    def format_errors(self) -> str:
        """
        Formats the validation errors into a human-readable string
        
        Returns:
            Formatted error message
        """
        if not self.errors:
            return str(self)
        
        formatted_errors = []
        for category, messages in self.errors.items():
            formatted_category = f"{category}:"
            formatted_messages = [f"  - {msg}" for msg in messages]
            formatted_errors.append(formatted_category + "\n" + "\n".join(formatted_messages))
        
        return f"{str(self)}\n" + "\n".join(formatted_errors)


class CompletenessValidationError(ForecastValidationError):
    """Exception raised when forecast data is missing required products or timestamps"""
    
    def __init__(self, message: str, errors: typing.Dict[str, typing.List[str]] = None):
        """
        Initializes a completeness validation error
        
        Args:
            message: The error message
            errors: Dictionary of validation errors by category
        """
        super().__init__(message, errors)


class PlausibilityValidationError(ForecastValidationError):
    """Exception raised when forecast values are physically implausible"""
    
    def __init__(self, message: str, errors: typing.Dict[str, typing.List[str]] = None):
        """
        Initializes a plausibility validation error
        
        Args:
            message: The error message
            errors: Dictionary of validation errors by category
        """
        super().__init__(message, errors)


class ConsistencyValidationError(ForecastValidationError):
    """Exception raised when forecast values violate consistency rules between products"""
    
    def __init__(self, message: str, errors: typing.Dict[str, typing.List[str]] = None):
        """
        Initializes a consistency validation error
        
        Args:
            message: The error message
            errors: Dictionary of validation errors by category
        """
        super().__init__(message, errors)


class SchemaValidationError(ForecastValidationError):
    """Exception raised when forecast data fails schema validation"""
    
    def __init__(self, message: str, errors: typing.Dict[str, typing.List[str]] = None):
        """
        Initializes a schema validation error
        
        Args:
            message: The error message
            errors: Dictionary of validation errors by category
        """
        super().__init__(message, errors)