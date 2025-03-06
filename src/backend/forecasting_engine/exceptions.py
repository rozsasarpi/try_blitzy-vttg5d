"""
Custom exception classes for the forecasting engine module of the Electricity Market Price Forecasting System.
Defines specific exceptions for various forecasting-related error conditions including model selection,
model execution, uncertainty estimation, and sample generation failures.
"""

from ..utils.logging_utils import get_logger

# Global logger for this module
logger = get_logger(__name__)

class ForecastingEngineError(Exception):
    """Base exception class for all forecasting engine-related errors"""
    
    def __init__(self, message: str):
        """
        Initialize the base forecasting engine error with a message
        
        Args:
            message: Error message describing the issue
        """
        super().__init__(message)
        logger.error(f"Forecasting Engine Error: {message}")


class ModelSelectionError(ForecastingEngineError):
    """Exception raised when a model cannot be selected for a product/hour combination"""
    
    def __init__(self, message: str, product: str, hour: int):
        """
        Initialize with message, product, and hour details
        
        Args:
            message: Error message describing the issue
            product: Price product identifier (e.g., DALMP, RTLMP)
            hour: Target hour for which model selection failed
        """
        super().__init__(message)
        self.product = product
        self.hour = hour
        logger.error(f"Model Selection Error for {product}, hour {hour}: {message}")


class ModelExecutionError(ForecastingEngineError):
    """Exception raised when a model fails to execute properly"""
    
    def __init__(self, message: str, product: str, hour: int, model_id: str):
        """
        Initialize with message, product, hour, and model ID details
        
        Args:
            message: Error message describing the issue
            product: Price product identifier (e.g., DALMP, RTLMP)
            hour: Target hour for which model execution failed
            model_id: Identifier of the model that failed
        """
        super().__init__(message)
        self.product = product
        self.hour = hour
        self.model_id = model_id
        logger.error(f"Model Execution Error for {product}, hour {hour}, model {model_id}: {message}")


class UncertaintyEstimationError(ForecastingEngineError):
    """Exception raised when uncertainty estimation fails"""
    
    def __init__(self, message: str, product: str, hour: int, point_forecast: float):
        """
        Initialize with message, product, hour, and point forecast details
        
        Args:
            message: Error message describing the issue
            product: Price product identifier (e.g., DALMP, RTLMP)
            hour: Target hour for which uncertainty estimation failed
            point_forecast: Point forecast value for which uncertainty estimation failed
        """
        super().__init__(message)
        self.product = product
        self.hour = hour
        self.point_forecast = point_forecast
        logger.error(f"Uncertainty Estimation Error for {product}, hour {hour}, point forecast {point_forecast}: {message}")


class SampleGenerationError(ForecastingEngineError):
    """Exception raised when probabilistic sample generation fails"""
    
    def __init__(self, message: str, product: str, hour: int, point_forecast: float, uncertainty_params: dict):
        """
        Initialize with message, product, hour, point forecast, and uncertainty parameters details
        
        Args:
            message: Error message describing the issue
            product: Price product identifier (e.g., DALMP, RTLMP)
            hour: Target hour for which sample generation failed
            point_forecast: Point forecast value for which sample generation failed
            uncertainty_params: Uncertainty parameters used for sample generation
        """
        super().__init__(message)
        self.product = product
        self.hour = hour
        self.point_forecast = point_forecast
        self.uncertainty_params = uncertainty_params
        logger.error(f"Sample Generation Error for {product}, hour {hour}, point forecast {point_forecast}: {message}")


class ModelRegistryError(ForecastingEngineError):
    """Exception raised when there are issues with the model registry"""
    
    def __init__(self, message: str, operation: str):
        """
        Initialize with message and operation details
        
        Args:
            message: Error message describing the issue
            operation: Operation being performed on the model registry
        """
        super().__init__(message)
        self.operation = operation
        logger.error(f"Model Registry Error during {operation}: {message}")


class InvalidFeatureError(ForecastingEngineError):
    """Exception raised when feature inputs are invalid or missing"""
    
    def __init__(self, message: str, product: str, hour: int, missing_features: list):
        """
        Initialize with message, product, hour, and missing features details
        
        Args:
            message: Error message describing the issue
            product: Price product identifier (e.g., DALMP, RTLMP)
            hour: Target hour for which features are invalid
            missing_features: List of missing or invalid features
        """
        super().__init__(message)
        self.product = product
        self.hour = hour
        self.missing_features = missing_features
        logger.error(f"Invalid Feature Error for {product}, hour {hour}: {message}. Missing features: {missing_features}")


class ForecastGenerationError(ForecastingEngineError):
    """Exception raised when the overall forecast generation process fails"""
    
    def __init__(self, message: str, product: str, hour: int, stage: str):
        """
        Initialize with message, product, hour, and stage details
        
        Args:
            message: Error message describing the issue
            product: Price product identifier (e.g., DALMP, RTLMP)
            hour: Target hour for which forecast generation failed
            stage: Stage of the forecast generation process that failed
        """
        super().__init__(message)
        self.product = product
        self.hour = hour
        self.stage = stage
        logger.error(f"Forecast Generation Error for {product}, hour {hour}, stage {stage}: {message}")