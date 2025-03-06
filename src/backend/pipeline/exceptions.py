"""
Custom exception classes for the pipeline module of the Electricity Market Price Forecasting System.

Defines a hierarchy of specialized exceptions to handle various error scenarios that can
occur during pipeline execution, including configuration issues, stage failures, and data
validation problems.
"""

import typing
import uuid
import datetime
from ..utils.logging_utils import get_logger

# Configure module logger
logger = get_logger(__name__)

class PipelineError(Exception):
    """
    Base exception class for all pipeline-related errors.
    """
    
    def __init__(self, message: str):
        """
        Initialize the base pipeline error.
        
        Args:
            message: Error message describing the issue
        """
        super().__init__(message)
        self.message = message
        logger.error(f"Pipeline error: {message}")
    
    def __str__(self) -> str:
        """
        String representation of the error.
        
        Returns:
            Error message
        """
        return self.message

class PipelineExecutionError(PipelineError):
    """
    Exception raised when the overall pipeline execution fails.
    """
    
    def __init__(
        self, 
        message: str, 
        pipeline_name: str, 
        execution_id: str,
        timestamp: typing.Optional[datetime.datetime] = None
    ):
        """
        Initialize pipeline execution error with context.
        
        Args:
            message: Error message describing the issue
            pipeline_name: Name of the pipeline that failed
            execution_id: Unique identifier for this execution
            timestamp: When the error occurred (defaults to current time)
        """
        self.pipeline_name = pipeline_name
        self.execution_id = execution_id
        self.timestamp = timestamp or datetime.datetime.now()
        
        detailed_message = (
            f"Pipeline execution failed: {message} | "
            f"Pipeline: {pipeline_name} | "
            f"Execution ID: {execution_id} | "
            f"Time: {self.timestamp}"
        )
        super().__init__(detailed_message)
        
        # Log with structured format for better readability
        logger.error(
            f"Pipeline execution failed | "
            f"Pipeline: {pipeline_name} | "
            f"Execution ID: {execution_id} | "
            f"Time: {self.timestamp} | "
            f"Details: {message}"
        )

class PipelineStageError(PipelineError):
    """
    Exception raised when a specific pipeline stage fails.
    """
    
    def __init__(
        self, 
        message: str, 
        pipeline_name: str, 
        stage_name: str,
        execution_id: str,
        timestamp: typing.Optional[datetime.datetime] = None
    ):
        """
        Initialize pipeline stage error with context.
        
        Args:
            message: Error message describing the issue
            pipeline_name: Name of the pipeline containing the stage
            stage_name: Name of the stage that failed
            execution_id: Unique identifier for this execution
            timestamp: When the error occurred (defaults to current time)
        """
        self.pipeline_name = pipeline_name
        self.stage_name = stage_name
        self.execution_id = execution_id
        self.timestamp = timestamp or datetime.datetime.now()
        
        detailed_message = (
            f"Pipeline stage failed: {message} | "
            f"Pipeline: {pipeline_name} | "
            f"Stage: {stage_name} | "
            f"Execution ID: {execution_id} | "
            f"Time: {self.timestamp}"
        )
        super().__init__(detailed_message)
        
        # Log with structured format for better readability
        logger.error(
            f"Pipeline stage failed | "
            f"Pipeline: {pipeline_name} | "
            f"Stage: {stage_name} | "
            f"Execution ID: {execution_id} | "
            f"Time: {self.timestamp} | "
            f"Details: {message}"
        )

class PipelineConfigurationError(PipelineError):
    """
    Exception raised when there is an issue with pipeline configuration.
    """
    
    def __init__(
        self, 
        message: str, 
        pipeline_name: str, 
        config_key: str
    ):
        """
        Initialize pipeline configuration error with context.
        
        Args:
            message: Error message describing the issue
            pipeline_name: Name of the pipeline with configuration issues
            config_key: The specific configuration key that has issues
        """
        self.pipeline_name = pipeline_name
        self.config_key = config_key
        
        detailed_message = (
            f"Pipeline configuration error: {message} | "
            f"Pipeline: {pipeline_name} | "
            f"Config key: {config_key}"
        )
        super().__init__(detailed_message)
        
        # Log with structured format for better readability
        logger.error(
            f"Pipeline configuration error | "
            f"Pipeline: {pipeline_name} | "
            f"Config key: {config_key} | "
            f"Details: {message}"
        )

class PipelineDataError(PipelineError):
    """
    Exception raised when there is an issue with data passed between pipeline stages.
    """
    
    def __init__(
        self, 
        message: str, 
        pipeline_name: str, 
        source_stage: str,
        target_stage: str,
        data_description: str
    ):
        """
        Initialize pipeline data error with context.
        
        Args:
            message: Error message describing the issue
            pipeline_name: Name of the pipeline
            source_stage: Stage that produced the data
            target_stage: Stage that received/expected the data
            data_description: Description of the problematic data
        """
        self.pipeline_name = pipeline_name
        self.source_stage = source_stage
        self.target_stage = target_stage
        self.data_description = data_description
        
        detailed_message = (
            f"Pipeline data error: {message} | "
            f"Pipeline: {pipeline_name} | "
            f"Source stage: {source_stage} | "
            f"Target stage: {target_stage} | "
            f"Data: {data_description}"
        )
        super().__init__(detailed_message)
        
        # Log with structured format for better readability
        logger.error(
            f"Pipeline data error | "
            f"Pipeline: {pipeline_name} | "
            f"Source stage: {source_stage} | "
            f"Target stage: {target_stage} | "
            f"Data: {data_description} | "
            f"Details: {message}"
        )

class PipelineLoggingError(PipelineError):
    """
    Exception raised when there is an issue with pipeline logging.
    """
    
    def __init__(
        self, 
        message: str, 
        pipeline_name: str, 
        logging_operation: str,
        original_error: typing.Optional[Exception] = None
    ):
        """
        Initialize pipeline logging error with context.
        
        Args:
            message: Error message describing the issue
            pipeline_name: Name of the pipeline
            logging_operation: The logging operation that failed
            original_error: The original exception that occurred (if any)
        """
        self.pipeline_name = pipeline_name
        self.logging_operation = logging_operation
        self.original_error = original_error
        
        detailed_message = (
            f"Pipeline logging error: {message} | "
            f"Pipeline: {pipeline_name} | "
            f"Operation: {logging_operation}"
        )
        if original_error:
            detailed_message += f" | Original error: {str(original_error)}"
        
        super().__init__(detailed_message)
        
        # Log with structured format for better readability
        logger.error(
            f"Pipeline logging error | "
            f"Pipeline: {pipeline_name} | "
            f"Operation: {logging_operation} | "
            f"Details: {message}"
        )

class PipelineFallbackError(PipelineError):
    """
    Exception raised when the fallback mechanism fails in the pipeline.
    """
    
    def __init__(
        self, 
        message: str, 
        pipeline_name: str, 
        execution_id: str,
        stage_name: str,
        original_error: typing.Optional[Exception] = None
    ):
        """
        Initialize pipeline fallback error with context.
        
        Args:
            message: Error message describing the issue
            pipeline_name: Name of the pipeline
            execution_id: Unique identifier for this execution
            stage_name: Name of the stage where fallback was attempted
            original_error: The original exception that triggered fallback
        """
        self.pipeline_name = pipeline_name
        self.execution_id = execution_id
        self.stage_name = stage_name
        self.original_error = original_error
        
        detailed_message = (
            f"Pipeline fallback failed: {message} | "
            f"Pipeline: {pipeline_name} | "
            f"Execution ID: {execution_id} | "
            f"Stage: {stage_name}"
        )
        if original_error:
            detailed_message += f" | Original error: {str(original_error)}"
        
        super().__init__(detailed_message)
        
        # Log with structured format for better readability
        logger.error(
            f"Pipeline fallback failed | "
            f"Pipeline: {pipeline_name} | "
            f"Execution ID: {execution_id} | "
            f"Stage: {stage_name} | "
            f"Details: {message}"
        )

class PipelineTimeoutError(PipelineError):
    """
    Exception raised when a pipeline operation exceeds its time limit.
    """
    
    def __init__(
        self, 
        message: str, 
        pipeline_name: str, 
        execution_id: str,
        operation: str,
        timeout_seconds: float,
        elapsed_seconds: float
    ):
        """
        Initialize pipeline timeout error with context.
        
        Args:
            message: Error message describing the issue
            pipeline_name: Name of the pipeline
            execution_id: Unique identifier for this execution
            operation: Name of the operation that timed out
            timeout_seconds: Maximum allowed time in seconds
            elapsed_seconds: Actual elapsed time in seconds
        """
        self.pipeline_name = pipeline_name
        self.execution_id = execution_id
        self.operation = operation
        self.timeout_seconds = timeout_seconds
        self.elapsed_seconds = elapsed_seconds
        
        detailed_message = (
            f"Pipeline timeout: {message} | "
            f"Pipeline: {pipeline_name} | "
            f"Execution ID: {execution_id} | "
            f"Operation: {operation} | "
            f"Timeout: {timeout_seconds}s | "
            f"Elapsed: {elapsed_seconds}s"
        )
        super().__init__(detailed_message)
        
        # Log with structured format for better readability
        logger.error(
            f"Pipeline timeout | "
            f"Pipeline: {pipeline_name} | "
            f"Execution ID: {execution_id} | "
            f"Operation: {operation} | "
            f"Timeout: {timeout_seconds}s | "
            f"Elapsed: {elapsed_seconds}s | "
            f"Details: {message}"
        )