# src/backend/pipeline/__init__.py
"""Initialization module for the pipeline package of the Electricity Market Price Forecasting System.
Exposes key classes and functions for pipeline execution, logging, and error handling to provide a clean public API for the forecasting pipeline.
"""

from .exceptions import (  # Module: src/backend/pipeline/exceptions.py
    PipelineError,  # Base exception for pipeline-related errors
    PipelineExecutionError,  # Exception for overall pipeline execution failures
    PipelineStageError,  # Exception for pipeline stage failures
    PipelineConfigurationError,  # Exception for pipeline configuration issues
    PipelineDataError,  # Exception for invalid data between pipeline stages
    PipelineLoggingError,  # Exception for pipeline logging failures
    PipelineFallbackError,  # Exception for fallback mechanism failures
    PipelineTimeoutError  # Exception for pipeline operation timeouts
)
from .pipeline_logger import (  # Module: src/backend/pipeline/pipeline_logger.py
    PipelineLogger,  # Class for pipeline-specific logging with execution context tracking
    log_pipeline_start,  # Log the start of a pipeline execution
    log_pipeline_completion,  # Log the successful completion of a pipeline execution
    log_pipeline_failure,  # Log the failure of a pipeline execution
    log_stage_start,  # Log the start of a pipeline stage
    log_stage_completion,  # Log the successful completion of a pipeline stage
    log_stage_failure,  # Log the failure of a pipeline stage
    log_data_transition,  # Log the transition of data between pipeline stages
    log_data_validation,  # Log the validation of data within the pipeline
    log_fallback_trigger,  # Log the triggering of the fallback mechanism
    generate_execution_id  # Generate a unique execution ID for a pipeline run
)
from .forecasting_pipeline import (  # Module: src/backend/pipeline/forecasting_pipeline.py
    ForecastingPipeline,  # Main forecasting pipeline implementation
    run_forecasting_pipeline  # Main entry point for the forecasting pipeline
)
from .pipeline_executor import (  # Module: src/backend/pipeline/pipeline_executor.py
    PipelineExecutor,  # Class for executing the forecasting pipeline with configuration management
    execute_forecasting_pipeline,  # Main entry point for executing the forecasting pipeline
    execute_with_default_config,  # Execute pipeline with default configuration
    get_default_config  # Get default pipeline configuration
)

__all__ = [
    "PipelineError",
    "PipelineExecutionError",
    "PipelineStageError",
    "PipelineConfigurationError",
    "PipelineDataError",
    "PipelineLoggingError",
    "PipelineFallbackError",
    "PipelineTimeoutError",
    "PipelineLogger",
    "log_pipeline_start",
    "log_pipeline_completion",
    "log_pipeline_failure",
    "log_stage_start",
    "log_stage_completion",
    "log_stage_failure",
    "log_data_transition",
    "log_data_validation",
    "log_fallback_trigger",
    "generate_execution_id",
    "ForecastingPipeline",
    "run_forecasting_pipeline",
    "PipelineExecutor",
    "execute_forecasting_pipeline",
    "execute_with_default_config",
    "get_default_config"
]