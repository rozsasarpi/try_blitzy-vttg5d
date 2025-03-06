"""
Specialized logging module for the pipeline component of the Electricity Market Price Forecasting System.

This module provides structured logging functions for pipeline execution, stage transitions,
data flow, and error handling to support monitoring and observability requirements.
"""

import logging
import datetime
import time
import typing
import uuid
import pandas as pd  # version: 2.0.0+

# Internal imports
from ..utils.logging_utils import (
    get_logger,
    ComponentLogger,
    format_exception,
    format_dict_for_logging
)
from .exceptions import PipelineLoggingError

# Global logger instances
logger = get_logger(__name__)
component_logger = ComponentLogger('pipeline', {'component': 'pipeline'})


def log_pipeline_start(pipeline_name: str, execution_id: str, config: dict = None) -> None:
    """
    Logs the start of a pipeline execution.
    
    Args:
        pipeline_name: Name of the pipeline being executed
        execution_id: Unique identifier for this execution
        config: Optional configuration details for the pipeline
    """
    try:
        context = {
            'pipeline': pipeline_name,
            'execution_id': execution_id,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if config:
            context['config'] = format_dict_for_logging(config)
            
        component_logger.info(f"Started pipeline execution: {pipeline_name} [ID: {execution_id}]", extra=context)
    except Exception as e:
        error_msg = f"Failed to log pipeline start: {str(e)}"
        logger.error(error_msg)
        raise PipelineLoggingError(error_msg, pipeline_name, "log_pipeline_start", e)


def log_pipeline_completion(
    pipeline_name: str, 
    execution_id: str, 
    start_time: float,
    metrics: dict = None
) -> None:
    """
    Logs the successful completion of a pipeline execution.
    
    Args:
        pipeline_name: Name of the pipeline that completed
        execution_id: Unique identifier for this execution
        start_time: Start time of the pipeline execution (time.time() value)
        metrics: Optional performance metrics for the execution
    """
    try:
        duration = time.time() - start_time
        
        context = {
            'pipeline': pipeline_name,
            'execution_id': execution_id,
            'duration_seconds': f"{duration:.3f}",
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if metrics:
            context['metrics'] = format_dict_for_logging(metrics)
            
        component_logger.info(
            f"Completed pipeline execution: {pipeline_name} [ID: {execution_id}] in {duration:.3f} seconds", 
            extra=context
        )
    except Exception as e:
        error_msg = f"Failed to log pipeline completion: {str(e)}"
        logger.error(error_msg)
        raise PipelineLoggingError(error_msg, pipeline_name, "log_pipeline_completion", e)


def log_pipeline_failure(
    pipeline_name: str, 
    execution_id: str, 
    start_time: float, 
    error: Exception,
    context: dict = None
) -> None:
    """
    Logs the failure of a pipeline execution.
    
    Args:
        pipeline_name: Name of the pipeline that failed
        execution_id: Unique identifier for this execution
        start_time: Start time of the pipeline execution (time.time() value)
        error: The exception that caused the failure
        context: Additional context information about the failure
    """
    try:
        duration = time.time() - start_time
        error_details = format_exception(error)
        
        log_context = {
            'pipeline': pipeline_name,
            'execution_id': execution_id,
            'duration_seconds': f"{duration:.3f}",
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_details': error_details,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if context:
            log_context.update(context)
            
        component_logger.error(
            f"Pipeline execution failed: {pipeline_name} [ID: {execution_id}] after {duration:.3f} seconds: {str(error)}", 
            extra=log_context
        )
    except Exception as e:
        # Only raise a new exception if it's different from the original error
        if e is not error:
            error_msg = f"Failed to log pipeline failure: {str(e)}"
            logger.error(error_msg)
            raise PipelineLoggingError(error_msg, pipeline_name, "log_pipeline_failure", e)


def log_stage_start(
    pipeline_name: str, 
    execution_id: str, 
    stage_name: str,
    stage_config: dict = None
) -> None:
    """
    Logs the start of a pipeline stage.
    
    Args:
        pipeline_name: Name of the pipeline containing the stage
        execution_id: Unique identifier for this execution
        stage_name: Name of the stage being started
        stage_config: Optional configuration for this stage
    """
    try:
        context = {
            'pipeline': pipeline_name,
            'execution_id': execution_id,
            'stage': stage_name,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if stage_config:
            context['stage_config'] = format_dict_for_logging(stage_config)
            
        component_logger.info(
            f"Started pipeline stage: {stage_name} in pipeline {pipeline_name} [ID: {execution_id}]", 
            extra=context
        )
    except Exception as e:
        error_msg = f"Failed to log stage start: {str(e)}"
        logger.error(error_msg)
        raise PipelineLoggingError(error_msg, pipeline_name, "log_stage_start", e)


def log_stage_completion(
    pipeline_name: str, 
    execution_id: str, 
    stage_name: str,
    start_time: float,
    output_summary: dict = None
) -> None:
    """
    Logs the successful completion of a pipeline stage.
    
    Args:
        pipeline_name: Name of the pipeline containing the stage
        execution_id: Unique identifier for this execution
        stage_name: Name of the stage that completed
        start_time: Start time of the stage execution (time.time() value)
        output_summary: Optional summary of the stage output
    """
    try:
        duration = time.time() - start_time
        
        context = {
            'pipeline': pipeline_name,
            'execution_id': execution_id,
            'stage': stage_name,
            'duration_seconds': f"{duration:.3f}",
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if output_summary:
            context['output_summary'] = format_dict_for_logging(output_summary)
            
        component_logger.info(
            f"Completed pipeline stage: {stage_name} in pipeline {pipeline_name} [ID: {execution_id}] in {duration:.3f} seconds", 
            extra=context
        )
    except Exception as e:
        error_msg = f"Failed to log stage completion: {str(e)}"
        logger.error(error_msg)
        raise PipelineLoggingError(error_msg, pipeline_name, "log_stage_completion", e)


def log_stage_failure(
    pipeline_name: str, 
    execution_id: str, 
    stage_name: str,
    start_time: float,
    error: Exception,
    context: dict = None
) -> None:
    """
    Logs the failure of a pipeline stage.
    
    Args:
        pipeline_name: Name of the pipeline containing the stage
        execution_id: Unique identifier for this execution
        stage_name: Name of the stage that failed
        start_time: Start time of the stage execution (time.time() value)
        error: The exception that caused the failure
        context: Additional context information about the failure
    """
    try:
        duration = time.time() - start_time
        error_details = format_exception(error)
        
        log_context = {
            'pipeline': pipeline_name,
            'execution_id': execution_id,
            'stage': stage_name,
            'duration_seconds': f"{duration:.3f}",
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_details': error_details,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if context:
            log_context.update(context)
            
        component_logger.error(
            f"Pipeline stage failed: {stage_name} in pipeline {pipeline_name} [ID: {execution_id}] after {duration:.3f} seconds: {str(error)}", 
            extra=log_context
        )
    except Exception as e:
        # Only raise a new exception if it's different from the original error
        if e is not error:
            error_msg = f"Failed to log stage failure: {str(e)}"
            logger.error(error_msg)
            raise PipelineLoggingError(error_msg, pipeline_name, "log_stage_failure", e)


def log_data_transition(
    pipeline_name: str, 
    execution_id: str, 
    source_stage: str,
    target_stage: str,
    data: object,
    metadata: dict = None
) -> None:
    """
    Logs the transition of data between pipeline stages.
    
    Args:
        pipeline_name: Name of the pipeline
        execution_id: Unique identifier for this execution
        source_stage: Stage that produced the data
        target_stage: Stage that will receive the data
        data: The data being transferred
        metadata: Additional metadata about the data
    """
    try:
        data_summary = get_data_summary(data)
        
        context = {
            'pipeline': pipeline_name,
            'execution_id': execution_id,
            'source_stage': source_stage,
            'target_stage': target_stage,
            'data_summary': data_summary,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if metadata:
            context['metadata'] = metadata
            
        component_logger.info(
            f"Data transition: {source_stage} → {target_stage} in pipeline {pipeline_name} [ID: {execution_id}]", 
            extra=context
        )
    except Exception as e:
        error_msg = f"Failed to log data transition: {str(e)}"
        logger.error(error_msg)
        raise PipelineLoggingError(error_msg, pipeline_name, "log_data_transition", e)


def log_data_validation(
    pipeline_name: str, 
    execution_id: str, 
    stage_name: str,
    validation_type: str,
    is_valid: bool,
    errors: list = None,
    context: dict = None
) -> None:
    """
    Logs the validation of data within the pipeline.
    
    Args:
        pipeline_name: Name of the pipeline
        execution_id: Unique identifier for this execution
        stage_name: Stage where validation occurs
        validation_type: Type of validation performed
        is_valid: Whether validation passed
        errors: List of validation errors if not valid
        context: Additional context information
    """
    try:
        validation_context = {
            'pipeline': pipeline_name,
            'execution_id': execution_id,
            'stage': stage_name,
            'validation_type': validation_type,
            'is_valid': is_valid,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if not is_valid and errors:
            validation_context['validation_errors'] = errors
            
        if context:
            validation_context.update(context)
        
        log_level = logging.WARNING if not is_valid else logging.INFO
        log_method = component_logger.warning if not is_valid else component_logger.info
        
        log_method(
            f"Data validation {validation_type}: {'PASSED' if is_valid else 'FAILED'} in stage {stage_name} of pipeline {pipeline_name} [ID: {execution_id}]",
            extra=validation_context
        )
    except Exception as e:
        error_msg = f"Failed to log data validation: {str(e)}"
        logger.error(error_msg)
        raise PipelineLoggingError(error_msg, pipeline_name, "log_data_validation", e)


def log_fallback_trigger(
    pipeline_name: str, 
    execution_id: str, 
    stage_name: str,
    reason: str,
    error: Exception = None,
    context: dict = None
) -> None:
    """
    Logs the triggering of the fallback mechanism from the pipeline.
    
    Args:
        pipeline_name: Name of the pipeline
        execution_id: Unique identifier for this execution
        stage_name: Stage where fallback was triggered
        reason: Reason for triggering fallback
        error: Optional exception that triggered fallback
        context: Additional context information
    """
    try:
        fallback_context = {
            'pipeline': pipeline_name,
            'execution_id': execution_id,
            'stage': stage_name,
            'fallback_reason': reason,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if error:
            fallback_context['error_type'] = type(error).__name__
            fallback_context['error_message'] = str(error)
            fallback_context['error_details'] = format_exception(error)
            
        if context:
            fallback_context.update(context)
            
        component_logger.warning(
            f"Fallback mechanism triggered in stage {stage_name} of pipeline {pipeline_name} [ID: {execution_id}]: {reason}",
            extra=fallback_context
        )
    except Exception as e:
        error_msg = f"Failed to log fallback trigger: {str(e)}"
        logger.error(error_msg)
        raise PipelineLoggingError(error_msg, pipeline_name, "log_fallback_trigger", e)


def log_pipeline_metrics(
    pipeline_name: str, 
    execution_id: str, 
    metrics: dict
) -> None:
    """
    Logs performance metrics for a pipeline execution.
    
    Args:
        pipeline_name: Name of the pipeline
        execution_id: Unique identifier for this execution
        metrics: Dictionary of performance metrics
    """
    try:
        formatted_metrics = format_dict_for_logging(metrics)
        
        context = {
            'pipeline': pipeline_name,
            'execution_id': execution_id,
            'metrics': formatted_metrics,
            'timestamp': datetime.datetime.now().isoformat()
        }
            
        component_logger.info(
            f"Pipeline metrics for {pipeline_name} [ID: {execution_id}]", 
            extra=context
        )
    except Exception as e:
        error_msg = f"Failed to log pipeline metrics: {str(e)}"
        logger.error(error_msg)
        raise PipelineLoggingError(error_msg, pipeline_name, "log_pipeline_metrics", e)


def log_pipeline_configuration(
    pipeline_name: str, 
    config: dict
) -> None:
    """
    Logs the configuration of a pipeline.
    
    Args:
        pipeline_name: Name of the pipeline
        config: Configuration dictionary
    """
    try:
        formatted_config = format_dict_for_logging(config)
        
        context = {
            'pipeline': pipeline_name,
            'config': formatted_config,
            'timestamp': datetime.datetime.now().isoformat()
        }
            
        component_logger.info(
            f"Pipeline configuration for {pipeline_name}", 
            extra=context
        )
    except Exception as e:
        error_msg = f"Failed to log pipeline configuration: {str(e)}"
        logger.error(error_msg)
        raise PipelineLoggingError(error_msg, pipeline_name, "log_pipeline_configuration", e)


def generate_execution_id() -> str:
    """
    Generates a unique execution ID for a pipeline run.
    
    Returns:
        Unique execution ID string
    """
    try:
        # Generate a UUID and use first 8 characters for brevity
        return str(uuid.uuid4())[:8]
    except Exception as e:
        # Fallback to timestamp-based ID if UUID generation fails
        logger.warning(f"Failed to generate UUID execution ID: {str(e)}. Using timestamp instead.")
        return datetime.datetime.now().strftime('%Y%m%d%H%M%S')


def get_data_summary(data: object) -> dict:
    """
    Creates a summary of data for logging purposes.
    
    Args:
        data: The data object to summarize
        
    Returns:
        Dictionary with summary of data characteristics
    """
    try:
        summary = {
            'type': type(data).__name__
        }
        
        # Handle pandas DataFrame
        if isinstance(data, pd.DataFrame):
            summary['shape'] = str(data.shape)
            summary['columns'] = list(data.columns)
            summary['memory_usage'] = f"{data.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB"
            summary['row_count'] = len(data)
            
        # Handle pandas Series
        elif isinstance(data, pd.Series):
            summary['length'] = len(data)
            summary['dtype'] = str(data.dtype)
            summary['memory_usage'] = f"{data.memory_usage(deep=True) / (1024 * 1024):.2f} MB"
            
        # Handle numpy array
        elif hasattr(data, 'shape') and hasattr(data, 'dtype'):
            summary['shape'] = str(data.shape)
            summary['dtype'] = str(data.dtype)
            if hasattr(data, 'nbytes'):
                summary['memory_usage'] = f"{data.nbytes / (1024 * 1024):.2f} MB"
                
        # Handle list, tuple, set
        elif isinstance(data, (list, tuple, set)):
            summary['length'] = len(data)
            if len(data) > 0:
                summary['first_item_type'] = type(data[0]).__name__ if isinstance(data, (list, tuple)) else 'unknown'
                
        # Handle dict
        elif isinstance(data, dict):
            summary['key_count'] = len(data)
            if len(data) > 0:
                summary['keys'] = list(data.keys())[:5]  # Show first 5 keys
                if len(data) > 5:
                    summary['keys'].append("...")
                    
        # String representation for other types
        else:
            str_rep = str(data)
            summary['string_representation'] = str_rep[:100] + ('...' if len(str_rep) > 100 else '')
            
        return summary
    except Exception as e:
        # Return minimal information if summarization fails
        logger.warning(f"Failed to create data summary: {str(e)}")
        return {
            'type': type(data).__name__,
            'summarization_error': str(e)
        }


class PipelineLogger:
    """
    Logger class for pipeline operations with execution context tracking.
    
    This class provides a convenient interface for logging pipeline events while
    maintaining execution context across log messages.
    """
    
    def __init__(self, pipeline_name: str, execution_id: str = None):
        """
        Initializes a pipeline logger with execution context.
        
        Args:
            pipeline_name: Name of the pipeline
            execution_id: Unique execution ID (generated if not provided)
        """
        self.pipeline_name = pipeline_name
        self.execution_id = execution_id or generate_execution_id()
        self.context = {
            'pipeline': pipeline_name,
            'execution_id': self.execution_id
        }
        self.logger = component_logger.with_context(self.context)
        self._start_time = None
        
    def log_pipeline_start(self, config: dict = None) -> None:
        """
        Logs the start of the pipeline execution.
        
        Args:
            config: Optional configuration details
        """
        try:
            self._start_time = time.time()
            operation_details = {}
            if config:
                operation_details['config'] = config
                
            self.logger.log_start('pipeline_execution', operation_details)
        except Exception as e:
            error_msg = f"Failed to log pipeline start: {str(e)}"
            logger.error(error_msg)
            raise PipelineLoggingError(error_msg, self.pipeline_name, "log_pipeline_start", e)
            
    def log_pipeline_completion(self, metrics: dict = None) -> None:
        """
        Logs the successful completion of the pipeline execution.
        
        Args:
            metrics: Optional performance metrics
        """
        try:
            if not self._start_time:
                self._start_time = time.time()  # Fallback in case start wasn't logged
                
            operation_details = {}
            if metrics:
                operation_details['metrics'] = metrics
                
            self.logger.log_completion('pipeline_execution', self._start_time, operation_details)
        except Exception as e:
            error_msg = f"Failed to log pipeline completion: {str(e)}"
            logger.error(error_msg)
            raise PipelineLoggingError(error_msg, self.pipeline_name, "log_pipeline_completion", e)
            
    def log_pipeline_failure(self, error: Exception, context: dict = None) -> None:
        """
        Logs the failure of the pipeline execution.
        
        Args:
            error: The exception that caused the failure
            context: Additional context about the failure
        """
        try:
            if not self._start_time:
                self._start_time = time.time()  # Fallback in case start wasn't logged
                
            operation_details = {}
            if context:
                operation_details.update(context)
                
            self.logger.log_failure('pipeline_execution', self._start_time, error, operation_details)
        except Exception as e:
            # Only raise a new exception if it's different from the original error
            if e is not error:
                error_msg = f"Failed to log pipeline failure: {str(e)}"
                logger.error(error_msg)
                raise PipelineLoggingError(error_msg, self.pipeline_name, "log_pipeline_failure", e)
    
    def log_stage(self, stage_name: str, stage_config: dict = None):
        """
        Context manager for logging a pipeline stage execution.
        
        Args:
            stage_name: Name of the stage being executed
            stage_config: Optional configuration for the stage
            
        Returns:
            Context manager for the stage execution
        """
        import contextlib
        
        @contextlib.contextmanager
        def stage_context():
            start_time = self.log_stage_start(stage_name, stage_config)
            try:
                yield
                self.log_stage_completion(stage_name, start_time)
            except Exception as e:
                self.log_stage_failure(stage_name, start_time, e)
                raise  # Re-raise the exception
                
        return stage_context()
    
    def log_stage_start(self, stage_name: str, stage_config: dict = None) -> float:
        """
        Logs the start of a pipeline stage.
        
        Args:
            stage_name: Name of the stage being started
            stage_config: Optional configuration for this stage
            
        Returns:
            Start time for duration calculation
        """
        try:
            start_time = time.time()
            
            operation_details = {
                'stage': stage_name
            }
            
            if stage_config:
                operation_details['stage_config'] = stage_config
                
            self.logger.log_start(f"stage_{stage_name}", operation_details)
            return start_time
        except Exception as e:
            error_msg = f"Failed to log stage start: {str(e)}"
            logger.error(error_msg)
            raise PipelineLoggingError(error_msg, self.pipeline_name, "log_stage_start", e)
            
    def log_stage_completion(self, stage_name: str, start_time: float, output_summary: dict = None) -> None:
        """
        Logs the successful completion of a pipeline stage.
        
        Args:
            stage_name: Name of the stage that completed
            start_time: Start time of the stage execution
            output_summary: Optional summary of the stage output
        """
        try:
            operation_details = {
                'stage': stage_name
            }
            
            if output_summary:
                operation_details['output_summary'] = output_summary
                
            self.logger.log_completion(f"stage_{stage_name}", start_time, operation_details)
        except Exception as e:
            error_msg = f"Failed to log stage completion: {str(e)}"
            logger.error(error_msg)
            raise PipelineLoggingError(error_msg, self.pipeline_name, "log_stage_completion", e)
            
    def log_stage_failure(self, stage_name: str, start_time: float, error: Exception, context: dict = None) -> None:
        """
        Logs the failure of a pipeline stage.
        
        Args:
            stage_name: Name of the stage that failed
            start_time: Start time of the stage execution
            error: The exception that caused the failure
            context: Additional context about the failure
        """
        try:
            operation_details = {
                'stage': stage_name
            }
            
            if context:
                operation_details.update(context)
                
            self.logger.log_failure(f"stage_{stage_name}", start_time, error, operation_details)
        except Exception as e:
            # Only raise a new exception if it's different from the original error
            if e is not error:
                error_msg = f"Failed to log stage failure: {str(e)}"
                logger.error(error_msg)
                raise PipelineLoggingError(error_msg, self.pipeline_name, "log_stage_failure", e)
    
    def log_data_transition(self, source_stage: str, target_stage: str, data: object, metadata: dict = None) -> None:
        """
        Logs the transition of data between pipeline stages.
        
        Args:
            source_stage: Stage that produced the data
            target_stage: Stage that will receive the data
            data: The data being transferred
            metadata: Additional metadata about the data
        """
        try:
            data_summary = get_data_summary(data)
            
            event_details = {
                'source_stage': source_stage,
                'target_stage': target_stage,
                'data_summary': data_summary
            }
            
            if metadata:
                event_details['metadata'] = metadata
                
            self.logger.log_data_event('data_transition', data, event_details)
        except Exception as e:
            error_msg = f"Failed to log data transition: {str(e)}"
            logger.error(error_msg)
            raise PipelineLoggingError(error_msg, self.pipeline_name, "log_data_transition", e)
    
    def log_data_validation(self, stage_name: str, validation_type: str, is_valid: bool, 
                          errors: list = None, context: dict = None) -> None:
        """
        Logs the validation of data within the pipeline.
        
        Args:
            stage_name: Stage where validation occurs
            validation_type: Type of validation performed
            is_valid: Whether validation passed
            errors: List of validation errors if not valid
            context: Additional context information
        """
        try:
            validation_details = {
                'stage': stage_name,
                'validation_type': validation_type,
                'is_valid': is_valid
            }
            
            if not is_valid and errors:
                validation_details['validation_errors'] = errors
            
            if context:
                validation_details.update(context)
                
            self.logger.log_validation(validation_type, is_valid, errors, validation_details)
        except Exception as e:
            error_msg = f"Failed to log data validation: {str(e)}"
            logger.error(error_msg)
            raise PipelineLoggingError(error_msg, self.pipeline_name, "log_data_validation", e)
    
    def log_fallback_trigger(self, stage_name: str, reason: str, error: Exception = None, context: dict = None) -> None:
        """
        Logs the triggering of the fallback mechanism.
        
        Args:
            stage_name: Stage where fallback was triggered
            reason: Reason for triggering fallback
            error: Optional exception that triggered fallback
            context: Additional context information
        """
        try:
            fallback_details = {
                'stage': stage_name,
                'fallback_reason': reason
            }
            
            if error:
                fallback_details['error_type'] = type(error).__name__
                fallback_details['error_message'] = str(error)
                fallback_details['error_details'] = format_exception(error)
            
            if context:
                fallback_details.update(context)
                
            self.logger.warning(
                f"Fallback mechanism triggered in stage {stage_name}: {reason}",
                extra=fallback_details
            )
        except Exception as e:
            error_msg = f"Failed to log fallback trigger: {str(e)}"
            logger.error(error_msg)
            raise PipelineLoggingError(error_msg, self.pipeline_name, "log_fallback_trigger", e)
    
    def log_metrics(self, metrics: dict) -> None:
        """
        Logs performance metrics for the pipeline execution.
        
        Args:
            metrics: Dictionary of performance metrics
        """
        try:
            formatted_metrics = format_dict_for_logging(metrics)
            
            metrics_context = {
                'metrics': formatted_metrics
            }
                
            self.logger.info(f"Pipeline metrics for {self.pipeline_name}", extra=metrics_context)
        except Exception as e:
            error_msg = f"Failed to log pipeline metrics: {str(e)}"
            logger.error(error_msg)
            raise PipelineLoggingError(error_msg, self.pipeline_name, "log_metrics", e)