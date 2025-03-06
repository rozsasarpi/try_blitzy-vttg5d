"""
Specialized logging module for the scheduler component of the Electricity Market Price Forecasting System.

This module provides structured logging functions for scheduler events, job execution, and error handling
to support monitoring and observability requirements. It implements a consistent logging format
and contextual information to facilitate troubleshooting and system monitoring.
"""

import logging  # standard library
import datetime  # standard library
import time  # standard library
from typing import Dict, Any, Optional, List  # standard library

# Internal imports
from ..utils.logging_utils import (
    get_logger,
    ComponentLogger,
    format_exception,
    format_dict_for_logging
)

# Global logger instance for the scheduler component
logger = ComponentLogger('scheduler', {'component': 'scheduler'})


def log_scheduler_startup(config: Dict[str, Any]) -> None:
    """
    Logs the startup of the scheduler service.
    
    Args:
        config: Configuration dictionary used for scheduler initialization
    """
    formatted_config = format_dict_for_logging(config)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    version = config.get('version', 'unknown')
    
    logger.info(f"Scheduler service starting at {timestamp} with version {version}. Configuration: {formatted_config}")


def log_scheduler_shutdown(reason: str, details: Dict[str, Any]) -> None:
    """
    Logs the shutdown of the scheduler service.
    
    Args:
        reason: Reason for shutdown (e.g., 'normal', 'error', 'restart')
        details: Additional details about the shutdown
    """
    formatted_details = format_dict_for_logging(details)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    uptime = details.get('uptime_seconds', 'unknown')
    
    logger.info(f"Scheduler service shutting down at {timestamp}. Reason: {reason}, Uptime: {uptime} seconds. Details: {formatted_details}")


def log_scheduler_error(message: str, error: Exception, context: Dict[str, Any]) -> None:
    """
    Logs an error that occurred in the scheduler.
    
    Args:
        message: Error message describing what happened
        error: Exception that was raised
        context: Contextual information about when/where the error occurred
    """
    formatted_exception = format_exception(error)
    formatted_context = format_dict_for_logging(context)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    location = context.get('location', 'unknown')
    
    logger.error(f"Scheduler error at {timestamp} in {location}: {message}. Exception: {formatted_exception}, Context: {formatted_context}")


def log_job_registration(job_id: str, job_type: str, schedule_time: datetime.datetime, job_params: Dict[str, Any]) -> None:
    """
    Logs the registration of a new job in the scheduler.
    
    Args:
        job_id: Unique identifier for the job
        job_type: Type of job (e.g., 'forecast_generation')
        schedule_time: When the job is scheduled to run
        job_params: Parameters for the job execution
    """
    job_details = {
        'job_id': job_id,
        'job_type': job_type,
        'schedule_time': schedule_time.strftime('%Y-%m-%d %H:%M:%S'),
        'job_params': job_params
    }
    
    formatted_details = format_dict_for_logging(job_details)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Job registered at {timestamp}: {job_id} ({job_type}). Details: {formatted_details}")


def log_job_execution_start(job_id: str, job_type: str, execution_details: Optional[Dict[str, Any]] = None) -> None:
    """
    Logs the start of a job execution.
    
    Args:
        job_id: Unique identifier for the job
        job_type: Type of job (e.g., 'forecast_generation')
        execution_details: Additional details about the execution
    """
    context = {
        'job_id': job_id,
        'job_type': job_type,
        'start_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    if execution_details:
        context.update(execution_details)
        
    expected_duration = execution_details.get('expected_duration_seconds', 'unknown') if execution_details else 'unknown'
    
    logger.log_start(f"job execution {job_id}", context)
    logger.info(f"Starting job execution: {job_id} ({job_type}). Expected duration: {expected_duration} seconds")


def log_job_execution_completion(job_id: str, job_type: str, execution_time: float, 
                                execution_details: Optional[Dict[str, Any]] = None) -> None:
    """
    Logs the successful completion of a job execution.
    
    Args:
        job_id: Unique identifier for the job
        job_type: Type of job (e.g., 'forecast_generation')
        execution_time: Time taken to execute the job in seconds
        execution_details: Additional details about the execution
    """
    context = {
        'job_id': job_id,
        'job_type': job_type,
        'execution_time_seconds': f"{execution_time:.3f}",
        'status': 'completed',
        'completion_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    if execution_details:
        context.update(execution_details)
    
    logger.log_completion(f"job execution {job_id}", time.time() - execution_time, context)
    logger.info(f"Job execution completed: {job_id} ({job_type}) in {execution_time:.3f} seconds")


def log_job_execution_failure(job_id: str, job_type: str, execution_time: float, error: Exception,
                             execution_details: Optional[Dict[str, Any]] = None) -> None:
    """
    Logs the failure of a job execution.
    
    Args:
        job_id: Unique identifier for the job
        job_type: Type of job (e.g., 'forecast_generation')
        execution_time: Time taken before failure in seconds
        error: Exception that caused the failure
        execution_details: Additional details about the execution
    """
    formatted_exception = format_exception(error)
    
    context = {
        'job_id': job_id,
        'job_type': job_type,
        'execution_time_seconds': f"{execution_time:.3f}",
        'status': 'failed',
        'error_type': type(error).__name__,
        'error_message': str(error),
        'error_details': formatted_exception,
        'failure_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    if execution_details:
        context.update(execution_details)
    
    logger.log_failure(f"job execution {job_id}", time.time() - execution_time, error, context)
    logger.error(f"Job execution failed: {job_id} ({job_type}) after {execution_time:.3f} seconds. Error: {str(error)}")


def log_job_timeout(job_id: str, job_type: str, timeout_seconds: float, elapsed_time: float,
                   execution_details: Optional[Dict[str, Any]] = None) -> None:
    """
    Logs a job execution timeout.
    
    Args:
        job_id: Unique identifier for the job
        job_type: Type of job (e.g., 'forecast_generation')
        timeout_seconds: Configured timeout in seconds
        elapsed_time: Actual elapsed time before timeout in seconds
        execution_details: Additional details about the execution
    """
    context = {
        'job_id': job_id,
        'job_type': job_type,
        'timeout_seconds': f"{timeout_seconds:.3f}",
        'elapsed_time_seconds': f"{elapsed_time:.3f}",
        'status': 'timeout',
        'timeout_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    if execution_details:
        context.update(execution_details)
    
    logger.error(f"Job execution timeout: {job_id} ({job_type}). Configured timeout: {timeout_seconds:.3f} seconds, Elapsed time: {elapsed_time:.3f} seconds. Context: {format_dict_for_logging(context)}")


def log_job_status_update(job_id: str, previous_status: str, new_status: str,
                         status_details: Optional[Dict[str, Any]] = None) -> None:
    """
    Logs an update to a job's status.
    
    Args:
        job_id: Unique identifier for the job
        previous_status: Previous status of the job
        new_status: New status of the job
        status_details: Additional details about the status change
    """
    context = {
        'job_id': job_id,
        'previous_status': previous_status,
        'new_status': new_status,
        'update_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    if status_details:
        context.update(status_details)
    
    # Determine log level based on new status
    if new_status.lower() in ('failed', 'timeout', 'error'):
        logger.error(f"Job status changed: {job_id} from '{previous_status}' to '{new_status}'. Context: {format_dict_for_logging(context)}")
    else:
        logger.info(f"Job status changed: {job_id} from '{previous_status}' to '{new_status}'. Context: {format_dict_for_logging(context)}")


def log_scheduler_configuration(config: Dict[str, Any]) -> None:
    """
    Logs the configuration of the scheduler.
    
    Args:
        config: Configuration dictionary
    """
    formatted_config = format_dict_for_logging(config)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Scheduler configuration at {timestamp}: {formatted_config}")


def log_scheduler_job_added(job_id: str, job_type: str, next_run_time: datetime.datetime,
                           job_details: Optional[Dict[str, Any]] = None) -> None:
    """
    Logs the addition of a job to the scheduler.
    
    Args:
        job_id: Unique identifier for the job
        job_type: Type of job (e.g., 'forecast_generation')
        next_run_time: When the job is scheduled to run next
        job_details: Additional details about the job
    """
    context = {
        'job_id': job_id,
        'job_type': job_type,
        'next_run_time': next_run_time.strftime('%Y-%m-%d %H:%M:%S'),
        'addition_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    if job_details:
        context.update(job_details)
    
    logger.info(f"Job added to scheduler: {job_id} ({job_type}). Next run scheduled for {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}. Context: {format_dict_for_logging(context)}")


def log_scheduler_job_removed(job_id: str, reason: str) -> None:
    """
    Logs the removal of a job from the scheduler.
    
    Args:
        job_id: Unique identifier for the job
        reason: Reason for removal
    """
    context = {
        'job_id': job_id,
        'reason': reason,
        'removal_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    logger.info(f"Job removed from scheduler: {job_id}. Reason: {reason}. Context: {format_dict_for_logging(context)}")


def log_scheduler_status(status: Dict[str, Any]) -> None:
    """
    Logs the current status of the scheduler.
    
    Args:
        status: Dictionary containing scheduler status information
    """
    formatted_status = format_dict_for_logging(status)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    running = status.get('running', False)
    job_count = status.get('job_count', 0)
    
    logger.info(f"Scheduler status at {timestamp}: Running: {running}, Jobs: {job_count}. Details: {formatted_status}")