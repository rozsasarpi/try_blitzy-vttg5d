"""
Custom exception classes for the scheduler component of the Electricity Market Price Forecasting System.

Defines specific exceptions for various scheduler-related error conditions including 
job scheduling, execution, monitoring, and configuration issues.
"""

import datetime
import typing

from ..pipeline.exceptions import PipelineError
from ..utils.logging_utils import format_exception


class SchedulerError(Exception):
    """
    Base exception class for all scheduler-related errors.
    """
    
    def __init__(self, message: str):
        """
        Initialize the base scheduler error with a message.
        
        Args:
            message: Error message
        """
        super().__init__(message)


class JobSchedulingError(SchedulerError):
    """
    Exception raised when a job cannot be scheduled.
    """
    
    def __init__(self, message: str, job_id: str, job_type: str, schedule_time: datetime.datetime):
        """
        Initialize with message, job ID, job type, and schedule time details.
        
        Args:
            message: Error message
            job_id: Identifier of the job that failed to schedule
            job_type: Type of the job
            schedule_time: Time when the job was scheduled to run
        """
        super().__init__(message)
        self.job_id = job_id
        self.job_type = job_type
        self.schedule_time = schedule_time


class JobExecutionError(SchedulerError):
    """
    Exception raised when a scheduled job fails to execute properly.
    """
    
    def __init__(self, message: str, job_id: str, job_type: str, 
                 execution_time: datetime.datetime, original_error: Exception):
        """
        Initialize with message, job ID, job type, execution time, and original error details.
        
        Args:
            message: Error message
            job_id: Identifier of the job that failed to execute
            job_type: Type of the job
            execution_time: Time when the job execution was attempted
            original_error: The original exception that caused the failure
        """
        super().__init__(message)
        self.job_id = job_id
        self.job_type = job_type
        self.execution_time = execution_time
        self.original_error = original_error
        self.formatted_error = format_exception(original_error)


class JobTimeoutError(SchedulerError):
    """
    Exception raised when a job execution exceeds its maximum allowed time.
    """
    
    def __init__(self, message: str, job_id: str, job_type: str, 
                 start_time: datetime.datetime, timeout_time: datetime.datetime, 
                 timeout_seconds: float):
        """
        Initialize with message, job ID, job type, start time, timeout time, and timeout seconds details.
        
        Args:
            message: Error message
            job_id: Identifier of the job that timed out
            job_type: Type of the job
            start_time: Time when the job started executing
            timeout_time: Time when the job was detected as timed out
            timeout_seconds: Maximum allowed execution time in seconds
        """
        super().__init__(message)
        self.job_id = job_id
        self.job_type = job_type
        self.start_time = start_time
        self.timeout_time = timeout_time
        self.timeout_seconds = timeout_seconds
        self.actual_runtime = (timeout_time - start_time).total_seconds()


class JobRegistryError(SchedulerError):
    """
    Exception raised when there are issues with the job registry.
    """
    
    def __init__(self, message: str, operation: str, job_id: str):
        """
        Initialize with message, operation, and job ID details.
        
        Args:
            message: Error message
            operation: Registry operation that failed
            job_id: Identifier of the job related to the registry operation
        """
        super().__init__(message)
        self.operation = operation
        self.job_id = job_id


class MonitoringError(SchedulerError):
    """
    Exception raised when there are issues with job execution monitoring.
    """
    
    def __init__(self, message: str, job_id: str, monitoring_operation: str):
        """
        Initialize with message, job ID, and monitoring operation details.
        
        Args:
            message: Error message
            job_id: Identifier of the job being monitored
            monitoring_operation: The monitoring operation that failed
        """
        super().__init__(message)
        self.job_id = job_id
        self.monitoring_operation = monitoring_operation


class ScheduleConfigurationError(SchedulerError):
    """
    Exception raised when there are issues with scheduler configuration.
    """
    
    def __init__(self, message: str, config_key: str, config_value: typing.Any):
        """
        Initialize with message, configuration key, and configuration value details.
        
        Args:
            message: Error message
            config_key: The configuration key with issues
            config_value: The problematic configuration value
        """
        super().__init__(message)
        self.config_key = config_key
        self.config_value = config_value


class SchedulerInitializationError(SchedulerError):
    """
    Exception raised when the scheduler fails to initialize properly.
    """
    
    def __init__(self, message: str, original_error: Exception):
        """
        Initialize with message and original error details.
        
        Args:
            message: Error message
            original_error: The original exception that caused the initialization failure
        """
        super().__init__(message)
        self.original_error = original_error
        self.formatted_error = format_exception(original_error)