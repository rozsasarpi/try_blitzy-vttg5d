"""
Execution monitoring module for the Electricity Market Price Forecasting System that tracks and manages job executions,
detects timeouts, and provides status information. Ensures reliable execution of the daily forecast generation
process scheduled at 7 AM CST.
"""

import datetime  # standard library - Date and time handling for job monitoring
import time  # standard library - Time measurement for execution duration
import threading  # standard library - Thread management for monitoring jobs
import typing  # standard library - Type hints for function signatures

# Internal imports
from .exceptions import MonitoringError, JobTimeoutError  # Exception for execution monitoring failures and job timeout errors
from .job_registry import get_job, update_job_status, JOB_STATUS_RUNNING, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED, JOB_STATUS_TIMEOUT  # Retrieve and update job details
from .scheduler_logging import log_job_execution_start, log_job_execution_completion, log_job_execution_failure, log_job_timeout, log_scheduler_error  # Logging functions
from ..utils.logging_utils import get_logger, format_exception  # Get a configured logger for the module
from ..utils.decorators import timing_decorator, log_exceptions  # Decorators for timing and exception logging

# Global variables
_monitored_jobs: typing.Dict[str, typing.Dict] = {}  # Dictionary to store monitored job details
_monitor_lock = threading.RLock()  # Lock for thread-safe access to _monitored_jobs
_monitor_thread: Optional[threading.Thread] = None  # Reference to the monitoring thread
_monitoring_active: bool = False  # Flag to control the monitoring thread's execution
DEFAULT_TIMEOUT_SECONDS: float = 3600  # Default timeout for job execution (1 hour)
MONITOR_CHECK_INTERVAL: float = 10  # Interval (in seconds) to check for job timeouts
logger = get_logger(__name__)  # Get a configured logger for the module


@timing_decorator
@log_exceptions
def start_job_monitoring(job_id: str, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
    """
    Starts monitoring a job execution with timeout detection.

    Args:
        job_id: Identifier of the job to monitor
        timeout_seconds: Maximum allowed execution time in seconds (default: DEFAULT_TIMEOUT_SECONDS)

    Returns:
        True if monitoring started successfully, False otherwise

    Raises:
        MonitoringError: If monitoring fails to start
    """
    try:
        with _monitor_lock:  # Acquire _monitor_lock to ensure thread safety
            job_details = get_job(job_id)  # Get job details from job registry using get_job(job_id)
            if not job_details:  # If job doesn't exist, log error and return False
                logger.error(f"Job with ID {job_id} not found in registry")
                return False

            # Create monitoring entry with job_id, start_time, timeout_seconds, and status
            monitoring_entry = {
                "job_id": job_id,
                "start_time": datetime.datetime.now(),
                "timeout_seconds": timeout_seconds,
                "job_type": job_details["job_type"] if "job_type" in job_details else "unknown",
            }

            _monitored_jobs[job_id] = monitoring_entry  # Add monitoring entry to _monitored_jobs dictionary

            if not _monitoring_active:  # Start the monitoring thread if not already running
                _start_monitor_thread()

            log_job_execution_start(job_id, monitoring_entry["job_type"])  # Log job execution start with log_job_execution_start
            update_job_status(job_id, JOB_STATUS_RUNNING)  # Update job status to JOB_STATUS_RUNNING

        return True  # Return True for successful monitoring start

    except Exception as e:
        error_msg = f"Failed to start monitoring job {job_id}: {str(e)}"
        log_scheduler_error(error_msg, e, {"operation": "start_job_monitoring", "job_id": job_id})
        raise MonitoringError(error_msg, job_id, "start_monitoring") from e  # Handle any exceptions, log with log_scheduler_error, and raise MonitoringError


@timing_decorator
@log_exceptions
def stop_job_monitoring(job_id: str, success: bool, error: Optional[Exception] = None, execution_details: Optional[typing.Dict] = None) -> bool:
    """
    Stops monitoring a job and records completion status.

    Args:
        job_id: Identifier of the job to stop monitoring
        success: True if the job completed successfully, False otherwise
        error: Exception raised during job execution, if any
        execution_details: Additional details about the job execution

    Returns:
        True if monitoring stopped successfully, False otherwise

    Raises:
        MonitoringError: If monitoring fails to stop
    """
    try:
        with _monitor_lock:  # Acquire _monitor_lock to ensure thread safety
            if job_id not in _monitored_jobs:  # Check if job_id exists in _monitored_jobs
                logger.error(f"Job with ID {job_id} is not currently being monitored")
                return False  # If job doesn't exist in monitored jobs, log error and return False

            monitoring_entry = _monitored_jobs[job_id]  # Get monitoring entry from _monitored_jobs
            start_time = monitoring_entry["start_time"]
            job_type = monitoring_entry["job_type"]

            execution_time = (datetime.datetime.now() - start_time).total_seconds()  # Calculate execution time from start_time to now
            del _monitored_jobs[job_id]  # Remove job from _monitored_jobs

            if success:  # If success is True, log completion with log_job_execution_completion
                log_job_execution_completion(job_id, job_type, execution_time, execution_details)
                update_job_status(job_id, JOB_STATUS_COMPLETED, execution_details)  # Update job status to JOB_STATUS_COMPLETED
            else:  # If success is False, log failure with log_job_execution_failure
                log_job_execution_failure(job_id, job_type, execution_time, error, execution_details)
                status_details = {"error": str(error)} if error else {}
                update_job_status(job_id, JOB_STATUS_FAILED, status_details)  # Update job status to JOB_STATUS_FAILED

        return True  # Return True for successful monitoring stop

    except Exception as e:
        error_msg = f"Failed to stop monitoring job {job_id}: {str(e)}"
        log_scheduler_error(error_msg, e, {"operation": "stop_job_monitoring", "job_id": job_id})
        raise MonitoringError(error_msg, job_id, "stop_monitoring") from e  # Handle any exceptions, log with log_scheduler_error, and raise MonitoringError


@log_exceptions
def get_monitored_jobs() -> typing.List[typing.Dict]:
    """
    Gets a list of all currently monitored jobs.

    Returns:
        List of monitored job details

    Raises:
        MonitoringError: If retrieval operation fails
    """
    try:
        with _monitor_lock:  # Acquire _monitor_lock to ensure thread safety
            monitored_jobs = list(_monitored_jobs.values())  # Create a copy of all monitored job entries
        return monitored_jobs  # Return the list of monitored jobs

    except Exception as e:
        error_msg = f"Failed to get monitored jobs: {str(e)}"
        log_scheduler_error(error_msg, e, {"operation": "get_monitored_jobs"})
        raise MonitoringError(error_msg, "all", "get_monitored_jobs") from e  # Handle any exceptions, log with log_scheduler_error, and raise MonitoringError


def _start_monitor_thread() -> None:
    """
    Starts the background thread that monitors job timeouts.

    Returns:
        None: Function performs side effects only

    Raises:
        MonitoringError: If thread fails to start
    """
    try:
        with _monitor_lock:  # Acquire _monitor_lock to ensure thread safety
            if _monitoring_active:  # Check if monitoring thread is already running
                logger.debug("Monitoring thread already running")
                return

            _monitoring_active = True  # Set _monitoring_active to True
            _monitor_thread_local = threading.Thread(target=_monitor_jobs_thread, daemon=True)  # Create and start a daemon thread that runs _monitor_jobs_thread function
            _monitor_thread = _monitor_thread_local  # Store thread reference in _monitor_thread
            _monitor_thread.start()

        logger.info("Started job monitoring thread")  # Log monitor thread start

    except Exception as e:
        error_msg = f"Failed to start monitoring thread: {str(e)}"
        log_scheduler_error(error_msg, e, {"operation": "start_monitor_thread"})
        raise MonitoringError(error_msg, "all", "start_monitor_thread") from e  # Handle any exceptions, log with log_scheduler_error, and raise MonitoringError


def _stop_monitor_thread() -> None:
    """
    Stops the background monitoring thread.

    Returns:
        None: Function performs side effects only

    Raises:
        MonitoringError: If thread fails to stop
    """
    try:
        with _monitor_lock:  # Acquire _monitor_lock to ensure thread safety
            _monitoring_active = False  # Set _monitoring_active to False
            _monitor_thread = None  # Clear _monitor_thread reference

        logger.info("Stopped job monitoring thread")  # Log monitor thread stop

    except Exception as e:
        error_msg = f"Failed to stop monitoring thread: {str(e)}"
        log_scheduler_error(error_msg, e, {"operation": "stop_monitor_thread"})
        raise MonitoringError(error_msg, "all", "stop_monitor_thread") from e  # Handle any exceptions, log with log_scheduler_error, and raise MonitoringError


def _monitor_jobs_thread() -> None:
    """
    Background thread function that checks for job timeouts.

    Returns:
        None: Function performs side effects only
    """
    while _monitoring_active:  # While _monitoring_active is True:
        time.sleep(MONITOR_CHECK_INTERVAL)  # Sleep for MONITOR_CHECK_INTERVAL seconds
        _check_for_timeouts()  # Check for timed out jobs with _check_for_timeouts
        if not _monitored_jobs:  # If no more jobs to monitor, stop the monitoring thread
            _stop_monitor_thread()


def _check_for_timeouts() -> None:
    """
    Checks all monitored jobs for timeout conditions.

    Returns:
        None: Function performs side effects only
    """
    try:
        with _monitor_lock:  # Acquire _monitor_lock to ensure thread safety
            current_time = datetime.datetime.now()  # Get current time
            timed_out_jobs = []  # Create a list of timed out jobs

            for job_id, job_info in _monitored_jobs.items():  # For each job in _monitored_jobs:
                start_time = job_info["start_time"]
                timeout_seconds = job_info["timeout_seconds"]
                elapsed_time = (current_time - start_time).total_seconds()  # Calculate elapsed time since job start

                if elapsed_time > timeout_seconds:  # If elapsed time exceeds timeout_seconds, add to timed out list
                    timed_out_jobs.append((job_id, job_info, elapsed_time))

            for job_id, job_info, elapsed_time in timed_out_jobs:  # For each timed out job:
                _handle_job_timeout(job_id, job_info, elapsed_time)  # Handle the timeout with _handle_job_timeout

    except Exception as e:
        log_scheduler_error(f"Error checking for timeouts: {str(e)}", e, {"operation": "check_for_timeouts"})  # Handle any exceptions, log with log_scheduler_error


def _handle_job_timeout(job_id: str, job_info: typing.Dict, elapsed_time: float) -> None:
    """
    Handles a job that has exceeded its timeout period.

    Args:
        job_id: Identifier of the job that timed out
        job_info: Details about the job
        elapsed_time: Time elapsed since the job started

    Returns:
        None: Function performs side effects only
    """
    try:
        with _monitor_lock:  # Acquire _monitor_lock to ensure thread safety
            del _monitored_jobs[job_id]  # Remove job from _monitored_jobs
            job_details = get_job(job_id)  # Get job details from job registry
            job_type = job_info["job_type"]

            timeout_details = {
                "elapsed_time": elapsed_time,
                "timeout_seconds": job_info["timeout_seconds"],
            }

            update_job_status(job_id, JOB_STATUS_TIMEOUT, timeout_details)  # Update job status to JOB_STATUS_TIMEOUT
            log_job_timeout(job_id, job_type, job_info["timeout_seconds"], elapsed_time, timeout_details)  # Log job timeout with log_job_timeout

    except Exception as e:
        log_scheduler_error(f"Error handling timeout for job {job_id}: {str(e)}", e, {"operation": "handle_job_timeout", "job_id": job_id})  # Handle any exceptions, log with log_scheduler_error


def is_job_monitored(job_id: str) -> bool:
    """
    Checks if a job is currently being monitored.

    Args:
        job_id: Identifier of the job to check

    Returns:
        True if job is monitored, False otherwise

    Raises:
        MonitoringError: If check operation fails
    """
    try:
        with _monitor_lock:  # Acquire _monitor_lock to ensure thread safety
            is_monitored = job_id in _monitored_jobs  # Check if job_id exists in _monitored_jobs
        return is_monitored  # Return True if job is monitored, False otherwise

    except Exception as e:
        error_msg = f"Failed to check if job {job_id} is monitored: {str(e)}"
        log_scheduler_error(error_msg, e, {"operation": "is_job_monitored", "job_id": job_id})
        raise MonitoringError(error_msg, job_id, "is_job_monitored") from e  # Handle any exceptions, log with log_scheduler_error, and raise MonitoringError


def get_job_monitoring_info(job_id: str) -> Optional[typing.Dict]:
    """
    Gets monitoring information for a specific job.

    Args:
        job_id: Identifier of the job to get monitoring information for

    Returns:
        Job monitoring information or None if not monitored

    Raises:
        MonitoringError: If retrieval operation fails
    """
    try:
        with _monitor_lock:  # Acquire _monitor_lock to ensure thread safety
            if job_id in _monitored_jobs:  # Check if job_id exists in _monitored_jobs
                job_info = _monitored_jobs[job_id].copy()  # If job is monitored, create a copy of the monitoring information
                elapsed_time = (datetime.datetime.now() - job_info["start_time"]).total_seconds()
                job_info["elapsed_time"] = elapsed_time  # Add elapsed time to the information
                return job_info
            else:
                return None  # Return None if job not monitored

    except Exception as e:
        error_msg = f"Failed to get monitoring info for job {job_id}: {str(e)}"
        log_scheduler_error(error_msg, e, {"operation": "get_job_monitoring_info", "job_id": job_id})
        raise MonitoringError(error_msg, job_id, "get_job_monitoring_info") from e  # Handle any exceptions, log with log_scheduler_error, and raise MonitoringError


def get_monitoring_status() -> typing.Dict:
    """
    Gets the overall status of the job monitoring system.

    Returns:
        Monitoring system status information

    Raises:
        MonitoringError: If retrieval operation fails
    """
    try:
        with _monitor_lock:  # Acquire _monitor_lock to ensure thread safety
            status = {  # Create status dictionary with active status, job count, and thread status
                "active": _monitoring_active,
                "job_count": len(_monitored_jobs),
                "thread_alive": _monitor_thread is not None and _monitor_thread.is_alive()
            }
        return status  # Return the status dictionary

    except Exception as e:
        error_msg = f"Failed to get monitoring status: {str(e)}"
        log_scheduler_error(error_msg, e, {"operation": "get_monitoring_status"})
        raise MonitoringError(error_msg, "all", "get_monitoring_status") from e  # Handle any exceptions, log with log_scheduler_error, and raise MonitoringError