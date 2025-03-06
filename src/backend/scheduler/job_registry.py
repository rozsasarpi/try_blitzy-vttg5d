"""
Registry for managing scheduled jobs in the Electricity Market Price Forecasting System.

This module provides functionality to register, retrieve, update, and query jobs with their
statuses, supporting the daily forecast generation process scheduled at 7 AM CST.
"""
import datetime
import uuid
import threading
from typing import List, Dict, Any, Optional

from .exceptions import JobRegistryError
from .scheduler_logging import (
    log_job_registration,
    log_job_status_update,
    log_scheduler_error
)
from ..utils.logging_utils import format_dict_for_logging

# Global registry dictionary and lock for thread safety
_job_registry = {}
_registry_lock = threading.RLock()

# Job status constants
JOB_STATUS_PENDING = "pending"
JOB_STATUS_RUNNING = "running"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_TIMEOUT = "timeout"
JOB_STATUS_INTERRUPTED = "interrupted"

VALID_JOB_STATUSES = [
    JOB_STATUS_PENDING,
    JOB_STATUS_RUNNING,
    JOB_STATUS_COMPLETED, 
    JOB_STATUS_FAILED,
    JOB_STATUS_TIMEOUT,
    JOB_STATUS_INTERRUPTED
]

def register_job(job_id: str = None, job_type: str = None,
               schedule_time: datetime.datetime = None,
               job_params: Dict = None) -> str:
    """
    Registers a new job in the registry with initial status.
    
    Args:
        job_id: Optional ID for the job, will be generated if not provided
        job_type: Type of job (e.g., 'forecast_generation')
        schedule_time: When the job is scheduled to run
        job_params: Additional parameters for the job
        
    Returns:
        ID of the registered job
        
    Raises:
        JobRegistryError: If job registration fails or job_id already exists
    """
    try:
        # Generate job_id if not provided
        if job_id is None:
            job_id = str(uuid.uuid4())
            
        with _registry_lock:
            # Check if job with same ID already exists
            if job_id in _job_registry:
                raise JobRegistryError(
                    f"Job with ID {job_id} already exists in registry",
                    "register_job",
                    job_id
                )
            
            # Create job entry
            job_entry = {
                "job_id": job_id,
                "job_type": job_type,
                "schedule_time": schedule_time,
                "creation_time": datetime.datetime.now(),
                "status": JOB_STATUS_PENDING,
                "job_params": job_params or {}
            }
            
            # Add to registry
            _job_registry[job_id] = job_entry
            
            # Log registration
            log_job_registration(
                job_id, 
                job_type,
                schedule_time, 
                job_params or {}
            )
            
            return job_id
    except Exception as e:
        if not isinstance(e, JobRegistryError):
            error_msg = f"Failed to register job: {str(e)}"
            log_scheduler_error(
                error_msg, 
                e, 
                {"operation": "register_job", "job_id": job_id}
            )
            raise JobRegistryError(error_msg, "register_job", job_id)
        raise

def get_job(job_id: str) -> Dict:
    """
    Retrieves job details by ID.
    
    Args:
        job_id: ID of the job to retrieve
        
    Returns:
        Job details dictionary or None if not found
        
    Raises:
        JobRegistryError: If retrieval operation fails
    """
    try:
        with _registry_lock:
            if job_id in _job_registry:
                # Return a copy to prevent modification of registry data
                return dict(_job_registry[job_id])
            else:
                return None
    except Exception as e:
        error_msg = f"Failed to retrieve job {job_id}: {str(e)}"
        log_scheduler_error(
            error_msg, 
            e, 
            {"operation": "get_job", "job_id": job_id}
        )
        raise JobRegistryError(error_msg, "get_job", job_id)

def update_job_status(job_id: str, status: str, 
                     status_details: Dict = None) -> bool:
    """
    Updates the status of a registered job.
    
    Args:
        job_id: ID of the job to update
        status: New status value (must be from VALID_JOB_STATUSES)
        status_details: Additional details about the status change
        
    Returns:
        True if update successful, False if job not found
        
    Raises:
        ValueError: If status is not valid
        JobRegistryError: If update operation fails
    """
    if status not in VALID_JOB_STATUSES:
        raise ValueError(f"Invalid job status: {status}. "
                         f"Must be one of {VALID_JOB_STATUSES}")
    try:
        with _registry_lock:
            if job_id not in _job_registry:
                return False
            
            # Store previous status for logging
            previous_status = _job_registry[job_id]["status"]
            
            # Update status
            _job_registry[job_id]["status"] = status
            
            # Add status update timestamp
            _job_registry[job_id]["status_update_time"] = datetime.datetime.now()
            
            # Add status details if provided
            if status_details:
                _job_registry[job_id]["status_details"] = status_details
            
            # Log status update
            log_job_status_update(
                job_id, 
                previous_status, 
                status, 
                status_details
            )
            
            return True
    except Exception as e:
        if not isinstance(e, ValueError):
            error_msg = f"Failed to update job status for {job_id}: {str(e)}"
            log_scheduler_error(
                error_msg, 
                e, 
                {"operation": "update_job_status", "job_id": job_id}
            )
            raise JobRegistryError(error_msg, "update_job_status", job_id)
        raise

def get_jobs_by_status(status: str) -> List[Dict]:
    """
    Gets all jobs with a specific status.
    
    Args:
        status: Status to filter by (must be from VALID_JOB_STATUSES)
        
    Returns:
        List of job details dictionaries
        
    Raises:
        ValueError: If status is not valid
        JobRegistryError: If operation fails
    """
    if status not in VALID_JOB_STATUSES:
        raise ValueError(f"Invalid job status: {status}. "
                         f"Must be one of {VALID_JOB_STATUSES}")
    try:
        with _registry_lock:
            matching_jobs = []
            for job_id, job_data in _job_registry.items():
                if job_data["status"] == status:
                    # Return copies to prevent modification of registry data
                    matching_jobs.append(dict(job_data))
            return matching_jobs
    except Exception as e:
        if not isinstance(e, ValueError):
            error_msg = f"Failed to get jobs by status {status}: {str(e)}"
            log_scheduler_error(
                error_msg, 
                e, 
                {"operation": "get_jobs_by_status", "status": status}
            )
            raise JobRegistryError(error_msg, "get_jobs_by_status", "")
        raise

def get_jobs_by_type(job_type: str) -> List[Dict]:
    """
    Gets all jobs of a specific type.
    
    Args:
        job_type: Job type to filter by
        
    Returns:
        List of job details dictionaries
        
    Raises:
        JobRegistryError: If operation fails
    """
    try:
        with _registry_lock:
            matching_jobs = []
            for job_id, job_data in _job_registry.items():
                if job_data["job_type"] == job_type:
                    # Return copies to prevent modification of registry data
                    matching_jobs.append(dict(job_data))
            return matching_jobs
    except Exception as e:
        error_msg = f"Failed to get jobs by type {job_type}: {str(e)}"
        log_scheduler_error(
            error_msg, 
            e, 
            {"operation": "get_jobs_by_type", "job_type": job_type}
        )
        raise JobRegistryError(error_msg, "get_jobs_by_type", "")

def get_all_jobs() -> List[Dict]:
    """
    Gets all jobs in the registry.
    
    Returns:
        List of all job details dictionaries
        
    Raises:
        JobRegistryError: If operation fails
    """
    try:
        with _registry_lock:
            # Return copies to prevent modification of registry data
            return [dict(job_data) for job_data in _job_registry.values()]
    except Exception as e:
        error_msg = f"Failed to get all jobs: {str(e)}"
        log_scheduler_error(
            error_msg, 
            e, 
            {"operation": "get_all_jobs"}
        )
        raise JobRegistryError(error_msg, "get_all_jobs", "")

def clear_registry() -> int:
    """
    Clears all jobs from the registry.
    
    Returns:
        Number of jobs cleared
        
    Raises:
        JobRegistryError: If operation fails
    """
    try:
        with _registry_lock:
            job_count = len(_job_registry)
            _job_registry.clear()
            return job_count
    except Exception as e:
        error_msg = f"Failed to clear registry: {str(e)}"
        log_scheduler_error(
            error_msg, 
            e, 
            {"operation": "clear_registry"}
        )
        raise JobRegistryError(error_msg, "clear_registry", "")

def remove_job(job_id: str) -> bool:
    """
    Removes a specific job from the registry.
    
    Args:
        job_id: ID of the job to remove
        
    Returns:
        True if job was removed, False if not found
        
    Raises:
        JobRegistryError: If operation fails
    """
    try:
        with _registry_lock:
            if job_id not in _job_registry:
                return False
            
            del _job_registry[job_id]
            return True
    except Exception as e:
        error_msg = f"Failed to remove job {job_id}: {str(e)}"
        log_scheduler_error(
            error_msg, 
            e, 
            {"operation": "remove_job", "job_id": job_id}
        )
        raise JobRegistryError(error_msg, "remove_job", job_id)

def get_job_count() -> int:
    """
    Gets the total number of jobs in the registry.
    
    Returns:
        Number of jobs in the registry
        
    Raises:
        JobRegistryError: If operation fails
    """
    try:
        with _registry_lock:
            return len(_job_registry)
    except Exception as e:
        error_msg = f"Failed to get job count: {str(e)}"
        log_scheduler_error(
            error_msg, 
            e, 
            {"operation": "get_job_count"}
        )
        raise JobRegistryError(error_msg, "get_job_count", "")