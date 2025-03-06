"""
Unit tests for the job registry module of the scheduler component.

These tests verify the functionality for registering, retrieving, updating,
and querying jobs with their statuses, ensuring the reliability of the daily
forecast generation process.
"""
import datetime
import uuid
from unittest import mock

import pytest

from ...scheduler.job_registry import (
    register_job,
    get_job,
    update_job_status,
    get_jobs_by_status,
    get_jobs_by_type,
    get_all_jobs,
    clear_registry,
    remove_job,
    get_job_count,
    JOB_STATUS_PENDING,
    JOB_STATUS_RUNNING,
    JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED,
    VALID_JOB_STATUSES
)
from ...scheduler.exceptions import JobRegistryError


def setup_function():
    """
    Setup function that runs before each test to ensure a clean registry.
    """
    clear_registry()
    assert get_job_count() == 0


def teardown_function():
    """
    Teardown function that runs after each test to clean up the registry.
    """
    clear_registry()
    assert get_job_count() == 0


def test_register_job():
    """
    Test that a job can be successfully registered in the registry.
    """
    # Arrange
    job_type = "forecast_generation"
    schedule_time = datetime.datetime.now() + datetime.timedelta(hours=1)
    job_params = {"product": "DALMP", "horizon": 72}
    
    # Act
    job_id = register_job(
        job_type=job_type,
        schedule_time=schedule_time,
        job_params=job_params
    )
    
    # Assert
    assert job_id is not None
    job = get_job(job_id)
    assert job is not None
    assert job["job_type"] == job_type
    assert job["schedule_time"] == schedule_time
    assert job["job_params"] == job_params
    assert job["status"] == JOB_STATUS_PENDING
    assert get_job_count() == 1


def test_register_job_with_custom_id():
    """
    Test that a job can be registered with a custom ID.
    """
    # Arrange
    custom_job_id = str(uuid.uuid4())
    job_type = "forecast_generation"
    schedule_time = datetime.datetime.now() + datetime.timedelta(hours=1)
    
    # Act
    job_id = register_job(
        job_id=custom_job_id,
        job_type=job_type,
        schedule_time=schedule_time
    )
    
    # Assert
    assert job_id == custom_job_id
    job = get_job(custom_job_id)
    assert job is not None
    assert job["job_id"] == custom_job_id
    assert job["job_type"] == job_type


def test_register_duplicate_job_id():
    """
    Test that registering a job with an existing ID raises an error.
    """
    # Arrange
    custom_job_id = str(uuid.uuid4())
    job_type = "forecast_generation"
    
    # Register first job
    register_job(
        job_id=custom_job_id,
        job_type=job_type
    )
    
    # Act & Assert
    with pytest.raises(JobRegistryError):
        register_job(
            job_id=custom_job_id,
            job_type="another_job_type"
        )
    
    # Verify only one job exists
    assert get_job_count() == 1


def test_get_job():
    """
    Test retrieving a job by ID.
    """
    # Arrange
    job_type = "forecast_generation"
    job_id = register_job(job_type=job_type)
    
    # Act
    job = get_job(job_id)
    
    # Assert
    assert job is not None
    assert job["job_id"] == job_id
    assert job["job_type"] == job_type
    
    # Test non-existent job
    non_existent_job = get_job("non_existent_id")
    assert non_existent_job is None


def test_update_job_status():
    """
    Test updating the status of a registered job.
    """
    # Arrange
    job_type = "forecast_generation"
    job_id = register_job(job_type=job_type)
    
    # Act - update to running
    result = update_job_status(job_id, JOB_STATUS_RUNNING)
    
    # Assert
    assert result is True
    job = get_job(job_id)
    assert job["status"] == JOB_STATUS_RUNNING
    
    # Update to completed
    update_job_status(job_id, JOB_STATUS_COMPLETED)
    job = get_job(job_id)
    assert job["status"] == JOB_STATUS_COMPLETED
    
    # Test updating non-existent job
    result = update_job_status("non_existent_id", JOB_STATUS_RUNNING)
    assert result is False


def test_update_job_status_with_details():
    """
    Test updating job status with additional status details.
    """
    # Arrange
    job_id = register_job(job_type="forecast_generation")
    status_details = {
        "progress": 50,
        "message": "Processing data",
        "timestamp": datetime.datetime.now().isoformat()
    }
    
    # Act
    update_job_status(job_id, JOB_STATUS_RUNNING, status_details)
    
    # Assert
    job = get_job(job_id)
    assert job["status"] == JOB_STATUS_RUNNING
    assert "status_details" in job
    assert job["status_details"]["progress"] == 50
    assert job["status_details"]["message"] == "Processing data"


def test_update_job_status_invalid_status():
    """
    Test that updating a job with an invalid status raises an error.
    """
    # Arrange
    job_id = register_job(job_type="forecast_generation")
    
    # Act & Assert
    with pytest.raises(ValueError):
        update_job_status(job_id, "invalid_status")
    
    # Verify status was not changed
    job = get_job(job_id)
    assert job["status"] == JOB_STATUS_PENDING


def test_get_jobs_by_status():
    """
    Test retrieving jobs by status.
    """
    # Arrange
    # Create multiple jobs with different statuses
    job1 = register_job(job_type="forecast_generation")
    job2 = register_job(job_type="data_collection")
    job3 = register_job(job_type="forecast_generation")
    
    # All jobs start as pending
    pending_jobs = get_jobs_by_status(JOB_STATUS_PENDING)
    assert len(pending_jobs) == 3
    
    # Update some jobs to running
    update_job_status(job1, JOB_STATUS_RUNNING)
    update_job_status(job3, JOB_STATUS_RUNNING)
    
    # Act
    pending_jobs = get_jobs_by_status(JOB_STATUS_PENDING)
    running_jobs = get_jobs_by_status(JOB_STATUS_RUNNING)
    
    # Assert
    assert len(pending_jobs) == 1
    assert len(running_jobs) == 2
    assert pending_jobs[0]["job_id"] == job2
    assert running_jobs[0]["status"] == JOB_STATUS_RUNNING
    assert running_jobs[1]["status"] == JOB_STATUS_RUNNING


def test_get_jobs_by_status_invalid_status():
    """
    Test that retrieving jobs with an invalid status raises an error.
    """
    # Act & Assert
    with pytest.raises(ValueError):
        get_jobs_by_status("invalid_status")


def test_get_jobs_by_type():
    """
    Test retrieving jobs by type.
    """
    # Arrange
    forecast_job1 = register_job(job_type="forecast_generation")
    forecast_job2 = register_job(job_type="forecast_generation")
    data_job = register_job(job_type="data_collection")
    
    # Act
    forecast_jobs = get_jobs_by_type("forecast_generation")
    data_jobs = get_jobs_by_type("data_collection")
    
    # Assert
    assert len(forecast_jobs) == 2
    assert len(data_jobs) == 1
    assert forecast_jobs[0]["job_type"] == "forecast_generation"
    assert forecast_jobs[1]["job_type"] == "forecast_generation"
    assert data_jobs[0]["job_type"] == "data_collection"


def test_get_all_jobs():
    """
    Test retrieving all jobs in the registry.
    """
    # Arrange
    job1 = register_job(job_type="forecast_generation")
    job2 = register_job(job_type="data_collection")
    
    # Act
    all_jobs = get_all_jobs()
    
    # Assert
    assert len(all_jobs) == 2
    job_ids = [job["job_id"] for job in all_jobs]
    assert job1 in job_ids
    assert job2 in job_ids


def test_clear_registry():
    """
    Test clearing all jobs from the registry.
    """
    # Arrange
    job1 = register_job(job_type="forecast_generation")
    job2 = register_job(job_type="data_collection")
    assert get_job_count() == 2
    
    # Act
    cleared_jobs = clear_registry()
    
    # Assert
    assert cleared_jobs == 2
    assert get_job_count() == 0
    assert get_job(job1) is None


def test_remove_job():
    """
    Test removing a specific job from the registry.
    """
    # Arrange
    job1 = register_job(job_type="forecast_generation")
    job2 = register_job(job_type="data_collection")
    assert get_job_count() == 2
    
    # Act
    result = remove_job(job1)
    
    # Assert
    assert result is True
    assert get_job_count() == 1
    assert get_job(job1) is None
    assert get_job(job2) is not None
    
    # Test removing non-existent job
    result = remove_job("non_existent_id")
    assert result is False


def test_get_job_count():
    """
    Test getting the total number of jobs in the registry.
    """
    # Arrange - initial state
    assert get_job_count() == 0
    
    # Add a job
    job1 = register_job(job_type="forecast_generation")
    assert get_job_count() == 1
    
    # Add another job
    job2 = register_job(job_type="data_collection")
    assert get_job_count() == 2
    
    # Remove a job
    remove_job(job1)
    assert get_job_count() == 1
    
    # Clear registry
    clear_registry()
    assert get_job_count() == 0


def test_thread_safety():
    """
    Test that the job registry is thread-safe.
    """
    # Mock the threading.RLock to verify it's being used
    with mock.patch('threading.RLock') as mock_lock:
        # Setup mock lock
        mock_lock_instance = mock.MagicMock()
        mock_lock.return_value = mock_lock_instance
        
        # Import the module again to apply the mock
        import importlib
        importlib.reload(sys.modules['src.backend.scheduler.job_registry'])
        from ...scheduler.job_registry import register_job, update_job_status, get_jobs_by_status, remove_job
        
        # Register a job
        job_id = register_job(job_type="thread_test")
        
        # Verify lock was used
        assert mock_lock_instance.acquire.called
        assert mock_lock_instance.release.called
        
        # Reset mock for next test
        mock_lock_instance.reset_mock()
        
        # Update job status
        update_job_status(job_id, JOB_STATUS_RUNNING)
        assert mock_lock_instance.acquire.called
        assert mock_lock_instance.release.called
        
        # Reset mock for next test
        mock_lock_instance.reset_mock()
        
        # Get jobs by status
        get_jobs_by_status(JOB_STATUS_RUNNING)
        assert mock_lock_instance.acquire.called
        assert mock_lock_instance.release.called
        
        # Reset mock for next test
        mock_lock_instance.reset_mock()
        
        # Remove job
        remove_job(job_id)
        assert mock_lock_instance.acquire.called
        assert mock_lock_instance.release.called


def test_error_handling():
    """
    Test error handling in job registry functions.
    """
    # Test with mocked functions that raise exceptions
    with mock.patch('src.backend.scheduler.job_registry._registry_lock.acquire', side_effect=Exception("Test error")):
        with pytest.raises(JobRegistryError) as excinfo:
            register_job(job_type="error_test")
        assert "Failed to register job" in str(excinfo.value)
        assert "Test error" in str(excinfo.value)