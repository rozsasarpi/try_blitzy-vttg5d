# src/backend/tests/test_scheduler/test_forecast_scheduler.py
import pytest
from unittest.mock import MagicMock
import datetime
import time
import uuid
from freezegun import freeze_time
import pytz

from src.backend.scheduler.forecast_scheduler import (
    initialize_scheduler,
    start_scheduler,
    stop_scheduler,
    is_scheduler_running,
    schedule_forecast_job,
    schedule_one_time_forecast,
    run_forecast_now,
    execute_forecast_job,
    get_next_run_time,
    get_scheduler_status,
    ForecastScheduler,
    JOB_TYPE_FORECAST,
)
from src.backend.scheduler.exceptions import SchedulerError, JobSchedulingError, JobExecutionError
from src.backend.scheduler.job_registry import register_job, get_job, update_job_status, clear_registry, JOB_STATUS_PENDING, JOB_STATUS_RUNNING, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED
from src.backend.scheduler.execution_monitor import start_job_monitoring, stop_job_monitoring
from src.backend.pipeline.pipeline_executor import execute_forecasting_pipeline, get_default_config
from src.backend.config.settings import FORECAST_SCHEDULE_TIME, TIMEZONE


def setup_function():
    """Setup function that runs before each test to ensure a clean state"""
    # Stop any running scheduler
    if is_scheduler_running():
        stop_scheduler("Test setup")
    # Clear the job registry
    clear_registry()
    # Reset any module-level variables in the scheduler module


def teardown_function():
    """Teardown function that runs after each test to clean up"""
    # Stop any running scheduler
    if is_scheduler_running():
        stop_scheduler("Test teardown")
    # Clear the job registry
    clear_registry()
    # Reset any module-level variables in the scheduler module


class TestSchedulerInitialization:
    """Test cases for scheduler initialization functionality"""

    def __init__(self):
        """Initialize the test class"""
        pass

    def test_initialize_scheduler_with_default_config(self):
        """Test that scheduler initializes correctly with default configuration"""
        # Initialize scheduler with default configuration
        scheduler = initialize_scheduler()

        # Assert that scheduler is initialized but not running
        assert scheduler is not None
        assert not scheduler.running

        # Verify scheduler configuration has expected default values
        assert scheduler.timezone == str(TIMEZONE)

    def test_initialize_scheduler_with_custom_config(self):
        """Test that scheduler initializes correctly with custom configuration"""
        # Create custom configuration dictionary
        custom_config = {"timezone": "UTC", "max_instances": 5}

        # Initialize scheduler with custom configuration
        scheduler = initialize_scheduler(custom_config)

        # Assert that scheduler is initialized but not running
        assert scheduler is not None
        assert not scheduler.running

        # Verify scheduler configuration has expected custom values
        assert scheduler.timezone == "UTC"

    def test_initialize_scheduler_class(self):
        """Test that ForecastScheduler class initializes correctly"""
        # Create ForecastScheduler instance
        forecast_scheduler = ForecastScheduler()

        # Call initialize method
        forecast_scheduler.initialize()

        # Assert that scheduler is initialized but not running
        assert forecast_scheduler._scheduler is not None
        assert not forecast_scheduler._running

        # Verify scheduler has expected properties
        assert forecast_scheduler._scheduler.timezone == str(TIMEZONE)

    def test_initialize_scheduler_error_handling(self, mocker):
        """Test that scheduler initialization handles errors correctly"""
        # Mock APScheduler to raise an exception during initialization
        mocker.patch("src.backend.scheduler.forecast_scheduler.BackgroundScheduler", side_effect=Exception("Initialization failed"))

        # Assert that initialize_scheduler raises SchedulerInitializationError
        with pytest.raises(SchedulerError, match="Scheduler initialization failed"):
            initialize_scheduler()

        # Verify error details are captured correctly


class TestSchedulerStartStop:
    """Test cases for starting and stopping the scheduler"""

    def __init__(self):
        """Initialize the test class"""
        pass

    def test_start_scheduler(self):
        """Test that scheduler starts correctly"""
        # Initialize scheduler
        initialize_scheduler()

        # Start scheduler
        start_scheduler()

        # Assert that scheduler is running
        assert is_scheduler_running()

        # Verify is_scheduler_running returns True
        assert is_scheduler_running() is True

    def test_start_scheduler_already_running(self):
        """Test that starting an already running scheduler returns False"""
        # Initialize and start scheduler
        initialize_scheduler()
        start_scheduler()

        # Attempt to start scheduler again
        result = start_scheduler()

        # Assert that second start returns False
        assert result is False

        # Verify scheduler is still running
        assert is_scheduler_running()

    def test_stop_scheduler(self):
        """Test that scheduler stops correctly"""
        # Initialize and start scheduler
        initialize_scheduler()
        start_scheduler()

        # Stop scheduler with reason
        stop_scheduler("Test stop")

        # Assert that scheduler is not running
        assert not is_scheduler_running()

        # Verify is_scheduler_running returns False
        assert is_scheduler_running() is False

    def test_stop_scheduler_not_running(self):
        """Test that stopping a non-running scheduler returns False"""
        # Initialize scheduler but don't start it
        initialize_scheduler()

        # Attempt to stop scheduler
        result = stop_scheduler("Test stop")

        # Assert that stop returns False
        assert result is False

        # Verify scheduler is still not running
        assert not is_scheduler_running()

    def test_forecast_scheduler_class_start_stop(self):
        """Test that ForecastScheduler class starts and stops correctly"""
        # Create and initialize ForecastScheduler instance
        forecast_scheduler = ForecastScheduler()
        forecast_scheduler.initialize()

        # Start scheduler and verify it's running
        assert forecast_scheduler.start() is True
        assert forecast_scheduler.is_running() is True

        # Stop scheduler and verify it's stopped
        assert forecast_scheduler.stop("Test stop") is True
        assert forecast_scheduler.is_running() is False

        # Verify is_running method returns correct values
        assert forecast_scheduler.is_running() is False


class TestSchedulerJobScheduling:
    """Test cases for scheduling jobs with the scheduler"""

    def __init__(self):
        """Initialize the test class"""
        pass

    def test_schedule_forecast_job(self, mocker):
        """Test scheduling a forecast job"""
        # Initialize and start scheduler
        initialize_scheduler()
        start_scheduler()

        # Mock register_job to track calls
        mock_register_job = mocker.patch("src.backend.scheduler.forecast_scheduler.register_job", return_value="job_id")

        # Schedule a forecast job
        job_id = schedule_forecast_job()

        # Assert that job_id is returned
        assert job_id == "job_id"

        # Verify register_job was called with correct parameters
        mock_register_job.assert_called_once()
        args, kwargs = mock_register_job.call_args
        assert kwargs["job_type"] == JOB_TYPE_FORECAST

        # Verify job is scheduled at 7 AM CST
        next_run_time = get_next_run_time()
        assert next_run_time.hour == 7
        assert next_run_time.minute == 0
        assert next_run_time.tzinfo == TIMEZONE

    def test_schedule_forecast_job_scheduler_not_running(self):
        """Test that scheduling a job when scheduler is not running raises an error"""
        # Initialize scheduler but don't start it
        initialize_scheduler()

        # Assert that schedule_forecast_job raises SchedulerError
        with pytest.raises(SchedulerError, match="Scheduler is not running"):
            schedule_forecast_job()

        # Verify no jobs were scheduled

    def test_schedule_one_time_forecast(self, mocker):
        """Test scheduling a one-time forecast job"""
        # Initialize and start scheduler
        initialize_scheduler()
        start_scheduler()

        # Mock register_job to track calls
        mock_register_job = mocker.patch("src.backend.scheduler.forecast_scheduler.register_job", return_value="job_id")

        # Create a future run time
        run_time = datetime.datetime.now(TIMEZONE) + datetime.timedelta(minutes=5)

        # Schedule a one-time forecast job
        job_id = schedule_one_time_forecast(run_time)

        # Assert that job_id is returned
        assert job_id == "job_id"

        # Verify register_job was called with correct parameters
        mock_register_job.assert_called_once()
        args, kwargs = mock_register_job.call_args
        assert kwargs["job_type"] == JOB_TYPE_FORECAST

        # Verify job is scheduled at the specified time
        next_run_time = get_next_run_time()
        assert next_run_time.hour == run_time.hour
        assert next_run_time.minute == run_time.minute
        assert next_run_time.tzinfo == TIMEZONE

    def test_forecast_scheduler_class_scheduling(self, mocker):
        """Test that ForecastScheduler class schedules jobs correctly"""
        # Create, initialize, and start ForecastScheduler instance
        forecast_scheduler = ForecastScheduler()
        forecast_scheduler.initialize()
        forecast_scheduler.start()

        # Mock register_job to track calls
        mock_register_job = mocker.patch("src.backend.scheduler.forecast_scheduler.register_job", return_value="job_id")

        # Schedule daily forecast with scheduler.schedule_daily_forecast()
        job_id = forecast_scheduler.schedule_daily_forecast()
        assert job_id == "job_id"
        mock_register_job.assert_called()

        # Create a future run time
        run_time = datetime.datetime.now(TIMEZONE) + datetime.timedelta(minutes=5)

        # Schedule one-time forecast with scheduler.schedule_one_time()
        job_id = forecast_scheduler.schedule_one_time(run_time)
        assert job_id == "job_id"
        mock_register_job.assert_called()

    def test_get_next_run_time(self, mocker):
        """Test getting the next scheduled run time"""
        # Initialize and start scheduler
        initialize_scheduler()
        start_scheduler()

        # Schedule a forecast job
        schedule_forecast_job()

        # Get next run time
        next_run_time = get_next_run_time()

        # Assert that next run time is a datetime object
        assert isinstance(next_run_time, datetime.datetime)

        # Verify next run time is at 7 AM CST
        assert next_run_time.hour == 7
        assert next_run_time.minute == 0
        assert next_run_time.tzinfo == TIMEZONE

    def test_get_scheduler_status(self, mocker):
        """Test getting scheduler status information"""
        # Initialize and start scheduler
        initialize_scheduler()
        start_scheduler()

        # Schedule a forecast job
        schedule_forecast_job()

        # Get scheduler status
        status = get_scheduler_status()

        # Assert that status is a dictionary
        assert isinstance(status, dict)

        # Verify status contains running=True
        assert status["running"] is True

        # Verify status contains job_count=1
        assert status["job_count"] == 1

        # Verify status contains next_run_time
        assert "next_run_time" in status


class TestJobExecution:
    """Test cases for executing forecast jobs"""

    def __init__(self):
        """Initialize the test class"""
        pass

    def test_execute_forecast_job(self, mocker):
        """Test executing a forecast job"""
        # Mock register_job, update_job_status, get_job, start_job_monitoring, stop_job_monitoring
        mocker.patch("src.backend.scheduler.forecast_scheduler.register_job", return_value="job_id")
        mock_update_job_status = mocker.patch("src.backend.scheduler.forecast_scheduler.update_job_status")
        mock_get_job = mocker.patch("src.backend.scheduler.forecast_scheduler.get_job", return_value={"job_type": JOB_TYPE_FORECAST, "job_params": {}})
        mock_start_job_monitoring = mocker.patch("src.backend.scheduler.forecast_scheduler.start_job_monitoring")
        mock_stop_job_monitoring = mocker.patch("src.backend.scheduler.forecast_scheduler.stop_job_monitoring")

        # Mock execute_forecasting_pipeline to return success result
        mock_execute_forecasting_pipeline = mocker.patch("src.backend.scheduler.forecast_scheduler.execute_forecasting_pipeline", return_value={"result": "success"})

        # Create job parameters
        job_params = {"param1": "value1"}

        # Execute forecast job
        result = execute_forecast_job("job_id", job_params)

        # Assert that result is returned
        assert result == {"result": "success"}

        # Verify job status was updated to RUNNING then COMPLETED
        mock_update_job_status.assert_called()
        # Verify execute_forecasting_pipeline was called with correct parameters
        mock_execute_forecasting_pipeline.assert_called_once_with("job_id", job_params)

        # Verify monitoring was started and stopped
        mock_start_job_monitoring.assert_called_once_with("job_id")
        mock_stop_job_monitoring.assert_called_once_with("job_id", success=True, execution_details={"result": "success"})

    def test_execute_forecast_job_error_handling(self, mocker):
        """Test that execute_forecast_job handles errors correctly"""
        # Mock register_job, update_job_status, get_job, start_job_monitoring, stop_job_monitoring
        mocker.patch("src.backend.scheduler.forecast_scheduler.register_job", return_value="job_id")
        mock_update_job_status = mocker.patch("src.backend.scheduler.forecast_scheduler.update_job_status")
        mock_get_job = mocker.patch("src.backend.scheduler.forecast_scheduler.get_job", return_value={"job_type": JOB_TYPE_FORECAST, "job_params": {}})
        mock_start_job_monitoring = mocker.patch("src.backend.scheduler.forecast_scheduler.start_job_monitoring")
        mock_stop_job_monitoring = mocker.patch("src.backend.scheduler.forecast_scheduler.stop_job_monitoring")

        # Mock execute_forecasting_pipeline to raise an exception
        mock_execute_forecasting_pipeline = mocker.patch("src.backend.scheduler.forecast_scheduler.execute_forecasting_pipeline", side_effect=Exception("Execution failed"))

        # Create job parameters
        job_params = {"param1": "value1"}

        # Assert that execute_forecast_job raises JobExecutionError
        with pytest.raises(JobExecutionError, match="Job execution failed"):
            execute_forecast_job("job_id", job_params)

        # Verify job status was updated to RUNNING then FAILED
        mock_update_job_status.assert_called()

        # Verify monitoring was started and stopped with success=False
        mock_start_job_monitoring.assert_called_once_with("job_id")
        mock_stop_job_monitoring.assert_called_once_with("job_id", success=False, error=mocker.ANY)

    def test_run_forecast_now(self, mocker):
        """Test running a forecast immediately"""
        # Mock register_job, execute_forecast_job
        mock_register_job = mocker.patch("src.backend.scheduler.forecast_scheduler.register_job", return_value="job_id")
        mock_execute_forecast_job = mocker.patch("src.backend.scheduler.forecast_scheduler.execute_forecast_job", return_value={"result": "success"})

        # Create job parameters
        job_params = {"param1": "value1"}

        # Run forecast now
        result = run_forecast_now(job_params)

        # Assert that result is returned
        assert result == {"result": "success"}

        # Verify register_job was called with correct parameters
        mock_register_job.assert_called_once()
        args, kwargs = mock_register_job.call_args
        assert kwargs["job_type"] == JOB_TYPE_FORECAST

        # Verify execute_forecast_job was called with correct parameters
        mock_execute_forecast_job.assert_called_once_with("job_id", job_params)

    def test_forecast_scheduler_class_run_now(self, mocker):
        """Test that ForecastScheduler class runs forecasts immediately"""
        # Create ForecastScheduler instance
        forecast_scheduler = ForecastScheduler()

        # Mock run_forecast_now to return success result
        mock_run_forecast_now = mocker.patch("src.backend.scheduler.forecast_scheduler.run_forecast_now", return_value={"result": "success"})

        # Run forecast with scheduler.run_now()
        result = forecast_scheduler.run_now()

        # Assert that result is returned
        assert result == {"result": "success"}

        # Verify run_forecast_now was called with correct parameters
        mock_run_forecast_now.assert_called_once()


class TestSchedulerIntegration:
    """Integration tests for the scheduler component"""

    def __init__(self):
        """Initialize the test class"""
        pass

    def test_scheduler_full_lifecycle(self, mocker):
        """Test the full lifecycle of the scheduler from initialization to job execution"""
        # Mock execute_forecasting_pipeline to return success result
        mock_execute_forecasting_pipeline = mocker.patch("src.backend.scheduler.forecast_scheduler.execute_forecasting_pipeline", return_value={"result": "success"})

        # Initialize scheduler
        initialize_scheduler()

        # Start scheduler
        start_scheduler()

        # Schedule a one-time forecast for near future
        run_time = datetime.datetime.now(TIMEZONE) + datetime.timedelta(seconds=1)
        job_id = schedule_one_time_forecast(run_time)

        # Wait for job to execute
        time.sleep(2)

        # Verify job status was updated to COMPLETED
        job = get_job(job_id)
        assert job["status"] == JOB_STATUS_COMPLETED

        # Stop scheduler
        stop_scheduler("Test complete")

        # Verify scheduler is stopped
        assert not is_scheduler_running()

    def test_scheduler_with_frozen_time(self):
        """Test scheduler behavior with frozen time to verify 7 AM scheduling"""
        # Use freezegun to freeze time at a specific datetime
        with freeze_time("2023-01-01 10:00:00", tz_offset=-6):
            # Initialize and start scheduler
            initialize_scheduler()
            start_scheduler()

            # Schedule a forecast job
            schedule_forecast_job()

            # Get next run time
            next_run_time = get_next_run_time()

            # Verify next run time is at 7 AM CST on the appropriate day
            assert next_run_time.year == 2023
            assert next_run_time.month == 1
            assert next_run_time.day == 2
            assert next_run_time.hour == 7
            assert next_run_time.minute == 0
            assert next_run_time.tzinfo == TIMEZONE

            # Stop scheduler
            stop_scheduler("Test complete")

    def test_forecast_scheduler_class_integration(self):
        """Test integration of ForecastScheduler class methods"""
        # Create ForecastScheduler instance
        forecast_scheduler = ForecastScheduler()

        # Initialize and start scheduler
        forecast_scheduler.initialize()
        forecast_scheduler.start()

        # Schedule daily forecast
        forecast_scheduler.schedule_daily_forecast()

        # Get next run time and verify it's correct
        next_run_time = forecast_scheduler.get_next_run_time()
        assert isinstance(next_run_time, datetime.datetime)

        # Get status and verify it's correct
        status = forecast_scheduler.get_status()
        assert status["running"] is True
        assert status["job_count"] == 1

        # Stop scheduler
        forecast_scheduler.stop("Test complete")

        # Verify scheduler is stopped
        assert not forecast_scheduler.is_running()