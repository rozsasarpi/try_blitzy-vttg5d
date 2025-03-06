"""
Unit tests for the scheduler_logging module, which provides specialized logging functionality for the
scheduler component of the Electricity Market Price Forecasting System. Tests verify that all logging
functions correctly format and output log messages with appropriate context and severity levels.
"""

import pytest
from unittest.mock import patch, MagicMock
import datetime
import json
import logging

# Import the scheduler logging module and its functions
from ../../scheduler.scheduler_logging import (
    log_scheduler_startup,
    log_scheduler_shutdown,
    log_scheduler_error,
    log_job_registration,
    log_job_execution_start,
    log_job_execution_completion,
    log_job_execution_failure,
    log_job_timeout,
    log_job_status_update,
    log_scheduler_configuration,
    log_scheduler_job_added,
    log_scheduler_job_removed,
    log_scheduler_status,
    logger
)
from ../../utils.logging_utils import (
    format_dict_for_logging,
    format_exception,
    ComponentLogger
)

def test_log_scheduler_startup():
    """Tests that log_scheduler_startup correctly logs scheduler startup with configuration details"""
    # Create test configuration
    test_config = {
        "version": "1.0.0",
        "execution_interval": "daily",
        "scheduler_type": "APScheduler",
        "max_instances": 1
    }
    
    # Mock the logger.info method
    with patch.object(logger, 'info') as mock_info:
        # Call the function
        log_scheduler_startup(test_config)
        
        # Assert that logger.info was called once
        assert mock_info.call_count == 1
        
        # Verify the log message contains 'Scheduler starting' text
        log_message = mock_info.call_args[0][0]
        assert "Scheduler service starting" in log_message
        assert "version 1.0.0" in log_message
        
        # Verify the log message includes formatted configuration details
        formatted_config = format_dict_for_logging(test_config)
        assert "Configuration: " + formatted_config in log_message

def test_log_scheduler_shutdown():
    """Tests that log_scheduler_shutdown correctly logs scheduler shutdown with reason and details"""
    # Create test reason and details
    test_reason = "normal"
    test_details = {
        "uptime_seconds": 3600,
        "shutdown_initiated_by": "admin",
        "completed_jobs": 5
    }
    
    # Mock the logger.info method
    with patch.object(logger, 'info') as mock_info:
        # Call log_scheduler_shutdown with the test data
        log_scheduler_shutdown(test_reason, test_details)
        
        # Assert that logger.info was called once
        assert mock_info.call_count == 1
        
        # Verify the log message contains 'Scheduler shutting down' text
        log_message = mock_info.call_args[0][0]
        assert "Scheduler service shutting down" in log_message
        
        # Verify the log message includes the reason and formatted details
        assert f"Reason: {test_reason}" in log_message
        assert "Uptime: 3600 seconds" in log_message
        formatted_details = format_dict_for_logging(test_details)
        assert "Details: " + formatted_details in log_message

def test_log_scheduler_error():
    """Tests that log_scheduler_error correctly logs scheduler errors with exception details"""
    # Create a test error message, exception, and context
    test_message = "Failed to start scheduler"
    test_error = ValueError("Invalid configuration parameter")
    test_context = {
        "location": "scheduler_service.py:start_scheduler",
        "params": {"interval": "invalid"}
    }
    
    # Mock the logger.error method
    with patch.object(logger, 'error') as mock_error:
        # Call log_scheduler_error with the test data
        log_scheduler_error(test_message, test_error, test_context)
        
        # Assert that logger.error was called once
        assert mock_error.call_count == 1
        
        # Verify the log message contains the error message
        log_message = mock_error.call_args[0][0]
        assert test_message in log_message
        
        # Verify the log message includes formatted exception details
        formatted_exception = format_exception(test_error)
        assert "Exception: " + formatted_exception in log_message
        
        # Verify the log message includes formatted context
        formatted_context = format_dict_for_logging(test_context)
        assert "Context: " + formatted_context in log_message

def test_log_job_registration():
    """Tests that log_job_registration correctly logs job registration events"""
    # Create test job_id, job_type, schedule_time, and job_params
    test_job_id = "forecast_generation_001"
    test_job_type = "forecast_generation"
    test_schedule_time = datetime.datetime(2023, 6, 1, 7, 0, 0)
    test_job_params = {
        "products": ["DALMP", "RTLMP"],
        "horizon_hours": 72,
        "sample_count": 100
    }
    
    # Mock the logger.info method
    with patch.object(logger, 'info') as mock_info:
        # Call log_job_registration with the test data
        log_job_registration(test_job_id, test_job_type, test_schedule_time, test_job_params)
        
        # Assert that logger.info was called once
        assert mock_info.call_count == 1
        
        # Verify the log message contains 'Job registered' text
        log_message = mock_info.call_args[0][0]
        assert "Job registered" in log_message
        
        # Verify the log message includes job_id, job_type, and schedule_time
        assert test_job_id in log_message
        assert f"({test_job_type})" in log_message
        assert test_schedule_time.strftime('%Y-%m-%d %H:%M:%S') in log_message
        
        # Verify the log message includes formatted job_params
        formatted_details = format_dict_for_logging(test_job_params)
        assert "Details: " + formatted_details in log_message

def test_log_job_execution_start():
    """Tests that log_job_execution_start correctly logs the start of job execution"""
    # Create test job_id, job_type, and execution_details
    test_job_id = "forecast_generation_001"
    test_job_type = "forecast_generation"
    test_execution_details = {
        "expected_duration_seconds": 600,
        "input_data_source": "historical_prices_api"
    }
    
    # Mock the logger methods
    with patch.object(logger, 'info') as mock_info, \
         patch.object(logger, 'log_start') as mock_log_start:
        # Call log_job_execution_start with the test data
        log_job_execution_start(test_job_id, test_job_type, test_execution_details)
        
        # Assert that logger.info was called once
        assert mock_info.call_count == 1
        assert mock_log_start.call_count == 1
        
        # Verify the log message contains 'Job execution started' text
        log_message = mock_info.call_args[0][0]
        assert "Starting job execution:" in log_message
        
        # Verify the log message includes job_id and job_type
        assert test_job_id in log_message
        assert f"({test_job_type})" in log_message
        
        # Verify the log message includes formatted execution_details
        assert "Expected duration: 600 seconds" in log_message

def test_log_job_execution_completion():
    """Tests that log_job_execution_completion correctly logs successful job completion"""
    # Create test job_id, job_type, execution_time, and execution_details
    test_job_id = "forecast_generation_001"
    test_job_type = "forecast_generation"
    test_execution_time = 578.45
    test_execution_details = {
        "output_records": 72,
        "data_quality_score": 0.98
    }
    
    # Mock the logger methods
    with patch.object(logger, 'info') as mock_info, \
         patch.object(logger, 'log_completion') as mock_log_completion:
        # Call log_job_execution_completion with the test data
        log_job_execution_completion(test_job_id, test_job_type, test_execution_time, test_execution_details)
        
        # Assert that logger.info was called once
        assert mock_info.call_count == 1
        assert mock_log_completion.call_count == 1
        
        # Verify the log message contains 'Job execution completed' text
        log_message = mock_info.call_args[0][0]
        assert "Job execution completed:" in log_message
        
        # Verify the log message includes job_id, job_type, and execution_time
        assert test_job_id in log_message
        assert f"({test_job_type})" in log_message
        assert f"in {test_execution_time:.3f} seconds" in log_message

def test_log_job_execution_failure():
    """Tests that log_job_execution_failure correctly logs job execution failures"""
    # Create test job_id, job_type, execution_time, error, and execution_details
    test_job_id = "forecast_generation_001"
    test_job_type = "forecast_generation"
    test_execution_time = 45.32
    test_error = RuntimeError("Failed to connect to data source")
    test_execution_details = {
        "attempts": 3,
        "failure_point": "data_collection"
    }
    
    # Mock the logger methods
    with patch.object(logger, 'error') as mock_error, \
         patch.object(logger, 'log_failure') as mock_log_failure:
        # Call log_job_execution_failure with the test data
        log_job_execution_failure(test_job_id, test_job_type, test_execution_time, test_error, test_execution_details)
        
        # Assert that logger.error was called once
        assert mock_error.call_count == 1
        assert mock_log_failure.call_count == 1
        
        # Verify the log message contains 'Job execution failed' text
        log_message = mock_error.call_args[0][0]
        assert "Job execution failed:" in log_message
        
        # Verify the log message includes job_id, job_type, and execution_time
        assert test_job_id in log_message
        assert f"({test_job_type})" in log_message
        assert f"after {test_execution_time:.3f} seconds" in log_message
        
        # Verify the log message includes formatted error details
        assert str(test_error) in log_message

def test_log_job_timeout():
    """Tests that log_job_timeout correctly logs job execution timeouts"""
    # Create test job_id, job_type, timeout_seconds, elapsed_time, and execution_details
    test_job_id = "forecast_generation_001"
    test_job_type = "forecast_generation"
    test_timeout_seconds = 600.0
    test_elapsed_time = 625.75
    test_execution_details = {
        "stage": "data_processing",
        "resource_utilization": "high"
    }
    
    # Mock the logger.error method
    with patch.object(logger, 'error') as mock_error:
        # Call log_job_timeout with the test data
        log_job_timeout(test_job_id, test_job_type, test_timeout_seconds, test_elapsed_time, test_execution_details)
        
        # Assert that logger.error was called once
        assert mock_error.call_count == 1
        
        # Verify the log message contains 'Job execution timed out' text
        log_message = mock_error.call_args[0][0]
        assert "Job execution timeout:" in log_message
        
        # Verify the log message includes job_id, job_type, timeout_seconds, and elapsed_time
        assert test_job_id in log_message
        assert f"({test_job_type})" in log_message
        assert f"Configured timeout: {test_timeout_seconds:.3f} seconds" in log_message
        assert f"Elapsed time: {test_elapsed_time:.3f} seconds" in log_message
        
        # Verify the log message includes formatted execution_details
        assert "Context:" in log_message

def test_log_job_status_update():
    """Tests that log_job_status_update correctly logs job status changes"""
    # Create test job_id, previous_status, new_status, and status_details
    test_job_id = "forecast_generation_001"
    test_previous_status = "pending"
    test_new_status = "running"
    test_status_details = {
        "updated_by": "scheduler",
        "reason": "scheduled_execution"
    }
    
    # Mock the logger.info and logger.error methods
    with patch.object(logger, 'info') as mock_info, \
         patch.object(logger, 'error') as mock_error:
        # Call log_job_status_update with the test data for normal status
        log_job_status_update(test_job_id, test_previous_status, test_new_status, test_status_details)
        
        # Assert that logger.info was called once
        assert mock_info.call_count == 1
        assert mock_error.call_count == 0
        
        # Verify the log message contains 'Job status updated' text
        log_message = mock_info.call_args[0][0]
        assert "Job status changed:" in log_message
        
        # Verify the log message includes job_id, previous_status, and new_status
        assert test_job_id in log_message
        assert f"from '{test_previous_status}' to '{test_new_status}'" in log_message
        
        # Verify the log message includes formatted status_details
        assert "Context:" in log_message
        
        # Reset mocks and test with 'failed' status to verify logger.error is used instead
        mock_info.reset_mock()
        mock_error.reset_mock()
        
        error_status = "failed"
        log_job_status_update(test_job_id, test_new_status, error_status, test_status_details)
        
        assert mock_info.call_count == 0
        assert mock_error.call_count == 1
        
        error_log_message = mock_error.call_args[0][0]
        assert "Job status changed:" in error_log_message
        assert test_job_id in error_log_message
        assert f"from '{test_new_status}' to '{error_status}'" in error_log_message

def test_log_scheduler_configuration():
    """Tests that log_scheduler_configuration correctly logs scheduler configuration"""
    # Create a test configuration dictionary
    test_config = {
        "scheduler_type": "APScheduler",
        "job_stores": {"default": "memory"},
        "executors": {"default": "ThreadPoolExecutor"},
        "max_instances": 3
    }
    
    # Mock the logger.info method
    with patch.object(logger, 'info') as mock_info:
        # Call log_scheduler_configuration with the test configuration
        log_scheduler_configuration(test_config)
        
        # Assert that logger.info was called once
        assert mock_info.call_count == 1
        
        # Verify the log message contains 'Scheduler configuration' text
        log_message = mock_info.call_args[0][0]
        assert "Scheduler configuration" in log_message
        
        # Verify the log message includes formatted configuration details
        formatted_config = format_dict_for_logging(test_config)
        assert formatted_config in log_message

def test_log_scheduler_job_added():
    """Tests that log_scheduler_job_added correctly logs when a job is added to the scheduler"""
    # Create test job_id, job_type, next_run_time, and job_details
    test_job_id = "forecast_generation_001"
    test_job_type = "forecast_generation"
    test_next_run_time = datetime.datetime(2023, 6, 1, 7, 0, 0)
    test_job_details = {
        "trigger": "cron",
        "hour": 7,
        "minute": 0,
        "timezone": "America/Chicago"
    }
    
    # Mock the logger.info method
    with patch.object(logger, 'info') as mock_info:
        # Call log_scheduler_job_added with the test data
        log_scheduler_job_added(test_job_id, test_job_type, test_next_run_time, test_job_details)
        
        # Assert that logger.info was called once
        assert mock_info.call_count == 1
        
        # Verify the log message contains 'Job added to scheduler' text
        log_message = mock_info.call_args[0][0]
        assert "Job added to scheduler:" in log_message
        
        # Verify the log message includes job_id, job_type, and next_run_time
        assert test_job_id in log_message
        assert f"({test_job_type})" in log_message
        assert test_next_run_time.strftime('%Y-%m-%d %H:%M:%S') in log_message
        
        # Verify the log message includes formatted job_details
        assert "Context:" in log_message

def test_log_scheduler_job_removed():
    """Tests that log_scheduler_job_removed correctly logs when a job is removed from the scheduler"""
    # Create test job_id and reason
    test_job_id = "forecast_generation_001"
    test_reason = "completed"
    
    # Mock the logger.info method
    with patch.object(logger, 'info') as mock_info:
        # Call log_scheduler_job_removed with the test data
        log_scheduler_job_removed(test_job_id, test_reason)
        
        # Assert that logger.info was called once
        assert mock_info.call_count == 1
        
        # Verify the log message contains 'Job removed from scheduler' text
        log_message = mock_info.call_args[0][0]
        assert "Job removed from scheduler:" in log_message
        
        # Verify the log message includes job_id and reason
        assert test_job_id in log_message
        assert f"Reason: {test_reason}" in log_message

def test_log_scheduler_status():
    """Tests that log_scheduler_status correctly logs scheduler status information"""
    # Create a test status dictionary
    test_status = {
        "running": True,
        "job_count": 5,
        "next_run_time": datetime.datetime(2023, 6, 1, 7, 0, 0).strftime('%Y-%m-%d %H:%M:%S'),
        "uptime_seconds": 3600
    }
    
    # Mock the logger.info method
    with patch.object(logger, 'info') as mock_info:
        # Call log_scheduler_status with the test status
        log_scheduler_status(test_status)
        
        # Assert that logger.info was called once
        assert mock_info.call_count == 1
        
        # Verify the log message contains 'Scheduler status' text
        log_message = mock_info.call_args[0][0]
        assert "Scheduler status" in log_message
        
        # Verify the log message includes formatted status details
        assert f"Running: {test_status['running']}" in log_message
        assert f"Jobs: {test_status['job_count']}" in log_message
        formatted_status = format_dict_for_logging(test_status)
        assert "Details: " + formatted_status in log_message

def test_logger_instance():
    """Tests that the exported logger instance is correctly configured"""
    # Verify that logger is an instance of ComponentLogger
    assert isinstance(logger, ComponentLogger)
    
    # Verify that logger has the correct component name ('scheduler')
    assert logger.component_name == 'scheduler'
    
    # Verify that logger has the expected default context
    assert 'component' in logger.default_context
    assert logger.default_context['component'] == 'scheduler'

class TestSchedulerLoggingWithCapture:
    """Integration tests for scheduler_logging using log capture to verify actual log output"""
    
    def test_log_capture_scheduler_startup(self, caplog):
        """Tests that log_scheduler_startup produces correctly formatted log messages"""
        # Set caplog level to INFO
        caplog.set_level(logging.INFO)
        
        # Create a test configuration dictionary
        test_config = {
            "version": "1.0.0",
            "scheduler_type": "APScheduler"
        }
        
        # Call log_scheduler_startup with the test configuration
        log_scheduler_startup(test_config)
        
        # Assert that a log record was created
        assert len(caplog.records) == 1
        
        # Verify the log record has INFO level
        assert caplog.records[0].levelname == 'INFO'
        
        # Verify the log record contains 'Scheduler starting' text
        log_message = caplog.records[0].message
        assert "Scheduler service starting" in log_message
        
        # Verify the log record includes formatted configuration details
        assert "version 1.0.0" in log_message
        assert "Configuration:" in log_message
    
    def test_log_capture_scheduler_error(self, caplog):
        """Tests that log_scheduler_error produces correctly formatted log messages"""
        # Set caplog level to ERROR
        caplog.set_level(logging.ERROR)
        
        # Create a test error message, exception, and context
        test_message = "Failed to start scheduler"
        test_error = ValueError("Invalid configuration parameter")
        test_context = {
            "location": "scheduler_service.py:start_scheduler"
        }
        
        # Call log_scheduler_error with the test data
        log_scheduler_error(test_message, test_error, test_context)
        
        # Assert that a log record was created
        assert len(caplog.records) == 1
        
        # Verify the log record has ERROR level
        assert caplog.records[0].levelname == 'ERROR'
        
        # Verify the log record contains the error message
        log_message = caplog.records[0].message
        assert test_message in log_message
        
        # Verify the log record includes formatted exception details
        assert "Exception:" in log_message
        assert str(test_error) in log_message
        
        # Verify the log record includes formatted context
        assert "Context:" in log_message
    
    def test_log_capture_job_execution_lifecycle(self, caplog):
        """Tests the complete job execution lifecycle logging from start to completion"""
        # Set caplog level to INFO
        caplog.set_level(logging.INFO)
        
        # Create test job_id, job_type, and execution_details
        test_job_id = "forecast_generation_001"
        test_job_type = "forecast_generation"
        test_execution_details = {
            "expected_duration_seconds": 600
        }
        
        # Call log_job_execution_start with the test data
        log_job_execution_start(test_job_id, test_job_type, test_execution_details)
        
        # Call log_job_execution_completion with the test data and execution time
        test_execution_time = 578.45
        log_job_execution_completion(test_job_id, test_job_type, test_execution_time)
        
        # Assert that two log records were created
        assert len(caplog.records) >= 2
        
        # Verify the first log record contains 'Job execution started' text
        start_log = caplog.records[0].message
        assert "Starting job execution:" in start_log
        
        # Verify the second log record contains 'Job execution completed' text
        complete_log = caplog.records[-1].message
        assert "Job execution completed:" in complete_log
        
        # Verify both log records include job_id and job_type
        assert test_job_id in start_log and test_job_id in complete_log
        assert test_job_type in start_log and test_job_type in complete_log
        
        # Verify the second log record includes execution time
        assert f"in {test_execution_time:.3f} seconds" in complete_log
    
    def test_log_capture_job_execution_failure_path(self, caplog):
        """Tests the job execution failure path logging from start to failure"""
        # Set caplog level to INFO
        caplog.set_level(logging.INFO)
        
        # Create test job_id, job_type, and execution_details
        test_job_id = "forecast_generation_001"
        test_job_type = "forecast_generation"
        test_execution_details = {
            "expected_duration_seconds": 600
        }
        
        # Call log_job_execution_start with the test data
        log_job_execution_start(test_job_id, test_job_type, test_execution_details)
        
        # Create a test exception
        test_error = RuntimeError("Failed to connect to data source")
        
        # Clear previous logs and set level to ERROR for failure logging
        caplog.clear()
        caplog.set_level(logging.ERROR)
        
        # Call log_job_execution_failure with the test data, execution time, and exception
        test_execution_time = 45.32
        log_job_execution_failure(test_job_id, test_job_type, test_execution_time, test_error)
        
        # Assert that two log records were created
        assert len(caplog.records) >= 1
        
        # Verify the second log record has ERROR level and contains 'Job execution failed' text
        assert caplog.records[0].levelname == 'ERROR'
        failure_log = caplog.records[0].message
        assert "Job execution failed:" in failure_log
        
        # Verify both log records include job_id and job_type
        assert test_job_id in failure_log
        assert test_job_type in failure_log
        
        # Verify the second log record includes formatted exception details
        assert str(test_error) in failure_log