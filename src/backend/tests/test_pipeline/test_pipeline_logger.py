# src/backend/tests/test_pipeline/test_pipeline_logger.py
"""Unit tests for the pipeline_logger module, which provides structured logging functionality for the forecasting pipeline.
Tests cover all logging functions and the PipelineLogger class, ensuring proper context handling, error logging, and integration with the monitoring system.
"""

import pytest  # pytest: 7.0.0+
import unittest.mock  # unittest: standard library
import pandas as pd  # pandas: 2.0.0+
import logging  # logging: standard library
import time  # time: standard library
from datetime import datetime  # datetime: standard library

# Internal imports
from ...pipeline.pipeline_logger import (
    log_pipeline_start,
    log_pipeline_completion,
    log_pipeline_failure,
    log_stage_start,
    log_stage_completion,
    log_stage_failure,
    log_data_transition,
    log_data_validation,
    log_fallback_trigger,
    log_pipeline_metrics,
    log_pipeline_configuration,
    generate_execution_id,
    PipelineLogger
)
from ...pipeline.exceptions import PipelineLoggingError
from ...utils.logging_utils import ComponentLogger
from ..fixtures.forecast_fixtures import create_mock_forecast_data


def setup_function():
    """Setup function that runs before each test"""
    # Reset any global state or mocks
    pass

    # Set up logging capture for testing
    logging.basicConfig(level=logging.DEBUG)


def teardown_function():
    """Teardown function that runs after each test"""
    # Clean up any resources or mocks
    pass

    # Reset logging configuration
    logging.shutdown()


def test_generate_execution_id():
    """Test that execution ID generation works correctly"""
    # Call generate_execution_id function
    execution_id1 = generate_execution_id()

    # Verify that the returned ID is a string
    assert isinstance(execution_id1, str)

    # Verify that the ID has the expected length
    assert len(execution_id1) > 0

    # Call generate_execution_id again and verify it returns a different ID
    execution_id2 = generate_execution_id()
    assert execution_id1 != execution_id2


def test_log_pipeline_start(caplog):
    """Test logging of pipeline start"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create test pipeline name and execution ID
    pipeline_name = "TestPipeline"
    execution_id = "test_id"

    # Create test configuration dictionary
    config = {"param1": "value1", "param2": 123}

    # Call log_pipeline_start with test parameters
    log_pipeline_start(pipeline_name, execution_id, config)

    # Verify that the correct log message was created
    assert f"Started pipeline execution: {pipeline_name} [ID: {execution_id}]" in caplog.text

    # Verify that the log contains the pipeline name and execution ID
    assert f"'pipeline': '{pipeline_name}'" in caplog.text
    assert f"'execution_id': '{execution_id}'" in caplog.text

    # Verify that the log contains the configuration details
    assert "'config': '{\\'param1\\': \\'value1\\', \\'param2\\': 123}'" in caplog.text


def test_log_pipeline_completion(caplog):
    """Test logging of pipeline completion"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create test pipeline name and execution ID
    pipeline_name = "TestPipeline"
    execution_id = "test_id"

    # Create test start time and metrics
    start_time = time.time()
    metrics = {"metric1": 456, "metric2": "success"}

    # Call log_pipeline_completion with test parameters
    log_pipeline_completion(pipeline_name, execution_id, start_time, metrics)

    # Verify that the correct log message was created
    assert f"Completed pipeline execution: {pipeline_name} [ID: {execution_id}]" in caplog.text

    # Verify that the log contains the pipeline name and execution ID
    assert f"'pipeline': '{pipeline_name}'" in caplog.text
    assert f"'execution_id': '{execution_id}'" in caplog.text

    # Verify that the log contains the duration and metrics
    assert "'duration_seconds':" in caplog.text
    assert "'metrics': '{\\'metric1\\': 456, \\'metric2\\': \\'success\\'}'" in caplog.text


def test_log_pipeline_failure(caplog):
    """Test logging of pipeline failure"""
    # Set up logging capture
    caplog.set_level(logging.ERROR)

    # Create test pipeline name and execution ID
    pipeline_name = "TestPipeline"
    execution_id = "test_id"

    # Create test start time and error
    start_time = time.time()
    error = ValueError("Test failure")

    # Create test context dictionary
    context = {"stage": "data_ingestion", "details": "API connection failed"}

    # Call log_pipeline_failure with test parameters
    log_pipeline_failure(pipeline_name, execution_id, start_time, error, context)

    # Verify that the correct log message was created at ERROR level
    assert f"Pipeline execution failed: {pipeline_name} [ID: {execution_id}]" in caplog.text
    assert "ERROR" in caplog.text

    # Verify that the log contains the pipeline name and execution ID
    assert f"'pipeline': '{pipeline_name}'" in caplog.text
    assert f"'execution_id': '{execution_id}'" in caplog.text

    # Verify that the log contains the error details and context
    assert "'error_type': 'ValueError'" in caplog.text
    assert "'error_message': 'Test failure'" in caplog.text
    assert "'stage': 'data_ingestion'" in caplog.text
    assert "'details': 'API connection failed'" in caplog.text


def test_log_stage_start(caplog):
    """Test logging of pipeline stage start"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create test pipeline name, execution ID, and stage name
    pipeline_name = "TestPipeline"
    execution_id = "test_id"
    stage_name = "data_ingestion"

    # Create test stage configuration
    stage_config = {"source": "API", "retries": 3}

    # Call log_stage_start with test parameters
    log_stage_start(pipeline_name, execution_id, stage_name, stage_config)

    # Verify that the correct log message was created
    assert f"Started pipeline stage: {stage_name} in pipeline {pipeline_name} [ID: {execution_id}]" in caplog.text

    # Verify that the log contains the pipeline name, execution ID, and stage name
    assert f"'pipeline': '{pipeline_name}'" in caplog.text
    assert f"'execution_id': '{execution_id}'" in caplog.text
    assert f"'stage': '{stage_name}'" in caplog.text

    # Verify that the log contains the stage configuration
    assert "'stage_config': '{\\'source\\': \\'API\\', \\'retries\\': 3}'" in caplog.text


def test_log_stage_completion(caplog):
    """Test logging of pipeline stage completion"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create test pipeline name, execution ID, and stage name
    pipeline_name = "TestPipeline"
    execution_id = "test_id"
    stage_name = "data_ingestion"

    # Create test start time and output summary
    start_time = time.time()
    output_summary = {"rows": 1000, "columns": ["timestamp", "load_mw"]}

    # Call log_stage_completion with test parameters
    log_stage_completion(pipeline_name, execution_id, stage_name, start_time, output_summary)

    # Verify that the correct log message was created
    assert f"Completed pipeline stage: {stage_name} in pipeline {pipeline_name} [ID: {execution_id}]" in caplog.text

    # Verify that the log contains the pipeline name, execution ID, and stage name
    assert f"'pipeline': '{pipeline_name}'" in caplog.text
    assert f"'execution_id': '{execution_id}'" in caplog.text
    assert f"'stage': '{stage_name}'" in caplog.text

    # Verify that the log contains the duration and output summary
    assert "'duration_seconds':" in caplog.text
    assert "'output_summary': '{\\'rows\\': 1000, \\'columns\\': [\\'timestamp\\', \\'load_mw\\']}'" in caplog.text


def test_log_stage_failure(caplog):
    """Test logging of pipeline stage failure"""
    # Set up logging capture
    caplog.set_level(logging.ERROR)

    # Create test pipeline name, execution ID, and stage name
    pipeline_name = "TestPipeline"
    execution_id = "test_id"
    stage_name = "data_ingestion"

    # Create test start time, error, and context
    start_time = time.time()
    error = APIConnectionError("http://example.com", "API", Exception("Connection refused"))
    context = {"retries": 3, "timeout": 30}

    # Call log_stage_failure with test parameters
    log_stage_failure(pipeline_name, execution_id, stage_name, start_time, error, context)

    # Verify that the correct log message was created at ERROR level
    assert f"Pipeline stage failed: {stage_name} in pipeline {pipeline_name} [ID: {execution_id}]" in caplog.text
    assert "ERROR" in caplog.text

    # Verify that the log contains the pipeline name, execution ID, and stage name
    assert f"'pipeline': '{pipeline_name}'" in caplog.text
    assert f"'execution_id': '{execution_id}'" in caplog.text
    assert f"'stage': '{stage_name}'" in caplog.text

    # Verify that the log contains the error details and context
    assert "'error_type': 'APIConnectionError'" in caplog.text
    assert "'error_message':" in caplog.text
    assert "'retries': 3" in caplog.text
    assert "'timeout': 30" in caplog.text


def test_log_data_transition(caplog):
    """Test logging of data transition between pipeline stages"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create test pipeline name, execution ID, source stage, and target stage
    pipeline_name = "TestPipeline"
    execution_id = "test_id"
    source_stage = "data_ingestion"
    target_stage = "feature_engineering"

    # Create test data (DataFrame) and metadata
    data = create_mock_forecast_data()
    metadata = {"data_source": "API", "transformation": "normalized"}

    # Call log_data_transition with test parameters
    log_data_transition(pipeline_name, execution_id, source_stage, target_stage, data, metadata)

    # Verify that the correct log message was created
    assert f"Data transition: {source_stage} → {target_stage} in pipeline {pipeline_name} [ID: {execution_id}]" in caplog.text

    # Verify that the log contains the pipeline name, execution ID, source stage, and target stage
    assert f"'pipeline': '{pipeline_name}'" in caplog.text
    assert f"'execution_id': '{execution_id}'" in caplog.text
    assert f"'source_stage': '{source_stage}'" in caplog.text
    assert f"'target_stage': '{target_stage}'" in caplog.text

    # Verify that the log contains the data summary and metadata
    assert "'data_summary':" in caplog.text
    assert "'metadata': {'data_source': 'API', 'transformation': 'normalized'}" in caplog.text


def test_log_data_validation(caplog):
    """Test logging of data validation results"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create test pipeline name, execution ID, stage name, and validation type
    pipeline_name = "TestPipeline"
    execution_id = "test_id"
    stage_name = "data_ingestion"
    validation_type = "schema"

    # Create test validation result (valid case)
    is_valid = True
    context = {"data_source": "API"}

    # Call log_data_validation with test parameters
    log_data_validation(pipeline_name, execution_id, stage_name, validation_type, is_valid, context=context)

    # Verify that the correct log message was created at INFO level
    assert f"Data validation {validation_type}: PASSED in stage {stage_name} of pipeline {pipeline_name} [ID: {execution_id}]" in caplog.text
    assert "INFO" in caplog.text

    # Create test validation result (invalid case with errors)
    is_valid = False
    errors = ["Missing required column", "Invalid data type"]
    context = {"data_source": "API"}

    # Call log_data_validation with invalid test parameters
    log_data_validation(pipeline_name, execution_id, stage_name, validation_type, is_valid, errors, context)

    # Verify that the correct log message was created at WARNING level
    assert f"Data validation {validation_type}: FAILED in stage {stage_name} of pipeline {pipeline_name} [ID: {execution_id}]" in caplog.text
    assert "WARNING" in caplog.text

    # Verify that the log contains the validation errors
    assert "'validation_errors': ['Missing required column', 'Invalid data type']" in caplog.text


def test_log_fallback_trigger(caplog):
    """Test logging of fallback mechanism triggering"""
    # Set up logging capture
    caplog.set_level(logging.WARNING)

    # Create test pipeline name, execution ID, stage name, and reason
    pipeline_name = "TestPipeline"
    execution_id = "test_id"
    stage_name = "data_ingestion"
    reason = "API connection failed"

    # Create test error and context
    error = APIConnectionError("http://example.com", "API", Exception("Connection refused"))
    context = {"retries": 3, "timeout": 30}

    # Call log_fallback_trigger with test parameters
    log_fallback_trigger(pipeline_name, execution_id, stage_name, reason, error, context)

    # Verify that the correct log message was created at WARNING level
    assert f"Fallback mechanism triggered in stage {stage_name} of pipeline {pipeline_name} [ID: {execution_id}]: {reason}" in caplog.text
    assert "WARNING" in caplog.text

    # Verify that the log contains the pipeline name, execution ID, stage name, and reason
    assert f"'pipeline': '{pipeline_name}'" in caplog.text
    assert f"'execution_id': '{execution_id}'" in caplog.text
    assert f"'stage': '{stage_name}'" in caplog.text
    assert f"'fallback_reason': '{reason}'" in caplog.text

    # Verify that the log contains the error details and context
    assert "'error_type': 'APIConnectionError'" in caplog.text
    assert "'error_message':" in caplog.text
    assert "'retries': 3" in caplog.text
    assert "'timeout': 30" in caplog.text


def test_log_pipeline_metrics(caplog):
    """Test logging of pipeline performance metrics"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create test pipeline name and execution ID
    pipeline_name = "TestPipeline"
    execution_id = "test_id"

    # Create test metrics dictionary
    metrics = {"data_ingestion_time": 15.5, "feature_engineering_memory": 2048}

    # Call log_pipeline_metrics with test parameters
    log_pipeline_metrics(pipeline_name, execution_id, metrics)

    # Verify that the correct log message was created
    assert f"Pipeline metrics for {pipeline_name} [ID: {execution_id}]" in caplog.text

    # Verify that the log contains the pipeline name and execution ID
    assert f"'pipeline': '{pipeline_name}'" in caplog.text
    assert f"'execution_id': '{execution_id}'" in caplog.text

    # Verify that the log contains the formatted metrics
    assert "'metrics': '{\\'data_ingestion_time\\': 15.5, \\'feature_engineering_memory\\': 2048}'" in caplog.text


def test_log_pipeline_configuration(caplog):
    """Test logging of pipeline configuration"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create test pipeline name and configuration dictionary
    pipeline_name = "TestPipeline"
    config = {"data_source": "API", "model_type": "linear"}

    # Call log_pipeline_configuration with test parameters
    log_pipeline_configuration(pipeline_name, config)

    # Verify that the correct log message was created
    assert f"Pipeline configuration for {pipeline_name}" in caplog.text

    # Verify that the log contains the pipeline name
    assert f"'pipeline': '{pipeline_name}'" in caplog.text

    # Verify that the log contains the formatted configuration
    assert "'config': '{\\'data_source\\': \\'API\\', \\'model_type\\': \\'linear\\'}'" in caplog.text


def test_pipeline_logger_initialization():
    """Test initialization of PipelineLogger class"""
    # Create a PipelineLogger with a test pipeline name
    logger = PipelineLogger("TestPipeline")

    # Verify that the pipeline_name property is set correctly
    assert logger.pipeline_name == "TestPipeline"

    # Verify that an execution_id was generated
    assert logger.execution_id is not None

    # Create a PipelineLogger with a provided execution ID
    logger_with_id = PipelineLogger("TestPipeline", execution_id="provided_id")

    # Verify that the provided execution_id is used
    assert logger_with_id.execution_id == "provided_id"


def test_pipeline_logger_log_pipeline_start(caplog):
    """Test PipelineLogger.log_pipeline_start method"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create a PipelineLogger instance
    logger = PipelineLogger("TestPipeline")

    # Create test configuration
    config = {"data_source": "API", "model_type": "linear"}

    # Call logger.log_pipeline_start with test configuration
    logger.log_pipeline_start(config)

    # Verify that the correct log message was created
    assert "Started pipeline execution: TestPipeline" in caplog.text

    # Verify that the log contains the pipeline name and execution ID
    assert f"'pipeline': 'TestPipeline'" in caplog.text
    assert f"'execution_id': '{logger.execution_id}'" in caplog.text

    # Verify that the log contains the configuration details
    assert "'config': '{\\'data_source\\': \\'API\\', \\'model_type\\': \\'linear\\'}'" in caplog.text


def test_pipeline_logger_log_pipeline_completion(caplog):
    """Test PipelineLogger.log_pipeline_completion method"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create a PipelineLogger instance
    logger = PipelineLogger("TestPipeline")

    # Call logger.log_pipeline_start to set start time
    logger.log_pipeline_start()

    # Create test metrics
    metrics = {"data_ingestion_time": 15.5, "feature_engineering_memory": 2048}

    # Call logger.log_pipeline_completion with test metrics
    logger.log_pipeline_completion(metrics=metrics)

    # Verify that the correct log message was created
    assert "Completed pipeline execution: TestPipeline" in caplog.text

    # Verify that the log contains the pipeline name and execution ID
    assert f"'pipeline': 'TestPipeline'" in caplog.text
    assert f"'execution_id': '{logger.execution_id}'" in caplog.text

    # Verify that the log contains the duration and metrics
    assert "'duration_seconds':" in caplog.text
    assert "'metrics': '{\\'data_ingestion_time\\': 15.5, \\'feature_engineering_memory\\': 2048}'" in caplog.text


def test_pipeline_logger_log_pipeline_failure(caplog):
    """Test PipelineLogger.log_pipeline_failure method"""
    # Set up logging capture
    caplog.set_level(logging.ERROR)

    # Create a PipelineLogger instance
    logger = PipelineLogger("TestPipeline")

    # Call logger.log_pipeline_start to set start time
    logger.log_pipeline_start()

    # Create test error and context
    error = ValueError("Test failure")
    context = {"stage": "data_ingestion", "details": "API connection failed"}

    # Call logger.log_pipeline_failure with test error and context
    logger.log_pipeline_failure(error, context)

    # Verify that the correct log message was created at ERROR level
    assert "Pipeline execution failed: TestPipeline" in caplog.text
    assert "ERROR" in caplog.text

    # Verify that the log contains the pipeline name and execution ID
    assert f"'pipeline': 'TestPipeline'" in caplog.text
    assert f"'execution_id': '{logger.execution_id}'" in caplog.text

    # Verify that the log contains the error details and context
    assert "'error_type': 'ValueError'" in caplog.text
    assert "'error_message': 'Test failure'" in caplog.text
    assert "'stage': 'data_ingestion'" in caplog.text
    assert "'details': 'API connection failed'" in caplog.text


def test_pipeline_logger_log_stage(caplog):
    """Test PipelineLogger.log_stage context manager"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create a PipelineLogger instance
    logger = PipelineLogger("TestPipeline")

    # Create test stage name and configuration
    stage_name = "data_ingestion"
    stage_config = {"source": "API", "retries": 3}

    # Use logger.log_stage as a context manager
    with logger.log_stage(stage_name, stage_config):
        # Verify that stage start was logged on entry
        assert f"Started pipeline stage: {stage_name} in pipeline TestPipeline" in caplog.text

        # Verify that stage completion was logged on exit
        assert f"Completed pipeline stage: {stage_name} in pipeline TestPipeline" not in caplog.text

    # Use logger.log_stage with an exception
    with pytest.raises(ValueError):
        with logger.log_stage(stage_name, stage_config):
            raise ValueError("Test exception")

    # Verify that stage failure was logged when exception occurred
    assert "Pipeline stage failed: data_ingestion in pipeline TestPipeline" in caplog.text


def test_pipeline_logger_log_data_transition(caplog):
    """Test PipelineLogger.log_data_transition method"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create a PipelineLogger instance
    logger = PipelineLogger("TestPipeline")

    # Create test source stage, target stage, data, and metadata
    source_stage = "data_ingestion"
    target_stage = "feature_engineering"
    data = create_mock_forecast_data()
    metadata = {"data_source": "API", "transformation": "normalized"}

    # Call logger.log_data_transition with test parameters
    logger.log_data_transition(source_stage, target_stage, data, metadata)

    # Verify that the correct log message was created
    assert f"Data transition: {source_stage} → {target_stage} in pipeline TestPipeline" in caplog.text

    # Verify that the log contains the pipeline name, execution ID, source stage, and target stage
    assert f"'pipeline': 'TestPipeline'" in caplog.text
    assert f"'execution_id': '{logger.execution_id}'" in caplog.text
    assert f"'source_stage': '{source_stage}'" in caplog.text
    assert f"'target_stage': '{target_stage}'" in caplog.text

    # Verify that the log contains the data summary and metadata
    assert "'data_summary':" in caplog.text
    assert "'metadata': {'data_source': 'API', 'transformation': 'normalized'}" in caplog.text


def test_pipeline_logger_log_data_validation(caplog):
    """Test PipelineLogger.log_data_validation method"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create a PipelineLogger instance
    logger = PipelineLogger("TestPipeline")

    # Create test stage name, validation type, and context
    stage_name = "data_ingestion"
    validation_type = "schema"
    context = {"data_source": "API"}

    # Call logger.log_data_validation with valid result
    logger.log_data_validation(stage_name, validation_type, is_valid=True, context=context)

    # Verify that the correct log message was created at INFO level
    assert f"Data validation {validation_type}: PASSED in stage {stage_name} of pipeline TestPipeline" in caplog.text
    assert "INFO" in caplog.text

    # Call logger.log_data_validation with invalid result and errors
    errors = ["Missing required column", "Invalid data type"]
    logger.log_data_validation(stage_name, validation_type, is_valid=False, errors=errors, context=context)

    # Verify that the correct log message was created at WARNING level
    assert f"Data validation {validation_type}: FAILED in stage {stage_name} of pipeline TestPipeline" in caplog.text
    assert "WARNING" in caplog.text

    # Verify that the log contains the validation errors
    assert "'validation_errors': ['Missing required column', 'Invalid data type']" in caplog.text


def test_pipeline_logger_log_fallback_trigger(caplog):
    """Test PipelineLogger.log_fallback_trigger method"""
    # Set up logging capture
    caplog.set_level(logging.WARNING)

    # Create a PipelineLogger instance
    logger = PipelineLogger("TestPipeline")

    # Create test stage name, reason, error, and context
    stage_name = "data_ingestion"
    reason = "API connection failed"
    error = APIConnectionError("http://example.com", "API", Exception("Connection refused"))
    context = {"retries": 3, "timeout": 30}

    # Call logger.log_fallback_trigger with test parameters
    logger.log_fallback_trigger(stage_name, reason, error, context)

    # Verify that the correct log message was created at WARNING level
    assert f"Fallback mechanism triggered in stage {stage_name}: {reason}" in caplog.text
    assert "WARNING" in caplog.text

    # Verify that the log contains the pipeline name, execution ID, stage name, and reason
    assert f"'pipeline': 'TestPipeline'" in caplog.text
    assert f"'execution_id': '{logger.execution_id}'" in caplog.text
    assert f"'stage': '{stage_name}'" in caplog.text
    assert f"'fallback_reason': '{reason}'" in caplog.text

    # Verify that the log contains the error details and context
    assert "'error_type': 'APIConnectionError'" in caplog.text
    assert "'error_message':" in caplog.text
    assert "'retries': 3" in caplog.text
    assert "'timeout': 30" in caplog.text


def test_pipeline_logger_log_metrics(caplog):
    """Test PipelineLogger.log_metrics method"""
    # Set up logging capture
    caplog.set_level(logging.INFO)

    # Create a PipelineLogger instance
    logger = PipelineLogger("TestPipeline")

    # Create test metrics dictionary
    metrics = {"data_ingestion_time": 15.5, "feature_engineering_memory": 2048}

    # Call logger.log_metrics with test metrics
    logger.log_metrics(metrics)

    # Verify that the correct log message was created
    assert f"Pipeline metrics for TestPipeline" in caplog.text

    # Verify that the log contains the pipeline name and execution ID
    assert f"'pipeline': 'TestPipeline'" in caplog.text
    assert f"'execution_id': '{logger.execution_id}'" in caplog.text

    # Verify that the log contains the formatted metrics
    assert "'metrics': '{\\'data_ingestion_time\\': 15.5, \\'feature_engineering_memory\\': 2048}'" in caplog.text


def test_error_handling():
    """Test error handling in logging functions"""
    # Mock ComponentLogger to raise an exception
    with unittest.mock.patch('src.backend.pipeline.pipeline_logger.component_logger.info') as mock_log:
        mock_log.side_effect = Exception("Logging failed")

        # Verify that log_pipeline_start raises PipelineLoggingError
        with pytest.raises(PipelineLoggingError):
            log_pipeline_start("TestPipeline", "test_id")

        # Verify that log_pipeline_completion raises PipelineLoggingError
        with pytest.raises(PipelineLoggingError):
            log_pipeline_completion("TestPipeline", "test_id", time.time())

    # Verify that log_pipeline_failure handles the logging error and doesn't mask the original error
    with pytest.raises(ValueError, match="Test failure"):
        try:
            log_pipeline_failure("TestPipeline", "test_id", time.time(), ValueError("Test failure"))
        except PipelineLoggingError as e:
            assert "Logging failed" in str(e)

    # Verify that other logging functions raise PipelineLoggingError when logging fails
    with pytest.raises(PipelineLoggingError):
        log_stage_start("TestPipeline", "test_id", "data_ingestion")
    with pytest.raises(PipelineLoggingError):
        log_stage_completion("TestPipeline", "test_id", "data_ingestion", time.time())
    with pytest.raises(PipelineLoggingError):
        log_stage_failure("TestPipeline", "test_id", "data_ingestion", time.time(), ValueError("Test failure"))
    with pytest.raises(PipelineLoggingError):
        log_data_transition("TestPipeline", "test_id", "data_ingestion", "feature_engineering", create_mock_forecast_data())
    with pytest.raises(PipelineLoggingError):
        log_data_validation("TestPipeline", "test_id", "data_ingestion", "schema", True)
    with pytest.raises(PipelineLoggingError):
        log_fallback_trigger("TestPipeline", "test_id", "data_ingestion", "API connection failed")
    with pytest.raises(PipelineLoggingError):
        log_pipeline_metrics("TestPipeline", "test_id", {"data_ingestion_time": 15.5})
    with pytest.raises(PipelineLoggingError):
        log_pipeline_configuration("TestPipeline", {"data_source": "API"})


def test_get_data_summary():
    """Test the get_data_summary function for different data types"""
    # Import get_data_summary from pipeline_logger
    from src.backend.pipeline.pipeline_logger import get_data_summary

    # Test with pandas DataFrame
    df = create_mock_forecast_data()
    summary = get_data_summary(df)
    assert "shape" in summary
    assert "memory_usage" in summary
    assert "columns" in summary

    # Test with numpy array
    array = np.array([1, 2, 3])
    summary = get_data_summary(array)
    assert "shape" in summary
    assert "dtype" in summary

    # Test with list
    lst = [1, 2, 3]
    summary = get_data_summary(lst)
    assert "length" in summary

    # Test with dictionary
    dct = {"a": 1, "b": 2}
    summary = get_data_summary(dct)
    assert "key_count" in summary

    # Test with other types
    integer = 10
    summary = get_data_summary(integer)
    assert "type" in summary