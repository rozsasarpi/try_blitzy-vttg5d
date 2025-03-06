"""Unit tests for the pipeline executor module of the Electricity Market Price Forecasting System.
Tests the functionality of the PipelineExecutor class and related functions for executing the
forecasting pipeline with proper configuration management, error handling, and fallback mechanisms.
"""

import pytest  # pytest: 7.0.0+
import unittest.mock  # package_version: standard library
from datetime import datetime  # package_version: standard library
import copy  # package_version: standard library

# Internal imports
from src.backend.pipeline.pipeline_executor import PipelineExecutor, execute_forecasting_pipeline, execute_with_default_config, get_default_config, validate_config, merge_configs  # Module: src/backend/pipeline/pipeline_executor.py
from src.backend.pipeline.forecasting_pipeline import ForecastingPipeline  # Module: src/backend/pipeline/forecasting_pipeline.py
from src.backend.pipeline.exceptions import PipelineError, PipelineExecutionError, PipelineConfigurationError  # Module: src/backend/pipeline/exceptions.py
from src.backend.config.settings import FORECAST_PRODUCTS, DATA_SOURCES  # Module: src/backend/config/settings.py
from src.backend.tests.fixtures.forecast_fixtures import create_mock_forecast_ensemble  # Module: src/backend/tests/fixtures/forecast_fixtures.py


def test_pipeline_executor_initialization():
    """Test that PipelineExecutor initializes correctly with provided parameters"""
    # Create a test target date using datetime
    target_date = datetime(2023, 1, 1)
    # Create a test configuration dictionary
    config = {"products": ["DALMP"]}
    # Create a test execution ID
    execution_id = "test-execution-id"
    # Initialize a PipelineExecutor with these parameters
    executor = PipelineExecutor(target_date, config, execution_id)

    # Assert that the executor's target_date matches the provided date
    assert executor.target_date == target_date
    # Assert that the executor's config matches the provided config
    assert executor.config == config
    # Assert that the executor's execution_id matches the provided ID
    assert executor.execution_id == execution_id
    # Assert that the executor's executed flag is False initially
    assert executor.executed is False
    # Assert that the executor's results dictionary contains the execution_id and status='pending'
    assert executor.results == {"execution_id": execution_id, "status": "pending"}


def test_pipeline_executor_execution_success():
    """Test that PipelineExecutor executes successfully and returns correct results"""
    # Create a test target date using datetime
    target_date = datetime(2023, 1, 1)
    # Create a test configuration dictionary
    config = {"products": ["DALMP"]}
    # Mock the ForecastingPipeline class
    with unittest.mock.patch("src.backend.tests.test_pipeline.test_pipeline_executor.ForecastingPipeline") as MockPipeline:
        # Configure the mock pipeline's run method to return True (success)
        MockPipeline.return_value.run.return_value = True
        # Configure the mock pipeline's get_results method to return a test result dictionary
        MockPipeline.return_value.get_results.return_value = {"forecast": "success"}
        # Configure the mock pipeline's was_fallback_used method to return False
        MockPipeline.return_value.was_fallback_used.return_value = False

        # Initialize a PipelineExecutor with the test parameters
        executor = PipelineExecutor(target_date, config, "test-execution-id")
        # Execute the pipeline by calling executor.execute()
        result = executor.execute()

        # Assert that the execution result is True
        assert result is True
        # Assert that the executor's executed flag is now True
        assert executor.executed is True
        # Assert that the executor's results dictionary contains the expected values
        assert executor.results == {"execution_id": "test-execution-id", "status": "pending", "forecast": "success"}
        # Assert that the pipeline's run method was called once
        MockPipeline.return_value.run.assert_called_once()
        # Assert that the pipeline's get_results method was called once
        MockPipeline.return_value.get_results.assert_called_once()


def test_pipeline_executor_execution_with_fallback():
    """Test that PipelineExecutor handles fallback correctly when pipeline uses fallback"""
    # Create a test target date using datetime
    target_date = datetime(2023, 1, 1)
    # Create a test configuration dictionary
    config = {"products": ["DALMP"]}
    # Mock the ForecastingPipeline class
    with unittest.mock.patch("src.backend.tests.test_pipeline.test_pipeline_executor.ForecastingPipeline") as MockPipeline:
        # Configure the mock pipeline's run method to return False (fallback used)
        MockPipeline.return_value.run.return_value = False
        # Configure the mock pipeline's get_results method to return a test result dictionary with fallback info
        MockPipeline.return_value.get_results.return_value = {"status": "fallback", "fallback_reason": "data_unavailable"}
        # Configure the mock pipeline's was_fallback_used method to return True
        MockPipeline.return_value.was_fallback_used.return_value = True

        # Initialize a PipelineExecutor with the test parameters
        executor = PipelineExecutor(target_date, config, "test-execution-id")
        # Execute the pipeline by calling executor.execute()
        result = executor.execute()

        # Assert that the execution result is False (indicating fallback)
        assert result is False
        # Assert that the executor's executed flag is now True
        assert executor.executed is True
        # Assert that the executor's results dictionary contains fallback status
        assert executor.results == {"execution_id": "test-execution-id", "status": "pending", "status": "fallback", "fallback_reason": "data_unavailable"}
        # Assert that the pipeline's run method was called once
        MockPipeline.return_value.run.assert_called_once()
        # Assert that the pipeline's get_results method was called once
        MockPipeline.return_value.get_results.assert_called_once()
        # Assert that was_fallback_used returns True
        assert executor.was_fallback_used() is True


def test_pipeline_executor_execution_failure():
    """Test that PipelineExecutor handles exceptions during execution correctly"""
    # Create a test target date using datetime
    target_date = datetime(2023, 1, 1)
    # Create a test configuration dictionary
    config = {"products": ["DALMP"]}
    # Mock the ForecastingPipeline class
    with unittest.mock.patch("src.backend.tests.test_pipeline.test_pipeline_executor.ForecastingPipeline") as MockPipeline:
        # Configure the mock pipeline's run method to raise an exception
        MockPipeline.return_value.run.side_effect = Exception("Pipeline failed")

        # Initialize a PipelineExecutor with the test parameters
        executor = PipelineExecutor(target_date, config, "test-execution-id")
        # Use pytest.raises to assert that executing the pipeline raises PipelineExecutionError
        with pytest.raises(PipelineExecutionError):
            executor.execute()

        # Assert that the executor's executed flag remains False
        assert executor.executed is False
        # Assert that the executor's results dictionary contains error status
        assert executor.results == {"execution_id": "test-execution-id", "status": "pending"}
        # Assert that the pipeline's run method was called once
        MockPipeline.return_value.run.assert_called_once()


def test_pipeline_executor_get_results_before_execution():
    """Test that get_results returns initial results when called before execution"""
    # Create a test target date using datetime
    target_date = datetime(2023, 1, 1)
    # Create a test configuration dictionary
    config = {"products": ["DALMP"]}
    # Initialize a PipelineExecutor with the test parameters
    executor = PipelineExecutor(target_date, config, "test-execution-id")
    # Call get_results without executing the pipeline
    results = executor.get_results()

    # Assert that the results dictionary contains the execution_id and status='pending'
    assert results == {"execution_id": "test-execution-id", "status": "pending"}


def test_pipeline_executor_get_results_after_execution():
    """Test that get_results returns complete results after successful execution"""
    # Create a test target date using datetime
    target_date = datetime(2023, 1, 1)
    # Create a test configuration dictionary
    config = {"products": ["DALMP"]}
    # Mock the ForecastingPipeline class
    with unittest.mock.patch("src.backend.tests.test_pipeline.test_pipeline_executor.ForecastingPipeline") as MockPipeline:
        # Configure the mock pipeline's run method to return True (success)
        MockPipeline.return_value.run.return_value = True
        # Configure the mock pipeline's get_results method to return a test result dictionary
        MockPipeline.return_value.get_results.return_value = {"forecast": "success"}

        # Initialize a PipelineExecutor with the test parameters
        executor = PipelineExecutor(target_date, config, "test-execution-id")
        # Execute the pipeline by calling executor.execute()
        executor.execute()
        # Call get_results after execution
        results = executor.get_results()

        # Assert that the results dictionary contains the expected values from the pipeline
        assert results == {"execution_id": "test-execution-id", "status": "pending", "forecast": "success"}


def test_pipeline_executor_was_fallback_used_before_execution():
    """Test that was_fallback_used returns False when called before execution"""
    # Create a test target date using datetime
    target_date = datetime(2023, 1, 1)
    # Create a test configuration dictionary
    config = {"products": ["DALMP"]}
    # Initialize a PipelineExecutor with the test parameters
    executor = PipelineExecutor(target_date, config, "test-execution-id")
    # Call was_fallback_used without executing the pipeline
    result = executor.was_fallback_used()

    # Assert that the result is False
    assert result is False


def test_execute_forecasting_pipeline_success():
    """Test that execute_forecasting_pipeline function works correctly with successful execution"""
    # Create a test target date using datetime
    target_date = datetime(2023, 1, 1)
    # Create a test configuration dictionary
    config = {"products": ["DALMP"]}
    # Mock the PipelineExecutor class
    with unittest.mock.patch("src.backend.tests.test_pipeline.test_pipeline_executor.PipelineExecutor") as MockExecutor:
        # Configure the mock executor's execute method to return True
        MockExecutor.return_value.execute.return_value = True
        # Configure the mock executor's get_results method to return a test result dictionary
        MockExecutor.return_value.get_results.return_value = {"forecast": "success"}

        # Call execute_forecasting_pipeline with the test parameters
        result = execute_forecasting_pipeline(target_date, config)

        # Assert that the result matches the expected dictionary
        assert result == {"forecast": "success"}
        # Assert that the executor's execute method was called once
        MockExecutor.return_value.execute.assert_called_once()
        # Assert that the executor's get_results method was called once
        MockExecutor.return_value.get_results.assert_called_once()


def test_execute_forecasting_pipeline_failure():
    """Test that execute_forecasting_pipeline handles exceptions correctly"""
    # Create a test target date using datetime
    target_date = datetime(2023, 1, 1)
    # Create a test configuration dictionary
    config = {"products": ["DALMP"]}
    # Mock the PipelineExecutor class
    with unittest.mock.patch("src.backend.tests.test_pipeline.test_pipeline_executor.PipelineExecutor") as MockExecutor:
        # Configure the mock executor's execute method to raise an exception
        MockExecutor.return_value.execute.side_effect = Exception("Pipeline failed")

        # Use pytest.raises to assert that calling execute_forecasting_pipeline raises PipelineExecutionError
        with pytest.raises(PipelineExecutionError):
            execute_forecasting_pipeline(target_date, config)

        # Assert that the executor's execute method was called once
        MockExecutor.return_value.execute.assert_called_once()


def test_execute_with_default_config():
    """Test that execute_with_default_config calls execute_forecasting_pipeline with default config"""
    # Create a test target date using datetime
    target_date = datetime(2023, 1, 1)
    # Mock the execute_forecasting_pipeline function
    with unittest.mock.patch("src.backend.tests.test_pipeline.test_pipeline_executor.execute_forecasting_pipeline") as MockExecute:
        # Configure the mock to return a test result dictionary
        MockExecute.return_value = {"forecast": "success"}

        # Call execute_with_default_config with the test target date
        result = execute_with_default_config(target_date)

        # Assert that execute_forecasting_pipeline was called with the target date and default config
        MockExecute.assert_called_with(target_date, get_default_config())
        # Assert that the result matches the expected dictionary
        assert result == {"forecast": "success"}


def test_get_default_config():
    """Test that get_default_config returns a copy of the default configuration"""
    # Call get_default_config
    config1 = get_default_config()
    # Assert that the result is a dictionary
    assert isinstance(config1, dict)
    # Assert that the result contains expected keys (data_sources, products, fallback, validation, storage)
    assert "data_sources" in config1
    assert "products" in config1
    assert "fallback" in config1
    assert "validation" in config1
    assert "storage" in config1

    # Modify the returned dictionary
    config1["products"].append("NewProduct")
    # Call get_default_config again
    config2 = get_default_config()
    # Assert that the second result is not affected by the modification (deep copy)
    assert config2["products"] == FORECAST_PRODUCTS


def test_validate_config_valid():
    """Test that validate_config returns True for valid configurations"""
    # Create a valid test configuration with all required sections
    valid_config = {
        "data_sources": {"source1": {"url": "http://example.com"}},
        "products": ["DALMP"],
        "fallback": {"enabled": True, "max_search_days": 7},
        "validation": {"schema": True, "completeness": True, "plausibility": True},
        "storage": {"format": "parquet", "compression": "snappy"}
    }
    # Call validate_config with the test configuration
    result = validate_config(valid_config)

    # Assert that the result is True
    assert result is True


def test_validate_config_invalid():
    """Test that validate_config returns False for invalid configurations"""
    # Create several invalid test configurations (missing sections, empty products list, etc.)
    invalid_configs = [
        {"products": ["DALMP"], "fallback": {"enabled": True, "max_search_days": 7}, "validation": {"schema": True, "completeness": True, "plausibility": True}, "storage": {"format": "parquet", "compression": "snappy"}},  # Missing data_sources
        {"data_sources": {"source1": {"url": "http://example.com"}}, "fallback": {"enabled": True, "max_search_days": 7}, "validation": {"schema": True, "completeness": True, "plausibility": True}, "storage": {"format": "parquet", "compression": "snappy"}},  # Missing products
        {"data_sources": {"source1": {"url": "http://example.com"}}, "products": [], "fallback": {"enabled": True, "max_search_days": 7}, "validation": {"schema": True, "completeness": True, "plausibility": True}, "storage": {"format": "parquet", "compression": "snappy"}},  # Empty products
        {"data_sources": {"source1": {"url": "http://example.com"}}, "products": ["DALMP"], "validation": {"schema": True, "completeness": True, "plausibility": True}, "storage": {"format": "parquet", "compression": "snappy"}},  # Missing fallback
        {"data_sources": {"source1": {"url": "http://example.com"}}, "products": ["DALMP"], "fallback": {"enabled": True, "max_search_days": 7}, "storage": {"format": "parquet", "compression": "snappy"}},  # Missing validation
        {"data_sources": {"source1": {"url": "http://example.com"}}, "products": ["DALMP"], "fallback": {"enabled": True, "max_search_days": 7}, "validation": {"schema": True, "completeness": True, "plausibility": True}}  # Missing storage
    ]
    # Call validate_config with each invalid configuration
    for config in invalid_configs:
        result = validate_config(config)
        # Assert that the result is False in each case
        assert result is False


def test_merge_configs():
    """Test that merge_configs correctly merges user config with default config"""
    # Create a default configuration dictionary
    default_config = {
        "data_sources": {"source1": {"url": "http://example.com", "api_key": "default"}},
        "products": ["DALMP", "RTLMP"],
        "fallback": {"enabled": True, "max_search_days": 7},
        "validation": {"schema": True, "completeness": True, "plausibility": True},
        "storage": {"format": "parquet", "compression": "snappy"}
    }
    # Create a user configuration dictionary with some overridden values
    user_config = {
        "data_sources": {"source1": {"api_key": "user"}},
        "products": ["DALMP"],
        "fallback": {"max_search_days": 10}
    }
    # Call merge_configs with the user and default configurations
    result = merge_configs(user_config, default_config)

    # Assert that the result contains all keys from the default config
    assert "data_sources" in result
    assert "products" in result
    assert "fallback" in result
    assert "validation" in result
    assert "storage" in result
    # Assert that values specified in user config override default values
    assert result["data_sources"]["source1"]["api_key"] == "user"
    assert result["products"] == ["DALMP"]
    assert result["fallback"]["max_search_days"] == 10
    # Assert that values not specified in user config retain default values
    assert result["data_sources"]["source1"]["url"] == "http://example.com"
    assert result["fallback"]["enabled"] is True
    # Assert that nested dictionaries are properly merged
    assert result["validation"] == {"schema": True, "completeness": True, "plausibility": True}
    assert result["storage"] == {"format": "parquet", "compression": "snappy"}