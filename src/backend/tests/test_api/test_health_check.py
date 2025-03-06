import pytest
from unittest import mock
from datetime import datetime
import requests  # package_version: 2.28.0+

# Internal imports
from ...api.health_check import get_health_status, check_system_health, SystemHealthCheck, check_data_source_health, check_storage_health, check_pipeline_health
from ...api.exceptions import APIError
from ...storage.storage_manager import StorageManager
from ...pipeline.pipeline_executor import PipelineExecutor
from ...config.settings import FORECAST_PRODUCTS, DATA_SOURCES

def test_get_health_status():
    """Test that get_health_status returns a valid response with expected fields"""
    # Call get_health_status()
    result = get_health_status()

    # Assert that the result is a dictionary
    assert isinstance(result, dict)

    # Assert that the result contains 'status' key with value 'ok'
    assert 'status' in result
    assert result['status'] == 'ok'

    # Assert that the result contains 'timestamp' key with a valid datetime string
    assert 'timestamp' in result
    try:
        datetime.fromisoformat(result['timestamp'])
    except ValueError:
        assert False, "timestamp is not a valid datetime string"

def test_system_health_check_initialization():
    """Test that SystemHealthCheck initializes correctly"""
    # Create a SystemHealthCheck instance
    health_check = SystemHealthCheck()

    # Assert that last_check_result is None
    assert health_check.last_check_result is None

    # Assert that last_check_time is None
    assert health_check.last_check_time is None

def test_system_health_check_all():
    """Test that SystemHealthCheck.check_all performs a comprehensive health check"""
    # Mock check_data_source_health to return a predefined result
    with mock.patch('src.backend.api.health_check.check_data_source_health') as mock_data_sources:
        mock_data_sources.return_value = {"data_source_1": {"status": "healthy"}}

        # Mock check_storage_health to return a predefined result
        with mock.patch('src.backend.api.health_check.check_storage_health') as mock_storage:
            mock_storage.return_value = {"status": "healthy"}

            # Mock check_pipeline_health to return a predefined result
            with mock.patch('src.backend.api.health_check.check_pipeline_health') as mock_pipeline:
                mock_pipeline.return_value = {"status": "healthy"}

                # Create a SystemHealthCheck instance
                health_check = SystemHealthCheck()

                # Call check_all() method
                result = health_check.check_all()

                # Assert that the result is a dictionary with expected structure
                assert isinstance(result, dict)
                assert 'overall_status' in result
                assert 'data_sources' in result
                assert 'storage' in result
                assert 'pipeline' in result

                # Assert that the result contains 'overall_status' key
                assert result['overall_status'] == 'healthy'

                # Assert that the result contains 'components' key with expected component statuses
                assert result['data_sources'] == {"data_source_1": {"status": "healthy"}}
                assert result['storage'] == {"status": "healthy"}
                assert result['pipeline'] == {"status": "healthy"}

                # Assert that last_check_result and last_check_time are updated
                assert health_check.last_check_result == result
                assert health_check.last_check_time is not None

def test_system_health_check_simple_status():
    """Test that SystemHealthCheck.get_simple_status returns a simplified status"""
    # Create a SystemHealthCheck instance
    health_check = SystemHealthCheck()

    # Mock check_all to return a predefined result
    with mock.patch.object(health_check, 'check_all') as mock_check_all:
        mock_check_all.return_value = {"overall_status": "healthy"}

        # Call get_simple_status() method
        result = health_check.get_simple_status()

        # Assert that the result is a dictionary with simplified structure
        assert isinstance(result, dict)
        assert 'status' in result
        assert 'timestamp' in result

        # Assert that the result contains 'status' key with expected value
        assert result['status'] == 'healthy'

        # Assert that the result contains 'timestamp' key
        assert health_check.last_check_time is not None
        assert result['timestamp'] == health_check.last_check_time.isoformat()

def test_system_health_check_component():
    """Test that SystemHealthCheck.check_component checks a specific component"""
    # Mock check_data_source_health to return a predefined result
    with mock.patch('src.backend.api.health_check.check_data_source_health') as mock_data_sources:
        mock_data_sources.return_value = {"data_source_1": {"status": "healthy"}}

        # Mock check_storage_health to return a predefined result
        with mock.patch('src.backend.api.health_check.check_storage_health') as mock_storage:
            mock_storage.return_value = {"status": "healthy"}

            # Mock check_pipeline_health to return a predefined result
            with mock.patch('src.backend.api.health_check.check_pipeline_health') as mock_pipeline:
                mock_pipeline.return_value = {"status": "healthy"}

                # Create a SystemHealthCheck instance
                health_check = SystemHealthCheck()

                # Call check_component('data_sources') method
                result = health_check.check_component('data_sources')

                # Assert that check_data_source_health was called with expected arguments
                mock_data_sources.assert_called_with(DATA_SOURCES)

                # Call check_component('storage') method
                result = health_check.check_component('storage')

                # Assert that check_storage_health was called
                mock_storage.assert_called()

                # Call check_component('pipeline') method
                result = health_check.check_component('pipeline')

                # Assert that check_pipeline_health was called
                mock_pipeline.assert_called()

                # Assert that calling check_component with invalid component raises ValueError
                with pytest.raises(ValueError):
                    health_check.check_component('invalid_component')

def test_check_system_health():
    """Test that check_system_health function performs a comprehensive health check"""
    # Mock SystemHealthCheck.check_all to return a predefined result
    with mock.patch('src.backend.api.health_check.SystemHealthCheck.check_all') as mock_check_all:
        mock_check_all.return_value = {"overall_status": "healthy"}

        # Call check_system_health()
        result = check_system_health()

        # Assert that SystemHealthCheck was instantiated
        # Assert that check_all was called
        mock_check_all.assert_called()

        # Assert that the result matches the expected structure
        assert result == {"overall_status": "healthy"}

def test_check_data_source_health():
    """Test that check_data_source_health correctly checks external data sources"""
    # Create mock data sources configuration
    mock_data_sources = {
        "source1": {"url": "http://example.com/api1"},
        "source2": {"url": "http://example.com/api2"}
    }

    # Mock requests.head to return different responses for different URLs
    with mock.patch('requests.head') as mock_head:
        mock_head.side_effect = lambda url, timeout: mock.Mock(status_code=200 if url == "http://example.com/api1" else 500, elapsed=datetime.now() - datetime.now())

        # Call check_data_source_health with mock data sources
        result = check_data_source_health(mock_data_sources)

        # Assert that the result contains status for each data source
        assert "source1" in result
        assert "source2" in result

        # Assert that healthy sources have 'healthy' status
        assert result["source1"]["status"] == "healthy"

        # Assert that unhealthy sources have 'unhealthy' status
        assert result["source2"]["status"] == "unhealthy"

        # Assert that response times are included
        assert "response_time" in result["source1"]
        assert "response_time" in result["source2"]

def test_check_storage_health():
    """Test that check_storage_health correctly checks storage system health"""
    # Mock StorageManager.get_storage_info to return predefined storage info
    with mock.patch('src.backend.api.health_check.StorageManager.get_storage_info') as mock_get_storage_info:
        mock_get_storage_info.return_value = {"storage_info": "mocked"}

        # Mock StorageManager.check_forecast_availability to return True for some products and False for others
        with mock.patch('src.backend.api.health_check.StorageManager.check_forecast_availability') as mock_check_forecast_availability:
            mock_check_forecast_availability.side_effect = lambda date, product: product in ["DALMP", "RTLMP"]

            # Call check_storage_health()
            result = check_storage_health()

            # Assert that the result contains expected storage health information
            assert "status" in result
            assert "details" in result
            assert "storage_info" in result["details"]
            assert result["details"]["storage_info"] == {"storage_info": "mocked"}

            # Assert that the result includes availability status for each product
            assert "forecast_availability" in result["details"]
            assert "DALMP" in result["details"]["forecast_availability"]
            assert "RTLMP" in result["details"]["forecast_availability"]
            assert "RegUp" in result["details"]["forecast_availability"]
            assert result["details"]["forecast_availability"]["DALMP"] == True
            assert result["details"]["forecast_availability"]["RTLMP"] == True
            assert result["details"]["forecast_availability"]["RegUp"] == False

            # Assert that the overall storage status is determined correctly
            assert result["status"] == "healthy"

def test_check_pipeline_health():
    """Test that check_pipeline_health correctly checks pipeline health"""
    # Mock PipelineExecutor._validate_execution_state to return True
    with mock.patch('src.backend.api.health_check.PipelineExecutor._validate_execution_state') as mock_validate_execution_state:
        mock_validate_execution_state.return_value = True

        # Call check_pipeline_health()
        result = check_pipeline_health()

        # Assert that PipelineExecutor was instantiated
        # Assert that _validate_execution_state was called
        mock_validate_execution_state.assert_called()

        # Assert that the result contains expected pipeline health information
        assert "status" in result
        assert "details" in result
        assert "valid_state" in result["details"]
        assert result["details"]["valid_state"] == True

        # Assert that the pipeline status is 'healthy'
        assert result["status"] == "healthy"

    # Repeat with _validate_execution_state returning False
    with mock.patch('src.backend.api.health_check.PipelineExecutor._validate_execution_state') as mock_validate_execution_state:
        mock_validate_execution_state.return_value = False

        # Call check_pipeline_health()
        result = check_pipeline_health()

        # Assert that the pipeline status is 'unhealthy'
        assert result["status"] == "unhealthy"