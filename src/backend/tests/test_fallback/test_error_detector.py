import pytest
from unittest.mock import patch

# Import error categories and functions to test
from ...fallback.error_detector import (
    ErrorCategory,
    detect_error,
    categorize_error,
    extract_error_details,
    should_activate_fallback
)

# Import error detection failure exception
from ...fallback.exceptions import ErrorDetectionFailure

# Import exception types from system components
from ...data_ingestion.exceptions import DataIngestionError, APIConnectionError, MissingDataError
from ...feature_engineering.exceptions import FeatureEngineeringError
from ...forecasting_engine.exceptions import ForecastingEngineError, ModelExecutionError
from ...forecast_validation.exceptions import ForecastValidationError
from ...storage.exceptions import StorageError
from ...pipeline.exceptions import PipelineError


def test_categorize_error_data_source_errors():
    """Tests that data ingestion and API errors are correctly categorized as DATA_SOURCE_ERROR"""
    # Create instances of DataIngestionError, APIConnectionError, and MissingDataError
    data_ingestion_error = DataIngestionError("Test data ingestion error")
    api_connection_error = APIConnectionError("api/endpoint", "load_forecast", Exception("Connection failed"))
    missing_data_error = MissingDataError("load_forecast", ["timestamp", "load_mw"])
    
    # Call categorize_error on each exception
    category1 = categorize_error(data_ingestion_error)
    category2 = categorize_error(api_connection_error)
    category3 = categorize_error(missing_data_error)
    
    # Assert that all are categorized as ErrorCategory.DATA_SOURCE_ERROR
    assert category1 == ErrorCategory.DATA_SOURCE_ERROR
    assert category2 == ErrorCategory.DATA_SOURCE_ERROR
    assert category3 == ErrorCategory.DATA_SOURCE_ERROR


def test_categorize_error_feature_errors():
    """Tests that feature engineering errors are correctly categorized as FEATURE_ERROR"""
    # Create an instance of FeatureEngineeringError
    feature_error = FeatureEngineeringError("Test feature engineering error")
    
    # Call categorize_error on the exception
    category = categorize_error(feature_error)
    
    # Assert that it is categorized as ErrorCategory.FEATURE_ERROR
    assert category == ErrorCategory.FEATURE_ERROR


def test_categorize_error_model_errors():
    """Tests that forecasting engine and model errors are correctly categorized as MODEL_ERROR"""
    # Create instances of ForecastingEngineError and ModelExecutionError
    forecasting_error = ForecastingEngineError("Test forecasting engine error")
    model_error = ModelExecutionError("Test model execution error", "DALMP", 12, "model123")
    
    # Call categorize_error on each exception
    category1 = categorize_error(forecasting_error)
    category2 = categorize_error(model_error)
    
    # Assert that all are categorized as ErrorCategory.MODEL_ERROR
    assert category1 == ErrorCategory.MODEL_ERROR
    assert category2 == ErrorCategory.MODEL_ERROR


def test_categorize_error_validation_errors():
    """Tests that forecast validation errors are correctly categorized as VALIDATION_ERROR"""
    # Create an instance of ForecastValidationError
    validation_error = ForecastValidationError("Test validation error")
    
    # Call categorize_error on the exception
    category = categorize_error(validation_error)
    
    # Assert that it is categorized as ErrorCategory.VALIDATION_ERROR
    assert category == ErrorCategory.VALIDATION_ERROR


def test_categorize_error_storage_errors():
    """Tests that storage errors are correctly categorized as STORAGE_ERROR"""
    # Create an instance of StorageError
    storage_error = StorageError("Test storage error")
    
    # Call categorize_error on the exception
    category = categorize_error(storage_error)
    
    # Assert that it is categorized as ErrorCategory.STORAGE_ERROR
    assert category == ErrorCategory.STORAGE_ERROR


def test_categorize_error_pipeline_errors():
    """Tests that pipeline errors are correctly categorized as PIPELINE_ERROR"""
    # Create an instance of PipelineError
    pipeline_error = PipelineError("Test pipeline error")
    
    # Call categorize_error on the exception
    category = categorize_error(pipeline_error)
    
    # Assert that it is categorized as ErrorCategory.PIPELINE_ERROR
    assert category == ErrorCategory.PIPELINE_ERROR


def test_categorize_error_unknown_errors():
    """Tests that unrecognized errors are correctly categorized as UNKNOWN_ERROR"""
    # Create instances of standard exceptions like ValueError, KeyError
    value_error = ValueError("Test value error")
    key_error = KeyError("Test key error")
    
    # Call categorize_error on each exception
    category1 = categorize_error(value_error)
    category2 = categorize_error(key_error)
    
    # Assert that all are categorized as ErrorCategory.UNKNOWN_ERROR
    assert category1 == ErrorCategory.UNKNOWN_ERROR
    assert category2 == ErrorCategory.UNKNOWN_ERROR


def test_extract_error_details_data_source_error():
    """Tests extraction of details from data source errors"""
    # Create an APIConnectionError with specific details
    api_error = APIConnectionError("api/load-forecast", "load_forecast", Exception("Connection timed out"))
    
    # Call extract_error_details with the error and ErrorCategory.DATA_SOURCE_ERROR
    details = extract_error_details(api_error, ErrorCategory.DATA_SOURCE_ERROR)
    
    # Assert that the returned details contain expected fields (api_endpoint, source_name)
    assert "api_endpoint" in details
    assert "source_name" in details
    
    # Assert that the details values match the error attributes
    assert details["api_endpoint"] == "api/load-forecast"
    assert details["source_name"] == "load_forecast"
    assert details["error_type"] == "APIConnectionError"


def test_extract_error_details_feature_error():
    """Tests extraction of details from feature engineering errors"""
    # Create a FeatureEngineeringError with specific details
    feature_error = FeatureEngineeringError("Failed to create feature")
    feature_error.feature_name = "load_ratio"  # Adding attribute for testing
    feature_error.source_features = ["load_mw", "generation_mw"]  # Adding attribute for testing
    
    # Call extract_error_details with the error and ErrorCategory.FEATURE_ERROR
    details = extract_error_details(feature_error, ErrorCategory.FEATURE_ERROR)
    
    # Assert that the returned details contain expected fields
    assert "feature_name" in details
    assert "source_features" in details
    
    # Assert that the details values match the error attributes
    assert details["feature_name"] == "load_ratio"
    assert details["source_features"] == ["load_mw", "generation_mw"]
    assert details["error_type"] == "FeatureEngineeringError"


def test_extract_error_details_model_error():
    """Tests extraction of details from model execution errors"""
    # Create a ModelExecutionError with specific details (product, hour, model_id)
    model_error = ModelExecutionError(
        "Model execution failed", "DALMP", 12, "dalmp_hour12_model"
    )
    
    # Call extract_error_details with the error and ErrorCategory.MODEL_ERROR
    details = extract_error_details(model_error, ErrorCategory.MODEL_ERROR)
    
    # Assert that the returned details contain expected fields
    assert "product" in details
    assert "hour" in details
    assert "model_id" in details
    
    # Assert that the details values match the error attributes
    assert details["product"] == "DALMP"
    assert details["hour"] == 12
    assert details["model_id"] == "dalmp_hour12_model"
    assert details["error_type"] == "ModelExecutionError"


def test_extract_error_details_validation_error():
    """Tests extraction of details from forecast validation errors"""
    # Create a ForecastValidationError with specific validation errors
    validation_errors = {
        "completeness": ["Missing hours 10-12 for DALMP"],
        "plausibility": ["Negative price for product RegUp at hour 15"]
    }
    validation_error = ForecastValidationError("Validation failed", validation_errors)
    
    # Call extract_error_details with the error and ErrorCategory.VALIDATION_ERROR
    details = extract_error_details(validation_error, ErrorCategory.VALIDATION_ERROR)
    
    # Assert that the returned details contain expected fields (validation_errors)
    assert "validation_errors" in details
    
    # Assert that the details values match the error attributes
    assert details["validation_errors"] == validation_errors
    assert details["error_type"] == "ForecastValidationError"


def test_extract_error_details_storage_error():
    """Tests extraction of details from storage errors"""
    # Create a StorageError with specific details (file_path, operation)
    storage_error = StorageError("Storage operation failed")
    storage_error.file_path = "/data/forecasts/2023/05/01_dalmp.parquet"  # Adding attribute for testing
    storage_error.operation = "write"  # Adding attribute for testing
    
    # Call extract_error_details with the error and ErrorCategory.STORAGE_ERROR
    details = extract_error_details(storage_error, ErrorCategory.STORAGE_ERROR)
    
    # Assert that the returned details contain expected fields
    assert "file_path" in details
    assert "operation" in details
    
    # Assert that the details values match the error attributes
    assert details["file_path"] == "/data/forecasts/2023/05/01_dalmp.parquet"
    assert details["operation"] == "write"
    assert details["error_type"] == "StorageError"


def test_extract_error_details_pipeline_error():
    """Tests extraction of details from pipeline errors"""
    # Create a PipelineError with specific details (pipeline_name, stage)
    pipeline_error = PipelineError("Pipeline execution failed")
    pipeline_error.pipeline_name = "forecast_generation"  # Adding attribute for testing
    pipeline_error.stage_name = "data_ingestion"  # Adding attribute for testing
    
    # Call extract_error_details with the error and ErrorCategory.PIPELINE_ERROR
    details = extract_error_details(pipeline_error, ErrorCategory.PIPELINE_ERROR)
    
    # Assert that the returned details contain expected fields
    assert "pipeline_name" in details
    assert "stage_name" in details
    
    # Assert that the details values match the error attributes
    assert details["pipeline_name"] == "forecast_generation"
    assert details["stage_name"] == "data_ingestion"
    assert details["error_type"] == "PipelineError"


def test_extract_error_details_unknown_error():
    """Tests extraction of details from unknown errors"""
    # Create a standard exception like ValueError with a message
    value_error = ValueError("Invalid value")
    
    # Call extract_error_details with the error and ErrorCategory.UNKNOWN_ERROR
    details = extract_error_details(value_error, ErrorCategory.UNKNOWN_ERROR)
    
    # Assert that the returned details contain basic fields (error_type, error_message)
    assert "error_type" in details
    assert "error_message" in details
    
    # Assert that the details values match the error attributes
    assert details["error_type"] == "ValueError"
    assert details["error_message"] == "Invalid value"


@pytest.mark.parametrize('source_name,expected', [
    ('load_forecast', True),
    ('historical_prices', True),
    ('generation_forecast', True),
    ('non_critical_source', True)
])
def test_should_activate_fallback_data_source_error(source_name, expected):
    """Tests fallback activation decision for data source errors"""
    # Create error details dictionary with specified source_name
    error_details = {"source_name": source_name}
    
    # Call should_activate_fallback with ErrorCategory.DATA_SOURCE_ERROR and the details
    result = should_activate_fallback(ErrorCategory.DATA_SOURCE_ERROR, error_details)
    
    # Assert that the result matches the expected value based on source criticality
    assert result == expected


@pytest.mark.parametrize('affected_features,expected', [
    (['load_ratio'], True),
    (['price_lag_24h'], True), 
    (['non_critical_feature'], True)
])
def test_should_activate_fallback_feature_error(affected_features, expected):
    """Tests fallback activation decision for feature engineering errors"""
    # Create error details dictionary with specified affected_features
    error_details = {"missing_features": affected_features}
    
    # Call should_activate_fallback with ErrorCategory.FEATURE_ERROR and the details
    result = should_activate_fallback(ErrorCategory.FEATURE_ERROR, error_details)
    
    # Assert that the result matches the expected value based on feature criticality
    assert result == expected


@pytest.mark.parametrize('product,hour,expected', [
    ('DALMP', 12, True),
    ('RTLMP', 18, True),
    ('RegUp', 5, True)
])
def test_should_activate_fallback_model_error(product, hour, expected):
    """Tests fallback activation decision for model execution errors"""
    # Create error details dictionary with specified product and hour
    error_details = {"product": product, "hour": hour}
    
    # Call should_activate_fallback with ErrorCategory.MODEL_ERROR and the details
    result = should_activate_fallback(ErrorCategory.MODEL_ERROR, error_details)
    
    # Assert that the result matches the expected value based on product/hour criticality
    assert result == expected


@pytest.mark.parametrize('severity,expected', [
    ('critical', True),
    ('warning', True)
])
def test_should_activate_fallback_validation_error(severity, expected):
    """Tests fallback activation decision for validation errors"""
    # Create error details dictionary with specified severity
    error_details = {"severity": severity}
    
    # Call should_activate_fallback with ErrorCategory.VALIDATION_ERROR and the details
    result = should_activate_fallback(ErrorCategory.VALIDATION_ERROR, error_details)
    
    # Assert that the result matches the expected value based on error severity
    assert result == expected


@pytest.mark.parametrize('operation,expected', [
    ('read', True),
    ('write', False)
])
def test_should_activate_fallback_storage_error(operation, expected):
    """Tests fallback activation decision for storage errors"""
    # Create error details dictionary with specified operation
    error_details = {"operation": operation}
    
    # Call should_activate_fallback with ErrorCategory.STORAGE_ERROR and the details
    result = should_activate_fallback(ErrorCategory.STORAGE_ERROR, error_details)
    
    # Assert that the result matches the expected value based on operation type
    assert result == expected


@pytest.mark.parametrize('stage,expected', [
    ('data_ingestion', True),
    ('feature_engineering', True),
    ('forecasting', True),
    ('validation', True),
    ('visualization', False)
])
def test_should_activate_fallback_pipeline_error(stage, expected):
    """Tests fallback activation decision for pipeline errors"""
    # Create error details dictionary with specified pipeline stage
    error_details = {"stage_name": stage}
    
    # Call should_activate_fallback with ErrorCategory.PIPELINE_ERROR and the details
    result = should_activate_fallback(ErrorCategory.PIPELINE_ERROR, error_details)
    
    # Assert that the result matches the expected value based on pipeline stage
    assert result == expected


def test_should_activate_fallback_unknown_error():
    """Tests fallback activation decision for unknown errors"""
    # Create basic error details dictionary
    error_details = {"error_type": "ValueError", "error_message": "Unknown error"}
    
    # Call should_activate_fallback with ErrorCategory.UNKNOWN_ERROR and the details
    result = should_activate_fallback(ErrorCategory.UNKNOWN_ERROR, error_details)
    
    # Assert that the result is False (unknown errors should not trigger fallback by default)
    assert result == False


def test_detect_error_integration():
    """Integration test for the detect_error function"""
    # Create a test error (e.g., APIConnectionError)
    test_error = APIConnectionError("api/load-forecast", "load_forecast", Exception("Connection failed"))
    
    # Call detect_error with the error and a component name
    category, details = detect_error(test_error, "data_ingestion")
    
    # Assert that the returned category matches the expected category
    assert category == ErrorCategory.DATA_SOURCE_ERROR
    
    # Assert that the returned details contain expected fields
    assert "api_endpoint" in details
    assert "source_name" in details
    assert "error_type" in details
    assert "error_message" in details
    
    # Assert that the details values match the error attributes
    assert details["api_endpoint"] == "api/load-forecast"
    assert details["source_name"] == "load_forecast"
    assert details["error_type"] == "APIConnectionError"


def test_detect_error_handles_exceptions():
    """Tests that detect_error properly handles exceptions during detection"""
    # Create a test error
    test_error = ValueError("Test error")
    
    # Mock categorize_error to raise an exception
    with patch('src.backend.fallback.error_detector.categorize_error', side_effect=Exception("Mock error")):
        # Assert that ErrorDetectionFailure is raised
        with pytest.raises(ErrorDetectionFailure) as excinfo:
            detect_error(test_error, "test_component")
        
        # Assert that the original error is preserved in the ErrorDetectionFailure
        assert excinfo.value.original_error == test_error
        assert excinfo.value.component == "test_component"