"""Integration tests for the complete forecasting pipeline, testing the end-to-end functionality."""

import pytest  # pytest: 7.0.0+
from datetime import datetime  # standard library
from unittest import mock  # standard library
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
import os  # standard library
import tempfile  # standard library

from ...pipeline.pipeline_executor import execute_forecasting_pipeline, execute_with_default_config, get_default_config, PipelineExecutor  # Main entry point for executing the forecasting pipeline
from ...pipeline.forecasting_pipeline import ForecastingPipeline  # Main forecasting pipeline implementation
from ...pipeline.exceptions import PipelineError, PipelineExecutionError, PipelineStageError  # Exceptions for pipeline-related errors
from ...data_ingestion.api_client import APIClient  # Client for fetching data from external sources
from ...data_ingestion.exceptions import APIConnectionError, APIResponseError  # Exceptions for API connection failures
from ...storage.storage_manager import save_forecast, get_forecast, check_forecast_availability  # Save and retrieve forecasts
from ...fallback.fallback_retriever import retrieve_fallback_forecast  # Retrieve previous forecast for fallback
from ...fallback.exceptions import NoFallbackAvailableError  # Exception when no suitable fallback is available
from ..fixtures.load_forecast_fixtures import create_mock_load_forecast_data, create_mock_api_response, MockLoadForecastClient  # Create mock load forecast data for tests
from ..fixtures.historical_prices_fixtures import create_mock_historical_price_data, MockHistoricalPriceClient  # Create mock historical price data for tests
from ..fixtures.generation_forecast_fixtures import create_mock_generation_forecast_data, MockGenerationForecastClient  # Create mock generation forecast data for tests
from ..fixtures.forecast_fixtures import create_mock_forecast_data, create_mock_forecast_ensemble  # Create mock forecast data as a DataFrame
from ...config.settings import FORECAST_PRODUCTS, FORECAST_HORIZON_HOURS  # List of valid forecast products
from ...utils.date_utils import localize_to_cst  # Convert datetime to CST timezone


TEST_PRODUCTS = ['DALMP', 'RTLMP', 'RegUp']


def setup_mock_clients(target_date: datetime) -> tuple[MockLoadForecastClient, MockHistoricalPriceClient, MockGenerationForecastClient]:
    """Sets up mock API clients for testing the pipeline"""
    load_client = MockLoadForecastClient()
    price_client = MockHistoricalPriceClient()
    generation_client = MockGenerationForecastClient()

    start_date = target_date - pandas.Timedelta(days=7)
    end_date = target_date + pandas.Timedelta(days=3)

    load_data = create_mock_load_forecast_data(start_date, hours=FORECAST_HORIZON_HOURS)
    load_client.add_response({}, create_mock_api_response(start_date, hours=FORECAST_HORIZON_HOURS))

    price_data = create_mock_historical_price_data(start_date, end_date)
    price_client.add_response({}, create_mock_api_response(price_data))

    generation_data = create_mock_generation_forecast_data(start_date, hours=FORECAST_HORIZON_HOURS)
    generation_client.add_response({}, create_mock_api_response(start_date, hours=FORECAST_HORIZON_HOURS))

    return load_client, price_client, generation_client


def create_test_config(mock_clients: tuple[MockLoadForecastClient, MockHistoricalPriceClient, MockGenerationForecastClient], override_config: dict = None) -> dict:
    """Creates a test configuration with mock clients"""
    config = get_default_config()
    config['data_sources']['load_forecast'] = mock_clients[0]
    config['data_sources']['historical_prices'] = mock_clients[1]
    config['data_sources']['generation_forecast'] = mock_clients[2]

    if override_config:
        config.update(override_config)

    return config


def setup_storage_for_tests() -> str:
    """Sets up temporary storage for testing"""
    temp_dir = tempfile.mkdtemp()
    os.environ['STORAGE_ROOT_DIR'] = temp_dir
    return temp_dir


def create_previous_forecasts(target_date: datetime, products: list[str], days_back: int) -> dict:
    """Creates previous forecasts for testing fallback mechanism"""
    saved_forecasts = {}
    for product in products:
        saved_forecasts[product] = []
        for day in range(1, days_back + 1):
            forecast_date = target_date - datetime.timedelta(days=day)
            forecast_data = create_mock_forecast_data(product=product, start_time=forecast_date)
            save_path = save_forecast(forecast_data, forecast_date, product)
            saved_forecasts[product].append(save_path)
    return saved_forecasts


@pytest.mark.integration
def test_full_pipeline_successful_execution():
    """Tests that the full pipeline executes successfully with valid inputs"""
    temp_dir = setup_storage_for_tests()
    target_date = localize_to_cst(datetime(2024, 1, 20, 7))
    mock_clients = setup_mock_clients(target_date)
    test_config = create_test_config(mock_clients)

    result = execute_forecasting_pipeline(target_date, test_config)

    assert result['status'] == 'success'
    for product in FORECAST_PRODUCTS:
        assert product in result
        assert check_forecast_availability(target_date, product)
        forecast_df = get_forecast(target_date, product)
        assert isinstance(forecast_df, pandas.DataFrame)
        assert len(forecast_df) == FORECAST_HORIZON_HOURS
        assert not result.get(f'fallback_{product}', False)


@pytest.mark.integration
def test_pipeline_with_data_ingestion_failure():
    """Tests that the pipeline handles data ingestion failures correctly"""
    temp_dir = setup_storage_for_tests()
    create_previous_forecasts(target_date=localize_to_cst(datetime(2024, 1, 20, 7)), products=FORECAST_PRODUCTS, days_back=2)
    target_date = localize_to_cst(datetime(2024, 1, 21, 7))
    mock_clients = setup_mock_clients(target_date)
    mock_clients[0].set_error(APIConnectionError("Data ingestion failed", "load_forecast", Exception()))
    test_config = create_test_config(mock_clients)

    result = execute_forecasting_pipeline(target_date, test_config)

    assert result['status'] == 'success'
    assert mock_clients[0].fetch_data.call_count == 0
    for product in FORECAST_PRODUCTS:
        assert result.get(f'fallback_{product}', False)
        assert check_forecast_availability(target_date, product)
        forecast_df = get_forecast(target_date, product)
        assert isinstance(forecast_df, pandas.DataFrame)
        assert len(forecast_df) == FORECAST_HORIZON_HOURS
        assert forecast_df['timestamp'].iloc[0].date() == target_date.date()


@pytest.mark.integration
def test_pipeline_with_feature_engineering_failure():
    """Tests that the pipeline handles feature engineering failures correctly"""
    temp_dir = setup_storage_for_tests()
    create_previous_forecasts(target_date=localize_to_cst(datetime(2024, 1, 20, 7)), products=FORECAST_PRODUCTS, days_back=2)
    target_date = localize_to_cst(datetime(2024, 1, 21, 7))
    mock_clients = setup_mock_clients(target_date)
    test_config = create_test_config(mock_clients)

    with mock.patch('src.backend.pipeline.forecasting_pipeline.ProductHourFeatureCreator.create_features') as mock_create_features:
        mock_create_features.side_effect = Exception("Feature engineering failed")
        result = execute_forecasting_pipeline(target_date, test_config)

    assert result['status'] == 'success'
    for product in FORECAST_PRODUCTS:
        assert result.get(f'fallback_{product}', False)
        assert check_forecast_availability(target_date, product)
        forecast_df = get_forecast(target_date, product)
        assert isinstance(forecast_df, pandas.DataFrame)
        assert len(forecast_df) == FORECAST_HORIZON_HOURS
        assert forecast_df['timestamp'].iloc[0].date() == target_date.date()


@pytest.mark.integration
def test_pipeline_with_forecast_generation_failure():
    """Tests that the pipeline handles forecast generation failures correctly"""
    temp_dir = setup_storage_for_tests()
    create_previous_forecasts(target_date=localize_to_cst(datetime(2024, 1, 20, 7)), products=FORECAST_PRODUCTS, days_back=2)
    target_date = localize_to_cst(datetime(2024, 1, 21, 7))
    mock_clients = setup_mock_clients(target_date)
    test_config = create_test_config(mock_clients)

    with mock.patch('src.backend.forecasting_pipeline.ProbabilisticForecaster.generate_ensemble') as mock_generate_ensemble:
        mock_generate_ensemble.side_effect = Exception("Forecast generation failed")
        result = execute_forecasting_pipeline(target_date, test_config)

    assert result['status'] == 'success'
    for product in FORECAST_PRODUCTS:
        assert result.get(f'fallback_{product}', False)
        assert check_forecast_availability(target_date, product)
        forecast_df = get_forecast(target_date, product)
        assert isinstance(forecast_df, pandas.DataFrame)
        assert len(forecast_df) == FORECAST_HORIZON_HOURS
        assert forecast_df['timestamp'].iloc[0].date() == target_date.date()


@pytest.mark.integration
def test_pipeline_with_validation_failure():
    """Tests that the pipeline handles forecast validation failures correctly"""
    temp_dir = setup_storage_for_tests()
    create_previous_forecasts(target_date=localize_to_cst(datetime(2024, 1, 20, 7)), products=FORECAST_PRODUCTS, days_back=2)
    target_date = localize_to_cst(datetime(2024, 1, 21, 7))
    mock_clients = setup_mock_clients(target_date)
    test_config = create_test_config(mock_clients)

    with mock.patch('src.backend.forecasting_pipeline.validate_forecast_schema') as mock_validate_forecast_schema:
        mock_validate_forecast_schema.return_value = False, {"validation_error": "Schema validation failed"}
        result = execute_forecasting_pipeline(target_date, test_config)

    assert result['status'] == 'success'
    for product in FORECAST_PRODUCTS:
        assert result.get(f'fallback_{product}', False)
        assert check_forecast_availability(target_date, product)
        forecast_df = get_forecast(target_date, product)
        assert isinstance(forecast_df, pandas.DataFrame)
        assert len(forecast_df) == FORECAST_HORIZON_HOURS
        assert forecast_df['timestamp'].iloc[0].date() == target_date.date()


@pytest.mark.integration
def test_pipeline_with_storage_failure():
    """Tests that the pipeline handles storage failures correctly"""
    temp_dir = setup_storage_for_tests()
    create_previous_forecasts(target_date=localize_to_cst(datetime(2024, 1, 20, 7)), products=FORECAST_PRODUCTS, days_back=2)
    target_date = localize_to_cst(datetime(2024, 1, 21, 7))
    mock_clients = setup_mock_clients(target_date)
    test_config = create_test_config(mock_clients)

    with mock.patch('src.backend.forecasting_pipeline.save_forecast') as mock_save_forecast:
        mock_save_forecast.side_effect = Exception("Storage failed")
        result = execute_forecasting_pipeline(target_date, test_config)

    assert result['status'] == 'success'
    for product in FORECAST_PRODUCTS:
        assert result.get(f'fallback_{product}', False)
        assert check_forecast_availability(target_date, product)
        forecast_df = get_forecast(target_date, product)
        assert isinstance(forecast_df, pandas.DataFrame)
        assert len(forecast_df) == FORECAST_HORIZON_HOURS
        assert forecast_df['timestamp'].iloc[0].date() == target_date.date()


@pytest.mark.integration
def test_pipeline_with_no_fallback_available():
    """Tests pipeline behavior when no fallback forecasts are available"""
    temp_dir = setup_storage_for_tests()
    target_date = localize_to_cst(datetime(2024, 1, 21, 7))
    mock_clients = setup_mock_clients(target_date)
    mock_clients[0].set_error(APIConnectionError("Data ingestion failed", "load_forecast", Exception()))
    test_config = create_test_config(mock_clients)
    test_config['fallback']['max_search_days'] = 0

    with pytest.raises(NoFallbackAvailableError):
        execute_forecasting_pipeline(target_date, test_config)


@pytest.mark.integration
def test_pipeline_with_partial_failures():
    """Tests that the pipeline handles partial failures correctly"""
    temp_dir = setup_storage_for_tests()
    create_previous_forecasts(target_date=localize_to_cst(datetime(2024, 1, 20, 7)), products=FORECAST_PRODUCTS, days_back=2)
    target_date = localize_to_cst(datetime(2024, 1, 21, 7))
    mock_clients = setup_mock_clients(target_date)
    mock_clients[2].set_error(APIConnectionError("Data ingestion failed", "generation_forecast", Exception()))
    test_config = create_test_config(mock_clients)
    test_config['products'] = ['DALMP', 'RTLMP']

    result = execute_forecasting_pipeline(target_date, test_config)

    assert result['status'] == 'success'
    for product in ['DALMP', 'RTLMP']:
        assert check_forecast_availability(target_date, product)
        forecast_df = get_forecast(target_date, product)
        assert isinstance(forecast_df, pandas.DataFrame)
        assert len(forecast_df) == FORECAST_HORIZON_HOURS
        assert forecast_df['timestamp'].iloc[0].date() == target_date.date()


@pytest.mark.integration
@pytest.mark.performance
def test_pipeline_performance():
    """Tests the performance of the full pipeline execution"""
    temp_dir = setup_storage_for_tests()
    target_date = localize_to_cst(datetime(2024, 1, 21, 7))
    mock_clients = setup_mock_clients(target_date)
    test_config = create_test_config(mock_clients)

    start_time = datetime.now()
    result = execute_forecasting_pipeline(target_date, test_config)
    end_time = datetime.now()

    execution_time = end_time - start_time
    assert execution_time.total_seconds() < 60
    assert result['status'] == 'success'


@pytest.mark.integration
def test_pipeline_with_custom_configuration():
    """Tests that the pipeline respects custom configuration options"""
    temp_dir = setup_storage_for_tests()
    target_date = localize_to_cst(datetime(2024, 1, 21, 7))
    mock_clients = setup_mock_clients(target_date)
    custom_config = {
        'products': ['DALMP'],
        'storage': {'format': 'csv'}
    }
    test_config = create_test_config(mock_clients, custom_config)

    result = execute_forecasting_pipeline(target_date, test_config)

    assert result['status'] == 'success'
    assert 'DALMP' in result
    assert 'RTLMP' not in result
    assert check_forecast_availability(target_date, 'DALMP')
    forecast_df = get_forecast(target_date, 'DALMP')
    assert isinstance(forecast_df, pandas.DataFrame)
    assert len(forecast_df) == FORECAST_HORIZON_HOURS
    assert os.path.exists(os.path.join(temp_dir, '2024', '01', '21_DALMP.csv'))