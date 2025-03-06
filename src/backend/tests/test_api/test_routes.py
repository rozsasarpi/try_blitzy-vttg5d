# pytest==7.0.0+
import pytest
import unittest.mock
from unittest.mock import patch
import flask # flask==2.3.0
import json # standard library
import datetime # standard library
import pandas # pandas==2.0.0+

# Internal imports
from src.backend.api.routes import api_blueprint # Flask Blueprint containing the API routes to test
from src.backend.main import create_app # Function to create a Flask application for testing
from src.backend.config.settings import FORECAST_PRODUCTS, API_VERSION # List of valid forecast products for testing
from src.backend.api.forecast_api import get_forecast_by_date, get_latest_forecast, get_forecasts_by_date_range, get_forecast_as_model, get_latest_forecast_as_model, get_storage_status # Function to mock for testing forecast retrieval
from src.backend.api.health_check import SystemHealthCheck # Class to mock for testing health check endpoints
from src.backend.api.exceptions import ResourceNotFoundError, InvalidFormatError # Exception to use in mocked functions for testing error handling
from src.backend.tests.fixtures.model_fixtures import create_test_forecast_dataframe # Create test forecast data for API response mocking
from src.backend.tests.fixtures.forecast_fixtures import create_mock_probabilistic_forecast # Create mock forecast objects for API response mocking

def create_test_app() -> flask.Flask:
    """Creates a Flask test application with the API blueprint registered"""
    app = flask.Flask(__name__)
    app.config['TESTING'] = True
    app.register_blueprint(api_blueprint)
    return app

class TestAPIRoutes:
    """Test class for API routes in the Electricity Market Price Forecasting System"""

    def setup_method(self, method):
        """Set up test environment before each test"""
        self.app = create_test_app()
        self.client = self.app.test_client()
        self.mock_get_forecast_by_date = patch('src.backend.api.routes.get_forecast_by_date').start()
        self.mock_get_latest_forecast = patch('src.backend.api.routes.get_latest_forecast').start()
        self.mock_get_forecasts_range = patch('src.backend.api.routes.get_forecasts_by_date_range').start()
        self.mock_get_forecast_model = patch('src.backend.api.routes.get_forecast_as_model').start()
        self.mock_get_latest_forecast_model = patch('src.backend.api.routes.get_latest_forecast_as_model').start()
        self.mock_get_storage_status = patch('src.backend.api.routes.get_storage_status').start()
        self.mock_health_check = patch('src.backend.api.routes.SystemHealthCheck').start()

    def teardown_method(self, method):
        """Clean up after each test"""
        patch.stopall()

    def test_index_endpoint(self):
        """Test the API root endpoint returns correct information"""
        response = self.client.get('/')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'version' in data
        assert data['version'] == API_VERSION
        assert 'endpoints' in data
        assert isinstance(data['endpoints'], list)

    def test_health_endpoint(self):
        """Test the simple health check endpoint"""
        self.mock_health_check.return_value.get_simple_status.return_value = {'status': 'ok', 'timestamp': '2023-10-26T12:00:00'}
        response = self.client.get('/health')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'status' in data
        assert data['status'] == 'ok'
        assert 'timestamp' in data

    def test_health_detailed_endpoint(self):
        """Test the detailed health check endpoint"""
        self.mock_health_check.return_value.check_all.return_value = {'component1': 'ok', 'component2': 'degraded'}
        response = self.client.get('/health/detailed')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'component1' in data
        assert data['component1'] == 'ok'
        assert 'component2' in data
        assert data['component2'] == 'degraded'

    def test_health_component_endpoint(self):
        """Test the component-specific health check endpoint"""
        self.mock_health_check.return_value.check_component.return_value = {'status': 'healthy', 'details': 'all good'}
        response = self.client.get('/health/component/storage')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'status' in data
        assert data['status'] == 'healthy'
        assert 'details' in data
        assert data['details'] == 'all good'

    def test_storage_status_endpoint(self):
        """Test the storage status endpoint"""
        self.mock_get_storage_status.return_value = {'status': 'ok', 'space_used': '500MB'}
        response = self.client.get('/storage/status')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'status' in data
        assert data['status'] == 'ok'
        assert 'space_used' in data
        assert data['space_used'] == '500MB'

    def test_get_forecast_endpoint(self):
        """Test the endpoint for retrieving a forecast by date and product"""
        test_data = create_test_forecast_dataframe()
        self.mock_get_forecast_by_date.return_value = test_data.to_dict(orient='records')
        response = self.client.get('/forecasts/2023-06-01/DALMP')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == len(test_data)

    def test_get_forecast_with_format(self):
        """Test the forecast endpoint with different format parameters"""
        test_data = create_test_forecast_dataframe()
        self.mock_get_forecast_by_date.return_value = test_data
        
        response_json = self.client.get('/forecasts/2023-06-01/DALMP?format=json')
        assert response_json.status_code == 200
        assert response_json.content_type == 'application/json'
        
        response_csv = self.client.get('/forecasts/2023-06-01/DALMP?format=csv')
        assert response_csv.status_code == 200
        assert response_csv.content_type == 'text/csv; charset=utf-8'
        
        response_excel = self.client.get('/forecasts/2023-06-01/DALMP?format=excel')
        assert response_excel.status_code == 200
        assert response_excel.content_type == 'application/vnd.ms-excel'

    def test_get_forecast_not_found(self):
        """Test the forecast endpoint when forecast is not found"""
        self.mock_get_forecast_by_date.side_effect = ResourceNotFoundError("Forecast not found", "forecast", "DALMP_2023-06-01")
        response = self.client.get('/forecasts/2023-06-01/DALMP')
        assert response.status_code == 404
        data = json.loads(response.data)
        assert 'error' in data
        assert "Forecast not found" in data['error']

    def test_get_latest_forecast_endpoint(self):
        """Test the endpoint for retrieving the latest forecast for a product"""
        test_data = create_test_forecast_dataframe()
        self.mock_get_latest_forecast.return_value = test_data.to_dict(orient='records')
        response = self.client.get('/forecasts/latest/DALMP')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == len(test_data)

    def test_get_forecasts_range_endpoint(self):
        """Test the endpoint for retrieving forecasts within a date range"""
        test_data = create_test_forecast_dataframe()
        self.mock_get_forecasts_by_date_range.return_value = test_data.to_dict(orient='records')
        response = self.client.get('/forecasts/range/2023-06-01/2023-06-03/DALMP')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == len(test_data)

    def test_get_forecast_model_endpoint(self):
        """Test the endpoint for retrieving a forecast as model objects"""
        mock_forecast = create_mock_probabilistic_forecast()
        self.mock_get_forecast_as_model.return_value = [mock_forecast]
        response = self.client.get('/forecasts/model/2023-06-01/DALMP')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 1
        assert 'timestamp' in data[0]
        assert 'product' in data[0]

    def test_get_latest_forecast_model_endpoint(self):
        """Test the endpoint for retrieving the latest forecast as model objects"""
        mock_forecast = create_mock_probabilistic_forecast()
        self.mock_get_latest_forecast_as_model.return_value = [mock_forecast]
        response = self.client.get('/forecasts/model/latest/DALMP')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 1
        assert 'timestamp' in data[0]
        assert 'product' in data[0]

    def test_get_products_endpoint(self):
        """Test the endpoint for retrieving available forecast products"""
        response = self.client.get('/products')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'products' in data
        assert isinstance(data['products'], list)
        assert data['products'] == FORECAST_PRODUCTS

    def test_invalid_format_error(self):
        """Test error handling for invalid format parameter"""
        self.mock_get_forecast_by_date.side_effect = InvalidFormatError("Invalid format", "invalid", ['json', 'csv', 'excel', 'parquet'])
        response = self.client.get('/forecasts/2023-06-01/DALMP?format=invalid')
        assert response.status_code == 400
        data = json.loads(response.data)
        assert 'error' in data
        assert "Invalid format" in data['error']