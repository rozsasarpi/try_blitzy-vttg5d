# flask==2.3.0
from flask import Blueprint, request, jsonify # package_version: 2.3.0

# Internal imports
from .forecast_api import get_forecast_by_date, get_latest_forecast, get_forecasts_by_date_range, get_forecast_as_model, get_latest_forecast_as_model, format_forecast_response, get_storage_status
from .health_check import SystemHealthCheck # Corrected import
from ..utils.logging_utils import get_logger
from ..config.settings import FORECAST_PRODUCTS, API_VERSION

# Initialize logger
logger = get_logger(__name__)

# Create a Flask Blueprint for the API
api_blueprint = Blueprint('api', __name__)

# Initialize health check
health_check = SystemHealthCheck()

@api_blueprint.route('/', methods=['GET'])
def index():
    """
    API root endpoint that returns API information
    
    Returns:
        dict: API information including version and available endpoints
    """
    # Create a dictionary with API version and available endpoints
    api_info = {
        "version": API_VERSION,
        "endpoints": [
            "/health",
            "/health/detailed",
            "/health/component/<component>",
            "/storage/status",
            "/forecasts/<date>/<product>",
            "/forecasts/latest/<product>",
            "/forecasts/range/<start_date>/<end_date>/<product>",
            "/forecasts/model/<date>/<product>",
            "/forecasts/model/latest/<product>",
            "/products"
        ]
    }
    
    # Return the dictionary as a JSON response
    return jsonify(api_info)

@api_blueprint.route('/health', methods=['GET'])
def health():
    """
    Simple health check endpoint
    
    Returns:
        dict: Health status with timestamp
    """
    # Get simple health status from health_check.get_simple_status()
    status = health_check.get_simple_status()
    
    # Return the status as a JSON response
    return jsonify(status)

@api_blueprint.route('/health/detailed', methods=['GET'])
def health_detailed():
    """
    Detailed health check endpoint
    
    Returns:
        dict: Detailed health status of all components
    """
    # Get detailed health status from health_check.check_all()
    detailed_status = health_check.check_all()
    
    # Return the detailed status as a JSON response
    return jsonify(detailed_status)

@api_blueprint.route('/health/component/<component>', methods=['GET'])
def health_component(component):
    """
    Health check for a specific component
    
    Args:
        component (str): Component to check
    
    Returns:
        dict: Health status of the specified component
    """
    # Get component health status from health_check.check_component(component)
    try:
        component_status = health_check.check_component(component)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    
    # Return the component status as a JSON response
    return jsonify(component_status)

@api_blueprint.route('/storage/status', methods=['GET'])
def storage_status():
    """
    Get storage system status
    
    Returns:
        dict: Storage system status and statistics
    """
    # Get storage status from get_storage_status()
    status = get_storage_status()
    
    # Return the storage status as a JSON response
    return jsonify(status)

@api_blueprint.route('/forecasts/<date>/<product>', methods=['GET'])
def get_forecast(date, product):
    """
    Get forecast for a specific date and product
    
    Args:
        date (str): Date in YYYY-MM-DD format
        product (str): Product identifier (e.g., DALMP)
    
    Returns:
        flask.Response: Forecast data in requested format
    """
    # Get format parameter from request args (default to 'json')
    format = request.args.get('format', 'json')
    
    # Log the forecast request
    logger.info(f"Request received for forecast: date={date}, product={product}, format={format}")
    
    # Get forecast data using get_forecast_by_date(date, product, format)
    try:
        forecast_data = get_forecast_by_date(date, product, format)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    # Format the response using format_forecast_response()
    try:
        response = format_forecast_response(forecast_data, format)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    # Return the formatted response
    return jsonify(response)

@api_blueprint.route('/forecasts/latest/<product>', methods=['GET'])
def get_latest_forecast(product):
    """
    Get latest forecast for a product
    
    Args:
        product (str): Product identifier (e.g., DALMP)
    
    Returns:
        flask.Response: Latest forecast data in requested format
    """
    # Get format parameter from request args (default to 'json')
    format = request.args.get('format', 'json')
    
    # Log the latest forecast request
    logger.info(f"Request received for latest forecast: product={product}, format={format}")
    
    # Get latest forecast data using get_latest_forecast(product, format)
    try:
        forecast_data = get_latest_forecast(product, format)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    # Format the response using format_forecast_response()
    try:
        response = format_forecast_response(forecast_data, format)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    # Return the formatted response
    return jsonify(response)

@api_blueprint.route('/forecasts/range/<start_date>/<end_date>/<product>', methods=['GET'])
def get_forecasts_range(start_date, end_date, product):
    """
    Get forecasts for a date range and product
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        product (str): Product identifier (e.g., DALMP)
    
    Returns:
        flask.Response: Forecast data for the date range in requested format
    """
    # Get format parameter from request args (default to 'json')
    format = request.args.get('format', 'json')
    
    # Log the forecast range request
    logger.info(f"Request received for forecast range: start_date={start_date}, end_date={end_date}, product={product}, format={format}")
    
    # Get forecast data using get_forecasts_by_date_range(start_date, end_date, product, format)
    try:
        forecast_data = get_forecasts_by_date_range(start_date, end_date, product, format)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    # Format the response using format_forecast_response()
    try:
        response = format_forecast_response(forecast_data, format)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    # Return the formatted response
    return jsonify(response)

@api_blueprint.route('/forecasts/model/<date>/<product>', methods=['GET'])
def get_forecast_model(date, product):
    """
    Get forecast as model objects for a specific date and product
    
    Args:
        date (str): Date in YYYY-MM-DD format
        product (str): Product identifier (e.g., DALMP)
    
    Returns:
        dict: Forecast data as model objects
    """
    # Log the forecast model request
    logger.info(f"Request received for forecast model: date={date}, product={product}")
    
    # Get forecast model data using get_forecast_as_model(date, product)
    try:
        forecast_models = get_forecast_as_model(date, product)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    # Convert model objects to dictionary for JSON serialization
    forecast_list = [model.to_dict() for model in forecast_models]
    
    # Return the dictionary as a JSON response
    return jsonify(forecast_list)

@api_blueprint.route('/forecasts/model/latest/<product>', methods=['GET'])
def get_latest_forecast_model(product):
    """
    Get latest forecast as model objects for a product
    
    Args:
        product (str): Product identifier (e.g., DALMP)
    
    Returns:
        dict: Latest forecast data as model objects
    """
    # Log the latest forecast model request
    logger.info(f"Request received for latest forecast model: product={product}")
    
    # Get latest forecast model data using get_latest_forecast_as_model(product)
    try:
        forecast_models = get_latest_forecast_as_model(product)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    # Convert model objects to dictionary for JSON serialization
    forecast_list = [model.to_dict() for model in forecast_models]
    
    # Return the dictionary as a JSON response
    return jsonify(forecast_list)

@api_blueprint.route('/products', methods=['GET'])
def get_products():
    """
    Get list of available forecast products
    
    Returns:
        dict: List of available forecast products
    """
    # Create a dictionary with the list of available products from FORECAST_PRODUCTS
    products = {"products": FORECAST_PRODUCTS}
    
    # Return the dictionary as a JSON response
    return jsonify(products)