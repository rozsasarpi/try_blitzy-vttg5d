"""
Main entry point for the Electricity Market Price Forecasting System's Dash-based visualization interface.
This file initializes the Dash application, configures its settings, loads the initial forecast data,
sets up the layout, registers callbacks, and starts the server.
"""

import os
import sys
import traceback

import dash  # version 2.9.0+
import dash_bootstrap_components as dbc  # version 1.0.0+
from flask import Flask  # version 2.2.0+

from .config import settings  # src/web/config/settings.py
from .config.logging_config import get_logger  # src/web/config/logging_config.py
from .config.themes import DEFAULT_THEME  # src/web/config/themes.py
from .config.product_config import DEFAULT_PRODUCT  # src/web/config/product_config.py
from .layouts.main_dashboard import create_main_dashboard, get_initial_dashboard_state  # src/web/layouts/main_dashboard.py
from .layouts.error_page import create_error_layout  # src/web/layouts/error_page.py
from .layouts.loading import create_loading_layout  # src/web/layouts/loading.py
from .callbacks import register_all_callbacks  # src/web/callbacks/__init__.py
from .data.forecast_loader import load_latest_forecast  # src/web/data/forecast_loader.py
from .middleware.auth_middleware import AuthMiddleware  # src/web/middleware/auth_middleware.py
from .middleware.error_middleware import ErrorMiddleware  # src/web/middleware/error_middleware.py
from .middleware.logging_middleware import LoggingMiddleware  # src/web/middleware/logging_middleware.py

# Initialize logger
logger = get_logger('app')

# Create Flask server instance
server = Flask(__name__)

# Create Dash application instance
app = dash.Dash(
    __name__,
    server=server,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title='Electricity Market Price Forecasting'
)

def initialize_app() -> dash.Dash:
    """
    Initializes the Dash application with appropriate settings and middleware

    Returns:
        dash.Dash: Configured Dash application
    """
    logger.info("Initializing the Dash application")

    # Configure Dash application with Bootstrap theme
    # Set application title to 'Electricity Market Price Forecasting'
    # Enable callback exceptions suppression for dynamic layouts
    app.config.suppress_callback_exceptions = True

    # Apply middleware if enabled (auth, error handling, logging)
    if settings.ENABLE_AUTH:
        auth_middleware = AuthMiddleware(app)
        auth_middleware.apply()

    error_middleware = ErrorMiddleware()
    error_middleware.apply(app)

    logging_middleware = LoggingMiddleware()
    logging_middleware.apply(app)

    # Configure server settings from environment variables
    app.server.config["SERVER_NAME"] = f"{settings.SERVER_HOST}:{settings.SERVER_PORT}"
    app.server.config["ALLOWED_HOSTS"] = settings.ALLOWED_HOSTS

    return app

def load_initial_data() -> dict:
    """
    Loads the initial forecast data for the dashboard

    Returns:
        dict: Dictionary containing forecast data or error information
    """
    logger.info("Attempting to load initial forecast data")
    try:
        # Try to load the latest forecast for the default product
        forecast_data = load_latest_forecast(product=DEFAULT_PRODUCT)
        return {"data": forecast_data, "success": True}
    except Exception as e:
        # If error occurs, log the error details
        logger.error(f"Error loading initial data: {e}")
        return {"error": str(e), "success": False}

def setup_layout(initial_data: dict) -> None:
    """
    Sets up the application layout based on initial data loading result

    Args:
        initial_data (dict): Initial data loading result

    Returns:
        None: No return value
    """
    # Check if initial data loading was successful
    if initial_data["success"]:
        # If successful, create main dashboard layout with the data
        layout = create_main_dashboard(forecast_data=initial_data["data"])
    else:
        # If failed, create error layout with error information
        layout = create_error_layout(error_message=initial_data["error"])

    # Set the application layout to the created layout
    app.layout = layout
    logger.info("Layout setup completed")

def register_callbacks() -> None:
    """
    Registers all interactive callbacks for the dashboard

    Returns:
        None: No return value
    """
    logger.info("Starting callback registration")
    # Call register_all_callbacks function with the app instance
    register_all_callbacks(app)
    logger.info("Callback registration completed")

def run_server() -> None:
    """
    Starts the Dash server with configured settings

    Returns:
        None: No return value
    """
    logger.info(f"Server startup with host: {settings.SERVER_HOST}, port: {settings.SERVER_PORT}")
    # Start the Dash server with settings from configuration
    app.run_server(
        debug=settings.DEBUG,
        host=settings.SERVER_HOST,
        port=settings.SERVER_PORT
    )

def main() -> int:
    """
    Main entry point function that orchestrates application startup

    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    logger.info("Application startup")

    try:
        # Initialize the Dash application
        initialize_app()

        # Load initial forecast data
        initial_data = load_initial_data()

        # Setup application layout based on data loading result
        setup_layout(initial_data)

        # Register all interactive callbacks
        register_callbacks()

        # Run the server if this file is executed directly
        if __name__ == '__main__':
            run_server()

        return 0  # Successful execution

    except Exception as e:
        # Catch any unexpected exceptions, log them, and return 1
        logger.error(f"Application error: {e}")
        traceback.print_exc()
        return 1

# Run the main function if this file is executed directly
if __name__ == '__main__':
    sys.exit(main())