"""
Module that implements error handling callbacks for the Electricity Market Price Forecasting System's Dash-based visualization interface.
This module registers callbacks that respond to errors during data loading, visualization rendering, and user interactions,
providing appropriate error messages and fallback indicators to ensure a robust user experience.
"""

# Import necessary modules
import dash  # version 2.9.0+
from dash.dependencies import Input, Output, State  # version 2.9.0+
import dash_html_components as html  # version 2.0.0+
import dash_core_components as dcc  # version 2.0.0+
from dash.exceptions import PreventUpdate  # version 2.9.0+
from dash import no_update  # version 2.9.0+

# Internal imports
from ..config.logging_config import get_logger  # src/web/config/logging_config.py
from ..utils.error_handlers import handle_callback_error, create_error_message, handle_data_loading_error, \
    handle_visualization_error, is_fallback_data, create_fallback_notice  # src/web/utils/error_handlers.py
from ..layouts.error_page import create_error_layout, ERROR_PAGE_ID, RETRY_BUTTON_ID  # src/web/layouts/error_page.py
from ..components.fallback_indicator import create_fallback_indicator, FALLBACK_INDICATOR_ID  # src/web/components/fallback_indicator.py
from ..layouts.main_dashboard import create_main_dashboard  # src/web/layouts/main_dashboard.py
from ..data.forecast_loader import load_forecast_data  # src/web/data/forecast_loader.py

# Initialize logger
logger = get_logger('error_callbacks')

# Define global constants
DEFAULT_ERROR_MESSAGE = "An unexpected error occurred while loading the dashboard."
DEFAULT_ERROR_TYPE = "unknown"
PAGE_CONTENT_ID = "page-content"


def register_error_callbacks(app: dash.Dash):
    """
    Registers all error-related callbacks with the Dash application

    Args:
        app (dash.Dash): The Dash application instance

    Returns:
        None: No return value
    """
    logger.info("Registering error callbacks...")

    # Register callback for handling retry button clicks
    @app.callback(
        Output(PAGE_CONTENT_ID, 'children'),
        Input(RETRY_BUTTON_ID, 'n_clicks'),
        prevent_initial_call=True
    )
    def handle_retry_click(n_clicks: int):
        """
        Callback function that handles retry button clicks on the error page

        Args:
            n_clicks (int): Number of times the retry button has been clicked

        Returns:
            dash_html_components.Div: Updated content after retry attempt
        """
        if n_clicks is None or n_clicks == 0:
            raise PreventUpdate

        logger.info("Retry button clicked. Attempting to reload forecast data...")
        try:
            # Attempt to load forecast data
            forecast_data = load_forecast_data()
            # If successful, create main dashboard with loaded data
            main_dashboard = create_main_dashboard(forecast_data)
            return main_dashboard
        except Exception as e:
            # If error occurs, handle the error and return error page
            error_info = create_error_store_update(e, "handle_retry_click")
            error_page = create_error_layout(
                error_message=error_info["message"],
                error_type=error_info["type"],
                error_details=error_info.get("details"),
            )
            return error_page

    # Register callback for displaying fallback indicators
    @app.callback(
        Output(FALLBACK_INDICATOR_ID, 'children'),
        Input('forecast-data-store', 'data'),
        State('theme-store', 'data')
    )
    def update_fallback_indicator(forecast_data: dict, theme: str):
        """
        Callback function that updates the fallback indicator based on forecast data

        Args:
            forecast_data (dict): Forecast data dictionary
            theme (str): The current theme

        Returns:
            dash_html_components.Div: Fallback indicator component or empty div
        """
        if forecast_data is None:
            return html.Div()

        if theme is None:
            theme = "light"

        if is_fallback_data(forecast_data):
            fallback_notice = create_fallback_notice()
            return fallback_notice
        else:
            return html.Div()

    # Register callback for handling global errors
    @app.callback(
        Output(PAGE_CONTENT_ID, 'children'),
        Input('error-store', 'data'),
        prevent_initial_call=True
    )
    def handle_global_error(error_info: dict):
        """
        Callback function that handles global errors in the application

        Args:
            error_info (dict): Dictionary containing error information

        Returns:
            dash_html_components.Div: Error page layout
        """
        if error_info is None:
            raise PreventUpdate

        logger.info("Handling global error...")
        error_message = error_info.get("message", DEFAULT_ERROR_MESSAGE)
        error_type = error_info.get("type", DEFAULT_ERROR_TYPE)
        error_details = error_info.get("details")

        error_page = create_error_layout(
            error_message=error_message,
            error_type=error_type,
            error_details=error_details,
        )
        return error_page

    logger.info("Error callback registration complete.")


def create_error_store_update(error: Exception, context: str) -> dict:
    """
    Creates an update for the error store with error information

    Args:
        error (Exception): The exception that occurred
        context (str): Context in which the error occurred

    Returns:
        dict: Error information dictionary
    """
    error_info = handle_callback_error(error, context)
    return error_info