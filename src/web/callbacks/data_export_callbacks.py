"""
Module that implements callback functions for the data export functionality in the Electricity Market Price
Forecasting System's Dash-based visualization interface. These callbacks handle user interactions with the 
export panel, allowing users to download forecast data in various formats (CSV, Excel, JSON) with customizable options.
"""

# External imports
from dash.dependencies import Input, Output, State
from dash import no_update
import typing

# Internal imports
from ..components.export_panel import (
    EXPORT_FORMAT_DROPDOWN_ID,
    EXPORT_BUTTON_ID,
    EXPORT_DOWNLOAD_ID,
    PERCENTILE_LOWER_INPUT_ID,
    PERCENTILE_UPPER_INPUT_ID
)
from ..components.control_panel import (
    PRODUCT_DROPDOWN_ID,
    DATE_RANGE_PICKER_ID
)
from ..data.data_exporter import (
    export_forecast_by_date_range,
    EXPORT_FORMATS,
    DEFAULT_EXPORT_FORMAT
)
from ..utils.date_helpers import parse_date, get_default_date_range
from ..config.product_config import DEFAULT_PRODUCT
from ..config.logging_config import get_logger

# Set up logger
logger = get_logger('data_export_callbacks')


def register_data_export_callbacks(app):
    """
    Registers all callback functions for the data export functionality with the Dash application
    
    Args:
        app: The Dash application instance
        
    Returns:
        None: No return value
    """
    logger.info("Registering data export callbacks")
    
    # Register callback for export button
    app.callback(
        Output(EXPORT_DOWNLOAD_ID, "data"),
        Input(EXPORT_BUTTON_ID, "n_clicks"),
        State(EXPORT_FORMAT_DROPDOWN_ID, "value"),
        State(PRODUCT_DROPDOWN_ID, "value"),
        State(DATE_RANGE_PICKER_ID, "start_date"),
        State(DATE_RANGE_PICKER_ID, "end_date"),
        State(PERCENTILE_LOWER_INPUT_ID, "value"),
        State(PERCENTILE_UPPER_INPUT_ID, "value")
    )(handle_export_button_click)
    
    logger.info("Data export callbacks registration complete")


def handle_export_button_click(
    n_clicks: int,
    export_format: str,
    selected_product: str,
    start_date: str,
    end_date: str,
    percentile_lower: str,
    percentile_upper: str
) -> dict:
    """
    Callback function that handles export button clicks and generates downloadable forecast data
    
    Args:
        n_clicks: Number of times the button has been clicked
        export_format: Selected export format (csv, excel, json)
        selected_product: Selected product to export
        start_date: Start date in string format
        end_date: End date in string format
        percentile_lower: Lower percentile value (0-100)
        percentile_upper: Upper percentile value (0-100)
        
    Returns:
        Dictionary with data for download component
    """
    # Only process if button was clicked
    if not n_clicks or n_clicks <= 0:
        return no_update
    
    try:
        logger.info(f"Export button clicked: format={export_format}, product={selected_product}")
        
        # Use default product if none selected
        if not selected_product:
            selected_product = DEFAULT_PRODUCT
        
        # Parse dates
        parsed_start_date = parse_date(start_date) if start_date else None
        parsed_end_date = parse_date(end_date) if end_date else None
        
        # If dates are invalid, use default range
        if parsed_start_date is None or parsed_end_date is None:
            parsed_start_date, parsed_end_date = get_default_date_range()
            logger.warning(f"Using default date range: {parsed_start_date} to {parsed_end_date}")
        
        # Parse percentile values
        lower_percentile = parse_percentile_value(percentile_lower, 10)
        upper_percentile = parse_percentile_value(percentile_upper, 90)
        
        # Create percentiles list
        percentiles = [lower_percentile, upper_percentile]
        
        # Generate export data
        export_data = export_forecast_by_date_range(
            product=selected_product,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
            export_format=export_format,
            percentiles=percentiles
        )
        
        logger.info(f"Export successful: {export_data['filename']}")
        return export_data
        
    except Exception as e:
        logger.error(f"Error during export: {str(e)}")
        # Return an error message to the user
        return {
            "content": "",
            "filename": "error.txt",
            "mime_type": "text/plain",
            "error": f"Export failed: {str(e)}"
        }


def parse_percentile_value(value: str, default_value: int) -> int:
    """
    Parses and validates a percentile input value
    
    Args:
        value: Percentile value as string
        default_value: Default value to use if parsing fails
        
    Returns:
        Parsed and validated percentile value
    """
    try:
        # Convert to integer
        percentile = int(value)
        
        # Ensure it's within valid range (0-100)
        if percentile < 0:
            return 0
        elif percentile > 100:
            return 100
        
        return percentile
    except (ValueError, TypeError):
        # Return default if value can't be parsed
        return default_value