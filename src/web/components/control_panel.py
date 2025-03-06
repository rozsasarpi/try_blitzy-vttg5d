"""
Component module that implements the control panel for the Electricity Market Price Forecasting 
System's Dash-based visualization interface. This control panel provides user interface elements 
for selecting products, date ranges, and visualization options, enabling users to customize 
their view of forecast data.
"""

from typing import Dict, Optional, List

# dash v2.9.0+
import dash
from dash import html, dcc
# dash_bootstrap_components v1.0.0+
import dash_bootstrap_components as dbc
import datetime

from ..config.dashboard_config import get_control_panel_config
from ..config.product_config import (
    PRODUCTS, 
    DEFAULT_PRODUCT, 
    get_product_dropdown_options
)
from ..utils.date_helpers import (
    get_default_date_range, 
    date_to_dash_format, 
    format_datetime, 
    get_current_time_cst
)
from ..config.settings import MAX_FORECAST_DAYS
from ..data.forecast_loader import forecast_loader
from .fallback_indicator import create_fallback_badge

# Constants for component IDs
CONTROL_PANEL_ID = 'control-panel'
PRODUCT_DROPDOWN_ID = 'product-dropdown'
DATE_RANGE_PICKER_ID = 'date-range-picker'
VISUALIZATION_OPTIONS_ID = 'visualization-options'
REFRESH_BUTTON_ID = 'refresh-button'
LAST_UPDATED_ID = 'last-updated-info'
FORECAST_STATUS_ID = 'forecast-status'


def create_control_panel(forecast_data: Dict, theme: str, viewport_size: str) -> dbc.Card:
    """
    Creates the control panel component with all interactive elements.
    
    Args:
        forecast_data: Dictionary containing forecast data
        theme: Current UI theme for styling
        viewport_size: Current viewport size category (desktop, tablet, mobile)
        
    Returns:
        Control panel component with all controls
    """
    # Get configuration for control panel elements
    config = get_control_panel_config()
    
    # List to collect control panel elements
    panel_elements = []
    
    # Add title
    panel_elements.append(html.H4("Controls", className="mb-3"))
    
    # Add product dropdown if enabled
    if config['show_product_dropdown']:
        selected_product = forecast_data.get('product', None) if forecast_data else None
        panel_elements.append(create_product_dropdown(selected_product))
    
    # Add date range picker if enabled
    if config['show_date_range']:
        start_date = None
        end_date = None
        if forecast_data and 'timestamp' in forecast_data:
            # Extract dates from forecast data if available
            timestamps = forecast_data['timestamp']
            start_date = timestamps.min().date() if hasattr(timestamps, 'min') else None
            end_date = timestamps.max().date() if hasattr(timestamps, 'max') else None
        
        panel_elements.append(create_date_range_picker(start_date, end_date))
    
    # Add visualization options if enabled
    if config['show_visualization_options']:
        selected_options = None  # This would typically come from app state
        panel_elements.append(create_visualization_options(selected_options))
    
    # Add refresh button if enabled
    if config['show_refresh_button']:
        panel_elements.append(create_refresh_button())
    
    # Add last updated info if enabled
    if config['show_last_updated']:
        panel_elements.append(create_last_updated_info(forecast_data))
    
    # Add forecast status if enabled
    if config['show_forecast_status']:
        panel_elements.append(create_forecast_status(forecast_data, theme))
    
    # Create the complete control panel
    control_panel = dbc.Card(
        dbc.CardBody(panel_elements),
        className="mb-4",
        id=CONTROL_PANEL_ID
    )
    
    return control_panel


def create_product_dropdown(selected_product: Optional[str] = None) -> dbc.FormGroup:
    """
    Creates a dropdown component for selecting electricity market products.
    
    Args:
        selected_product: Currently selected product
        
    Returns:
        Product dropdown form group
    """
    # Use default product if none selected
    if selected_product is None:
        selected_product = DEFAULT_PRODUCT
    
    # Get options for the dropdown
    options = get_product_dropdown_options()
    
    # Create the form group with dropdown
    dropdown_group = dbc.FormGroup([
        dbc.Label("Product Selection:", html_for=PRODUCT_DROPDOWN_ID),
        dcc.Dropdown(
            id=PRODUCT_DROPDOWN_ID,
            options=options,
            value=selected_product,
            clearable=False,
            searchable=True,
            className="mb-3"
        )
    ])
    
    return dropdown_group


def create_date_range_picker(start_date: Optional[datetime.date] = None, 
                             end_date: Optional[datetime.date] = None) -> dbc.FormGroup:
    """
    Creates a date range picker component for selecting forecast timeframe.
    
    Args:
        start_date: Start date for the range
        end_date: End date for the range
        
    Returns:
        Date range picker form group
    """
    # Use default date range if not provided
    if start_date is None or end_date is None:
        start_date, end_date = get_default_date_range()
    
    # Convert dates to format expected by Dash
    start_date_str = date_to_dash_format(start_date)
    end_date_str = date_to_dash_format(end_date)
    
    # Calculate min/max selectable dates
    current_date = datetime.date.today()
    min_date = date_to_dash_format(current_date - datetime.timedelta(days=30))  # Allow looking back 30 days
    max_date = date_to_dash_format(current_date + datetime.timedelta(days=MAX_FORECAST_DAYS))
    
    # Create the form group with date picker
    date_picker_group = dbc.FormGroup([
        dbc.Label("Timeframe:", html_for=DATE_RANGE_PICKER_ID),
        dcc.DatePickerRange(
            id=DATE_RANGE_PICKER_ID,
            start_date=start_date_str,
            end_date=end_date_str,
            min_date_allowed=min_date,
            max_date_allowed=max_date,
            initial_visible_month=start_date_str,
            display_format='YYYY-MM-DD',
            first_day_of_week=0,  # Sunday
            className="mb-3"
        )
    ])
    
    return date_picker_group


def create_visualization_options(selected_options: Optional[List[str]] = None) -> dbc.FormGroup:
    """
    Creates checkboxes for toggling visualization elements.
    
    Args:
        selected_options: List of currently selected options
        
    Returns:
        Visualization options form group
    """
    # Default options if none provided
    if selected_options is None:
        selected_options = ['point_forecast', 'uncertainty']
    
    # Define available options
    options = [
        {'label': 'Show point forecast', 'value': 'point_forecast'},
        {'label': 'Show uncertainty', 'value': 'uncertainty'},
        {'label': 'Show historical', 'value': 'historical'}
    ]
    
    # Create the form group with checkboxes
    options_group = dbc.FormGroup([
        dbc.Label("Visualization Options:", html_for=VISUALIZATION_OPTIONS_ID),
        dcc.Checklist(
            id=VISUALIZATION_OPTIONS_ID,
            options=options,
            value=selected_options,
            className="mb-3",
            labelStyle={'display': 'block', 'margin-bottom': '0.5rem'}
        )
    ])
    
    return options_group


def create_refresh_button() -> dbc.Button:
    """
    Creates a button for manually refreshing forecast data.
    
    Returns:
        Refresh button component
    """
    refresh_button = dbc.Button(
        [html.I(className="fas fa-sync-alt me-2"), "Refresh Data"],
        id=REFRESH_BUTTON_ID,
        color="primary",
        className="mb-3",
        n_clicks=0
    )
    
    return refresh_button


def create_last_updated_info(forecast_data: Dict) -> dbc.FormGroup:
    """
    Creates an information display showing when forecast was last updated.
    
    Args:
        forecast_data: Dictionary containing forecast data
        
    Returns:
        Last updated info component
    """
    # Get generation time from forecast data or use current time as fallback
    generation_time = get_forecast_generation_time(forecast_data)
    
    # Format the time for display
    formatted_time = format_datetime(generation_time)
    
    # Create the form group with info text
    last_updated_group = dbc.FormGroup([
        dbc.Label("Last updated:", html_for=LAST_UPDATED_ID),
        html.Div(
            formatted_time,
            id=LAST_UPDATED_ID,
            className="text-muted mb-3"
        )
    ])
    
    return last_updated_group


def create_forecast_status(forecast_data: Dict, theme: str) -> dbc.FormGroup:
    """
    Creates a status indicator showing if forecast is normal or using fallback data.
    
    Args:
        forecast_data: Dictionary containing forecast data
        theme: Current UI theme for styling
        
    Returns:
        Forecast status component
    """
    # Create label for status
    status_label = dbc.Label("Forecast Status:", html_for=FORECAST_STATUS_ID)
    
    # Determine status content based on forecast data
    if forecast_data is None:
        # No data yet
        status_content = dbc.Badge("Loading...", color="secondary", className="ms-2")
    else:
        # Check if using fallback data
        metadata = forecast_loader.get_forecast_metadata(forecast_data)
        is_fallback = metadata.get('is_fallback', False)
        
        if is_fallback:
            # Using fallback data
            status_content = create_fallback_badge(forecast_data, theme)
        else:
            # Normal forecast
            status_content = dbc.Badge("Normal", color="success", className="ms-2")
    
    # Create the form group with status
    status_group = dbc.FormGroup([
        status_label,
        html.Div(
            status_content,
            id=FORECAST_STATUS_ID,
            className="mb-3"
        )
    ])
    
    return status_group


def get_forecast_generation_time(forecast_data: Dict) -> datetime.datetime:
    """
    Extracts the generation timestamp from forecast data.
    
    Args:
        forecast_data: Dictionary containing forecast data
        
    Returns:
        Timestamp when forecast was generated
    """
    # Use current time as fallback if no data
    if forecast_data is None:
        return get_current_time_cst()
    
    # Try to get generation timestamp from metadata
    try:
        metadata = forecast_loader.get_forecast_metadata(forecast_data)
        generation_time = metadata.get('generation_timestamp')
        
        # If timestamp is available, return it
        if generation_time is not None:
            return generation_time
    except:
        # If any error occurs, return current time
        pass
    
    # Return current time as fallback
    return get_current_time_cst()