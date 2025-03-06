"""
Component module that implements the export panel for the Electricity Market Price Forecasting System's
Dash-based visualization interface. This panel allows users to export forecast data in various formats 
(CSV, Excel, JSON) and provides controls for selecting export options.
"""

# External library imports
import dash_bootstrap_components as dbc  # version 1.0.0+
import dash_html_components as html  # version 2.9.0+
import dash_core_components as dcc  # version 2.9.0+
import datetime  # standard library
import logging  # standard library
from typing import List  # standard library

# Internal imports
from ..config.dashboard_config import get_export_config
from ..data.data_exporter import EXPORT_FORMATS
from ..utils.formatting import format_date

# Configure logger
logger = logging.getLogger(__name__)

# Component IDs for callbacks and styling
EXPORT_PANEL_ID = "export-panel"
EXPORT_FORMAT_DROPDOWN_ID = "export-format-dropdown"
EXPORT_BUTTON_ID = "export-button"
EXPORT_DOWNLOAD_ID = "export-download"
PERCENTILE_LOWER_INPUT_ID = "percentile-lower-input"
PERCENTILE_UPPER_INPUT_ID = "percentile-upper-input"

# Default percentile values
DEFAULT_PERCENTILE_LOWER = 10
DEFAULT_PERCENTILE_UPPER = 90


def create_export_panel(viewport_size: str) -> dbc.Card:
    """
    Creates the export panel component with format selection and export button.
    
    Args:
        viewport_size: The current viewport size category ('desktop', 'tablet', 'mobile')
        
    Returns:
        Export panel component
    """
    # Get export configuration
    export_config = get_export_config()
    
    # Create export format dropdown
    format_dropdown = create_format_dropdown(
        formats=export_config['formats'],
        default_format=export_config['default_format']
    )
    
    # Create percentile range inputs
    percentile_inputs = create_percentile_inputs()
    
    # Create export button
    export_button = create_export_button()
    
    # Create hidden download component
    download_component = create_download_component()
    
    # Create the panel
    panel = dbc.Card(
        [
            dbc.CardHeader("Export Forecast Data"),
            dbc.CardBody(
                [
                    html.P("Download the current forecast data in your preferred format."),
                    format_dropdown,
                    percentile_inputs,
                    html.Div(
                        [export_button],
                        className="mt-3"
                    ),
                    download_component
                ]
            )
        ],
        id=EXPORT_PANEL_ID,
        className="mb-4 shadow-sm"
    )
    
    logger.debug(f"Created export panel with viewport size: {viewport_size}")
    return panel


def create_format_dropdown(formats: List[str], default_format: str) -> dbc.FormGroup:
    """
    Creates a dropdown for selecting the export format.
    
    Args:
        formats: List of available export formats
        default_format: Default export format
        
    Returns:
        Format selection form group
    """
    # Create options for the dropdown
    options = get_export_formats_options(formats)
    
    # Create the dropdown
    dropdown = dbc.FormGroup(
        [
            dbc.Label("Export Format", html_for=EXPORT_FORMAT_DROPDOWN_ID),
            dcc.Dropdown(
                id=EXPORT_FORMAT_DROPDOWN_ID,
                options=options,
                value=default_format,
                clearable=False,
                className="mb-3"
            )
        ]
    )
    
    return dropdown


def create_percentile_inputs() -> dbc.FormGroup:
    """
    Creates input fields for specifying percentile ranges for export.
    
    Returns:
        Percentile inputs form group
    """
    # Create the form group for percentile inputs
    form_group = dbc.FormGroup(
        [
            dbc.Label("Percentile Range"),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Input(
                            id=PERCENTILE_LOWER_INPUT_ID,
                            type="number",
                            min=0,
                            max=100,
                            step=1,
                            value=DEFAULT_PERCENTILE_LOWER,
                            placeholder="Lower"
                        ),
                        width=6,
                    ),
                    dbc.Col(
                        dbc.Input(
                            id=PERCENTILE_UPPER_INPUT_ID,
                            type="number",
                            min=0,
                            max=100,
                            step=1,
                            value=DEFAULT_PERCENTILE_UPPER,
                            placeholder="Upper"
                        ),
                        width=6,
                    ),
                ],
                className="mb-2"
            ),
            dbc.FormText(
                "Specify the percentile range to include in the export (0-100)."
            ),
        ]
    )
    
    return form_group


def create_export_button() -> dbc.Button:
    """
    Creates a button that triggers the export process.
    
    Returns:
        Export button component
    """
    # Create the export button
    button = dbc.Button(
        "Export Data",
        id=EXPORT_BUTTON_ID,
        color="primary",
        block=True,
        className="mt-2"
    )
    
    return button


def create_download_component() -> dcc.Download:
    """
    Creates a hidden download component for handling file downloads.
    
    Returns:
        Download component
    """
    # Create the download component
    download = dcc.Download(id=EXPORT_DOWNLOAD_ID)
    
    return download


def get_export_formats_options(formats: List[str]) -> List[dict]:
    """
    Generates options for the export format dropdown.
    
    Args:
        formats: List of available export formats
        
    Returns:
        List of format options for dropdown
    """
    # Create options list for the dropdown
    options = []
    
    for format_name in formats:
        if format_name in EXPORT_FORMATS:
            # Create a user-friendly label (capitalize first letter)
            label = format_name.upper()
            
            # Add option to the list
            options.append({
                'label': label,
                'value': format_name
            })
    
    return options