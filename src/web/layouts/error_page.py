"""
Layout module that creates the error page for the Electricity Market Price Forecasting System's 
Dash-based visualization interface. This module provides a user-friendly error display with 
appropriate messaging, styling, and recovery options when errors occur during data loading, 
processing, or visualization.
"""

import dash_bootstrap_components as dbc  # v1.0.0+
from dash import html, dcc  # v2.9.0+

# Internal imports
from .responsive import create_responsive_container
from ..components.header import create_header
from ..components.footer import create_footer
from ..config.themes import get_theme_colors, DEFAULT_THEME, get_status_color
from ..config.logging_config import get_logger
from ..utils.error_handlers import ERROR_TYPES

# Set up logger
logger = get_logger('error_page')

# Element IDs for styling and callbacks
ERROR_PAGE_ID = 'error-page'
ERROR_CONTAINER_ID = 'error-container'
ERROR_TITLE_ID = 'error-title'
ERROR_MESSAGE_ID = 'error-message'
ERROR_DETAILS_ID = 'error-details'
RETRY_BUTTON_ID = 'retry-button'

# Default values
DEFAULT_ERROR_TYPE = 'unknown'
DEFAULT_ERROR_MESSAGE = 'An unexpected error occurred.'


def create_error_layout(error_message=None, error_type=None, error_details=None, theme=None):
    """
    Creates the error page layout with appropriate error information and styling.
    
    Args:
        error_message (str): The main error message to display
        error_type (str): Type of error (must be key in ERROR_TYPES)
        error_details (str): Additional technical details about the error
        theme (str): The theme name for styling
    
    Returns:
        dash_bootstrap_components.Container: Container with error page layout
    """
    logger.info(f"Creating error page layout for {error_type}: {error_message}")
    
    # Set defaults if not provided
    if error_message is None:
        error_message = DEFAULT_ERROR_MESSAGE
    
    if error_type is None:
        error_type = DEFAULT_ERROR_TYPE
    
    if theme is None:
        theme = DEFAULT_THEME
    
    # Get theme colors for styling
    theme_colors = get_theme_colors(theme)
    
    # Create header
    header = create_header(theme)
    
    # Create main error container
    error_container = create_error_container(error_message, error_type, error_details, theme_colors)
    
    # Create footer
    footer = create_footer(theme)
    
    # Combine all elements in a responsive container
    layout = create_responsive_container(
        children=[
            header,
            dbc.Container(
                error_container,
                fluid=True,
                className="mb-5 py-5",
                style={"min-height": "60vh"}
            ),
            footer
        ],
        id=ERROR_PAGE_ID,
        className="error-page",
        viewport_size="desktop",
        style={"background-color": theme_colors["background"]}
    )
    
    return layout


def create_error_container(error_message, error_type, error_details, theme_colors):
    """
    Creates the main container for displaying error information.
    
    Args:
        error_message (str): The main error message to display
        error_type (str): Type of error 
        error_details (str): Additional technical details about the error
        theme_colors (dict): Dictionary of theme colors for styling
        
    Returns:
        dash_bootstrap_components.Card: Card component containing error information
    """
    # Get appropriate error title
    error_title = get_error_title(error_type)
    
    # Get appropriate color for error alerts
    error_color = get_status_color('error', theme_colors)
    
    # Create error title component
    title_component = create_error_title(error_type, theme_colors)
    
    # Create error message component
    message_component = create_error_message(error_message, theme_colors)
    
    # Create error details component (if details are provided)
    details_component = None
    if error_details:
        details_component = create_error_details(error_details, theme_colors)
    
    # Create retry button component
    retry_button = create_retry_button(theme_colors)
    
    # Create the card with all error elements
    card_children = [
        title_component,
        html.Hr(style={"border-color": theme_colors["grid"]}),
        message_component
    ]
    
    # Add details if provided
    if details_component:
        card_children.append(details_component)
    
    # Add retry button
    card_children.append(
        html.Div(
            retry_button,
            className="mt-4 text-center"
        )
    )
    
    # Create the card component
    error_card = dbc.Card(
        dbc.CardBody(card_children),
        id=ERROR_CONTAINER_ID,
        className="shadow-sm",
        style={
            "border": f"1px solid {theme_colors['grid']}",
            "background-color": theme_colors["paper"],
            "border-radius": "8px",
        }
    )
    
    return error_card


def create_error_title(error_type, theme_colors):
    """
    Creates the title component for the error display.
    
    Args:
        error_type (str): The type of error that occurred
        theme_colors (dict): Dictionary of theme colors for styling
        
    Returns:
        dash_html_components.H3: H3 component with error title
    """
    # Get appropriate error title
    error_title = get_error_title(error_type)
    
    return html.H3(
        error_title,
        id=ERROR_TITLE_ID,
        className="error-title text-center mb-3",
        style={
            "color": theme_colors["text"],
            "font-weight": "bold"
        }
    )


def create_error_message(error_message, theme_colors):
    """
    Creates the message component for the error display.
    
    Args:
        error_message (str): The main error message to display
        theme_colors (dict): Dictionary of theme colors for styling
        
    Returns:
        dash_html_components.P: Paragraph component with error message
    """
    return html.P(
        error_message,
        id=ERROR_MESSAGE_ID,
        className="error-message text-center",
        style={
            "color": theme_colors["text"],
            "margin-bottom": "20px",
            "font-size": "1.1rem"
        }
    )


def create_error_details(error_details, theme_colors):
    """
    Creates the details component for the error display if details are provided.
    
    Args:
        error_details (str): Additional technical details about the error
        theme_colors (dict): Dictionary of theme colors for styling
        
    Returns:
        dash_bootstrap_components.Collapse: Collapsible component with error details
    """
    # If no details provided, return None
    if not error_details:
        return None
    
    # Create a pre component to preserve formatting of error details
    pre_component = html.Pre(
        error_details,
        style={
            "background-color": theme_colors["background"],
            "color": theme_colors["text"],
            "padding": "15px",
            "border-radius": "4px",
            "white-space": "pre-wrap",
            "font-size": "0.9rem",
            "overflow-x": "auto"
        }
    )
    
    # Create a collapsible component for the details
    return dbc.Collapse(
        pre_component,
        id=ERROR_DETAILS_ID,
        is_open=True,
        className="mt-3 mb-3"
    )


def create_retry_button(theme_colors):
    """
    Creates a button that allows users to retry the operation that failed.
    
    Args:
        theme_colors (dict): Dictionary of theme colors for styling
        
    Returns:
        dash_bootstrap_components.Button: Button component for retrying
    """
    return dbc.Button(
        "Retry",
        id=RETRY_BUTTON_ID,
        color="primary",
        className="mt-3",
        style={
            "min-width": "120px",
            "background-color": theme_colors["accent"],
            "border-color": theme_colors["accent"]
        }
    )


def get_error_title(error_type):
    """
    Gets the appropriate title for an error type.
    
    Args:
        error_type (str): The type of error that occurred
        
    Returns:
        str: User-friendly error title
    """
    # Check if the error type is in ERROR_TYPES dictionary
    if error_type in ERROR_TYPES:
        return ERROR_TYPES[error_type]
    
    # If not found, return the title for the default error type
    return ERROR_TYPES[DEFAULT_ERROR_TYPE]