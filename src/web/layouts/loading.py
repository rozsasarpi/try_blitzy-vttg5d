"""
Layout module that creates the loading screen for the Electricity Market Price Forecasting System's Dash-based visualization interface. This component is displayed during data loading, initial rendering, and when refreshing forecast data, providing users with visual feedback about the loading process.
"""

import dash_bootstrap_components as dbc  # v1.0.0+
from dash import html, dcc  # v2.9.0+

# Internal imports
from ..components.header import create_header
from ..components.footer import create_footer
from ..config.themes import get_theme_colors, DEFAULT_THEME
from .responsive import create_responsive_container
from ..config.logging_config import get_logger

# Component IDs for styling and callbacks
LOADING_PAGE_ID = 'loading-page'
LOADING_CONTAINER_ID = 'loading-container'
LOADING_SPINNER_ID = 'loading-spinner'
LOADING_MESSAGE_ID = 'loading-message'

# Set up logger
logger = get_logger('loading_page')

def create_loading_layout(message=None, theme=None):
    """
    Creates the loading screen layout with spinner and message.
    
    Args:
        message (str): Message to display during loading
        theme (str): Theme name for styling
        
    Returns:
        dash_bootstrap_components.Container: A Dash Bootstrap Container component with the loading layout
    """
    # Get theme colors
    theme_colors = get_theme_colors(theme or DEFAULT_THEME)
    
    # Create header
    header = create_header(theme)
    
    # Create loading content
    loading_content = create_loading_content(
        message or get_default_loading_message(),
        theme_colors
    )
    
    # Create footer
    footer = create_footer(theme)
    
    # Create responsive container with all components
    container = create_responsive_container(
        [header, loading_content, footer],
        id=LOADING_PAGE_ID,
        style={
            'backgroundColor': theme_colors['background'],
            'minHeight': '100vh',
            'display': 'flex',
            'flexDirection': 'column'
        }
    )
    
    logger.info(f"Created loading screen with message: {message or get_default_loading_message()}")
    
    return container

def create_loading_content(message, theme_colors):
    """
    Creates the main loading content with spinner and message.
    
    Args:
        message (str): Message to display during loading
        theme_colors (dict): Dictionary of color values for the current theme
        
    Returns:
        dash_bootstrap_components.Card: A Dash Bootstrap Card component with loading spinner and message
    """
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    create_loading_spinner(theme_colors),
                    create_loading_message(message, theme_colors)
                ],
                className="d-flex flex-column align-items-center justify-content-center",
                style={
                    'minHeight': '50vh',
                    'backgroundColor': theme_colors['paper']
                }
            )
        ],
        id=LOADING_CONTAINER_ID,
        className="mx-auto my-5 shadow",
        style={
            'width': '80%',
            'maxWidth': '800px',
            'borderRadius': '10px',
            'border': f"1px solid {theme_colors['grid']}"
        },
        # Add ARIA attributes for accessibility
        **{
            'role': 'status',
            'aria-live': 'polite',
            'aria-atomic': 'true'
        }
    )

def create_loading_spinner(theme_colors):
    """
    Creates the loading spinner component.
    
    Args:
        theme_colors (dict): Dictionary of color values for the current theme
        
    Returns:
        dash_core_components.Loading: A Dash Loading component
    """
    return dcc.Loading(
        id=LOADING_SPINNER_ID,
        type="circle",
        color=theme_colors['accent'],
        children=html.Div(style={'height': '50px'}),
        style={
            'margin': '20px'
        }
    )

def create_loading_message(message, theme_colors):
    """
    Creates the loading message component.
    
    Args:
        message (str): Message to display during loading
        theme_colors (dict): Dictionary of color values for the current theme
        
    Returns:
        dash_html_components.H4: A Dash HTML H4 component with the loading message
    """
    return html.H4(
        message,
        id=LOADING_MESSAGE_ID,
        style={
            'color': theme_colors['text'],
            'textAlign': 'center',
            'margin': '20px',
            'fontWeight': 'normal'
        }
    )

def get_default_loading_message():
    """
    Returns the default loading message if none is provided.
    
    Returns:
        str: Default loading message
    """
    return "Loading forecast data..."