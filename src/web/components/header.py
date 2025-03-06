"""
Component module that creates the header for the Electricity Market Price Forecasting System's 
Dash-based visualization interface. The header includes the application title, navigation elements, 
theme selector, and last update information.
"""

import dash_bootstrap_components as dbc  # v1.0.0+
from dash import html, dcc  # v2.0.0+
import datetime

# Internal imports
from ..config.themes import get_theme_colors, AVAILABLE_THEMES, DEFAULT_THEME
from ..config.settings import FORECAST_GENERATION_TIME, CST_TIMEZONE

# Component IDs for styling and callbacks
HEADER_ID = "dashboard-header"
TITLE_ID = "dashboard-title"
THEME_SELECTOR_ID = "theme-selector"
LAST_UPDATE_ID = "last-update-info"
HELP_BUTTON_ID = "help-button"


def create_header(theme):
    """
    Creates the header component for the dashboard.
    
    Args:
        theme (str): The current theme for styling
        
    Returns:
        dash_bootstrap_components.Navbar: A Dash Bootstrap Navbar component containing the header elements
    """
    theme_colors = get_theme_colors(theme)
    
    # Create the navbar with appropriate styling
    navbar = dbc.Navbar(
        [
            # Logo/title section
            dbc.Container(
                [
                    # Brand section (left)
                    dbc.Row(
                        [
                            dbc.Col(create_title(theme_colors), className="me-auto")
                        ],
                        align="center",
                        className="g-0",
                    ),
                    
                    # Theme and info section (right)
                    dbc.Row(
                        [
                            dbc.Col(
                                create_last_update_info(theme_colors),
                                className="me-3"
                            ),
                            dbc.Col(
                                create_theme_selector(theme_colors, theme),
                                width="auto",
                                className="me-2"
                            ),
                            dbc.Col(
                                create_help_button(theme_colors),
                                width="auto"
                            )
                        ],
                        align="center",
                        className="g-0",
                    ),
                ],
                fluid=True,
            )
        ],
        color=theme_colors["paper"],
        dark=theme == "dark",
        className="mb-3",
        id=HEADER_ID,
    )
    
    return navbar


def create_title(theme_colors):
    """
    Creates the title element for the header.
    
    Args:
        theme_colors (dict): Dictionary of color values for the current theme
        
    Returns:
        dash_html_components.H1: A Dash HTML H1 component containing the dashboard title
    """
    return html.H1(
        "Electricity Market Price Forecasting",
        id=TITLE_ID,
        style={
            "color": theme_colors["text"],
            "margin-bottom": "0px",
            "font-size": "24px",
            "font-weight": "bold"
        }
    )


def create_theme_selector(theme_colors, current_theme):
    """
    Creates a dropdown for selecting the dashboard theme.
    
    Args:
        theme_colors (dict): Dictionary of color values for the current theme
        current_theme (str): The currently selected theme
        
    Returns:
        dash_core_components.Dropdown: A Dash dropdown component for theme selection
    """
    # Create options list with capitalized labels
    options = [
        {"label": theme.capitalize(), "value": theme}
        for theme in AVAILABLE_THEMES
    ]
    
    return dcc.Dropdown(
        id=THEME_SELECTOR_ID,
        options=options,
        value=current_theme,
        clearable=False,
        style={
            "width": "150px",
            "color": theme_colors["text"],
            "background-color": theme_colors["paper"]
        }
    )


def create_last_update_info(theme_colors):
    """
    Creates an element showing when the forecast was last updated.
    
    Args:
        theme_colors (dict): Dictionary of color values for the current theme
        
    Returns:
        dash_html_components.Div: A Dash HTML Div component showing last update information
    """
    # Get current time in CST
    current_time = datetime.datetime.now()
    formatted_time = format_time_cst(current_time)
    
    return html.Div(
        [
            html.Span(f"Last updated: {formatted_time} CST", 
                      style={"font-weight": "normal"}),
            html.Br(),
            html.Span(f"Forecasts generated daily at {FORECAST_GENERATION_TIME} CST", 
                      style={"font-size": "12px", "color": theme_colors["line"]})
        ],
        id=LAST_UPDATE_ID,
        style={
            "text-align": "right",
            "color": theme_colors["text"],
            "font-size": "14px"
        }
    )


def create_help_button(theme_colors):
    """
    Creates a help button that opens documentation or help modal.
    
    Args:
        theme_colors (dict): Dictionary of color values for the current theme
        
    Returns:
        dash_bootstrap_components.Button: A Dash Bootstrap Button component for accessing help
    """
    return dbc.Button(
        "?",
        id=HELP_BUTTON_ID,
        color="link",
        className="text-decoration-none",
        style={
            "color": theme_colors["accent"],
            "font-weight": "bold",
            "font-size": "16px",
            "padding": "0px 10px",
            "border-radius": "50%"
        }
    )


def format_time_cst(dt):
    """
    Formats a datetime object to CST timezone string.
    
    Args:
        dt (datetime.datetime): The datetime to format
        
    Returns:
        str: Formatted time string in CST timezone
    """
    # Convert to CST timezone
    dt_cst = dt.astimezone(CST_TIMEZONE)
    return dt_cst.strftime("%Y-%m-%d %H:%M:%S")