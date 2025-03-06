"""
Footer component for the Electricity Market Price Forecasting System visualization.

This module provides a footer component for the Dash-based dashboard, including
copyright information, version details, and links to documentation and support.
"""

from datetime import datetime
import dash_bootstrap_components as dbc  # v1.0.0+
import dash_html_components as html  # v2.0.0+

from ..config.themes import get_theme_colors
from ..config.settings import is_production, ENVIRONMENT

# Footer component ID for styling and callbacks
FOOTER_ID = 'dashboard-footer'

# Current version of the application
VERSION = '1.0.0'

# Copyright text template
COPYRIGHT_TEXT = '© {} Electricity Market Price Forecasting System. All rights reserved.'

# URLs for external resources
DOCUMENTATION_URL = '#'
SUPPORT_URL = '#'


def create_footer(theme: str) -> dbc.Container:
    """
    Creates the footer component for the dashboard.
    
    Args:
        theme: The current theme name for styling
        
    Returns:
        A Dash Bootstrap Container component containing the footer elements
    """
    # Get theme colors for styling
    theme_colors = get_theme_colors(theme)
    
    # Create footer container with styling
    footer = dbc.Container(
        [
            dbc.Row(
                [
                    # Left column with copyright
                    dbc.Col(
                        create_copyright_text(theme_colors),
                        xs=12, sm=12, md=6, lg=6, xl=6,
                        className="d-flex align-items-center"
                    ),
                    
                    # Right column with links and version
                    dbc.Col(
                        create_footer_links(theme_colors),
                        xs=12, sm=12, md=6, lg=6, xl=6,
                        className="d-flex align-items-center justify-content-end"
                    ),
                ],
                className="py-3"
            ),
            
            # Environment indicator (only shown in non-production)
            create_environment_indicator(theme_colors) if not is_production() else None
        ],
        fluid=True,
        id=FOOTER_ID,
        style={
            'backgroundColor': theme_colors['paper'],
            'color': theme_colors['text'],
            'borderTop': f"1px solid {theme_colors['grid']}",
            'marginTop': '20px',
            'paddingTop': '10px',
            'paddingBottom': '10px',
            'fontSize': '0.9rem'
        }
    )
    
    return footer


def create_copyright_text(theme_colors: dict) -> html.P:
    """
    Creates the copyright text element for the footer.
    
    Args:
        theme_colors: Dictionary of theme colors for styling
        
    Returns:
        A Dash HTML paragraph component containing the copyright text
    """
    # Get current year for copyright
    current_year = datetime.now().year
    
    # Create paragraph with copyright text
    return html.P(
        COPYRIGHT_TEXT.format(current_year),
        style={
            'margin': 0,
            'color': theme_colors['text']
        }
    )


def create_footer_links(theme_colors: dict) -> html.Div:
    """
    Creates links to documentation and support in the footer.
    
    Args:
        theme_colors: Dictionary of theme colors for styling
        
    Returns:
        A Dash HTML Div component containing footer links
    """
    # Create container for links
    return html.Div(
        [
            # Documentation link
            html.A(
                "Documentation",
                href=DOCUMENTATION_URL,
                target="_blank",
                rel="noopener noreferrer",
                style={
                    'marginRight': '15px',
                    'color': theme_colors['accent'],
                    'textDecoration': 'none'
                }
            ),
            
            # Support link
            html.A(
                "Support",
                href=SUPPORT_URL,
                target="_blank",
                rel="noopener noreferrer",
                style={
                    'marginRight': '15px',
                    'color': theme_colors['accent'],
                    'textDecoration': 'none'
                }
            ),
            
            # Version text
            html.Span(
                f"v{VERSION}",
                style={
                    'color': theme_colors['line']
                }
            )
        ],
        style={
            'display': 'flex',
            'justifyContent': 'flex-end',
            'alignItems': 'center'
        },
        className="d-flex justify-content-end"
    )


def create_environment_indicator(theme_colors: dict) -> dbc.Badge:
    """
    Creates an indicator showing the current environment (development/staging).
    
    Args:
        theme_colors: Dictionary of theme colors for styling
        
    Returns:
        A Dash Bootstrap Badge component showing the environment
    """
    # Create badge with environment name
    return dbc.Badge(
        ENVIRONMENT.upper(),
        color="warning" if ENVIRONMENT == "staging" else "info",
        className="position-absolute top-0 end-0 m-2",
        style={
            'fontSize': '0.7rem',
            'opacity': '0.8'
        }
    )