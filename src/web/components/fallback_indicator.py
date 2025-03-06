"""
Component module that provides visual indicators for fallback forecast data in the Electricity Market 
Price Forecasting System's Dash-based visualization interface. This module creates warning indicators 
and badges to clearly communicate to users when they are viewing fallback data (previous day's 
forecast) rather than current forecast data.
"""

from typing import Dict, Optional

# dash v2.9.0+
import dash
from dash import html
# dash_bootstrap_components v1.0.0+
import dash_bootstrap_components as dbc

from ..config.logging_config import get_logger
from ..config.themes import get_status_color
from ..utils.error_handlers import is_fallback_data, FALLBACK_MESSAGE

# Initialize logger for this component
logger = get_logger(__name__)

# Constants for component IDs (used for CSS styling and callbacks)
FALLBACK_INDICATOR_ID = 'fallback-indicator'
FALLBACK_BADGE_ID = 'fallback-badge'


def create_fallback_indicator(forecast_data: Dict, theme: str) -> html.Div:
    """
    Creates a visual indicator component for fallback forecast data.
    
    Args:
        forecast_data: Dictionary containing forecast data
        theme: Current UI theme for styling
        
    Returns:
        A Dash HTML Div component containing the fallback indicator or an empty div if not using fallback
    """
    # Return empty div if no data provided
    if forecast_data is None:
        logger.debug("No forecast data provided to create_fallback_indicator")
        return html.Div(id=FALLBACK_INDICATOR_ID)
    
    # Check if forecast is using fallback data
    if is_fallback_data(forecast_data):
        logger.info("Displaying fallback indicator for fallback forecast data")
        
        # Get appropriate warning color for current theme
        warning_color = get_status_color('fallback', theme)
        
        # Create alert component with fallback message
        alert = dbc.Alert(
            [
                html.I(className="fas fa-exclamation-triangle me-2"),
                FALLBACK_MESSAGE
            ],
            color="warning",
            style={
                "backgroundColor": warning_color,
                "border": "none",
                "margin-bottom": "1rem"
            },
            id=FALLBACK_INDICATOR_ID,
            className="fallback-indicator",
            is_open=True,
            # Accessibility attributes
            role="alert",
            aria_live="polite"
        )
        
        return html.Div(alert, className="mt-2 mb-3")
    else:
        # Return empty div if not using fallback
        return html.Div(id=FALLBACK_INDICATOR_ID)


def create_fallback_badge(forecast_data: Dict, theme: str) -> Optional[dbc.Badge]:
    """
    Creates a badge-style indicator for fallback status.
    
    Args:
        forecast_data: Dictionary containing forecast data
        theme: Current UI theme for styling
        
    Returns:
        A Dash Bootstrap Badge component or None if not using fallback
    """
    # Return None if no data provided
    if forecast_data is None:
        logger.debug("No forecast data provided to create_fallback_badge")
        return None
    
    # Check if forecast is using fallback data
    if is_fallback_data(forecast_data):
        logger.info("Creating fallback badge for fallback forecast data")
        
        # Get appropriate warning color for current theme
        warning_color = get_status_color('fallback', theme)
        
        # Create badge component
        badge = dbc.Badge(
            "Fallback",
            color="warning",
            style={
                "backgroundColor": warning_color,
                "fontSize": "0.8rem",
                "fontWeight": "normal"
            },
            id=FALLBACK_BADGE_ID,
            className="ms-2 fallback-badge"
        )
        
        return badge
    else:
        # Return None if not using fallback
        return None


def is_using_fallback(forecast_data: Dict) -> bool:
    """
    Checks if the forecast data is from the fallback mechanism.
    
    Args:
        forecast_data: Dictionary containing forecast data
        
    Returns:
        True if data is from fallback mechanism, False otherwise
    """
    result = is_fallback_data(forecast_data)
    if result:
        logger.info("Forecast data is using fallback mechanism")
    else:
        logger.debug("Forecast data is using primary mechanism (not fallback)")
    
    return result