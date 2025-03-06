"""
Utility module providing helper functions for implementing responsive design in the 
Electricity Market Price Forecasting System's Dash-based visualization interface.

This module enables the dashboard to adapt to different screen sizes (mobile, tablet, 
desktop) for optimal user experience across devices.
"""

from typing import Dict, Any, Optional

from ..config.settings import ENABLE_RESPONSIVE_UI
from ..config.dashboard_config import VIEWPORT_BREAKPOINTS, DEFAULT_VIEWPORT
from ..config.themes import RESPONSIVE_LAYOUTS

# List of supported viewport sizes
VIEWPORT_SIZES = ['mobile', 'tablet', 'desktop']

# Default style adjustments for different viewport sizes
DEFAULT_STYLE_ADJUSTMENTS = {
    "mobile": {
        "margin": "5px", 
        "padding": "10px", 
        "font-size": "12px"
    },
    "tablet": {
        "margin": "8px", 
        "padding": "12px", 
        "font-size": "14px"
    },
    "desktop": {
        "margin": "10px", 
        "padding": "15px", 
        "font-size": "16px"
    }
}

# Scaling factors for dimensions in different viewport sizes
COMPONENT_DIMENSIONS = {
    "height": {
        "mobile": 0.7,
        "tablet": 0.85,
        "desktop": 1.0
    },
    "width": {
        "mobile": 1.0,
        "tablet": 1.0,
        "desktop": 1.0
    },
    "margin": {
        "mobile": 0.5,
        "tablet": 0.75,
        "desktop": 1.0
    },
    "padding": {
        "mobile": 0.6,
        "tablet": 0.8,
        "desktop": 1.0
    },
    "font-size": {
        "mobile": 0.75,
        "tablet": 0.875,
        "desktop": 1.0
    }
}


def detect_viewport_size(width: int) -> str:
    """
    Detects the viewport size category based on screen width.
    
    Args:
        width: The viewport width in pixels
        
    Returns:
        Viewport size category (mobile, tablet, desktop)
    """
    if width <= VIEWPORT_BREAKPOINTS['mobile']:
        return 'mobile'
    elif width <= VIEWPORT_BREAKPOINTS['tablet']:
        return 'tablet'
    else:
        return 'desktop'


def get_responsive_style(viewport_size: str, base_style: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Returns CSS styles adjusted for the specified viewport size.
    
    Args:
        viewport_size: The viewport size category (mobile, tablet, desktop)
        base_style: The base style dictionary to adjust (optional)
        
    Returns:
        Style dictionary adjusted for viewport size
    """
    # If responsive UI is disabled, return base_style unchanged
    if not ENABLE_RESPONSIVE_UI:
        return base_style or {}
    
    # Check if viewport_size is valid
    if viewport_size not in VIEWPORT_SIZES:
        viewport_size = DEFAULT_VIEWPORT
    
    # Initialize result with base_style or empty dict
    result = dict(base_style) if base_style else {}
    
    # Get style adjustments for the viewport size
    style_adjustments = DEFAULT_STYLE_ADJUSTMENTS.get(viewport_size, DEFAULT_STYLE_ADJUSTMENTS['desktop'])
    
    # Apply adjustments to the base style
    for key, value in style_adjustments.items():
        result[key] = value
    
    return result


def get_responsive_dimension(viewport_size: str, base_value: int, dimension_type: str) -> int:
    """
    Calculates a responsive dimension value based on viewport size.
    
    Args:
        viewport_size: The viewport size category (mobile, tablet, desktop)
        base_value: The base dimension value (at desktop size)
        dimension_type: The type of dimension (height, width, margin, padding, font-size)
        
    Returns:
        Adjusted dimension value for the viewport size
    """
    # If responsive UI is disabled, return base_value unchanged
    if not ENABLE_RESPONSIVE_UI:
        return base_value
    
    # Check if viewport_size is valid
    if viewport_size not in VIEWPORT_SIZES:
        viewport_size = DEFAULT_VIEWPORT
    
    # Check if dimension_type is supported
    if dimension_type not in COMPONENT_DIMENSIONS:
        return base_value
    
    # Get the scaling factor for the dimension and viewport size
    scaling_factor = COMPONENT_DIMENSIONS[dimension_type].get(
        viewport_size, COMPONENT_DIMENSIONS[dimension_type]['desktop']
    )
    
    # Calculate the adjusted value
    adjusted_value = int(base_value * scaling_factor)
    
    return adjusted_value


def create_responsive_style(base_style: Optional[Dict[str, Any]] = None, 
                            viewport_size: str = 'desktop',
                            overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Creates a complete style dictionary with responsive adjustments.
    
    Args:
        base_style: The base style dictionary (optional)
        viewport_size: The viewport size category (mobile, tablet, desktop)
        overrides: Style overrides to apply after responsive adjustments (optional)
        
    Returns:
        Complete style dictionary with responsive adjustments
    """
    # Initialize with base_style or empty dict
    result = dict(base_style) if base_style else {}
    
    # Apply responsive adjustments
    result = get_responsive_style(viewport_size, result)
    
    # Apply any overrides
    if overrides:
        result.update(overrides)
    
    return result


def get_viewport_meta_tag() -> Dict[str, str]:
    """
    Returns the viewport meta tag for responsive design.
    
    Returns:
        Meta tag dictionary for Dash
    """
    return {
        "name": "viewport",
        "content": "width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"
    }


def create_viewport_detection_script() -> str:
    """
    Creates a JavaScript function for client-side viewport detection.
    
    Returns:
        JavaScript code for viewport detection
    """
    return """
        function detectViewportSize() {
            const width = window.innerWidth;
            
            if (width <= %s) {
                return 'mobile';
            } else if (width <= %s) {
                return 'tablet';
            } else {
                return 'desktop';
            }
        }
    """ % (VIEWPORT_BREAKPOINTS['mobile'], VIEWPORT_BREAKPOINTS['tablet'])


def apply_responsive_font_size(style: Dict[str, Any], viewport_size: str, base_size: int = 16) -> Dict[str, Any]:
    """
    Applies responsive font size to a style dictionary.
    
    Args:
        style: The style dictionary to update
        viewport_size: The viewport size category (mobile, tablet, desktop)
        base_size: The base font size in pixels (for desktop)
        
    Returns:
        Style with responsive font size
    """
    result = dict(style) if style else {}
    
    # If responsive UI is disabled, set font-size to base_size and return
    if not ENABLE_RESPONSIVE_UI:
        result['font-size'] = f"{base_size}px"
        return result
    
    # Calculate responsive font size
    responsive_size = get_responsive_dimension(viewport_size, base_size, 'font-size')
    result['font-size'] = f"{responsive_size}px"
    
    return result


def get_responsive_class(viewport_size: str, base_class: Optional[str] = None) -> str:
    """
    Returns CSS class names for responsive styling.
    
    Args:
        viewport_size: The viewport size category (mobile, tablet, desktop)
        base_class: The base CSS class name (optional)
        
    Returns:
        CSS class string with responsive classes
    """
    # Initialize with base_class or empty string
    result = base_class or ""
    
    # If responsive UI is disabled, return base_class unchanged
    if not ENABLE_RESPONSIVE_UI:
        return result
    
    # Check if viewport_size is valid
    if viewport_size not in VIEWPORT_SIZES:
        viewport_size = DEFAULT_VIEWPORT
    
    # Add viewport-specific class
    if result:
        result += f" {viewport_size}"
    else:
        result = viewport_size
    
    return result