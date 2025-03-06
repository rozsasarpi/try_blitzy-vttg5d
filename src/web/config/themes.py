"""
Theme configuration for the Electricity Market Price Forecasting System's Dash visualization.

This module provides color palettes, chart configurations, and styling options for different
visualization components to ensure consistent visual presentation across the dashboard.
"""

# Import color utilities from plotly (v5.14.0+)
import plotly.colors

# Default theme to use when none is specified
DEFAULT_THEME = 'light'

# List of all available themes
AVAILABLE_THEMES = ['light', 'dark', 'colorblind']

# Color palettes for each theme
THEME_COLORS = {
    "light": {
        "background": "#f8f9fa",  # Light gray background
        "paper": "#ffffff",        # White paper/card background
        "text": "#343a40",         # Dark gray text
        "grid": "#e9ecef",         # Light gray grid lines
        "line": "#6c757d",         # Medium gray line
        "accent": "#007bff"        # Blue accent color
    },
    "dark": {
        "background": "#212529",   # Dark background
        "paper": "#343a40",        # Dark paper/card background
        "text": "#f8f9fa",         # Light text
        "grid": "#495057",         # Medium dark grid lines
        "line": "#adb5bd",         # Light gray line
        "accent": "#0d6efd"        # Blue accent color
    },
    "colorblind": {
        "background": "#f8f9fa",   # Light gray background
        "paper": "#ffffff",        # White paper/card background
        "text": "#343a40",         # Dark gray text
        "grid": "#e9ecef",         # Light gray grid lines
        "line": "#6c757d",         # Medium gray line
        "accent": "#0072B2"        # Colorblind-friendly blue
    }
}

# Product-specific colors for each theme
PRODUCT_COLORS = {
    "DALMP": {
        "light": "#007bff",       # Blue
        "dark": "#0d6efd",        # Brighter blue for dark theme
        "colorblind": "#0072B2"   # Colorblind-friendly blue
    },
    "RTLMP": {
        "light": "#28a745",       # Green
        "dark": "#2dce4f",        # Brighter green for dark theme
        "colorblind": "#009E73"   # Colorblind-friendly green
    },
    "RegUp": {
        "light": "#6f42c1",       # Purple
        "dark": "#8540f5",        # Brighter purple for dark theme
        "colorblind": "#CC79A7"   # Colorblind-friendly pink/purple
    },
    "RegDown": {
        "light": "#fd7e14",       # Orange
        "dark": "#ff922b",        # Brighter orange for dark theme
        "colorblind": "#E69F00"   # Colorblind-friendly orange
    },
    "RRS": {
        "light": "#dc3545",       # Red
        "dark": "#f55a6a",        # Brighter red for dark theme
        "colorblind": "#D55E00"   # Colorblind-friendly red
    },
    "NSRS": {
        "light": "#20c997",       # Teal
        "dark": "#25e1ab",        # Brighter teal for dark theme
        "colorblind": "#56B4E9"   # Colorblind-friendly light blue
    }
}

# Status indicator colors for each theme
STATUS_COLORS = {
    "success": {
        "light": "#28a745",       # Green
        "dark": "#2dce4f",        # Brighter green for dark theme
        "colorblind": "#009E73"   # Colorblind-friendly green
    },
    "warning": {
        "light": "#ffc107",       # Yellow
        "dark": "#ffca2c",        # Brighter yellow for dark theme
        "colorblind": "#E69F00"   # Colorblind-friendly orange
    },
    "error": {
        "light": "#dc3545",       # Red
        "dark": "#f55a6a",        # Brighter red for dark theme
        "colorblind": "#D55E00"   # Colorblind-friendly red
    },
    "info": {
        "light": "#17a2b8",       # Cyan
        "dark": "#1fc8e3",        # Brighter cyan for dark theme
        "colorblind": "#56B4E9"   # Colorblind-friendly light blue
    },
    "fallback": {
        "light": "#ffc107",       # Yellow (same as warning)
        "dark": "#ffca2c",        # Brighter yellow for dark theme
        "colorblind": "#E69F00"   # Colorblind-friendly orange
    }
}

# Styling for uncertainty bands in each theme
UNCERTAINTY_STYLES = {
    "light": {
        "opacity": 0.3,
        "line": {"width": 0}
    },
    "dark": {
        "opacity": 0.4,
        "line": {"width": 0}
    },
    "colorblind": {
        "opacity": 0.3,
        "line": {"width": 0}
    }
}

# Default configuration for Plotly charts
CHART_CONFIG = {
    "displayModeBar": True,
    "responsive": True,
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d", "autoScale2d"],
    "toImageButtonOptions": {
        "format": "png",
        "filename": "forecast_chart",
        "height": 800,
        "width": 1200,
        "scale": 2
    }
}

# Default layouts for plots in each theme
PLOT_LAYOUTS = {
    "light": {
        "plot_bgcolor": THEME_COLORS["light"]["background"],
        "paper_bgcolor": THEME_COLORS["light"]["paper"],
        "font": {
            "color": THEME_COLORS["light"]["text"],
            "family": "Arial, sans-serif"
        },
        "margin": {"l": 50, "r": 50, "t": 50, "b": 50},
        "hovermode": "x unified",
        "legend": {"orientation": "h", "y": 1.1}
    },
    "dark": {
        "plot_bgcolor": THEME_COLORS["dark"]["background"],
        "paper_bgcolor": THEME_COLORS["dark"]["paper"],
        "font": {
            "color": THEME_COLORS["dark"]["text"],
            "family": "Arial, sans-serif"
        },
        "margin": {"l": 50, "r": 50, "t": 50, "b": 50},
        "hovermode": "x unified",
        "legend": {"orientation": "h", "y": 1.1}
    },
    "colorblind": {
        "plot_bgcolor": THEME_COLORS["colorblind"]["background"],
        "paper_bgcolor": THEME_COLORS["colorblind"]["paper"],
        "font": {
            "color": THEME_COLORS["colorblind"]["text"],
            "family": "Arial, sans-serif"
        },
        "margin": {"l": 50, "r": 50, "t": 50, "b": 50},
        "hovermode": "x unified",
        "legend": {"orientation": "h", "y": 1.1}
    }
}

# Layout adjustments for different viewport sizes
RESPONSIVE_LAYOUTS = {
    "mobile": {
        "margin": {"l": 30, "r": 30, "t": 40, "b": 40},
        "legend": {"orientation": "h", "y": 1.1, "x": 0.5, "xanchor": "center"},
        "height": 350,
        "font": {"size": 10}
    },
    "tablet": {
        "margin": {"l": 40, "r": 40, "t": 45, "b": 45},
        "legend": {"orientation": "h", "y": 1.1},
        "height": 400,
        "font": {"size": 11}
    },
    "desktop": {
        "margin": {"l": 50, "r": 50, "t": 50, "b": 50},
        "legend": {"orientation": "h", "y": 1.1},
        "height": 500,
        "font": {"size": 12}
    }
}

# Colorscales for heatmaps in each theme
COLORSCALES = {
    "light": [[0, "#f8f9fa"], [0.5, "#adb5bd"], [1, "#343a40"]],
    "dark": [[0, "#343a40"], [0.5, "#adb5bd"], [1, "#f8f9fa"]],
    "colorblind": [[0, "#f8f9fa"], [0.5, "#56B4E9"], [1, "#0072B2"]]
}


def get_theme_colors(theme):
    """
    Returns the color palette for the specified theme.
    
    Args:
        theme (str): The theme name to get colors for
        
    Returns:
        dict: Dictionary of color values for the theme
    """
    if theme in THEME_COLORS:
        return THEME_COLORS[theme]
    return THEME_COLORS[DEFAULT_THEME]


def get_product_color(product_id, theme):
    """
    Returns the color for a specific product in the specified theme.
    
    Args:
        product_id (str): The product identifier (e.g., 'DALMP', 'RTLMP')
        theme (str): The theme name
        
    Returns:
        str: Hex color code for the product
    """
    if product_id in PRODUCT_COLORS:
        product_theme = PRODUCT_COLORS[product_id]
        if theme in product_theme:
            return product_theme[theme]
        return product_theme[DEFAULT_THEME]
    
    # If product not found, return the accent color from the theme
    theme_colors = get_theme_colors(theme)
    return theme_colors["accent"]


def get_status_color(status, theme):
    """
    Returns the color for a status indicator in the specified theme.
    
    Args:
        status (str): The status identifier (e.g., 'success', 'warning', 'error')
        theme (str): The theme name
        
    Returns:
        str: Hex color code for the status
    """
    if status in STATUS_COLORS:
        status_theme = STATUS_COLORS[status]
        if theme in status_theme:
            return status_theme[theme]
        return status_theme[DEFAULT_THEME]
    
    # If status not found, return the info color
    if "info" in STATUS_COLORS and theme in STATUS_COLORS["info"]:
        return STATUS_COLORS["info"][theme]
    return STATUS_COLORS["info"][DEFAULT_THEME]


def get_plot_layout(theme):
    """
    Returns the default plot layout configuration for the specified theme.
    
    Args:
        theme (str): The theme name
        
    Returns:
        dict: Layout configuration dictionary for Plotly
    """
    if theme in PLOT_LAYOUTS:
        # Return a copy to prevent modifying the global default
        return dict(PLOT_LAYOUTS[theme])
    return dict(PLOT_LAYOUTS[DEFAULT_THEME])


def get_uncertainty_style(theme, product_id):
    """
    Returns the styling for uncertainty bands in the specified theme.
    
    Args:
        theme (str): The theme name
        product_id (str): The product identifier
        
    Returns:
        dict: Styling configuration for uncertainty bands
    """
    # Get the base uncertainty style for the theme
    if theme in UNCERTAINTY_STYLES:
        base_style = UNCERTAINTY_STYLES[theme]
    else:
        base_style = UNCERTAINTY_STYLES[DEFAULT_THEME]
    
    # Get the product color
    product_color = get_product_color(product_id, theme)
    
    # Create the combined style dictionary
    style = {
        "fillcolor": product_color,
        "opacity": base_style["opacity"],
        "line": dict(base_style["line"])
    }
    
    return style


def get_responsive_layout(viewport_size):
    """
    Returns responsive layout adjustments based on viewport size.
    
    Args:
        viewport_size (str): The viewport size category ('mobile', 'tablet', 'desktop')
        
    Returns:
        dict: Responsive layout adjustments
    """
    if viewport_size in RESPONSIVE_LAYOUTS:
        # Return a copy to prevent modifying the global default
        return dict(RESPONSIVE_LAYOUTS[viewport_size])
    return dict(RESPONSIVE_LAYOUTS["desktop"])


def get_colorscale(theme):
    """
    Returns a colorscale for heatmaps and other color-mapped visualizations.
    
    Args:
        theme (str): The theme name
        
    Returns:
        list: Colorscale definition for Plotly
    """
    if theme in COLORSCALES:
        return COLORSCALES[theme]
    return COLORSCALES[DEFAULT_THEME]


def create_custom_colorscale(product_id, theme):
    """
    Creates a custom colorscale based on a product color.
    
    Args:
        product_id (str): The product identifier
        theme (str): The theme name
        
    Returns:
        list: Custom colorscale for the product
    """
    # Get the product color
    product_color = get_product_color(product_id, theme)
    
    # Get the background color from the theme
    background_color = get_theme_colors(theme)["background"]
    
    # Create a colorscale transitioning from background to product color
    colorscale = [
        [0.0, background_color],
        [1.0, product_color]
    ]
    
    return colorscale