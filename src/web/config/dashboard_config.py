"""
Configuration file defining the layout, structure, and behavior of the Electricity Market Price
Forecasting System's Dash-based visualization dashboard.

This file provides configuration for dashboard sections, responsive layouts, and component-specific settings.
"""

import os
from settings import ENABLE_RESPONSIVE_UI, FALLBACK_INDICATOR_ENABLED, MAX_FORECAST_DAYS
from themes import RESPONSIVE_LAYOUTS

# List of dashboard sections in display order
DASHBOARD_SECTIONS = [
    "control_panel",
    "time_series",
    "distribution",
    "table",
    "comparison",
    "export"
]

# Layout configurations for different viewport sizes
DASHBOARD_LAYOUT = {
    "desktop": {
        "rows": [
            {"columns": [
                {"name": "control_panel", "width": 3},
                {"name": "time_series", "width": 9}
            ]},
            {"columns": [
                {"name": "distribution", "width": 6},
                {"name": "table", "width": 6}
            ]},
            {"columns": [
                {"name": "comparison", "width": 12}
            ]},
            {"columns": [
                {"name": "export", "width": 12}
            ]}
        ]
    },
    "tablet": {
        "rows": [
            {"columns": [
                {"name": "control_panel", "width": 12}
            ]},
            {"columns": [
                {"name": "time_series", "width": 12}
            ]},
            {"columns": [
                {"name": "distribution", "width": 6},
                {"name": "table", "width": 6}
            ]},
            {"columns": [
                {"name": "comparison", "width": 12}
            ]},
            {"columns": [
                {"name": "export", "width": 12}
            ]}
        ]
    },
    "mobile": {
        "rows": [
            {"columns": [
                {"name": "control_panel", "width": 12}
            ]},
            {"columns": [
                {"name": "time_series", "width": 12}
            ]},
            {"columns": [
                {"name": "distribution", "width": 12}
            ]},
            {"columns": [
                {"name": "table", "width": 12}
            ]},
            {"columns": [
                {"name": "comparison", "width": 12}
            ]},
            {"columns": [
                {"name": "export", "width": 12}
            ]}
        ]
    }
}

# Control panel component configuration
CONTROL_PANEL_CONFIG = {
    "show_product_dropdown": True,
    "show_date_range": True,
    "show_visualization_options": True,
    "show_refresh_button": True,
    "show_last_updated": True,
    "show_forecast_status": FALLBACK_INDICATOR_ENABLED
}

# Time series visualization configuration
TIME_SERIES_CONFIG = {
    "height": {
        "desktop": 500,
        "tablet": 400,
        "mobile": 350
    },
    "show_legend": True,
    "show_grid": True,
    "enable_zoom": True,
    "show_hover_info": True,
    "y_axis_title": "Price ($)",
    "x_axis_title": "Time"
}

# Probability distribution visualization configuration
DISTRIBUTION_CONFIG = {
    "height": {
        "desktop": 400,
        "tablet": 350,
        "mobile": 300
    },
    "show_legend": False,
    "show_grid": True,
    "bin_count": 30,
    "show_percentiles": True,
    "percentiles": [10, 50, 90],
    "y_axis_title": "Probability",
    "x_axis_title": "Price ($)"
}

# Forecast table component configuration
TABLE_CONFIG = {
    "page_size": 12,
    "max_rows": 72,
    "columns": ["Hour", "Point Forecast", "10th Percentile", "90th Percentile", "Range"],
    "sortable": True,
    "filterable": False,
    "export_format": "csv"
}

# Product comparison visualization configuration
COMPARISON_CONFIG = {
    "height": {
        "desktop": 450,
        "tablet": 400,
        "mobile": 350
    },
    "show_legend": True,
    "show_grid": True,
    "enable_zoom": True,
    "max_products": 6,
    "default_products": ["DALMP", "RTLMP"],
    "y_axis_title": "Price ($)",
    "x_axis_title": "Time"
}

# Export panel configuration
EXPORT_CONFIG = {
    "formats": ["csv", "excel", "json"],
    "default_format": "csv",
    "include_metadata": True,
    "filename_template": "forecast_{product}_{start_date}_{end_date}"
}

# Viewport breakpoints in pixels
VIEWPORT_BREAKPOINTS = {
    "mobile": 768,
    "tablet": 1024,
    "desktop": float('inf')
}

# Default viewport size (can be overridden by environment variable)
DEFAULT_VIEWPORT = os.getenv('DEFAULT_VIEWPORT', 'desktop')


def get_layout_config(viewport_size):
    """
    Returns the layout configuration for the specified viewport size.
    
    Args:
        viewport_size (str): The viewport size category ('desktop', 'tablet', 'mobile')
        
    Returns:
        dict: Layout configuration for the specified viewport size
    """
    if viewport_size in DASHBOARD_LAYOUT:
        return dict(DASHBOARD_LAYOUT[viewport_size])
    return dict(DASHBOARD_LAYOUT['desktop'])


def get_control_panel_config():
    """
    Returns the configuration for the control panel component.
    
    Returns:
        dict: Control panel configuration
    """
    return dict(CONTROL_PANEL_CONFIG)


def get_time_series_config(viewport_size):
    """
    Returns the configuration for the time series visualization component.
    
    Args:
        viewport_size (str): The viewport size category
        
    Returns:
        dict: Time series configuration for the specified viewport size
    """
    config = dict(TIME_SERIES_CONFIG)
    if viewport_size in config['height']:
        config['height'] = config['height'][viewport_size]
    else:
        config['height'] = config['height']['desktop']
    return config


def get_distribution_config(viewport_size):
    """
    Returns the configuration for the probability distribution component.
    
    Args:
        viewport_size (str): The viewport size category
        
    Returns:
        dict: Distribution configuration for the specified viewport size
    """
    config = dict(DISTRIBUTION_CONFIG)
    if viewport_size in config['height']:
        config['height'] = config['height'][viewport_size]
    else:
        config['height'] = config['height']['desktop']
    return config


def get_table_config(viewport_size):
    """
    Returns the configuration for the forecast table component.
    
    Args:
        viewport_size (str): The viewport size category
        
    Returns:
        dict: Table configuration for the specified viewport size
    """
    config = dict(TABLE_CONFIG)
    if viewport_size == 'mobile':
        config['page_size'] = 6
    elif viewport_size == 'tablet':
        config['page_size'] = 8
    return config


def get_comparison_config(viewport_size):
    """
    Returns the configuration for the product comparison component.
    
    Args:
        viewport_size (str): The viewport size category
        
    Returns:
        dict: Comparison configuration for the specified viewport size
    """
    config = dict(COMPARISON_CONFIG)
    if viewport_size in config['height']:
        config['height'] = config['height'][viewport_size]
    else:
        config['height'] = config['height']['desktop']
    return config


def get_export_config():
    """
    Returns the configuration for the export panel component.
    
    Returns:
        dict: Export panel configuration
    """
    return dict(EXPORT_CONFIG)


def get_viewport_size(width):
    """
    Determines the viewport size category based on width.
    
    Args:
        width (int): The viewport width in pixels
        
    Returns:
        str: Viewport size category ('mobile', 'tablet', 'desktop')
    """
    if width <= VIEWPORT_BREAKPOINTS['mobile']:
        return 'mobile'
    elif width <= VIEWPORT_BREAKPOINTS['tablet']:
        return 'tablet'
    else:
        return 'desktop'


def is_responsive_enabled():
    """
    Checks if responsive UI features are enabled.
    
    Returns:
        bool: True if responsive UI is enabled, False otherwise
    """
    return ENABLE_RESPONSIVE_UI


def get_section_config(section_name, viewport_size):
    """
    Returns the configuration for a specific dashboard section.
    
    Args:
        section_name (str): The name of the dashboard section
        viewport_size (str): The viewport size category
        
    Returns:
        dict: Configuration for the specified section
    """
    if section_name == "control_panel":
        return get_control_panel_config()
    elif section_name == "time_series":
        return get_time_series_config(viewport_size)
    elif section_name == "distribution":
        return get_distribution_config(viewport_size)
    elif section_name == "table":
        return get_table_config(viewport_size)
    elif section_name == "comparison":
        return get_comparison_config(viewport_size)
    elif section_name == "export":
        return get_export_config()
    else:
        raise ValueError(f"Unknown section name: {section_name}")