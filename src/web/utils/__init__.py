"""
Initialization module for the utilities package in the Electricity Market Price Forecasting System's web visualization interface.

This module exports commonly used utility functions from various submodules to provide
a clean, unified interface for the rest of the application. It collects functionality for
date handling, formatting, plotting, error handling, caching, responsive design, and URL manipulation.

Exported Modules:
    - date_helpers: Functions for date and time manipulation
    - formatting: Functions for formatting display values
    - plot_helpers: Functions for creating visualizations
    - error_handlers: Functions for handling errors
    - caching: Functions for performance optimization
    - responsive_helpers: Functions for responsive design
    - url_helpers: Functions for URL manipulation

Version: 1.0.0
"""

# Module version
__version__ = '1.0.0'

# Import all utilities from submodules
from .date_helpers import *
from .formatting import *
from .plot_helpers import *
from .error_handlers import *
from .caching import *
from .responsive_helpers import *
from .url_helpers import *

# For better IDE support and documentation, explicitly list imported items
# Date helper functions
__all__ = [
    # From date_helpers
    'get_current_time_cst', 'format_date', 'format_time', 'format_datetime',
    'parse_date', 'parse_datetime', 'get_default_date_range', 
    'get_forecast_date_range', 'date_to_dash_format', 'dash_date_to_datetime',
    'get_date_hour_label',
    
    # From formatting
    'format_price', 'format_percentage', 'format_date', 'format_time', 
    'format_datetime', 'format_hour', 'format_range', 'format_large_number', 
    'format_with_unit', 'format_confidence_interval', 'truncate_string', 
    'format_tooltip_value',
    
    # From plot_helpers
    'create_time_series_plot', 'add_uncertainty_bands', 
    'create_probability_distribution_plot', 'create_product_comparison_plot', 
    'create_heatmap_plot', 'create_forecast_accuracy_plot', 'configure_axes', 
    'create_hover_template', 'apply_responsive_layout', 'format_axis_date', 
    'add_fallback_indicator',
    
    # From error_handlers
    'handle_callback_error', 'create_error_message', 'format_exception',
    'handle_data_loading_error', 'handle_visualization_error', 'is_fallback_data',
    'create_fallback_notice', 'ErrorHandler',
    
    # From caching
    'memoize', 'timed_cache', 'cache_forecast_data', 'clear_cache', 
    'get_cache_stats',
    
    # From responsive_helpers
    'detect_viewport_size', 'get_responsive_style', 'create_responsive_container',
    'create_responsive_grid', 'is_component_visible', 'get_responsive_font_size',
    'get_responsive_dimension', 'create_viewport_callback', 'clear_visibility_cache',
    'VIEWPORT_SIZES', 'DEFAULT_VIEWPORT',
    
    # From url_helpers
    'build_api_url', 'add_query_params', 'get_query_params', 
    'build_forecast_api_url', 'build_dashboard_url', 'is_valid_url',
    
    # Module version
    '__version__'
]