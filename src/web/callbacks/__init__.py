"""
Initialization module for the callbacks package in the Electricity Market Price Forecasting System's Dash-based visualization interface.
This module provides a centralized entry point for registering all callback functions with the Dash application, organizing them by functional area to maintain a clean architecture.
"""

# Import necessary modules
import dash  # version 2.9.0+

# Internal imports
from .error_callbacks import register_error_callbacks  # src/web/callbacks/error_callbacks.py
from .control_callbacks import register_control_callbacks  # src/web/callbacks/control_callbacks.py
from .time_series_callbacks import register_time_series_callbacks  # src/web/callbacks/time_series_callbacks.py
from .visualization_callbacks import register_visualization_callbacks  # src/web/callbacks/visualization_callbacks.py
from .product_comparison_callbacks import register_product_comparison_callbacks  # src/web/callbacks/product_comparison_callbacks.py
from .data_export_callbacks import register_data_export_callbacks  # src/web/callbacks/data_export_callbacks.py
from ..config.logging_config import get_logger  # src/web/config/logging_config.py

# Initialize logger
logger = get_logger('callbacks')


def register_all_callbacks(app: dash.Dash):
    """
    Registers all callback functions with the Dash application, organizing them by functional area

    Args:
        app (dash.Dash): The Dash application instance

    Returns:
        None: No return value
    """
    logger.info("Registering all callbacks...")

    # Register error handling callbacks
    register_error_callbacks(app)

    # Register control panel callbacks
    register_control_callbacks(app)

    # Register time series visualization callbacks
    register_time_series_callbacks(app)

    # Register general visualization callbacks
    register_visualization_callbacks(app)

    # Register product comparison callbacks
    register_product_comparison_callbacks(app)

    # Register data export callbacks
    register_data_export_callbacks(app)

    logger.info("All callbacks registration complete.")