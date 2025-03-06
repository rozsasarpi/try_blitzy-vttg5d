"""
Initialization module for the web configuration package of the Electricity Market Price Forecasting System.

This file imports and exposes key configuration components from various configuration modules,
providing a centralized access point for all configuration settings used in the web visualization interface.
"""

# Import from settings module
from .settings import (
    BASE_DIR, DEBUG, ENVIRONMENT, API_BASE_URL, 
    CACHE_TIMEOUT, LOG_LEVEL, MAX_FORECAST_DAYS,
    DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT, DEFAULT_DATETIME_FORMAT,
    SERVER_HOST, SERVER_PORT, ENABLE_RESPONSIVE_UI
)

# Import from themes module
from .themes import (
    DEFAULT_THEME, AVAILABLE_THEMES, THEME_COLORS, PRODUCT_COLORS, STATUS_COLORS,
    CHART_CONFIG, get_theme_colors, get_product_color, get_status_color,
    get_plot_layout, get_uncertainty_style
)

# Import from dashboard_config module
from .dashboard_config import (
    DASHBOARD_TITLE, DASHBOARD_SUBTITLE, DASHBOARD_REFRESH_INTERVAL_SECONDS as REFRESH_INTERVAL,
    DEFAULT_TIMEFRAME_DAYS, DASHBOARD_SECTIONS, DASHBOARD_LAYOUT, 
    RESPONSIVE_LAYOUTS, VIEWPORT_BREAKPOINTS as BREAKPOINTS,
    get_layout_config, get_section_config as get_component_config,
    is_responsive_enabled as is_component_visible
)

# Import from product_config module
from .product_config import (
    PRODUCTS, PRODUCT_DETAILS, DEFAULT_PRODUCT, PRODUCT_COMPARISON_DEFAULTS,
    get_product_display_name, get_product_description, get_product_unit,
    get_product_dropdown_options
)

# Import from logging_config module
from .logging_config import configure_logging as setup_logging, get_logger, LOG_DIR

# Define PRODUCT_DISPLAY_NAMES based on PRODUCT_DETAILS
PRODUCT_DISPLAY_NAMES = {
    product_id: get_product_display_name(product_id) for product_id in PRODUCTS
}

# Define ASSETS_FOLDER based on BASE_DIR
ASSETS_FOLDER = BASE_DIR / 'assets'

def load_env_vars():
    """
    Load environment variables from .env file if available.
    """
    import os
    from pathlib import Path
    
    env_file = Path(BASE_DIR).parent / '.env'
    if env_file.exists():
        try:
            from dotenv import load_dotenv  # version 1.0.0
            load_dotenv(str(env_file))
            logger = get_logger("web.config")
            logger.info(f"Loaded environment variables from {env_file}")
        except ImportError:
            print("dotenv package not installed, skipping .env file loading")
    else:
        print(f"No .env file found at {env_file}")

def ensure_directories():
    """
    Ensure required directories exist for the application.
    """
    import os
    from pathlib import Path
    
    dirs = [
        Path(BASE_DIR) / 'logs',
        Path(BASE_DIR) / 'cache',
        Path(BASE_DIR) / 'assets'
    ]
    
    for directory in dirs:
        directory.mkdir(parents=True, exist_ok=True)

def get_asset_path(relative_path):
    """
    Get the full path to an asset file.
    
    Args:
        relative_path: Path relative to assets folder
        
    Returns:
        Full path to the asset
    """
    from pathlib import Path
    return Path(ASSETS_FOLDER) / relative_path

def get_default_date_range():
    """
    Function to get the default date range for the dashboard.
    
    Returns:
        Tuple of (start_date, end_date) with default values
    """
    import datetime
    
    today = datetime.datetime.now()
    start_date = today.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
    end_date = start_date + datetime.timedelta(days=MAX_FORECAST_DAYS - 1)
    return start_date, end_date

def get_product_endpoint(product_id):
    """
    Get API endpoint for a product's forecast data.
    
    Args:
        product_id: Product identifier
        
    Returns:
        API endpoint URL for the product's forecast data
    """
    return f"forecasts/{product_id.lower()}"

def initialize_config():
    """
    Initializes the configuration by loading environment variables and setting up logging.
    """
    # Call necessary initialization functions
    load_env_vars()
    ensure_directories()
    setup_logging()
    
    # Log that configuration has been initialized
    logger = get_logger("web.config")
    logger.info("Web configuration initialized")