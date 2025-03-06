"""
Main dashboard layout module for the Electricity Market Price Forecasting System's Dash-based visualization interface.
This module defines the structure and organization of the dashboard, integrating various components like control panel,
time series visualization, probability distribution, forecast table, product comparison, and export functionality
into a cohesive and responsive layout.
"""

import logging
from typing import Dict, Optional

# dash_bootstrap_components v1.0.0+
import dash_bootstrap_components as dbc
# dash v2.9.0+
from dash_html_components import Div
from dash_core_components import Store

from ..layouts.responsive import create_responsive_container, create_responsive_row, create_viewport_store, VIEWPORT_STORE_ID
from ..config.dashboard_config import get_layout_config, DASHBOARD_SECTIONS, DEFAULT_VIEWPORT
from ..components.control_panel import create_control_panel, CONTROL_PANEL_ID
from ..components.time_series import create_time_series_component, TIME_SERIES_GRAPH_ID
from ..components.probability_distribution import create_distribution_component, DISTRIBUTION_GRAPH_ID
from ..components.forecast_table import create_forecast_table, FORECAST_TABLE_ID
from ..components.product_comparison import create_product_comparison, PRODUCT_COMPARISON_GRAPH_ID
from ..components.export_panel import create_export_panel, EXPORT_PANEL_ID
from ..config.product_config import DEFAULT_PRODUCT
from ..config.themes import DEFAULT_THEME
from ..utils.date_helpers import get_default_date_range

# Initialize logger
logger = logging.getLogger(__name__)

# Define IDs for dashboard sections
MAIN_DASHBOARD_ID = 'main-dashboard'
TIME_SERIES_SECTION_ID = 'time-series-section'
DISTRIBUTION_SECTION_ID = 'distribution-section'
TABLE_SECTION_ID = 'table-section'
COMPARISON_SECTION_ID = 'comparison-section'
EXPORT_SECTION_ID = 'export-section'


def create_main_dashboard(forecast_data: Dict, viewport_size: Optional[str] = None, theme: Optional[str] = None) -> dbc.Container:
    """
    Creates the main dashboard layout with all visualization components

    Args:
        forecast_data: Dictionary containing forecast data
        viewport_size: Optional viewport size category (desktop, tablet, mobile)
        theme: Optional theme name for styling

    Returns:
        Main dashboard container with all components
    """
    logger.info("Creating main dashboard")

    # Use default viewport if not provided
    if viewport_size is None:
        viewport_size = DEFAULT_VIEWPORT

    # Use default theme if not provided
    if theme is None:
        theme = DEFAULT_THEME

    # Get layout configuration for the current viewport size
    layout_config = get_layout_config(viewport_size)

    # Create viewport store for responsive layout
    viewport_store = create_viewport_store()

    # Create empty components dictionary
    components = {}

    # Create control panel component and add to components
    components['control_panel'] = create_control_panel(forecast_data, theme, viewport_size)

    # Create time series visualization component and add to components
    components['time_series'] = create_time_series_component(forecast_data, viewport_size=viewport_size, theme=theme)

    # Create probability distribution component and add to components
    components['distribution'] = create_distribution_component(forecast_data, viewport_size=viewport_size, theme=theme)

    # Create forecast table component and add to components
    components['table'] = create_forecast_table(forecast_data, viewport_size=viewport_size, theme=theme)

    # Create product comparison component and add to components
    components['comparison'] = create_product_comparison(forecast_data, viewport_size=viewport_size, theme=theme)

    # Create export panel component and add to components
    components['export'] = create_export_panel(viewport_size)

    # Create rows and columns based on layout configuration
    rows = create_dashboard_rows(layout_config, components, viewport_size)

    # Arrange components in responsive container
    container = create_responsive_container(
        children=[viewport_store] + rows,
        viewport_size=viewport_size,
        id=MAIN_DASHBOARD_ID
    )

    # Return the complete dashboard container
    return container


def get_initial_dashboard_state() -> dict:
    """
    Returns the initial state for the dashboard

    Returns:
        Initial state dictionary with default values
    """
    # Get default date range
    start_date, end_date = get_default_date_range()

    # Create initial state dictionary with default product
    initial_state = {
        'product': DEFAULT_PRODUCT,
    }

    # Add default date range to state
    initial_state['start_date'] = start_date.isoformat()
    initial_state['end_date'] = end_date.isoformat()

    # Add default visualization options to state
    initial_state['show_uncertainty'] = True
    initial_state['show_historical'] = False

    # Return the initial state dictionary
    return initial_state


def create_dashboard_section(component: Div, section_id: str, title: str, viewport_size: Optional[str] = None) -> dbc.Card:
    """
    Creates a section of the dashboard with appropriate styling

    Args:
        component: Dash HTML component to include in the section
        section_id: ID for the section
        title: Title for the section
        viewport_size: Optional viewport size category

    Returns:
        Card containing the section component
    """
    # Use default viewport if not provided
    if viewport_size is None:
        viewport_size = DEFAULT_VIEWPORT

    # Create card header with section title
    card_header = dbc.CardHeader(title)

    # Create card body with the provided component
    card_body = dbc.CardBody(component)

    # Create complete card with appropriate styling and ID
    card = dbc.Card(
        [card_header, card_body],
        className="mb-4 shadow-sm",
        id=section_id
    )

    # Return the card component
    return card


def create_dashboard_rows(layout_config: dict, components: dict, viewport_size: str) -> list:
    """
    Creates rows and columns for the dashboard based on layout configuration

    Args:
        layout_config: Layout configuration dictionary
        components: Dictionary of components to include in the layout
        viewport_size: Viewport size category

    Returns:
        List of responsive row components
    """
    # Initialize empty list for rows
    rows = []

    # For each row in layout_config['rows']:
    for row_config in layout_config['rows']:
        # Create columns list for the row
        columns = []

        # For each column in row['columns']:
        for col_config in row_config['columns']:
            # Get component for column['name'] from components dictionary
            component_name = col_config['name']
            component = components.get(component_name)

            # Add component to columns list with specified width
            columns.append({
                'children': component,
                'width': col_config['width']
            })

        # Create responsive row with columns
        row = create_responsive_row(columns, viewport_size=viewport_size)

        # Add row to rows list
        rows.append(row)

    # Return the list of rows
    return rows


def update_dashboard_for_viewport(dashboard: dbc.Container, new_viewport_size: str, forecast_data: Dict, theme: Optional[str] = None) -> dbc.Container:
    """
    Updates the dashboard layout for a new viewport size

    Args:
        dashboard: The existing dashboard container
        new_viewport_size: The new viewport size category
        forecast_data: Dictionary containing forecast data
        theme: Optional theme name for styling

    Returns:
        Updated dashboard container
    """
    logger.info(f"Updating dashboard for new viewport size: {new_viewport_size}")

    # Create new dashboard with create_main_dashboard using new viewport size
    new_dashboard = create_main_dashboard(forecast_data, new_viewport_size, theme)

    # Return the new dashboard container
    return new_dashboard