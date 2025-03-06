"""
Module that implements general visualization callbacks for the Electricity Market Price Forecasting Dashboard.
This module handles dashboard state management, coordinated updates between visualization components,
and responsive layout adjustments based on viewport changes.
"""
import json
import logging
from typing import List

import dash  # version 2.9.0+
from dash.dependencies import Input, Output, State, ClientsideFunction  # version 2.9.0+
from dash.exceptions import PreventUpdate  # version 2.9.0+
import dash_core_components as dcc  # version 2.0.0+
import dash_html_components as html  # version 2.0.0+
import pandas  # version 2.0.0+

from ..components.time_series import TIME_SERIES_GRAPH_ID, handle_viewport_change as handle_time_series_viewport_change  # src/web/components/time_series.py
from ..components.probability_distribution import DISTRIBUTION_GRAPH_ID, handle_viewport_change as handle_distribution_viewport_change  # src/web/components/probability_distribution.py
from ..components.forecast_table import FORECAST_TABLE_ID, handle_viewport_change as handle_table_viewport_change  # src/web/components/forecast_table.py
from ..components.product_comparison import PRODUCT_COMPARISON_GRAPH_ID, handle_viewport_change as handle_comparison_viewport_change  # src/web/components/product_comparison.py
from ..layouts.responsive import VIEWPORT_STORE_ID  # src/web/layouts/responsive.py
from ..components.control_panel import PRODUCT_DROPDOWN_ID, DATE_RANGE_PICKER_ID, VISUALIZATION_OPTIONS_ID  # src/web/components/control_panel.py
from ..data.forecast_loader import forecast_loader  # src/web/data/forecast_loader.py
from ..config.logging_config import get_logger  # src/web/config/logging_config.py
from ..config.product_config import DEFAULT_PRODUCT  # src/web/config/product_config.py
from ..utils.date_helpers import parse_date, get_default_date_range  # src/web/utils/date_helpers.py

# Initialize logger
logger = get_logger('visualization_callbacks')

# Global constants for component IDs
DASHBOARD_STATE_STORE_ID = 'dashboard-state-store'
MAIN_CONTENT_DIV_ID = 'main-content'
VISUALIZATION_CONTAINER_ID = 'visualization-container'


def register_visualization_callbacks(app: dash.Dash):
    """
    Registers all general visualization callbacks with the Dash application
    """
    logger.info("Registering visualization callbacks...")

    # Register callback for dashboard state management
    register_dashboard_state_callback(app)

    # Register callback for coordinated viewport changes
    register_coordinated_viewport_callback(app)

    # Register client-side callbacks for responsive layout
    register_clientside_callbacks(app)

    logger.info("Visualization callbacks registration complete.")


def create_dashboard_state_store() -> dcc.Store:
    """
    Creates a store component to maintain dashboard state across callbacks
    """
    store = dcc.Store(
        id=DASHBOARD_STATE_STORE_ID,
        storage_type='session',  # Persist during user session
        data={}  # Initialize with empty data object
    )
    return store


def register_dashboard_state_callback(app: dash.Dash):
    """
    Registers the callback function that updates the dashboard state based on user interactions
    """
    @dash.callback(
        Output(DASHBOARD_STATE_STORE_ID, 'data'),
        Input(PRODUCT_DROPDOWN_ID, 'value'),
        Input(DATE_RANGE_PICKER_ID, 'value'),
        Input(VISUALIZATION_OPTIONS_ID, 'value'),
        Input(VIEWPORT_STORE_ID, 'data'),
        State(DASHBOARD_STATE_STORE_ID, 'data')
    )
    def update_dashboard_state(product_id, date_range, visualization_options, viewport_size, current_state):
        """
        Callback function that updates the dashboard state based on user interactions
        """
        logger.info("Updating dashboard state...")

        # Use DEFAULT_PRODUCT if product_id is None
        if product_id is None:
            product_id = DEFAULT_PRODUCT

        # Use default date range if date_range contains None values
        if date_range is None or any(d is None for d in date_range):
            start_date, end_date = get_default_date_range()
        else:
            start_date, end_date = date_range
            start_date = parse_date(start_date)
            end_date = parse_date(end_date)

        # Determine if uncertainty should be shown based on visualization_options
        show_uncertainty = 'uncertainty' in visualization_options

        # Determine if historical data should be shown based on visualization_options
        show_historical = 'historical' in visualization_options

        # Initialize empty dictionary if current_state is None
        if current_state is None:
            current_state = {}

        # Update state with product_id, date_range, visualization_options, and viewport_size
        state = {
            'product_id': product_id,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'show_uncertainty': show_uncertainty,
            'show_historical': show_historical,
            'viewport_size': viewport_size,
        }

        # If forecast data not in state or parameters changed, load forecast data
        if 'forecast_data' not in current_state or \
                current_state['product_id'] != product_id or \
                current_state['start_date'] != start_date.isoformat() or \
                current_state['end_date'] != end_date.isoformat():

            logger.info(f"Loading forecast data for product: {product_id}, date range: {start_date} to {end_date}")
            forecast_data = forecast_loader.load_forecast_by_date_range(product_id, start_date, end_date)
            state['forecast_data'] = forecast_data.to_dict('records') if forecast_data is not None else None

        # Update state with loaded forecast data
        current_state.update(state)

        logger.info("Dashboard state updated.")
        return current_state


def register_coordinated_viewport_callback(app: dash.Dash):
    """
    Registers the callback function that coordinates viewport changes across all visualization components
    """
    @dash.callback(
        Output(TIME_SERIES_GRAPH_ID, 'figure'),
        Output(DISTRIBUTION_GRAPH_ID, 'figure'),
        Output(FORECAST_TABLE_ID, 'style'),
        Output(PRODUCT_COMPARISON_GRAPH_ID, 'figure'),
        Input(VIEWPORT_STORE_ID, 'data'),
        State(TIME_SERIES_GRAPH_ID, 'figure'),
        State(DISTRIBUTION_GRAPH_ID, 'figure'),
        State(FORECAST_TABLE_ID, 'style'),
        State(PRODUCT_COMPARISON_GRAPH_ID, 'figure')
    )
    def handle_coordinated_viewport_change(viewport_size, time_series_figure, distribution_figure, forecast_table_style, comparison_figure):
        """
        Callback function that coordinates viewport changes across all visualization components
        """
        logger.info(f"Viewport size changed: {viewport_size}")

        # Update time series visualization for new viewport
        updated_time_series = handle_time_series_viewport_change(
            graph_component=dcc.Graph(id=TIME_SERIES_GRAPH_ID, figure=time_series_figure),
            new_viewport_size=viewport_size['size']
        )

        # Update distribution visualization for new viewport
        updated_distribution = handle_distribution_viewport_change(
            graph_component=dcc.Graph(id=DISTRIBUTION_GRAPH_ID, figure=distribution_figure),
            new_viewport_size=viewport_size['size']
        )

        # Update forecast table styles for new viewport
        updated_table_style = handle_table_viewport_change(
            table_component=dash_table.DataTable(id=FORECAST_TABLE_ID, style=forecast_table_style),
            new_viewport_size=viewport_size['size']
        )

        # Update product comparison visualization for new viewport
        updated_comparison = handle_comparison_viewport_change(
            graph_component=dcc.Graph(id=PRODUCT_COMPARISON_GRAPH_ID, figure=comparison_figure),
            new_viewport_size=viewport_size['size']
        )

        return (
            updated_time_series.figure,
            updated_distribution.figure,
            updated_table_style.style,
            updated_comparison.figure
        )


def register_clientside_callbacks(app: dash.Dash):
    """
    Registers client-side callbacks for responsive layout adjustments
    """
    logger.info("Registering client-side callbacks...")

    # Register client-side callback for container resizing
    app.clientside_callback(
        ClientsideFunction(
            namespace='clientside',
            function_name='resizeContainer'
        ),
        Output(VISUALIZATION_CONTAINER_ID, 'style'),
        [Input('window', 'resize')]
    )

    # Register client-side callback for layout adjustments
    app.clientside_callback(
        ClientsideFunction(
            namespace='clientside',
            function_name='adjustLayoutForViewport'
        ),
        Output(MAIN_CONTENT_DIV_ID, 'className'),
        [Input(VIEWPORT_STORE_ID, 'data')]
    )

    logger.info("Client-side callbacks registered.")


def adjust_layout_for_viewport(viewport_size: str) -> dict:
    """
    Client-side callback function that adjusts layout based on viewport size
    """
    # This function is implemented as a client-side callback
    # Determine layout configuration based on viewport_size
    # For mobile: single column layout, reduced margins
    # For tablet: controls above visualizations, medium margins
    # For desktop: side-by-side layout, normal margins
    # For large desktop: expanded layout with larger visualizations
    # Return the appropriate style dictionary for the layout
    pass


def load_forecast_data(product_id: str, start_date: pandas.Timestamp, end_date: pandas.Timestamp) -> pandas.DataFrame:
    """
    Helper function to load forecast data for visualization
    """
    logger.info(f"Loading forecast data for product {product_id} from {start_date} to {end_date}")
    try:
        # Load forecast data using forecast_loader.load_forecast_by_date_range
        forecast_df = forecast_loader.load_forecast_by_date_range(product_id, start_date, end_date)
        return forecast_df
    except Exception as e:
        logger.error(f"Error loading forecast data: {e}")
        return pandas.DataFrame()
    finally:
        logger.info("Data loading complete.")