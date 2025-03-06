"""
Module that implements Dash callbacks for time series visualization components in the Electricity Market Price Forecasting Dashboard.
This module registers callbacks that handle interactions with the time series plot, including uncertainty toggle, time point selection, and responsive adjustments based on viewport changes.
"""
import json
import logging

import dash  # version 2.9.0+
from dash.dependencies import Input, Output, State, ClientsideFunction  # version 2.9.0+
from dash.exceptions import PreventUpdate  # version 2.9.0+
import pandas  # version 2.0.0+

from ..components.time_series import TIME_SERIES_GRAPH_ID, UNCERTAINTY_TOGGLE_ID, update_time_series, handle_viewport_change  # src/web/components/time_series.py
from ..components.probability_distribution import DISTRIBUTION_GRAPH_ID, update_distribution  # src/web/components/probability_distribution.py
from ..layouts.responsive import VIEWPORT_STORE_ID  # src/web/layouts/responsive.py
from ..utils.plot_helpers import extract_timestamp_from_click  # src/web/utils/plot_helpers.py
from ..data.forecast_loader import forecast_loader  # src/web/data/forecast_loader.py
from ..config.logging_config import get_logger  # src/web/config/logging_config.py
from ..callbacks.visualization_callbacks import DASHBOARD_STATE_STORE_ID  # src/web/callbacks/visualization_callbacks.py

# Initialize logger
logger = get_logger('time_series_callbacks')


def register_time_series_callbacks(app: dash.Dash):
    """
    Registers all callback functions for time series visualization with the Dash application

    Args:
        app (dash.Dash): The Dash application instance

    Returns:
        None: No return value
    """
    logger.info("Starting time series callbacks registration")

    # Register callback for handling uncertainty toggle changes
    @dash.callback(
        Output(TIME_SERIES_GRAPH_ID, 'figure'),
        Input(UNCERTAINTY_TOGGLE_ID, 'value'),
        State(TIME_SERIES_GRAPH_ID, 'figure'),
        State(DASHBOARD_STATE_STORE_ID, 'data')
    )
    def handle_uncertainty_toggle(show_uncertainty, current_time_series, dashboard_state):
        """
        Callback function that handles changes to the uncertainty toggle and updates the time series visualization

        Args:
            show_uncertainty (bool): Indicates whether to show uncertainty bands
            current_time_series (dict): The current time series visualization
            dashboard_state (dict): The current dashboard state

        Returns:
            dict: Updated time series visualization
        """
        logger.info("Uncertainty toggle changed")

        # Extract forecast data, product, and viewport size from dashboard state
        forecast_data = dashboard_state.get('forecast_data')
        product_id = dashboard_state.get('product_id')
        viewport_size = dashboard_state.get('viewport_size')

        # Update time series visualization with new uncertainty setting
        forecast_df = pandas.DataFrame(forecast_data) if forecast_data else None
        updated_time_series = update_time_series(
            graph_component=dcc.Graph(id=TIME_SERIES_GRAPH_ID, figure=current_time_series),
            forecast_df=forecast_df,
            product_id=product_id,
            show_uncertainty=show_uncertainty,
            viewport_size=viewport_size['size'] if viewport_size else 'desktop'
        )

        # Return the updated time series figure
        return updated_time_series.figure

    # Register callback for handling time series click events
    @dash.callback(
        Output(DISTRIBUTION_GRAPH_ID, 'figure'),
        Input(TIME_SERIES_GRAPH_ID, 'clickData'),
        State(DISTRIBUTION_GRAPH_ID, 'figure'),
        State(DASHBOARD_STATE_STORE_ID, 'data')
    )
    def handle_time_series_click(click_data, current_distribution, dashboard_state):
        """
        Callback function that handles clicks on the time series plot and updates the probability distribution

        Args:
            click_data (dict): Data from the click event on the time series plot
            current_distribution (dict): The current probability distribution visualization
            dashboard_state (dict): The current dashboard state

        Returns:
            dict: Updated probability distribution visualization
        """
        # Check if click_data is None (no click), prevent update if so
        if click_data is None:
            raise PreventUpdate

        # Extract timestamp from click_data using extract_timestamp_from_click
        timestamp = extract_timestamp_from_click(click_data)

        logger.info(f"Time point selected: {timestamp}")

        # Extract product from dashboard state
        product_id = dashboard_state.get('product_id')

        # Get forecast data for the selected timestamp
        forecast_data = dashboard_state.get('forecast_data')
        forecast_df = pandas.DataFrame(forecast_data) if forecast_data else None

        # Update probability distribution visualization with data for selected timestamp
        updated_distribution = update_distribution(
            graph_component=dcc.Graph(id=DISTRIBUTION_GRAPH_ID, figure=current_distribution),
            forecast_df=forecast_df,
            product_id=product_id,
            timestamp=timestamp,
            viewport_size=dashboard_state.get('viewport_size')['size'] if dashboard_state.get('viewport_size') else 'desktop'
        )

        # Return the updated distribution figure
        return updated_distribution.figure

    # Register callback for handling viewport size changes
    @dash.callback(
        Output(TIME_SERIES_GRAPH_ID, 'figure'),
        Input(VIEWPORT_STORE_ID, 'data'),
        State(TIME_SERIES_GRAPH_ID, 'figure')
    )
    def handle_viewport_change(viewport_size, current_time_series):
        """
        Callback function that handles viewport size changes and updates the time series visualization

        Args:
            viewport_size (str): The new viewport size
            current_time_series (dict): The current time series visualization

        Returns:
            dict: Updated time series visualization for new viewport
        """
        logger.info(f"Viewport size changed: {viewport_size}")

        # Update time series visualization for new viewport size using handle_viewport_change
        updated_time_series = handle_time_series_viewport_change(
            graph_component=dcc.Graph(id=TIME_SERIES_GRAPH_ID, figure=current_time_series),
            new_viewport_size=viewport_size['size'] if viewport_size else 'desktop'
        )

        # Return the updated time series figure
        return updated_time_series.figure

    # Register client-side callbacks for hover interactions
    register_clientside_callbacks(app)

    logger.info("Time series callbacks registration complete.")


def register_clientside_callbacks(app: dash.Dash):
    """
    Registers client-side callbacks for time series interactions

    Args:
        app (dash.Dash): The Dash application instance

    Returns:
        None: No return value
    """
    # Register client-side callback for hover interactions
    app.clientside_callback(
        ClientsideFunction(
            namespace='clientside',
            function_name='syncHoverData'
        ),
        Output('hidden-div', 'children'),  # Dummy output
        [Input(TIME_SERIES_GRAPH_ID, 'hoverData')]
    )

    # Register client-side callback for zoom synchronization
    app.clientside_callback(
        ClientsideFunction(
            namespace='clientside',
            function_name='syncZoom'
        ),
        Output('hidden-div', 'children'),  # Dummy output
        [Input(TIME_SERIES_GRAPH_ID, 'relayoutData')]
    )

    logger.info("Registered client-side callbacks")


def sync_hover_data(hover_data: dict) -> dict:
    """
    Client-side callback function that synchronizes hover data between visualizations

    Args:
        hover_data (dict): Hover data from the time series plot

    Returns:
        dict: Synchronized hover data for other components
    """
    # This function is implemented as a client-side callback
    # Extract timestamp from hover data
    # Format hover data for other components
    # Return the formatted hover data
    pass