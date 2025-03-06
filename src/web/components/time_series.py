"""
Component module that implements the time series visualization for the 
Electricity Market Price Forecasting Dashboard. This module provides functions 
to create and update interactive time series plots showing forecast data with 
uncertainty bands, supporting responsive design and various user interactions.
"""

import logging
from typing import Optional, List

import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import pandas as pd

from ..utils.plot_helpers import (
    create_time_series_plot,
    add_uncertainty_bands,
    apply_responsive_layout,
    add_fallback_indicator
)
from ..utils.error_handlers import is_fallback_data
from ..config.dashboard_config import get_time_series_config
from ..config.themes import DEFAULT_THEME, CHART_CONFIG
from ..config.product_config import get_product_display_name, DEFAULT_PRODUCT
from ..data.forecast_loader import extract_forecast_percentiles

# Global constants
TIME_SERIES_GRAPH_ID = "time-series-graph"
UNCERTAINTY_TOGGLE_ID = "uncertainty-toggle"
DEFAULT_PERCENTILES = [10, 90]

# Configure logger
logger = logging.getLogger(__name__)


def create_time_series_component(
    forecast_df: pd.DataFrame,
    product_id: str = None,
    viewport_size: Optional[str] = None,
    show_uncertainty: Optional[bool] = None,
    theme: Optional[str] = None
) -> dcc.Graph:
    """
    Creates a Dash component containing the time series visualization.
    
    Args:
        forecast_df: DataFrame containing forecast data
        product_id: Product identifier for styling and formatting
        viewport_size: Viewport size category (mobile, tablet, desktop)
        show_uncertainty: Whether to show uncertainty bands
        theme: Theme name for styling
        
    Returns:
        Dash Graph component with time series visualization
    """
    # Use default values if not provided
    if theme is None:
        theme = DEFAULT_THEME
    if product_id is None:
        product_id = DEFAULT_PRODUCT
    if viewport_size is None:
        viewport_size = 'desktop'
    if show_uncertainty is None:
        show_uncertainty = True
    
    # Check for empty dataframe
    if forecast_df is None or forecast_df.empty:
        logger.warning("Empty or None forecast DataFrame provided")
        return create_empty_time_series("No forecast data available", viewport_size)
    
    try:
        # Get component configuration based on viewport size
        config = get_time_series_config(viewport_size)
        
        # Create title using product display name
        title = f"{get_product_display_name(product_id)} Price Forecast"
        
        # Create time series plot
        fig = create_time_series_plot(forecast_df, product_id, title, theme)
        
        # Add uncertainty bands if requested
        if show_uncertainty:
            fig = add_uncertainty_bands(fig, forecast_df, product_id, DEFAULT_PERCENTILES[0], DEFAULT_PERCENTILES[1], theme)
        
        # Apply responsive layout adjustments
        fig = apply_responsive_layout(fig, viewport_size, config['height'])
        
        # Add fallback indicator if using fallback data
        if is_fallback_data(forecast_df):
            fig = add_fallback_indicator(fig, forecast_df)
        
        # Create and return the Graph component
        return dcc.Graph(
            id=TIME_SERIES_GRAPH_ID,
            figure=fig,
            config=CHART_CONFIG,
            style={'height': config['height']}
        )
    except Exception as e:
        logger.error(f"Error creating time series component: {str(e)}")
        return create_empty_time_series(f"Error creating visualization: {str(e)}", viewport_size)


def update_time_series(
    graph_component: dcc.Graph,
    forecast_df: pd.DataFrame,
    product_id: str,
    show_uncertainty: Optional[bool] = None,
    viewport_size: Optional[str] = None,
    theme: Optional[str] = None
) -> dcc.Graph:
    """
    Updates an existing time series visualization with new data or options.
    
    Args:
        graph_component: Existing Graph component to update
        forecast_df: New forecast DataFrame
        product_id: Product identifier
        show_uncertainty: Whether to show uncertainty bands
        viewport_size: Viewport size category
        theme: Theme name for styling
        
    Returns:
        Updated Graph component
    """
    # Use default values if not provided
    if theme is None:
        theme = DEFAULT_THEME
    if viewport_size is None:
        viewport_size = 'desktop'
    
    # Check for empty dataframe
    if forecast_df is None or forecast_df.empty:
        logger.warning("Empty or None forecast DataFrame provided for update")
        empty_fig = create_empty_time_series("No forecast data available", viewport_size)
        graph_component.figure = empty_fig.figure
        return graph_component
    
    try:
        # Determine show_uncertainty from current state if not provided
        if show_uncertainty is None:
            # Try to determine from the current figure
            current_fig = getattr(graph_component, 'figure', None)
            # Default to True if can't determine
            show_uncertainty = True
            # Check if there are more than one trace (main line + uncertainty band)
            if current_fig and hasattr(current_fig, 'data'):
                show_uncertainty = len(current_fig.data) > 1
        
        # Get component configuration
        config = get_time_series_config(viewport_size)
        
        # Create title using product display name
        title = f"{get_product_display_name(product_id)} Price Forecast"
        
        # Create new time series plot
        fig = create_time_series_plot(forecast_df, product_id, title, theme)
        
        # Add uncertainty bands if requested
        if show_uncertainty:
            fig = add_uncertainty_bands(fig, forecast_df, product_id, DEFAULT_PERCENTILES[0], DEFAULT_PERCENTILES[1], theme)
        
        # Apply responsive layout
        fig = apply_responsive_layout(fig, viewport_size, config['height'])
        
        # Add fallback indicator if using fallback data
        if is_fallback_data(forecast_df):
            fig = add_fallback_indicator(fig, forecast_df)
        
        # Update the graph component's figure
        graph_component.figure = fig
        
        return graph_component
    except Exception as e:
        logger.error(f"Error updating time series component: {str(e)}")
        # Create a simple empty figure if there's an error
        empty_fig = go.Figure()
        empty_fig.add_annotation(
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            text=f"Error updating visualization: {str(e)}",
            showarrow=False,
            font=dict(size=14),
            align="center"
        )
        graph_component.figure = empty_fig
        return graph_component


def create_uncertainty_toggle(initial_state: Optional[bool] = None) -> html.Div:
    """
    Creates a toggle switch for showing/hiding uncertainty bands.
    
    Args:
        initial_state: Initial state of the toggle (checked or unchecked)
        
    Returns:
        Div containing the toggle switch
    """
    if initial_state is None:
        initial_state = True
    
    return html.Div([
        html.Label([
            dcc.Checklist(
                id=UNCERTAINTY_TOGGLE_ID,
                options=[{'label': 'Show uncertainty bands', 'value': 'show'}],
                value=['show'] if initial_state else [],
                inline=True
            )
        ]),
    ], style={'margin-top': '10px', 'margin-bottom': '10px'})


def get_time_series_with_uncertainty(
    forecast_df: pd.DataFrame,
    product_id: str,
    percentiles: Optional[List[int]] = None,
    theme: Optional[str] = None
) -> go.Figure:
    """
    Creates a time series visualization with uncertainty bands.
    
    Args:
        forecast_df: DataFrame containing forecast data
        product_id: Product identifier for styling and formatting
        percentiles: List of percentiles to use for uncertainty bands
        theme: Theme name for styling
        
    Returns:
        Plotly figure with time series and uncertainty bands
    """
    # Use default values if not provided
    if theme is None:
        theme = DEFAULT_THEME
    if percentiles is None:
        percentiles = DEFAULT_PERCENTILES
    
    # Check for empty dataframe
    if forecast_df is None or forecast_df.empty:
        logger.warning("Empty or None forecast DataFrame provided")
        # Create empty figure with message
        fig = go.Figure()
        fig.add_annotation(
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            text="No forecast data available",
            showarrow=False,
            font=dict(size=14),
            align="center"
        )
        return fig
    
    try:
        # Create title using product display name
        title = f"{get_product_display_name(product_id)} Price Forecast"
        
        # Create base time series plot
        fig = create_time_series_plot(forecast_df, product_id, title, theme)
        
        # Add uncertainty bands
        fig = add_uncertainty_bands(fig, forecast_df, product_id, percentiles[0], percentiles[1], theme)
        
        return fig
    except Exception as e:
        logger.error(f"Error creating time series with uncertainty: {str(e)}")
        # Create error figure
        fig = go.Figure()
        fig.add_annotation(
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            text=f"Error creating visualization: {str(e)}",
            showarrow=False,
            font=dict(size=14),
            align="center"
        )
        return fig


def handle_viewport_change(
    graph_component: dcc.Graph,
    new_viewport_size: str
) -> dcc.Graph:
    """
    Updates time series visualization for a new viewport size.
    
    Args:
        graph_component: Existing Graph component to update
        new_viewport_size: New viewport size category
        
    Returns:
        Updated Graph component for new viewport
    """
    try:
        # Get the current figure
        fig = getattr(graph_component, 'figure', None)
        if fig is None:
            logger.warning("No figure found in graph component")
            return graph_component
        
        # Get configuration for new viewport size
        config = get_time_series_config(new_viewport_size)
        
        # Apply responsive layout adjustments
        fig = apply_responsive_layout(fig, new_viewport_size, config['height'])
        
        # Update the graph component's figure
        graph_component.figure = fig
        
        return graph_component
    except Exception as e:
        logger.error(f"Error handling viewport change: {str(e)}")
        return graph_component


def create_empty_time_series(
    message: str,
    viewport_size: Optional[str] = None
) -> dcc.Graph:
    """
    Creates an empty time series visualization with a message.
    
    Args:
        message: Message to display in the empty visualization
        viewport_size: Viewport size category
        
    Returns:
        Graph component with empty visualization
    """
    # Use default viewport if not provided
    if viewport_size is None:
        viewport_size = 'desktop'
    
    try:
        # Get configuration for viewport size
        config = get_time_series_config(viewport_size)
        
        # Create an empty figure with just the message
        fig = go.Figure()
        
        # Add annotation with the message
        fig.add_annotation(
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            text=message,
            showarrow=False,
            font=dict(size=14),
            align="center"
        )
        
        # Apply responsive layout
        fig = apply_responsive_layout(fig, viewport_size, config['height'])
        
        # Create and return the Graph component
        return dcc.Graph(
            id=TIME_SERIES_GRAPH_ID,
            figure=fig,
            config=CHART_CONFIG,
            style={'height': config['height']}
        )
    except Exception as e:
        logger.error(f"Error creating empty time series: {str(e)}")
        # Create bare-minimum graph if everything else fails
        return dcc.Graph(
            id=TIME_SERIES_GRAPH_ID,
            figure=go.Figure(),
            config=CHART_CONFIG
        )