"""
Component module that implements the probability distribution visualization for the
Electricity Market Price Forecasting Dashboard. This module provides functions to create
and update interactive probability distribution plots showing the uncertainty in price
forecasts for specific timestamps, supporting responsive design and various user interactions.
"""

from typing import Dict, List, Optional, Union

import dash_core_components as dcc  # version: 2.9.0+
import dash_html_components as html  # version: 2.9.0+
import pandas as pd  # version: 2.0.0+
import plotly.graph_objects as go  # version: 5.14.0+
import logging  # standard library

from ..utils.plot_helpers import (
    create_probability_distribution_plot,
    apply_responsive_layout,
    add_fallback_indicator
)
from ..config.dashboard_config import get_distribution_config
from ..config.themes import DEFAULT_THEME, CHART_CONFIG
from ..config.product_config import get_product_display_name, DEFAULT_PRODUCT
from ..data.data_processor import prepare_distribution_data
from ..utils.formatting import format_datetime, format_price
from ..utils.error_handlers import is_fallback_data

# Component ID for the distribution graph
DISTRIBUTION_GRAPH_ID = 'distribution-graph'

# Default percentiles to display in distribution
DEFAULT_PERCENTILES = [10, 50, 90]

# Initialize logger
logger = logging.getLogger(__name__)


def create_distribution_component(
    forecast_df: pd.DataFrame,
    product_id: str = None,
    timestamp: pd.Timestamp = None,
    viewport_size: Optional[str] = None,
    theme: Optional[str] = None
) -> dcc.Graph:
    """
    Creates a Dash component containing the probability distribution visualization.
    
    Args:
        forecast_df: DataFrame containing forecast data
        product_id: Product identifier (defaults to DEFAULT_PRODUCT if None)
        timestamp: Specific timestamp to show distribution for
        viewport_size: Viewport size category for responsive layout
        theme: Theme name for styling
        
    Returns:
        Dash Graph component with probability distribution visualization
    """
    logger.debug(f"Creating distribution component for product: {product_id}, timestamp: {timestamp}")
    
    # Use defaults if not provided
    if theme is None:
        theme = DEFAULT_THEME
    
    if product_id is None:
        product_id = DEFAULT_PRODUCT
    
    if viewport_size is None:
        viewport_size = 'desktop'
    
    # If no timestamp is provided or forecast_df is empty, return empty distribution
    if timestamp is None or forecast_df.empty:
        return create_empty_distribution(
            message="Select a timestamp to view distribution",
            viewport_size=viewport_size
        )
    
    try:
        # Get distribution configuration for the viewport size
        config = get_distribution_config(viewport_size)
        
        # Prepare distribution data
        distribution_data = prepare_distribution_data(forecast_df, product_id, timestamp)
        
        # Create distribution plot
        fig = create_probability_distribution_plot(
            distribution_data,
            product_id,
            timestamp,
            theme
        )
        
        # Apply responsive layout
        fig = apply_responsive_layout(
            fig,
            viewport_size,
            config['height']
        )
        
        # Add fallback indicator if using fallback data
        if is_fallback_data(forecast_df):
            fig = add_fallback_indicator(fig, forecast_df)
        
        # Create Dash Graph component
        graph = dcc.Graph(
            id=DISTRIBUTION_GRAPH_ID,
            figure=fig,
            config=CHART_CONFIG,
            style={'height': config['height']}
        )
        
        return graph
    
    except Exception as e:
        logger.error(f"Error creating distribution component: {str(e)}")
        return create_empty_distribution(
            message=f"Error creating distribution: {str(e) if viewport_size == 'desktop' else 'An error occurred'}",
            viewport_size=viewport_size
        )


def update_distribution(
    graph_component: dcc.Graph,
    forecast_df: pd.DataFrame,
    product_id: str,
    timestamp: pd.Timestamp,
    viewport_size: Optional[str] = None,
    theme: Optional[str] = None
) -> dcc.Graph:
    """
    Updates an existing probability distribution visualization with new data or options.
    
    Args:
        graph_component: Existing Dash Graph component to update
        forecast_df: DataFrame containing forecast data
        product_id: Product identifier
        timestamp: Specific timestamp to show distribution for
        viewport_size: Viewport size category for responsive layout
        theme: Theme name for styling
        
    Returns:
        Updated Graph component
    """
    logger.debug(f"Updating distribution component for product: {product_id}, timestamp: {timestamp}")
    
    # Use defaults if not provided
    if theme is None:
        theme = DEFAULT_THEME
    
    if viewport_size is None:
        viewport_size = 'desktop'
    
    try:
        # Get distribution configuration for the viewport size
        config = get_distribution_config(viewport_size)
        
        # Prepare distribution data
        distribution_data = prepare_distribution_data(forecast_df, product_id, timestamp)
        
        # Create new distribution plot
        fig = create_probability_distribution_plot(
            distribution_data,
            product_id,
            timestamp,
            theme
        )
        
        # Apply responsive layout
        fig = apply_responsive_layout(
            fig,
            viewport_size,
            config['height']
        )
        
        # Add fallback indicator if using fallback data
        if is_fallback_data(forecast_df):
            fig = add_fallback_indicator(fig, forecast_df)
        
        # Update the graph's figure
        updated_graph = graph_component
        updated_graph.figure = fig
        
        return updated_graph
    
    except Exception as e:
        logger.error(f"Error updating distribution component: {str(e)}")
        # Return the original component if update fails
        return graph_component


def get_distribution_with_percentiles(
    forecast_df: pd.DataFrame,
    product_id: str,
    timestamp: pd.Timestamp,
    percentiles: Optional[List[int]] = None,
    theme: Optional[str] = None
) -> go.Figure:
    """
    Creates a probability distribution visualization with percentile indicators.
    
    Args:
        forecast_df: DataFrame containing forecast data
        product_id: Product identifier
        timestamp: Specific timestamp to show distribution for
        percentiles: List of percentiles to indicate (defaults to DEFAULT_PERCENTILES)
        theme: Theme name for styling
        
    Returns:
        Plotly figure with distribution and percentile indicators
    """
    logger.debug(f"Creating distribution with percentiles for product: {product_id}, timestamp: {timestamp}")
    
    # Use defaults if not provided
    if theme is None:
        theme = DEFAULT_THEME
    
    if percentiles is None:
        percentiles = DEFAULT_PERCENTILES
    
    try:
        # Prepare distribution data
        distribution_data = prepare_distribution_data(forecast_df, product_id, timestamp)
        
        # Create base distribution plot
        fig = create_probability_distribution_plot(
            distribution_data,
            product_id,
            timestamp,
            theme
        )
        
        # Extract samples from distribution data
        if not distribution_data.empty:
            sample_columns = [col for col in distribution_data.columns if col.startswith('value')]
            if sample_columns:
                samples = distribution_data[sample_columns[0]].values
                
                # Add vertical lines for each percentile
                for percentile in percentiles:
                    percentile_value = np.percentile(samples, percentile)
                    
                    # Add vertical line
                    fig.add_shape(
                        type="line",
                        x0=percentile_value,
                        x1=percentile_value,
                        y0=0,
                        y1=1,
                        yref="paper",
                        line=dict(
                            color="rgba(0, 0, 0, 0.7)",
                            width=1.5,
                            dash="dash"
                        ),
                        name=f"{percentile}th Percentile"
                    )
                    
                    # Add annotation for percentile value
                    fig.add_annotation(
                        x=percentile_value,
                        y=0.95,
                        yref="paper",
                        text=f"{percentile}%: {format_price(percentile_value, product_id)}",
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=1,
                        arrowcolor="rgba(0, 0, 0, 0.5)"
                    )
        
        return fig
    
    except Exception as e:
        logger.error(f"Error creating distribution with percentiles: {str(e)}")
        # Create an empty figure if there's an error
        fig = go.Figure()
        fig.add_annotation(
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            text=f"Error: {str(e)}",
            showarrow=False,
            font=dict(
                size=14
            )
        )
        return fig


def handle_viewport_change(
    graph_component: dcc.Graph,
    new_viewport_size: str
) -> dcc.Graph:
    """
    Updates probability distribution visualization for a new viewport size.
    
    Args:
        graph_component: Existing Graph component to update
        new_viewport_size: New viewport size category
        
    Returns:
        Updated Graph component for new viewport
    """
    logger.debug(f"Handling viewport change to {new_viewport_size} for distribution component")
    
    try:
        # Extract current figure from graph component
        fig = graph_component.figure
        
        # Get distribution configuration for new viewport size
        config = get_distribution_config(new_viewport_size)
        
        # Apply responsive layout adjustments
        fig = apply_responsive_layout(
            fig,
            new_viewport_size,
            config['height']
        )
        
        # Update the graph's figure
        updated_graph = graph_component
        updated_graph.figure = fig
        updated_graph.style = {'height': config['height']}
        
        return updated_graph
    
    except Exception as e:
        logger.error(f"Error handling viewport change for distribution: {str(e)}")
        # Return the original component if update fails
        return graph_component


def create_empty_distribution(
    message: str,
    viewport_size: Optional[str] = None
) -> dcc.Graph:
    """
    Creates an empty probability distribution visualization with a message.
    
    Args:
        message: Message to display in the empty visualization
        viewport_size: Viewport size category for responsive layout
        
    Returns:
        Graph component with empty visualization
    """
    logger.debug(f"Creating empty distribution with message: {message}")
    
    # Use default viewport size if not provided
    if viewport_size is None:
        viewport_size = 'desktop'
    
    # Get distribution configuration for the viewport size
    config = get_distribution_config(viewport_size)
    
    # Create empty figure with just the message
    fig = go.Figure()
    
    # Add message as annotation
    fig.add_annotation(
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        text=message,
        showarrow=False,
        font=dict(
            size=14
        )
    )
    
    # Set layout with appropriate height
    fig.update_layout(
        height=config['height'],
        xaxis=dict(
            showticklabels=False,
            showgrid=False
        ),
        yaxis=dict(
            showticklabels=False,
            showgrid=False
        ),
        plot_bgcolor='rgba(0,0,0,0.02)',
        margin=dict(l=40, r=40, t=40, b=40)
    )
    
    # Create Dash Graph component
    graph = dcc.Graph(
        id=DISTRIBUTION_GRAPH_ID,
        figure=fig,
        config=CHART_CONFIG,
        style={'height': config['height']}
    )
    
    return graph


def format_distribution_title(
    product_id: str,
    timestamp: pd.Timestamp
) -> str:
    """
    Creates a formatted title for the probability distribution visualization.
    
    Args:
        product_id: Product identifier
        timestamp: Specific timestamp of the distribution
        
    Returns:
        Formatted title string
    """
    # Get product display name
    product_name = get_product_display_name(product_id)
    
    # Format timestamp
    formatted_timestamp = format_datetime(timestamp)
    
    # Combine into title
    return f"{product_name} - {formatted_timestamp}"