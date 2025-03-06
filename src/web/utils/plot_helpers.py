"""
Utility module providing helper functions for creating and configuring visualizations in the 
Electricity Market Price Forecasting System's Dash-based dashboard. This module implements 
reusable plotting functions for time series, probability distributions, product comparisons, 
and other visualization types with consistent styling and responsive design.
"""

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.colors
from plotly.subplots import make_subplots

from ..config.themes import (
    get_theme_colors,
    get_product_color,
    get_plot_layout,
    get_uncertainty_style,
    CHART_CONFIG,
    DEFAULT_THEME
)
from ..config.product_config import (
    get_product_display_name,
    get_product_unit,
    get_product_line_style
)
from ..utils.formatting import (
    format_price,
    format_datetime,
    format_confidence_interval
)
from ..utils.date_helpers import get_date_hour_label
from ..utils.responsive_helpers import get_responsive_dimension
from ..utils.error_handlers import is_fallback_data

# Default values for plots
DEFAULT_PLOT_HEIGHT = 500
DEFAULT_PLOT_WIDTH = None
DEFAULT_PERCENTILE_LOWER = 10
DEFAULT_PERCENTILE_UPPER = 90
FALLBACK_INDICATOR_TEXT = "Using fallback forecast (previous day's data)"


def create_time_series_plot(
    forecast_df: pd.DataFrame,
    product_id: str,
    title: str = None,
    theme: str = DEFAULT_THEME
) -> go.Figure:
    """
    Creates a time series plot for forecast data with appropriate styling.
    
    Args:
        forecast_df: DataFrame containing forecast data
        product_id: Product identifier for styling and formatting
        title: Optional title for the plot
        theme: Theme name for styling
        
    Returns:
        Plotly figure with time series plot
    """
    # Get theme colors and product color
    theme_colors = get_theme_colors(theme)
    product_color = get_product_color(product_id, theme)
    
    # Get base layout for the theme
    layout = get_plot_layout(theme)
    
    # Create figure with layout
    fig = go.Figure(layout=layout)
    
    # Extract timestamps and point forecast values
    timestamps = forecast_df['timestamp'].tolist()
    forecasts = forecast_df['point_forecast'].tolist()
    
    # Get product line style
    line_style = get_product_line_style(product_id)
    
    # Create scatter trace for point forecast
    fig.add_trace(
        go.Scatter(
            x=timestamps,
            y=forecasts,
            mode='lines',
            name=get_product_display_name(product_id),
            line=dict(
                color=product_color,
                dash=line_style.get('dash', 'solid'),
                width=line_style.get('width', 2)
            ),
            hovertemplate=create_hover_template(product_id, False)
        )
    )
    
    # Configure axes
    fig = configure_axes(
        fig,
        x_title="Time",
        y_title=f"Price ({get_product_unit(product_id)})",
        date_x_axis=True,
        price_y_axis=True
    )
    
    # Set title if provided
    if title:
        fig.update_layout(title=title)
    
    return fig


def add_uncertainty_bands(
    fig: go.Figure,
    forecast_df: pd.DataFrame,
    product_id: str,
    lower_percentile: int = None,
    upper_percentile: int = None,
    theme: str = DEFAULT_THEME
) -> go.Figure:
    """
    Adds uncertainty bands to a time series plot to show forecast uncertainty.
    
    Args:
        fig: Existing figure to add uncertainty bands to
        forecast_df: DataFrame containing forecast data
        product_id: Product identifier for styling and formatting
        lower_percentile: Lower percentile for uncertainty band (default: DEFAULT_PERCENTILE_LOWER)
        upper_percentile: Upper percentile for uncertainty band (default: DEFAULT_PERCENTILE_UPPER)
        theme: Theme name for styling
        
    Returns:
        Figure with uncertainty bands added
    """
    # Use default percentiles if not provided
    if lower_percentile is None:
        lower_percentile = DEFAULT_PERCENTILE_LOWER
    if upper_percentile is None:
        upper_percentile = DEFAULT_PERCENTILE_UPPER
    
    # Extract timestamps and percentile columns
    timestamps = forecast_df['timestamp'].tolist()
    lower_values = forecast_df[f'sample_{str(lower_percentile).zfill(3)}'].tolist()
    upper_values = forecast_df[f'sample_{str(upper_percentile).zfill(3)}'].tolist()
    
    # Get uncertainty styling
    uncertainty_style = get_uncertainty_style(theme, product_id)
    
    # Add fill area between lower and upper percentiles
    fig.add_trace(
        go.Scatter(
            x=timestamps + timestamps[::-1],  # x, then x reversed
            y=upper_values + lower_values[::-1],  # upper, then lower reversed
            fill='toself',
            fillcolor=uncertainty_style.get('fillcolor', 'rgba(0,0,0,0.1)'),
            line=dict(color='rgba(255,255,255,0)'),
            hoverinfo='skip',
            showlegend=False,
            opacity=uncertainty_style.get('opacity', 0.3),
            name=f"{lower_percentile}-{upper_percentile}% Confidence Interval"
        )
    )
    
    # Ensure the original trace is on top of the uncertainty band
    fig.data = fig.data[-1:] + fig.data[:-1]
    
    return fig


def create_probability_distribution_plot(
    distribution_df: pd.DataFrame,
    product_id: str,
    timestamp: pd.Timestamp,
    theme: str = DEFAULT_THEME
) -> go.Figure:
    """
    Creates a probability distribution plot for a specific forecast timestamp.
    
    Args:
        distribution_df: DataFrame containing forecast samples
        product_id: Product identifier for styling and formatting
        timestamp: Specific timestamp to show distribution for
        theme: Theme name for styling
        
    Returns:
        Plotly figure with distribution plot
    """
    # Get theme colors and product color
    theme_colors = get_theme_colors(theme)
    product_color = get_product_color(product_id, theme)
    
    # Get base layout for the theme
    layout = get_plot_layout(theme)
    
    # Create figure with layout
    fig = go.Figure(layout=layout)
    
    # Filter data for the specific timestamp
    row = distribution_df[distribution_df['timestamp'] == timestamp]
    if row.empty:
        # Handle case when timestamp is not found
        return fig
    
    # Extract all sample columns (sample_001 to sample_100)
    sample_columns = [col for col in distribution_df.columns if col.startswith('sample_')]
    if not sample_columns:
        return fig
    
    # Get all samples for this timestamp
    samples = row[sample_columns].values.flatten()
    
    # Get point forecast for reference
    point_forecast = row['point_forecast'].values[0] if 'point_forecast' in row else np.mean(samples)
    
    # Calculate lower and upper percentiles
    lower_percentile = np.percentile(samples, DEFAULT_PERCENTILE_LOWER)
    upper_percentile = np.percentile(samples, DEFAULT_PERCENTILE_UPPER)
    
    # Create histogram trace for samples
    fig.add_trace(
        go.Histogram(
            x=samples,
            histnorm='probability density',
            marker=dict(color=product_color, opacity=0.7),
            name="Probability Distribution"
        )
    )
    
    # Add vertical line for point forecast
    fig.add_shape(
        type="line",
        x0=point_forecast,
        x1=point_forecast,
        y0=0,
        y1=1,
        yref="paper",
        line=dict(color=product_color, width=2, dash="solid"),
    )
    
    # Add vertical lines for confidence interval bounds
    fig.add_shape(
        type="line",
        x0=lower_percentile,
        x1=lower_percentile,
        y0=0,
        y1=1,
        yref="paper",
        line=dict(color=product_color, width=1.5, dash="dash"),
    )
    
    fig.add_shape(
        type="line",
        x0=upper_percentile,
        x1=upper_percentile,
        y0=0,
        y1=1,
        yref="paper",
        line=dict(color=product_color, width=1.5, dash="dash"),
    )
    
    # Configure axes
    fig = configure_axes(
        fig,
        x_title=f"Price ({get_product_unit(product_id)})",
        y_title="Probability Density",
        date_x_axis=False,
        price_y_axis=True
    )
    
    # Set title with product name and timestamp
    product_name = get_product_display_name(product_id)
    formatted_time = format_datetime(timestamp)
    fig.update_layout(
        title=f"{product_name} Price Distribution - {formatted_time}"
    )
    
    # Add annotation for point forecast and confidence interval
    fig.add_annotation(
        x=0.5,
        y=0,
        xref="paper",
        yref="paper",
        text=(
            f"Point Forecast: {format_price(point_forecast, product_id)}<br>"
            f"{DEFAULT_PERCENTILE_LOWER}%-{DEFAULT_PERCENTILE_UPPER}% Range: "
            f"{format_confidence_interval(lower_percentile, upper_percentile, product_id)}"
        ),
        showarrow=False,
        font=dict(size=12),
        bordercolor=theme_colors.get('line', '#6c757d'),
        borderwidth=1,
        borderpad=4,
        bgcolor=theme_colors.get('background', '#f8f9fa'),
        opacity=0.8
    )
    
    return fig


def create_product_comparison_plot(
    forecast_dfs: Dict[str, pd.DataFrame],
    product_ids: List[str],
    title: str = None,
    theme: str = DEFAULT_THEME
) -> go.Figure:
    """
    Creates a plot comparing forecasts for multiple products.
    
    Args:
        forecast_dfs: Dictionary mapping product_ids to forecast DataFrames
        product_ids: List of product IDs to include in comparison
        title: Optional title for the plot
        theme: Theme name for styling
        
    Returns:
        Plotly figure with product comparison
    """
    # Get theme colors
    theme_colors = get_theme_colors(theme)
    
    # Get base layout for the theme
    layout = get_plot_layout(theme)
    
    # Create figure with layout
    fig = go.Figure(layout=layout)
    
    # Add a trace for each product
    for product_id in product_ids:
        # Check if we have data for this product
        if product_id not in forecast_dfs:
            continue
            
        forecast_df = forecast_dfs[product_id]
        
        # Extract timestamps and point forecast values
        timestamps = forecast_df['timestamp'].tolist()
        forecasts = forecast_df['point_forecast'].tolist()
        
        # Get product color and line style
        product_color = get_product_color(product_id, theme)
        line_style = get_product_line_style(product_id)
        
        # Get product display name
        product_name = get_product_display_name(product_id)
        
        # Create scatter trace for this product
        fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=forecasts,
                mode='lines',
                name=product_name,
                line=dict(
                    color=product_color,
                    dash=line_style.get('dash', 'solid'),
                    width=line_style.get('width', 2)
                ),
                hovertemplate=create_hover_template(product_id, False)
            )
        )
    
    # Configure axes
    fig = configure_axes(
        fig,
        x_title="Time",
        y_title="Price ($)",
        date_x_axis=True,
        price_y_axis=True
    )
    
    # Set title
    if title:
        fig.update_layout(title=title)
    else:
        fig.update_layout(title="Product Comparison")
    
    # Configure legend
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        )
    )
    
    return fig


def create_heatmap_plot(
    forecast_df: pd.DataFrame,
    product_id: str,
    title: str = None,
    theme: str = DEFAULT_THEME
) -> go.Figure:
    """
    Creates a heatmap visualization of forecast values over time.
    
    Args:
        forecast_df: DataFrame containing forecast data
        product_id: Product identifier for styling and formatting
        title: Optional title for the plot
        theme: Theme name for styling
        
    Returns:
        Plotly figure with heatmap
    """
    # Get theme colors and product color
    theme_colors = get_theme_colors(theme)
    product_color = get_product_color(product_id, theme)
    
    # Get base layout for the theme
    layout = get_plot_layout(theme)
    
    # Create figure with layout
    fig = go.Figure(layout=layout)
    
    # Prepare data for heatmap - need to pivot to create a matrix of dates vs hours
    # Extract date and hour from timestamp
    forecast_df = forecast_df.copy()
    forecast_df['date'] = forecast_df['timestamp'].dt.date
    forecast_df['hour'] = forecast_df['timestamp'].dt.hour
    
    # Pivot to create matrix with dates as rows and hours as columns
    pivot_df = forecast_df.pivot(index='date', columns='hour', values='point_forecast')
    
    # Create heatmap trace
    fig.add_trace(
        go.Heatmap(
            z=pivot_df.values,
            x=pivot_df.columns,  # hours
            y=pivot_df.index,    # dates
            colorscale=[
                [0, "rgba(255,255,255,0)"],  # transparent for low values
                [1, product_color]           # product color for high values
            ],
            hovertemplate=(
                "Date: %{y}<br>"
                "Hour: %{x}:00<br>"
                f"Forecast: %{{z:${'.2f'}}}<br>"
                "<extra></extra>"
            )
        )
    )
    
    # Configure axes
    fig.update_layout(
        xaxis=dict(
            title="Hour of Day",
            tickmode='array',
            tickvals=list(range(0, 24, 3)),  # Show every 3 hours
            ticktext=[f"{h:02d}:00" for h in range(0, 24, 3)]
        ),
        yaxis=dict(
            title="Date",
            autorange="reversed"  # Latest date at the top
        )
    )
    
    # Set title
    if title:
        fig.update_layout(title=title)
    else:
        product_name = get_product_display_name(product_id)
        fig.update_layout(title=f"{product_name} Forecast Heatmap")
    
    # Add colorbar with appropriate formatting
    fig.update_traces(
        colorbar=dict(
            title=get_product_unit(product_id),
            titleside="right",
            tickprefix="$",
            tickformat=".2f"
        )
    )
    
    return fig


def create_forecast_accuracy_plot(
    forecast_df: pd.DataFrame,
    actual_df: pd.DataFrame,
    product_id: str,
    title: str = None,
    theme: str = DEFAULT_THEME
) -> go.Figure:
    """
    Creates a plot showing forecast accuracy compared to actual values.
    
    Args:
        forecast_df: DataFrame containing forecast data
        actual_df: DataFrame containing actual price data
        product_id: Product identifier for styling and formatting
        title: Optional title for the plot
        theme: Theme name for styling
        
    Returns:
        Plotly figure with accuracy comparison
    """
    # Get theme colors and product color
    theme_colors = get_theme_colors(theme)
    product_color = get_product_color(product_id, theme)
    
    # Get base layout for the theme
    layout = get_plot_layout(theme)
    
    # Create figure with layout
    fig = go.Figure(layout=layout)
    
    # Merge forecast and actual data on timestamp
    merged_df = pd.merge(
        forecast_df[['timestamp', 'point_forecast']], 
        actual_df[['timestamp', 'price']], 
        on='timestamp', 
        how='inner'
    )
    
    # Extract timestamps, forecast values, and actual values
    timestamps = merged_df['timestamp'].tolist()
    forecasts = merged_df['point_forecast'].tolist()
    actuals = merged_df['price'].tolist()
    
    # Create scatter trace for forecast values
    fig.add_trace(
        go.Scatter(
            x=timestamps,
            y=forecasts,
            mode='lines',
            name='Forecast',
            line=dict(
                color=product_color,
                width=2
            ),
            hovertemplate=(
                "%{x|%b %d, %H:%M}<br>"
                f"Forecast: %{{y:${'.2f'}}}<br>"
                "<extra></extra>"
            )
        )
    )
    
    # Create scatter trace for actual values
    fig.add_trace(
        go.Scatter(
            x=timestamps,
            y=actuals,
            mode='lines',
            name='Actual',
            line=dict(
                color=theme_colors.get('line', '#6c757d'),
                width=2,
                dash='dot'
            ),
            hovertemplate=(
                "%{x|%b %d, %H:%M}<br>"
                f"Actual: %{{y:${'.2f'}}}<br>"
                "<extra></extra>"
            )
        )
    )
    
    # Calculate error metrics
    if merged_df.shape[0] > 0:
        errors = np.array(forecasts) - np.array(actuals)
        mae = np.mean(np.abs(errors))
        rmse = np.sqrt(np.mean(np.square(errors)))
        
        # Add error metrics annotation
        fig.add_annotation(
            x=0.02,
            y=0.98,
            xref="paper",
            yref="paper",
            text=(
                f"MAE: {format_price(mae, product_id, decimal_places=2)}<br>"
                f"RMSE: {format_price(rmse, product_id, decimal_places=2)}"
            ),
            showarrow=False,
            font=dict(size=12),
            align="left",
            bordercolor=theme_colors.get('line', '#6c757d'),
            borderwidth=1,
            borderpad=4,
            bgcolor=theme_colors.get('background', '#f8f9fa'),
            opacity=0.8
        )
    
    # Configure axes
    fig = configure_axes(
        fig,
        x_title="Time",
        y_title=f"Price ({get_product_unit(product_id)})",
        date_x_axis=True,
        price_y_axis=True
    )
    
    # Set title
    if title:
        fig.update_layout(title=title)
    else:
        product_name = get_product_display_name(product_id)
        fig.update_layout(title=f"{product_name} Forecast vs Actual")
    
    # Configure legend
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        )
    )
    
    return fig


def configure_axes(
    fig: go.Figure,
    x_title: str = None,
    y_title: str = None,
    date_x_axis: bool = False,
    price_y_axis: bool = False
) -> go.Figure:
    """
    Configures axes for a plot with appropriate formatting.
    
    Args:
        fig: Plotly figure to configure axes for
        x_title: Title for x-axis
        y_title: Title for y-axis
        date_x_axis: Whether x-axis contains date values
        price_y_axis: Whether y-axis contains price values
        
    Returns:
        Figure with configured axes
    """
    # Configure x-axis
    x_axis_config = {}
    
    if x_title:
        x_axis_config['title'] = x_title
    
    if date_x_axis:
        x_axis_config.update({
            'type': 'date',
            'tickformat': '%b %d, %H:%M'
        })
    
    # Configure y-axis
    y_axis_config = {}
    
    if y_title:
        y_axis_config['title'] = y_title
    
    if price_y_axis:
        y_axis_config.update({
            'tickprefix': '$',
            'tickformat': '.2f'
        })
    
    # Update figure layout with axis configurations
    fig.update_layout(
        xaxis=x_axis_config,
        yaxis=y_axis_config
    )
    
    return fig


def create_hover_template(product_id: str, include_percentiles: bool = False) -> str:
    """
    Creates a hover template for displaying information on hover.
    
    Args:
        product_id: Product identifier for formatting
        include_percentiles: Whether to include percentile information
        
    Returns:
        Hover template string
    """
    # Get product display name
    product_name = get_product_display_name(product_id)
    
    # Create base template with date and time
    template = "<b>%{x|%b %d, %H:%M}</b><br>"
    
    # Add product name and point forecast
    template += f"Product: {product_name}<br>"
    template += "Forecast: %{y:$.2f}"
    
    # Add percentile information if requested
    if include_percentiles:
        template += f"<br>{DEFAULT_PERCENTILE_LOWER}th Percentile: %{{customdata[0]:$.2f}}"
        template += f"<br>{DEFAULT_PERCENTILE_UPPER}th Percentile: %{{customdata[1]:$.2f}}"
    
    # Add unit information
    template += f"<br>Unit: {get_product_unit(product_id)}"
    
    # Add extra empty field to remove trace name
    template += "<extra></extra>"
    
    return template


def apply_responsive_layout(
    fig: go.Figure,
    viewport_size: str,
    base_height: int = None
) -> go.Figure:
    """
    Applies responsive layout adjustments to a figure based on viewport size.
    
    Args:
        fig: Plotly figure to apply responsive layout to
        viewport_size: Viewport size category (mobile, tablet, desktop)
        base_height: Base height for desktop viewport
        
    Returns:
        Figure with responsive layout
    """
    # Use default height if not provided
    if base_height is None:
        base_height = DEFAULT_PLOT_HEIGHT
    
    # Calculate responsive height
    height = get_responsive_dimension(viewport_size, base_height, 'height')
    
    # Update figure layout with responsive height
    fig.update_layout(height=height)
    
    # Adjust margins based on viewport size
    if viewport_size == 'mobile':
        fig.update_layout(margin=dict(l=30, r=30, t=40, b=40))
    elif viewport_size == 'tablet':
        fig.update_layout(margin=dict(l=40, r=40, t=45, b=45))
    
    # Adjust font sizes based on viewport size
    if viewport_size == 'mobile':
        fig.update_layout(font=dict(size=10))
    elif viewport_size == 'tablet':
        fig.update_layout(font=dict(size=11))
    
    # Adjust legend position based on viewport size
    if viewport_size == 'mobile':
        fig.update_layout(legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ))
    
    return fig


def format_axis_date(fig: go.Figure, axis: str = 'xaxis', date_format: str = None) -> go.Figure:
    """
    Formats date axis with appropriate tick formatting.
    
    Args:
        fig: Plotly figure to format axis for
        axis: Which axis to format ('xaxis' or 'yaxis')
        date_format: Format string for date ticks
        
    Returns:
        Figure with formatted date axis
    """
    # Use default format if not provided
    if date_format is None:
        date_format = '%b %d, %H:%M'
    
    # Update the specified axis
    fig.update_layout(**{
        axis: {
            'type': 'date',
            'tickformat': date_format,
            'tickmode': 'auto',
            'nticks': 10
        }
    })
    
    return fig


def add_fallback_indicator(fig: go.Figure, forecast_df: pd.DataFrame) -> go.Figure:
    """
    Adds an indicator to the plot when using fallback forecast data.
    
    Args:
        fig: Plotly figure to add indicator to
        forecast_df: Forecast DataFrame to check for fallback status
        
    Returns:
        Figure with fallback indicator if needed
    """
    # Check if this is fallback data
    if is_fallback_data(forecast_df):
        # Add annotation at the top of the plot
        fig.add_annotation(
            x=0.5,
            y=1.05,
            xref="paper",
            yref="paper",
            text=FALLBACK_INDICATOR_TEXT,
            showarrow=False,
            font=dict(
                size=12,
                color="#856404"  # Warning text color
            ),
            align="center",
            bordercolor="#ffeeba",  # Warning border color
            borderwidth=1,
            borderpad=4,
            bgcolor="#fff3cd",  # Warning background color
            opacity=0.8
        )
    
    return fig