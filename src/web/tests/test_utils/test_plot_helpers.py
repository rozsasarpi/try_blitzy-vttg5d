import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
import plotly.graph_objects as go  # plotly: 5.14.0+
from datetime import datetime  # standard library

from src.web.utils.plot_helpers import create_time_series_plot  # Create time series visualization of forecast data
from src.web.utils.plot_helpers import add_uncertainty_bands  # Add uncertainty visualization to time series plots
from src.web.utils.plot_helpers import create_probability_distribution_plot  # Create visualization of forecast probability distribution
from src.web.utils.plot_helpers import create_product_comparison_plot  # Create visualization comparing multiple product forecasts
from src.web.utils.plot_helpers import create_heatmap_plot  # Create heatmap visualization of forecast values
from src.web.utils.plot_helpers import create_forecast_accuracy_plot  # Create visualization comparing forecasts to actual values
from src.web.utils.plot_helpers import configure_axes  # Configure plot axes with appropriate formatting
from src.web.utils.plot_helpers import create_hover_template  # Create template for hover information
from src.web.utils.plot_helpers import apply_responsive_layout  # Apply responsive adjustments to plot layout
from src.web.utils.plot_helpers import format_axis_date  # Format date axis with appropriate tick formatting
from src.web.utils.plot_helpers import add_fallback_indicator  # Add indicator when using fallback forecast data
from src.web.utils.plot_helpers import DEFAULT_PLOT_HEIGHT  # Default height for plots
from src.web.utils.plot_helpers import DEFAULT_PERCENTILE_LOWER  # Default lower percentile for uncertainty bands
from src.web.utils.plot_helpers import DEFAULT_PERCENTILE_UPPER  # Default upper percentile for uncertainty bands
from src.web.utils.plot_helpers import FALLBACK_INDICATOR_TEXT  # Text for fallback data indicator
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe  # Create sample forecast dataframe for testing
from src.web.tests.fixtures.forecast_fixtures import create_sample_fallback_dataframe  # Create sample fallback forecast dataframe for testing
from src.web.tests.fixtures.forecast_fixtures import create_multi_product_forecast_dataframe  # Create sample multi-product forecast dataframe for testing
from src.web.tests.fixtures.forecast_fixtures import create_forecast_with_extreme_values  # Create sample forecast with extreme values for testing
from src.web.tests.fixtures.forecast_fixtures import DEFAULT_PERCENTILES  # Default percentiles for uncertainty bands
from src.web.config.product_config import PRODUCTS  # List of valid electricity market products for testing
from src.web.config.product_config import DEFAULT_PRODUCT  # Default product for testing
from src.web.config.themes import get_theme_colors  # Get color palette for current theme
from src.web.config.themes import DEFAULT_THEME  # Default theme for visualizations


def test_create_time_series_plot_with_valid_data():
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    sample_df = create_sample_visualization_dataframe()
    # Call create_time_series_plot with the sample data and DEFAULT_PRODUCT
    fig = create_time_series_plot(sample_df, DEFAULT_PRODUCT)
    # Verify the returned object is a plotly.graph_objects.Figure
    assert isinstance(fig, go.Figure)
    # Verify the figure contains at least one trace
    assert len(fig.data) >= 1
    # Verify the trace type is 'scatter'
    assert fig.data[0].type == 'scatter'
    # Verify the x-axis contains datetime values
    assert all(isinstance(x, str) for x in fig.data[0].x)
    # Verify the y-axis contains price values
    assert all(isinstance(y, (int, float)) for y in fig.data[0].y)
    # Verify the figure has appropriate title and axis labels
    assert fig.layout.xaxis.title.text == "Time"
    assert fig.layout.yaxis.title.text == "Price ($/MWh)"


@pytest.mark.parametrize('product_id', PRODUCTS)
def test_create_time_series_plot_with_different_products(product_id):
    # Create sample visualization dataframe for each product in PRODUCTS
    sample_df = create_sample_visualization_dataframe(product=product_id)
    # Call create_time_series_plot with each product's data
    fig = create_time_series_plot(sample_df, product_id)
    # Verify the figure contains appropriate product-specific styling
    assert fig.data[0].line.color is not None
    # Verify the figure title contains the product name
    assert product_id in fig.layout.title.text
    # Verify the y-axis label includes the appropriate unit for the product
    assert "Price" in fig.layout.yaxis.title.text


def test_create_time_series_plot_with_custom_title():
    # Create sample visualization dataframe
    sample_df = create_sample_visualization_dataframe()
    # Define custom title string
    custom_title = "Custom Time Series Title"
    # Call create_time_series_plot with custom title
    fig = create_time_series_plot(sample_df, DEFAULT_PRODUCT, title=custom_title)
    # Verify the figure title matches the custom title
    assert fig.layout.title.text == custom_title


@pytest.mark.parametrize('theme', ['light', 'dark'])
def test_create_time_series_plot_with_different_themes(theme):
    # Create sample visualization dataframe
    sample_df = create_sample_visualization_dataframe()
    # Call create_time_series_plot with each theme
    fig = create_time_series_plot(sample_df, DEFAULT_PRODUCT, theme=theme)
    # Verify the figure styling matches the expected theme colors
    theme_colors = get_theme_colors(theme)
    # Verify the background color, text color, and grid color match the theme
    assert fig.layout.plot_bgcolor == theme_colors['background']
    assert fig.layout.paper_bgcolor == theme_colors['paper']
    assert fig.layout.font.color == theme_colors['text']


def test_add_uncertainty_bands_with_valid_data():
    # Create sample visualization dataframe
    sample_df = create_sample_visualization_dataframe()
    # Create base time series plot using create_time_series_plot
    fig = create_time_series_plot(sample_df, DEFAULT_PRODUCT)
    # Call add_uncertainty_bands with the figure and sample data
    fig = add_uncertainty_bands(fig, sample_df, DEFAULT_PRODUCT)
    # Verify the figure contains additional traces for uncertainty bands
    assert len(fig.data) >= 2
    # Verify the uncertainty band traces have fill property set
    assert fig.data[0].fill == 'toself'
    # Verify the uncertainty bands use the correct percentile values
    assert "10-90% Confidence Interval" in fig.data[0].name


@pytest.mark.parametrize('percentiles', [(5, 95), (25, 75)])
def test_add_uncertainty_bands_with_custom_percentiles(percentiles):
    # Create sample visualization dataframe
    sample_df = create_sample_visualization_dataframe()
    # Create base time series plot using create_time_series_plot
    fig = create_time_series_plot(sample_df, DEFAULT_PRODUCT)
    # Call add_uncertainty_bands with custom percentile values
    fig = add_uncertainty_bands(fig, sample_df, DEFAULT_PRODUCT, lower_percentile=percentiles[0], upper_percentile=percentiles[1])
    # Verify the uncertainty bands use the specified percentile values
    assert f"{percentiles[0]}-{percentiles[1]}% Confidence Interval" in fig.data[0].name


def test_create_probability_distribution_plot_with_valid_data():
    # Create sample visualization dataframe
    sample_df = create_sample_visualization_dataframe()
    # Select a specific timestamp from the dataframe
    timestamp = sample_df['timestamp'].iloc[0]
    # Call create_probability_distribution_plot with the data and timestamp
    fig = create_probability_distribution_plot(sample_df, DEFAULT_PRODUCT, timestamp)
    # Verify the returned object is a plotly.graph_objects.Figure
    assert isinstance(fig, go.Figure)
    # Verify the figure contains histogram or distribution trace
    assert any(trace.type == 'histogram' for trace in fig.data)
    # Verify the figure contains vertical lines for point forecast and confidence intervals
    assert len([shape for shape in fig.layout.shapes if shape.type == 'line']) == 3
    # Verify the figure has appropriate title and axis labels
    assert "Price Distribution" in fig.layout.title.text
    assert fig.layout.xaxis.title.text == "Price ($/MWh)"
    assert fig.layout.yaxis.title.text == "Probability Density"


@pytest.mark.parametrize('product_id', PRODUCTS)
def test_create_probability_distribution_plot_with_different_products(product_id):
    # Create sample visualization dataframe for each product in PRODUCTS
    sample_df = create_sample_visualization_dataframe(product=product_id)
    # Select a specific timestamp from each dataframe
    timestamp = sample_df['timestamp'].iloc[0]
    # Call create_probability_distribution_plot with each product's data
    fig = create_probability_distribution_plot(sample_df, product_id, timestamp)
    # Verify the figure contains appropriate product-specific styling
    assert fig.data[0].marker.color is not None
    # Verify the figure title contains the product name
    assert product_id in fig.layout.title.text
    # Verify the x-axis label includes the appropriate unit for the product
    assert "Price" in fig.layout.xaxis.title.text


def test_create_product_comparison_plot_with_valid_data():
    # Create sample multi-product forecast dataframe
    multi_product_df = create_multi_product_forecast_dataframe()
    # Call create_product_comparison_plot with the multi-product data
    fig = create_product_comparison_plot({product: multi_product_df[multi_product_df['product'] == product] for product in multi_product_df['product'].unique()}, list(multi_product_df['product'].unique()))
    # Verify the returned object is a plotly.graph_objects.Figure
    assert isinstance(fig, go.Figure)
    # Verify the figure contains multiple traces (one per product)
    assert len(fig.data) == len(multi_product_df['product'].unique())
    # Verify each trace has appropriate product-specific styling
    for trace in fig.data:
        assert trace.line.color is not None
    # Verify the figure has appropriate title and axis labels
    assert "Product Comparison" in fig.layout.title.text
    assert fig.layout.xaxis.title.text == "Time"
    assert fig.layout.yaxis.title.text == "Price ($)"
    # Verify the legend contains entries for each product
    assert len(fig.layout.legend.title.text) >= 1


def test_create_product_comparison_plot_with_subset_of_products():
    # Create sample multi-product forecast dataframe with all products
    multi_product_df = create_multi_product_forecast_dataframe()
    # Select a subset of products to display
    subset_products = [DEFAULT_PRODUCT]
    # Call create_product_comparison_plot with the subset of products
    fig = create_product_comparison_plot({product: multi_product_df[multi_product_df['product'] == product] for product in subset_products}, subset_products)
    # Verify the figure contains only traces for the selected products
    assert len(fig.data) == len(subset_products)
    # Verify the legend contains only entries for the selected products
    assert len(fig.layout.legend.title.text) >= 1


def test_create_heatmap_plot_with_valid_data():
    # Create sample visualization dataframe
    sample_df = create_sample_visualization_dataframe()
    # Call create_heatmap_plot with the sample data
    fig = create_heatmap_plot(sample_df, DEFAULT_PRODUCT)
    # Verify the returned object is a plotly.graph_objects.Figure
    assert isinstance(fig, go.Figure)
    # Verify the figure contains a heatmap trace
    assert any(trace.type == 'heatmap' for trace in fig.data)
    # Verify the x-axis represents hours
    assert fig.layout.xaxis.title.text == "Hour of Day"
    # Verify the y-axis represents dates
    assert fig.layout.yaxis.title.text == "Date"
    # Verify the figure has appropriate title and axis labels
    assert "Forecast Heatmap" in fig.layout.title.text
    # Verify the colorbar is present and properly formatted
    assert fig.data[0].colorbar.title.text == "$/MWh"


def test_create_forecast_accuracy_plot_with_valid_data():
    # Create sample visualization dataframe for forecast data
    forecast_df = create_sample_visualization_dataframe()
    # Create sample dataframe for actual price data
    actual_df = create_sample_visualization_dataframe()
    # Call create_forecast_accuracy_plot with forecast and actual data
    fig = create_forecast_accuracy_plot(forecast_df, actual_df, DEFAULT_PRODUCT)
    # Verify the returned object is a plotly.graph_objects.Figure
    assert isinstance(fig, go.Figure)
    # Verify the figure contains at least two traces (forecast and actual)
    assert len(fig.data) >= 2
    # Verify the traces have appropriate styling to distinguish forecast from actual
    assert fig.data[0].line.color is not None
    assert fig.data[1].line.dash == 'dot'
    # Verify the figure has appropriate title and axis labels
    assert "Forecast vs Actual" in fig.layout.title.text
    assert fig.layout.xaxis.title.text == "Time"
    assert fig.layout.yaxis.title.text == "Price ($/MWh)"
    # Verify error metrics are displayed in the figure
    assert len([annotation for annotation in fig.layout.annotations if "MAE" in annotation.text]) >= 1
    assert len([annotation for annotation in fig.layout.annotations if "RMSE" in annotation.text]) >= 1


def test_configure_axes_with_date_x_axis():
    # Create empty plotly figure
    fig = go.Figure()
    # Call configure_axes with date_x_axis=True
    fig = configure_axes(fig, date_x_axis=True)
    # Verify the x-axis has appropriate date formatting
    assert fig.layout.xaxis.type == 'date'
    # Verify the x-axis title is set correctly
    assert fig.layout.xaxis.title.text is None
    # Verify the tick format is appropriate for dates
    assert fig.layout.xaxis.tickformat == '%b %d, %H:%M'


def test_configure_axes_with_price_y_axis():
    # Create empty plotly figure
    fig = go.Figure()
    # Call configure_axes with price_y_axis=True
    fig = configure_axes(fig, price_y_axis=True)
    # Verify the y-axis has appropriate price formatting
    assert fig.layout.yaxis.tickprefix == '$'
    # Verify the y-axis title is set correctly
    assert fig.layout.yaxis.title.text is None
    # Verify the tick format includes currency symbol
    assert fig.layout.yaxis.tickformat == '.2f'


@pytest.mark.parametrize('include_percentiles', [True, False])
def test_create_hover_template_with_and_without_percentiles(include_percentiles):
    # Call create_hover_template with DEFAULT_PRODUCT and include_percentiles
    template = create_hover_template(DEFAULT_PRODUCT, include_percentiles)
    # Verify the returned template is a string
    assert isinstance(template, str)
    # Verify the template includes date and time formatting
    assert "%{x|%b %d, %H:%M}" in template
    # Verify the template includes product name and forecast value
    assert "Product: Day-Ahead LMP" in template
    assert "Forecast: %{y:$.2f}" in template
    # If include_percentiles is True, verify percentile information is included
    if include_percentiles:
        assert f"<br>{DEFAULT_PERCENTILE_LOWER}th Percentile: %{{customdata[0]:$.2f}}" in template
        assert f"<br>{DEFAULT_PERCENTILE_UPPER}th Percentile: %{{customdata[1]:$.2f}}" in template
    # If include_percentiles is False, verify percentile information is not included
    else:
        assert f"<br>{DEFAULT_PERCENTILE_LOWER}th Percentile: %{{customdata[0]:$.2f}}" not in template
        assert f"<br>{DEFAULT_PERCENTILE_UPPER}th Percentile: %{{customdata[1]:$.2f}}" not in template
    # Verify the template includes appropriate unit information
    assert "Unit: $/MWh" in template


@pytest.mark.parametrize('viewport_size', ['small', 'medium', 'large'])
def test_apply_responsive_layout_with_different_viewport_sizes(viewport_size):
    # Create sample time series plot
    fig = go.Figure()
    # Call apply_responsive_layout with different viewport sizes
    fig = apply_responsive_layout(fig, viewport_size)
    # Verify the figure height is adjusted appropriately for each viewport size
    assert fig.layout.height is not None
    # Verify the margins are adjusted appropriately for each viewport size
    assert fig.layout.margin.l is not None
    assert fig.layout.margin.r is not None
    assert fig.layout.margin.t is not None
    assert fig.layout.margin.b is not None
    # Verify the font sizes are adjusted appropriately for each viewport size
    assert fig.layout.font.size is not None
    # Verify the legend position is adjusted appropriately for each viewport size
    assert fig.layout.legend.orientation == 'h'


@pytest.mark.parametrize('date_format', ['%b %d', '%Y-%m-%d', '%H:%M', '%b %d, %H:%M'])
def test_format_axis_date_with_different_formats(date_format):
    # Create empty plotly figure
    fig = go.Figure()
    # Call format_axis_date with different date formats
    fig = format_axis_date(fig, date_format=date_format)
    # Verify the axis tickformat is set to the specified format
    assert fig.layout.xaxis.tickformat == date_format
    # Verify the axis type is set to 'date'
    assert fig.layout.xaxis.type == 'date'


def test_add_fallback_indicator_with_fallback_data():
    # Create sample fallback dataframe using create_sample_fallback_dataframe
    fallback_df = create_sample_fallback_dataframe()
    # Create time series plot with the fallback data
    fig = create_time_series_plot(fallback_df, DEFAULT_PRODUCT)
    # Call add_fallback_indicator with the figure and fallback data
    fig = add_fallback_indicator(fig, fallback_df)
    # Verify the figure contains an annotation with the fallback indicator text
    assert len(fig.layout.annotations) == 1
    assert fig.layout.annotations[0].text == FALLBACK_INDICATOR_TEXT
    # Verify the annotation has appropriate styling (warning color)
    assert fig.layout.annotations[0].font.color == "#856404"
    # Verify the annotation is positioned at the top of the plot
    assert fig.layout.annotations[0].y == 1.05


def test_add_fallback_indicator_with_normal_data():
    # Create sample visualization dataframe (non-fallback)
    normal_df = create_sample_visualization_dataframe()
    # Create time series plot with the normal data
    fig = create_time_series_plot(normal_df, DEFAULT_PRODUCT)
    # Call add_fallback_indicator with the figure and normal data
    fig = add_fallback_indicator(fig, normal_df)
    # Verify the figure does not contain a fallback indicator annotation
    assert not fig.layout.annotations


def test_create_time_series_plot_with_extreme_values():
    # Create sample forecast with extreme values using create_forecast_with_extreme_values
    extreme_df = create_forecast_with_extreme_values()
    # Call create_time_series_plot with the extreme value data
    fig = create_time_series_plot(extreme_df, DEFAULT_PRODUCT)
    # Verify the figure y-axis range accommodates the extreme values
    assert fig.layout.yaxis.range is not None
    # Verify the plot is still readable and properly formatted despite extreme values
    assert fig.layout.xaxis.title.text == "Time"
    assert fig.layout.yaxis.title.text == "Price ($/MWh)"


def test_create_time_series_plot_with_empty_data():
    # Create empty pandas DataFrame with appropriate columns
    empty_df = pandas.DataFrame(columns=['timestamp', 'point_forecast'])
    # Call create_time_series_plot with the empty dataframe
    fig = create_time_series_plot(empty_df, DEFAULT_PRODUCT)
    # Verify the function handles empty data gracefully
    assert isinstance(fig, go.Figure)
    # Verify an appropriate message or empty plot is returned
    assert len(fig.data) == 0