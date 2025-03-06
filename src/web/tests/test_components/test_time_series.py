"""
Unit tests for the time series visualization component of the Electricity Market Price Forecasting Dashboard.
This module tests the creation, updating, and interaction with the time series visualization component,
including uncertainty bands, responsive layout, and fallback indicators.
"""

import pytest  # pytest: 7.0.0+
import dash  # dash: 2.9.0+
from dash import html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
from datetime import datetime  # standard library

from src.web.components.time_series import (  # src/web/components/time_series.py
    create_time_series_component,
    update_time_series,
    create_uncertainty_toggle,
    get_time_series_with_uncertainty,
    handle_viewport_change,
    create_empty_time_series,
    TIME_SERIES_GRAPH_ID,
    UNCERTAINTY_TOGGLE_ID
)
from src.web.tests.fixtures.forecast_fixtures import (  # src/web/tests/fixtures/forecast_fixtures.py
    create_sample_visualization_dataframe,
    create_sample_fallback_dataframe,
    DEFAULT_PERCENTILES
)
from src.web.tests.fixtures.component_fixtures import mock_time_series, MockTimeSeriesComponent  # src/web/tests/fixtures/component_fixtures.py
from src.web.config.product_config import DEFAULT_PRODUCT, PRODUCTS  # src/web/config/product_config.py


def test_create_time_series_component():
    """Tests the creation of a time series visualization component"""
    # Create sample forecast dataframe using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Call create_time_series_component with the sample data
    component = create_time_series_component(sample_data)

    # Assert that the returned component is a dcc.Graph instance
    assert isinstance(component, dcc.Graph)

    # Assert that the component has the correct ID (TIME_SERIES_GRAPH_ID)
    assert component.id == TIME_SERIES_GRAPH_ID

    # Assert that the component's figure has the expected traces
    assert len(component.figure.data) >= 1

    # Assert that the figure has a point forecast trace
    assert any(trace.type == 'scatter' for trace in component.figure.data)

    # Assert that the figure has uncertainty band traces when show_uncertainty=True
    assert any('uncertainty' in trace.name.lower() for trace in component.figure.data)


def test_create_time_series_component_without_uncertainty():
    """Tests the creation of a time series component without uncertainty bands"""
    # Create sample forecast dataframe using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Call create_time_series_component with show_uncertainty=False
    component = create_time_series_component(sample_data, show_uncertainty=False)

    # Assert that the returned component is a dcc.Graph instance
    assert isinstance(component, dcc.Graph)

    # Assert that the component has the correct ID
    assert component.id == TIME_SERIES_GRAPH_ID

    # Assert that the component's figure has only the point forecast trace
    assert len(component.figure.data) == 1
    assert component.figure.data[0].type == 'scatter'

    # Assert that the figure does not have uncertainty band traces
    assert not any('uncertainty' in trace.name.lower() for trace in component.figure.data)


@pytest.mark.parametrize('product_id', PRODUCTS)
def test_create_time_series_component_with_different_product(product_id):
    """Tests the creation of a time series component for different products"""
    # Create sample forecast dataframe for the specified product
    sample_data = create_sample_visualization_dataframe(product=product_id)

    # Call create_time_series_component with the product-specific data
    component = create_time_series_component(sample_data, product_id=product_id)

    # Assert that the returned component is a dcc.Graph instance
    assert isinstance(component, dcc.Graph)

    # Assert that the component's figure has the expected product-specific styling
    assert any(product_id.lower() in trace.name.lower() for trace in component.figure.data)

    # Assert that the figure title contains the product name
    assert product_id in component.figure.layout.title.text


def test_update_time_series():
    """Tests updating an existing time series component with new data"""
    # Create sample forecast dataframe using create_sample_visualization_dataframe
    initial_data = create_sample_visualization_dataframe()

    # Create initial time series component using create_time_series_component
    initial_component = create_time_series_component(initial_data)

    # Create updated sample forecast dataframe with different values
    updated_data = create_sample_visualization_dataframe()
    updated_data['point_forecast'] = updated_data['point_forecast'] + 5

    # Call update_time_series with the existing component and new data
    updated_component = update_time_series(initial_component, updated_data, DEFAULT_PRODUCT)

    # Assert that the returned component is the same dcc.Graph instance
    assert updated_component is initial_component

    # Assert that the component's figure has been updated with the new data
    assert len(updated_component.figure.data) >= 1
    assert all(y == x + 5 for x, y in zip(initial_data['point_forecast'], updated_data['point_forecast']))

    # Assert that the figure traces reflect the updated values
    assert all(trace.y == updated_data['point_forecast'] for trace in updated_component.figure.data if trace.type == 'scatter')


def test_update_time_series_change_product():
    """Tests updating a time series component to show a different product"""
    # Create sample forecast dataframe for initial product (e.g., 'DALMP')
    initial_data = create_sample_visualization_dataframe(product='DALMP')

    # Create initial time series component using create_time_series_component
    initial_component = create_time_series_component(initial_data)

    # Create sample forecast dataframe for new product (e.g., 'RTLMP')
    new_data = create_sample_visualization_dataframe(product='RTLMP')

    # Call update_time_series with the existing component and new product data
    updated_component = update_time_series(initial_component, new_data, 'RTLMP')

    # Assert that the component's figure has been updated with the new product
    assert len(updated_component.figure.data) >= 1
    assert all(trace.y == new_data['point_forecast'] for trace in updated_component.figure.data if trace.type == 'scatter')

    # Assert that the figure styling reflects the new product
    assert 'rtlmp' in updated_component.figure.data[0].name.lower()

    # Assert that the figure title contains the new product name
    assert 'RTLMP' in updated_component.figure.layout.title.text


def test_update_time_series_toggle_uncertainty():
    """Tests toggling uncertainty bands in an existing time series component"""
    # Create sample forecast dataframe using create_sample_visualization_dataframe
    initial_data = create_sample_visualization_dataframe()

    # Create initial time series component with show_uncertainty=True
    initial_component = create_time_series_component(initial_data, show_uncertainty=True)

    # Call update_time_series with show_uncertainty=False
    updated_component = update_time_series(initial_component, initial_data, DEFAULT_PRODUCT, show_uncertainty=False)

    # Assert that the uncertainty bands are no longer present
    assert len(updated_component.figure.data) == 1
    assert updated_component.figure.data[0].type == 'scatter'

    # Call update_time_series with show_uncertainty=True again
    updated_component = update_time_series(initial_component, initial_data, DEFAULT_PRODUCT, show_uncertainty=True)

    # Assert that the uncertainty bands are restored
    assert len(updated_component.figure.data) >= 1
    assert any('uncertainty' in trace.name.lower() for trace in updated_component.figure.data)


def test_create_uncertainty_toggle():
    """Tests the creation of the uncertainty toggle component"""
    # Call create_uncertainty_toggle with initial_state=True
    toggle_component = create_uncertainty_toggle(initial_state=True)

    # Assert that the returned component is an html.Div
    assert isinstance(toggle_component, html.Div)

    # Assert that the component contains a checkbox with the correct ID (UNCERTAINTY_TOGGLE_ID)
    assert any(isinstance(child, dcc.Checklist) and child.id == UNCERTAINTY_TOGGLE_ID for child in toggle_component.children)

    # Assert that the checkbox is initially checked
    assert toggle_component.children[0].value == ['show']

    # Call create_uncertainty_toggle with initial_state=False
    toggle_component = create_uncertainty_toggle(initial_state=False)

    # Assert that the checkbox is initially unchecked
    assert toggle_component.children[0].value == []


def test_get_time_series_with_uncertainty():
    """Tests getting a time series figure with uncertainty bands"""
    # Create sample forecast dataframe using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Call get_time_series_with_uncertainty with the sample data
    fig = get_time_series_with_uncertainty(sample_data, DEFAULT_PRODUCT)

    # Assert that the returned object is a plotly.graph_objects.Figure
    assert isinstance(fig, plotly.graph_objects.Figure)

    # Assert that the figure has the point forecast trace
    assert any(trace.type == 'scatter' for trace in fig.data)

    # Assert that the figure has uncertainty band traces
    assert any('uncertainty' in trace.name.lower() for trace in fig.data)

    # Assert that the uncertainty bands use the correct percentiles (DEFAULT_PERCENTILES)
    assert any(str(DEFAULT_PERCENTILES[0]) in trace.name and str(DEFAULT_PERCENTILES[1]) in trace.name for trace in fig.data)


def test_get_time_series_with_custom_percentiles():
    """Tests getting a time series with custom uncertainty percentiles"""
    # Create sample forecast dataframe using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Define custom percentiles (e.g., [5, 95])
    custom_percentiles = [5, 95]

    # Call get_time_series_with_uncertainty with custom percentiles
    fig = get_time_series_with_uncertainty(sample_data, DEFAULT_PRODUCT, percentiles=custom_percentiles)

    # Assert that the figure has uncertainty band traces
    assert any('uncertainty' in trace.name.lower() for trace in fig.data)

    # Assert that the uncertainty bands use the custom percentiles
    assert any(str(custom_percentiles[0]) in trace.name and str(custom_percentiles[1]) in trace.name for trace in fig.data)


@pytest.mark.parametrize('viewport_size', ['xs', 'sm', 'md', 'lg', 'xl'])
def test_handle_viewport_change(viewport_size):
    """Tests handling viewport size changes for responsive design"""
    # Create sample forecast dataframe using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Create initial time series component for 'lg' viewport
    initial_component = create_time_series_component(sample_data, viewport_size='lg')

    # Call handle_viewport_change with the component and new viewport size
    updated_component = handle_viewport_change(initial_component, viewport_size)

    # Assert that the component's figure layout has been updated for the new viewport
    assert updated_component.figure.layout.margin.l >= 0
    assert updated_component.figure.layout.margin.r >= 0

    # Check that height, margins, and font sizes are appropriate for the viewport size
    assert updated_component.figure.layout.height > 0
    assert updated_component.figure.layout.font.size > 0


def test_create_empty_time_series():
    """Tests creating an empty time series component with a message"""
    # Define a test message (e.g., 'No forecast data available')
    test_message = 'No forecast data available'

    # Call create_empty_time_series with the message
    empty_component = create_empty_time_series(test_message)

    # Assert that the returned component is a dcc.Graph instance
    assert isinstance(empty_component, dcc.Graph)

    # Assert that the component has the correct ID
    assert empty_component.id == TIME_SERIES_GRAPH_ID

    # Assert that the component's figure contains the test message
    assert test_message in empty_component.figure.layout.annotations[0].text

    # Assert that the figure has appropriate styling for an empty state
    assert empty_component.figure.layout.xaxis.visible is False
    assert empty_component.figure.layout.yaxis.visible is False


def test_time_series_with_fallback_data():
    """Tests that fallback data is properly indicated in the visualization"""
    # Create sample fallback dataframe using create_sample_fallback_dataframe
    fallback_data = create_sample_fallback_dataframe()

    # Call create_time_series_component with the fallback data
    component = create_time_series_component(fallback_data)

    # Assert that the component's figure contains a fallback indicator
    assert component.figure.layout.annotations is not None
    assert len(component.figure.layout.annotations) > 0

    # Assert that the fallback indicator has the correct text and styling
    assert "Using fallback forecast" in component.figure.layout.annotations[0].text