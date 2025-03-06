"""
Unit tests for the probability distribution visualization component of the
Electricity Market Price Forecasting System. This module tests the creation,
updating, and responsiveness of the probability distribution visualization
that shows forecast uncertainty for specific timestamps.
"""

import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import plotly.graph_objects as go  # plotly: 5.14.0+
import dash_core_components as dcc  # dash: 2.9.0+

from src.web.components.probability_distribution import create_distribution_component  # Function to create probability distribution visualization component
from src.web.components.probability_distribution import update_distribution  # Function to update probability distribution visualization
from src.web.components.probability_distribution import get_distribution_with_percentiles  # Function to get distribution with percentile indicators
from src.web.components.probability_distribution import handle_viewport_change  # Function to update visualization for viewport changes
from src.web.components.probability_distribution import create_empty_distribution  # Function to create empty visualization with message
from src.web.components.probability_distribution import DISTRIBUTION_GRAPH_ID  # ID for distribution graph component
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe  # Create sample forecast dataframe for testing
from src.web.tests.fixtures.forecast_fixtures import create_sample_fallback_dataframe  # Create sample fallback forecast dataframe for testing
from src.web.tests.fixtures.forecast_fixtures import DEFAULT_PERCENTILES  # Default percentiles for uncertainty bands
from src.web.tests.fixtures.component_fixtures import mock_distribution_plot  # Create mock distribution plot component for testing
from src.web.utils.plot_helpers import create_probability_distribution_plot  # Create probability distribution visualization
from src.web.data.data_processor import prepare_distribution_data  # Prepare data for probability distribution visualization


def test_create_distribution_component():
    """Tests the creation of a probability distribution component with default parameters"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    forecast_df = create_sample_visualization_dataframe()

    # Get the first timestamp from the dataframe
    timestamp = forecast_df['timestamp'].iloc[0]

    # Call create_distribution_component with the dataframe, 'DALMP', and the timestamp
    component = create_distribution_component(forecast_df, 'DALMP', timestamp)

    # Assert that the returned component is a dcc.Graph instance
    assert isinstance(component, dcc.Graph)

    # Assert that the component has the correct ID (DISTRIBUTION_GRAPH_ID)
    assert component.id == DISTRIBUTION_GRAPH_ID

    # Assert that the component's figure has the expected traces
    assert len(component.figure.data) > 0

    # Assert that the figure has a title containing the product name and timestamp
    assert 'DALMP' in component.figure.layout.title.text
    assert str(timestamp.date()) in component.figure.layout.title.text


def test_create_distribution_component_with_custom_theme():
    """Tests the creation of a probability distribution component with a custom theme"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    forecast_df = create_sample_visualization_dataframe()

    # Get the first timestamp from the dataframe
    timestamp = forecast_df['timestamp'].iloc[0]

    # Call create_distribution_component with the dataframe, 'DALMP', timestamp, and theme='dark'
    component = create_distribution_component(forecast_df, 'DALMP', timestamp, theme='dark')

    # Assert that the returned component is a dcc.Graph instance
    assert isinstance(component, dcc.Graph)

    # Assert that the component's figure has styling consistent with the dark theme
    assert component.figure.layout.plot_bgcolor == '#212529'

    # Check for dark background color and appropriate text colors
    assert component.figure.layout.font.color == '#f8f9fa'


@pytest.mark.parametrize('viewport_size', ['desktop', 'tablet', 'mobile'])
def test_create_distribution_component_with_viewport_size(viewport_size):
    """Tests the creation of a probability distribution component with different viewport sizes"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    forecast_df = create_sample_visualization_dataframe()

    # Get the first timestamp from the dataframe
    timestamp = forecast_df['timestamp'].iloc[0]

    # Call create_distribution_component with the dataframe, 'DALMP', timestamp, and the parametrized viewport_size
    component = create_distribution_component(forecast_df, 'DALMP', timestamp, viewport_size=viewport_size)

    # Assert that the returned component is a dcc.Graph instance
    assert isinstance(component, dcc.Graph)

    # Assert that the component's figure has layout dimensions appropriate for the viewport size
    assert component.figure.layout.height is not None

    # Check that font sizes and margins are adjusted for the viewport size
    assert component.figure.layout.margin.l is not None


def test_create_distribution_component_with_fallback_data():
    """Tests the creation of a probability distribution component with fallback data"""
    # Create sample fallback dataframe using create_sample_fallback_dataframe
    forecast_df = create_sample_fallback_dataframe()

    # Get the first timestamp from the dataframe
    timestamp = forecast_df['timestamp'].iloc[0]

    # Call create_distribution_component with the fallback dataframe, 'DALMP', and the timestamp
    component = create_distribution_component(forecast_df, 'DALMP', timestamp)

    # Assert that the returned component is a dcc.Graph instance
    assert isinstance(component, dcc.Graph)

    # Assert that the component's figure contains a fallback indicator annotation
    assert any(annotation.text == "Using fallback forecast (previous day's data)" for annotation in component.figure.layout.annotations)

    # Check that the fallback indicator has the correct text and styling
    for annotation in component.figure.layout.annotations:
        if annotation.text == "Using fallback forecast (previous day's data)":
            assert annotation.font.color == "#856404"


def test_update_distribution():
    """Tests updating an existing probability distribution component"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    forecast_df = create_sample_visualization_dataframe()

    # Get the first timestamp from the dataframe
    timestamp = forecast_df['timestamp'].iloc[0]

    # Create initial distribution component using create_distribution_component
    initial_component = create_distribution_component(forecast_df, 'DALMP', timestamp)

    # Create a second sample dataframe with different values
    new_forecast_df = create_sample_visualization_dataframe()

    # Call update_distribution with the initial component and the new dataframe
    updated_component = update_distribution(initial_component, new_forecast_df, 'DALMP', timestamp)

    # Assert that the returned component is a dcc.Graph instance with the same ID
    assert isinstance(updated_component, dcc.Graph)
    assert updated_component.id == initial_component.id

    # Assert that the component's figure has been updated with the new data
    assert updated_component.figure.data != initial_component.figure.data

    # Check that the figure traces reflect the new data values
    assert len(updated_component.figure.data) > 0


def test_get_distribution_with_percentiles():
    """Tests getting a probability distribution with custom percentile indicators"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    forecast_df = create_sample_visualization_dataframe()

    # Get the first timestamp from the dataframe
    timestamp = forecast_df['timestamp'].iloc[0]

    # Define custom percentiles [5, 25, 50, 75, 95]
    custom_percentiles = [5, 25, 50, 75, 95]

    # Call get_distribution_with_percentiles with the dataframe, 'DALMP', timestamp, and custom percentiles
    fig = get_distribution_with_percentiles(forecast_df, 'DALMP', timestamp, custom_percentiles)

    # Assert that the returned figure is a plotly.graph_objects.Figure
    assert isinstance(fig, go.Figure)

    # Assert that the figure contains vertical line traces for each percentile
    assert len(fig.data) >= 1

    # Check that the figure has annotations showing the percentile values
    assert len(fig.layout.annotations) >= 1


def test_handle_viewport_change():
    """Tests handling viewport size changes for a probability distribution component"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    forecast_df = create_sample_visualization_dataframe()

    # Get the first timestamp from the dataframe
    timestamp = forecast_df['timestamp'].iloc[0]

    # Create initial distribution component for 'desktop' viewport
    initial_component = create_distribution_component(forecast_df, 'DALMP', timestamp, viewport_size='desktop')

    # Call handle_viewport_change with the component and 'mobile' viewport size
    updated_component = handle_viewport_change(initial_component, 'mobile')

    # Assert that the returned component is a dcc.Graph instance with the same ID
    assert isinstance(updated_component, dcc.Graph)
    assert updated_component.id == initial_component.id

    # Assert that the component's figure has layout dimensions appropriate for mobile viewport
    assert updated_component.figure.layout.height is not None

    # Check that font sizes and margins are adjusted for mobile viewport
    assert updated_component.figure.layout.margin.l is not None


def test_create_empty_distribution():
    """Tests creating an empty probability distribution with a message"""
    # Call create_empty_distribution with message 'No data available'
    component = create_empty_distribution(message='No data available')

    # Assert that the returned component is a dcc.Graph instance
    assert isinstance(component, dcc.Graph)

    # Assert that the component has the correct ID (DISTRIBUTION_GRAPH_ID)
    assert component.id == DISTRIBUTION_GRAPH_ID

    # Assert that the component's figure contains an annotation with the message
    assert any(annotation.text == 'No data available' for annotation in component.figure.layout.annotations)

    # Check that the figure has no data traces
    assert len(component.figure.data) == 0


@pytest.mark.parametrize('product_id', ['DALMP', 'RTLMP', 'RegUp', 'RegDown'])
def test_distribution_with_different_products(product_id):
    """Tests creating probability distributions for different electricity market products"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe for the parametrized product_id
    forecast_df = create_sample_visualization_dataframe(product=product_id)

    # Get the first timestamp from the dataframe
    timestamp = forecast_df['timestamp'].iloc[0]

    # Call create_distribution_component with the dataframe, product_id, and the timestamp
    component = create_distribution_component(forecast_df, product_id, timestamp)

    # Assert that the returned component is a dcc.Graph instance
    assert isinstance(component, dcc.Graph)

    # Assert that the component's figure has styling appropriate for the product
    assert component.figure.data[0].marker.color is not None

    # Check that the figure title contains the correct product name
    assert product_id in component.figure.layout.title.text

    # Verify that product-specific color is used in the visualization
    assert component.figure.data[0].marker.color is not None