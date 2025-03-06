"""
Unit tests for the forecast table component of the Electricity Market Price Forecasting Dashboard.
This module tests the creation, updating, and display of tabular forecast data with various
configurations and data scenarios.
"""

# External imports
import pytest  # pytest: 7.0.0+
import dash_table  # version 2.9.0+
import dash_html_components as html  # version 2.9.0+
import pandas  # pandas: 2.0.0+
from datetime import datetime  # standard library

# Internal imports
from src.web.components.forecast_table import create_forecast_table  # Function to create forecast table component
from src.web.components.forecast_table import update_forecast_table  # Function to update existing forecast table
from src.web.components.forecast_table import handle_viewport_change  # Function to update table for viewport changes
from src.web.components.forecast_table import create_empty_table  # Function to create empty table with message
from src.web.components.forecast_table import FORECAST_TABLE_ID  # ID constant for forecast table component
from src.web.data.data_processor import prepare_hourly_table_data  # Function to prepare data for tabular display
from src.web.data.data_processor import DEFAULT_PERCENTILES  # Default percentiles for uncertainty bands
from src.web.config.product_config import DEFAULT_PRODUCT  # Default product to display
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe  # Create sample forecast dataframe for testing
from src.web.tests.fixtures.forecast_fixtures import create_sample_fallback_dataframe  # Create sample fallback forecast for testing


def test_create_forecast_table_with_valid_data():
    """Tests that create_forecast_table correctly creates a table component with valid forecast data"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()
    # Call create_forecast_table with the sample data and DEFAULT_PRODUCT
    component = create_forecast_table(sample_data, DEFAULT_PRODUCT)
    # Assert that the returned component is a dash_html_components.Div
    assert isinstance(component, html.Div)
    # Assert that the component has the correct ID (FORECAST_TABLE_ID)
    assert component.id == f"{FORECAST_TABLE_ID}-container"
    # Assert that the component contains a dash_table.DataTable
    assert any(isinstance(child, dash_table.DataTable) for child in component.children)
    table = next(child for child in component.children if isinstance(child, dash_table.DataTable))
    # Assert that the table has the expected number of rows and columns
    assert len(table.data) == len(sample_data)
    assert len(table.columns) == len(prepare_hourly_table_data(sample_data, DEFAULT_PRODUCT).columns)
    # Assert that the table contains the expected data values
    assert table.data[0]['hour'] == prepare_hourly_table_data(sample_data, DEFAULT_PRODUCT).iloc[0]['hour']


def test_create_forecast_table_with_custom_percentiles():
    """Tests that create_forecast_table correctly handles custom percentile values"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()
    # Define custom percentiles [5, 95] different from DEFAULT_PERCENTILES
    custom_percentiles = [5, 95]
    # Call create_forecast_table with the sample data, DEFAULT_PRODUCT, and custom percentiles
    component = create_forecast_table(sample_data, DEFAULT_PRODUCT, percentiles=custom_percentiles)
    # Assert that the returned component contains a dash_table.DataTable
    assert any(isinstance(child, dash_table.DataTable) for child in component.children)
    table = next(child for child in component.children if isinstance(child, dash_table.DataTable))
    # Assert that the table columns include the custom percentile labels
    column_ids = [col['id'] for col in table.columns]
    assert 'percentile_5_formatted' in column_ids
    assert 'percentile_95_formatted' in column_ids
    # Assert that the table data contains values for the custom percentiles
    assert all('percentile_5_formatted' in row for row in table.data)
    assert all('percentile_95_formatted' in row for row in table.data)


@pytest.mark.parametrize('viewport_size', ['desktop', 'tablet', 'mobile'])
def test_create_forecast_table_with_different_viewport_sizes(viewport_size):
    """Tests that create_forecast_table adapts to different viewport sizes"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()
    # Call create_forecast_table with the sample data, DEFAULT_PRODUCT, and the parametrized viewport_size
    component = create_forecast_table(sample_data, DEFAULT_PRODUCT, viewport_size=viewport_size)
    # Assert that the returned component contains a dash_table.DataTable
    assert any(isinstance(child, dash_table.DataTable) for child in component.children)
    table = next(child for child in component.children if isinstance(child, dash_table.DataTable))
    # Assert that the table's page_size property is appropriate for the viewport_size
    if viewport_size == 'mobile':
        assert table.page_size == 6
    elif viewport_size == 'tablet':
        assert table.page_size == 8
    else:
        assert table.page_size == 12
    # Assert that the table's style properties are appropriate for the viewport_size
    assert 'fontSize' in table.style_cell


def test_create_forecast_table_with_fallback_data():
    """Tests that create_forecast_table correctly handles fallback forecast data"""
    # Create sample fallback dataframe using create_sample_fallback_dataframe
    sample_data = create_sample_fallback_dataframe()
    # Call create_forecast_table with the fallback data and DEFAULT_PRODUCT
    component = create_forecast_table(sample_data, DEFAULT_PRODUCT)
    # Assert that the returned component contains a dash_table.DataTable
    assert any(isinstance(child, dash_table.DataTable) for child in component.children)
    # Assert that the component includes a fallback indicator element
    assert any(isinstance(child, html.Div) and "fallback-indicator" in child.className for child in component.children)
    table = next(child for child in component.children if isinstance(child, dash_table.DataTable))
    # Assert that the table contains the expected fallback data values
    assert len(table.data) == len(sample_data)


def test_create_forecast_table_with_empty_data():
    """Tests that create_forecast_table handles empty dataframes gracefully"""
    # Create an empty pandas DataFrame with the correct columns
    empty_df = pandas.DataFrame(columns=['timestamp', 'hour', 'point_forecast_formatted', 'percentile_10_formatted', 'percentile_90_formatted', 'range_formatted'])
    # Call create_forecast_table with the empty dataframe and DEFAULT_PRODUCT
    component = create_forecast_table(empty_df, DEFAULT_PRODUCT)
    # Assert that the returned component is a dash_html_components.Div
    assert isinstance(component, html.Div)
    # Assert that the component has the correct ID (FORECAST_TABLE_ID)
    assert component.id == f"{FORECAST_TABLE_ID}-container"
    # Assert that the component contains an appropriate message about no data
    assert "No forecast data available" in component.children


def test_update_forecast_table():
    """Tests that update_forecast_table correctly updates an existing table with new data"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    initial_data = create_sample_visualization_dataframe()
    # Call create_forecast_table to create an initial table component
    initial_component = create_forecast_table(initial_data, DEFAULT_PRODUCT)
    # Extract the dash_table.DataTable from the component
    initial_table = next(child for child in initial_component.children if isinstance(child, dash_table.DataTable))
    # Create a different sample dataframe with new values
    new_data = create_sample_visualization_dataframe()
    # Call update_forecast_table with the table component and new data
    updated_table = update_forecast_table(initial_table, new_data, DEFAULT_PRODUCT)
    # Assert that the updated table contains the new data values
    assert updated_table.data == prepare_hourly_table_data(new_data, DEFAULT_PRODUCT).to_dict('records')
    # Assert that the table structure (columns, styling) remains consistent
    assert updated_table.columns == initial_table.columns
    assert updated_table.style_cell == initial_table.style_cell


def test_handle_viewport_change():
    """Tests that handle_viewport_change correctly updates a table for a new viewport size"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()
    # Call create_forecast_table with viewport_size='desktop' to create an initial table
    initial_component = create_forecast_table(sample_data, viewport_size='desktop')
    # Extract the dash_table.DataTable from the component
    initial_table = next(child for child in initial_component.children if isinstance(child, dash_table.DataTable))
    # Call handle_viewport_change with the table and new_viewport_size='mobile'
    updated_table = handle_viewport_change(initial_table, new_viewport_size='mobile')
    # Assert that the updated table has page_size appropriate for mobile
    assert updated_table.page_size == 6
    # Assert that the updated table has styling appropriate for mobile
    assert 'fontSize' in updated_table.style_cell
    # Assert that the table data remains unchanged
    assert updated_table.data == initial_table.data


def test_create_empty_table():
    """Tests that create_empty_table correctly creates a placeholder with a message"""
    # Define a test message "No forecast data available"
    test_message = "No forecast data available"
    # Call create_empty_table with the test message
    component = create_empty_table(test_message)
    # Assert that the returned component is a dash_html_components.Div
    assert isinstance(component, html.Div)
    # Assert that the component has the correct ID (FORECAST_TABLE_ID)
    assert component.id == f"{FORECAST_TABLE_ID}-container"
    # Assert that the component contains the test message text
    assert test_message in component.children


def test_table_pagination():
    """Tests that the forecast table implements pagination correctly"""
    # Create a large sample visualization dataframe (>20 hours) using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe(hours=25)
    # Call create_forecast_table with the sample data and DEFAULT_PRODUCT
    component = create_forecast_table(sample_data, DEFAULT_PRODUCT)
    # Assert that the returned component contains a dash_table.DataTable
    assert any(isinstance(child, dash_table.DataTable) for child in component.children)
    table = next(child for child in component.children if isinstance(child, dash_table.DataTable))
    # Assert that the table has pagination enabled
    assert table.page_action == 'native'
    # Assert that the table's page_size property is set correctly
    assert table.page_size == 12


def test_table_sorting():
    """Tests that the forecast table implements column sorting correctly"""
    # Create sample visualization dataframe using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()
    # Call create_forecast_table with the sample data and DEFAULT_PRODUCT
    component = create_forecast_table(sample_data, DEFAULT_PRODUCT)
    # Assert that the returned component contains a dash_table.DataTable
    assert any(isinstance(child, dash_table.DataTable) for child in component.children)
    table = next(child for child in component.children if isinstance(child, dash_table.DataTable))
    # Assert that the table's sort_action property is set to 'native'
    assert table.sort_action == 'native'
    # Assert that the table's columns have sort_mode property set appropriately
    for col in table.columns:
        assert 'sort_by' in col