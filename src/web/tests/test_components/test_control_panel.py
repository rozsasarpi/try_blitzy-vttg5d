"""
Unit tests for the control panel component of the Electricity Market Price Forecasting System's Dash-based visualization interface.
This module tests the functionality, rendering, and behavior of the control panel component, including product selection, date range picking,
visualization options, and status indicators.
"""

import pytest  # pytest: 7.0.0+
import dash  # dash: 2.9.0+
from dash import html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+
from datetime import datetime  # standard library
from unittest import mock  # standard library

from src.web.components.control_panel import create_control_panel, CONTROL_PANEL_ID, PRODUCT_DROPDOWN_ID, \
    DATE_RANGE_PICKER_ID, VISUALIZATION_OPTIONS_ID, REFRESH_BUTTON_ID, LAST_UPDATED_ID, FORECAST_STATUS_ID  # Import the control panel component function for testing
from src.web.tests.fixtures.component_fixtures import mock_component, mock_control_panel, mock_dash_app  # Create mock components for testing
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe, create_sample_fallback_dataframe  # Create sample forecast data for testing
from src.web.config.product_config import DEFAULT_PRODUCT, PRODUCTS, get_default_date_range  # Default product for testing


def test_control_panel_creation():
    """Tests that the control panel component is created correctly with default parameters"""
    # Arrange
    forecast_data = create_sample_visualization_dataframe()  # Create a sample forecast dataframe

    # Act
    component = create_control_panel(forecast_data, theme='light', viewport_size='md')  # Call create_control_panel with the sample data

    # Assert
    assert isinstance(component, dbc.Card), "The returned component is not a dbc.Card"  # Assert that the returned component is a dbc.Card
    assert component.id == CONTROL_PANEL_ID, "The component does not have the correct ID (CONTROL_PANEL_ID)"  # Assert that the component has the correct ID (CONTROL_PANEL_ID)
    assert any(isinstance(child, dbc.CardBody) for child in component.children), "The component does not contain a CardBody"  # Assert that the component contains a CardBody
    card_body = next(child for child in component.children if isinstance(child, dbc.CardBody))
    assert len(card_body.children) > 0, "The component does not contain all expected sub-components"  # Assert that the component contains all expected sub-components


def test_control_panel_with_fallback_data():
    """Tests that the control panel correctly displays fallback status when using fallback data"""
    # Arrange
    forecast_data = create_sample_fallback_dataframe()  # Create a sample fallback dataframe using create_sample_fallback_dataframe

    # Act
    component = create_control_panel(forecast_data, theme='light', viewport_size='md')  # Call create_control_panel with the fallback data

    # Assert
    status_component = next((child for child in component.children[0].children if getattr(child, 'id', None) == FORECAST_STATUS_ID), None)  # Find the forecast status component by ID (FORECAST_STATUS_ID)
    assert status_component is not None, "The component does not contain a forecast status component"  # Assert that the status component exists
    assert "Fallback" in str(status_component.children[0].children), "The status component does not contain a fallback indicator"  # Assert that the status component contains a fallback indicator
    assert "color:warning" in str(status_component.children[0]), "The fallback indicator does not have the correct styling"  # Assert that the fallback indicator has the correct styling
    assert "Fallback" in str(status_component.children[0].children), "The fallback indicator does not have the correct text"  # Assert that the fallback indicator has the correct text


def test_product_dropdown():
    """Tests that the product dropdown component is created correctly with the expected options"""
    # Arrange
    forecast_data = create_sample_visualization_dataframe()  # Create a sample forecast dataframe

    # Act
    component = create_control_panel(forecast_data, theme='light', viewport_size='md')  # Call create_control_panel with the sample data

    # Assert
    dropdown_component = next((child for child in component.children[0].children if getattr(child, 'id', None) == PRODUCT_DROPDOWN_ID), None)  # Find the product dropdown component by ID (PRODUCT_DROPDOWN_ID)
    assert dropdown_component is not None, "The component does not contain a product dropdown component"  # Assert that the product dropdown component exists
    dropdown_options = [option['value'] for option in dropdown_component.options]
    assert all(product in dropdown_options for product in PRODUCTS), "The dropdown does not contain options for all products in PRODUCTS"  # Assert that the dropdown contains options for all products in PRODUCTS
    assert dropdown_component.value == DEFAULT_PRODUCT, "The default selected value is not DEFAULT_PRODUCT"  # Assert that the default selected value is DEFAULT_PRODUCT


def test_date_range_picker():
    """Tests that the date range picker component is created correctly with the expected date range"""
    # Arrange
    forecast_data = create_sample_visualization_dataframe()  # Create a sample forecast dataframe
    start_date, end_date = get_default_date_range()  # Get the default date range using get_default_date_range

    # Act
    component = create_control_panel(forecast_data, theme='light', viewport_size='md')  # Call create_control_panel with the sample data

    # Assert
    date_picker_component = next((child for child in component.children[0].children if getattr(child, 'id', None) == DATE_RANGE_PICKER_ID), None)  # Find the date range picker component by ID (DATE_RANGE_PICKER_ID)
    assert date_picker_component is not None, "The component does not contain a date range picker component"  # Assert that the date range picker component exists
    assert date_picker_component.start_date == start_date.strftime('%Y-%m-%d'), "The date picker does not have the correct start date"  # Assert that the date picker has the correct start date
    assert date_picker_component.end_date == end_date.strftime('%Y-%m-%d'), "The date picker does not have the correct end date"  # Assert that the date picker has the correct end date
    assert date_picker_component.min_date_allowed == (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d'), "The date picker does not have the correct min selectable date"  # Assert that the date picker has the correct min selectable dates
    assert date_picker_component.max_date_allowed == (datetime.today() + timedelta(days=3)).strftime('%Y-%m-%d'), "The date picker does not have the correct max selectable date"  # Assert that the date picker has the correct max selectable dates


def test_visualization_options():
    """Tests that the visualization options component is created correctly with the expected options"""
    # Arrange
    forecast_data = create_sample_visualization_dataframe()  # Create a sample forecast dataframe

    # Act
    component = create_control_panel(forecast_data, theme='light', viewport_size='md')  # Call create_control_panel with the sample data

    # Assert
    visualization_options_component = next((child for child in component.children[0].children if getattr(child, 'id', None) == VISUALIZATION_OPTIONS_ID), None)  # Find the visualization options component by ID (VISUALIZATION_OPTIONS_ID)
    assert visualization_options_component is not None, "The component does not contain a visualization options component"  # Assert that the visualization options component exists
    options = [option['value'] for option in visualization_options_component.options]
    assert 'point_forecast' in options and 'uncertainty' in options and 'historical' in options, "The options do not include 'point_forecast', 'uncertainty', and 'historical'"  # Assert that the options include 'point_forecast', 'uncertainty', and 'historical'
    assert 'point_forecast' in visualization_options_component.value and 'uncertainty' in visualization_options_component.value, "The 'point_forecast' and 'uncertainty' are not selected by default"  # Assert that 'point_forecast' and 'uncertainty' are selected by default
    assert 'historical' not in visualization_options_component.value, "The 'historical' is selected by default"  # Assert that 'historical' is not selected by default


def test_refresh_button():
    """Tests that the refresh button component is created correctly"""
    # Arrange
    forecast_data = create_sample_visualization_dataframe()  # Create a sample forecast dataframe

    # Act
    component = create_control_panel(forecast_data, theme='light', viewport_size='md')  # Call create_control_panel with the sample data

    # Assert
    refresh_button_component = next((child for child in component.children[0].children if getattr(child, 'id', None) == REFRESH_BUTTON_ID), None)  # Find the refresh button component by ID (REFRESH_BUTTON_ID)
    assert refresh_button_component is not None, "The component does not contain a refresh button component"  # Assert that the refresh button component exists
    assert "Refresh Data" in str(refresh_button_component.children[1]), "The button does not have the correct text ('Refresh Data')"  # Assert that the button has the correct text ('Refresh Data')
    assert "color:primary" in str(refresh_button_component), "The button does not have the correct color and styling"  # Assert that the button has the correct color and styling


def test_last_updated_info():
    """Tests that the last updated info component is created correctly with the expected timestamp"""
    # Arrange
    generation_timestamp = datetime(2023, 1, 1, 7, 15)  # Create a sample forecast dataframe with a known generation timestamp
    forecast_data = create_sample_visualization_dataframe(start_time=generation_timestamp)

    # Act
    component = create_control_panel(forecast_data, theme='light', viewport_size='md')  # Call create_control_panel with the sample data

    # Assert
    last_updated_component = next((child for child in component.children[0].children if getattr(child, 'id', None) == LAST_UPDATED_ID), None)  # Find the last updated info component by ID (LAST_UPDATED_ID)
    assert last_updated_component is not None, "The component does not contain a last updated info component"  # Assert that the last updated info component exists
    assert "2023-01-01 07:15:00" in str(last_updated_component.children), "The component does not contain the expected formatted timestamp"  # Assert that the component contains the expected formatted timestamp


def test_forecast_status():
    """Tests that the forecast status component is created correctly for normal forecasts"""
    # Arrange
    forecast_data = create_sample_visualization_dataframe()  # Create a sample forecast dataframe (not fallback)

    # Act
    component = create_control_panel(forecast_data, theme='light', viewport_size='md')  # Call create_control_panel with the sample data

    # Assert
    status_component = next((child for child in component.children[0].children if getattr(child, 'id', None) == FORECAST_STATUS_ID), None)  # Find the forecast status component by ID (FORECAST_STATUS_ID)
    assert status_component is not None, "The component does not contain a forecast status component"  # Assert that the forecast status component exists
    assert "Normal" in str(status_component.children[0].children), "The status component does not contain a 'Normal' status indicator"  # Assert that the status component contains a 'Normal' status indicator
    assert "color:success" in str(status_component.children[0]), "The status indicator does not have the correct styling"  # Assert that the status indicator has the correct styling


def test_control_panel_responsive_layout():
    """Tests that the control panel layout adapts correctly to different viewport sizes"""
    # Arrange
    forecast_data = create_sample_visualization_dataframe()  # Create a sample forecast dataframe

    # Act
    component_sm = create_control_panel(forecast_data, theme='light', viewport_size='sm')  # Call create_control_panel with viewport_size='sm' (small)
    component_lg = create_control_panel(forecast_data, theme='light', viewport_size='lg')  # Call create_control_panel with viewport_size='lg' (large)

    # Assert
    assert "col-sm-12" in str(component_sm), "The layout does not use appropriate styling for small screens"  # Assert that the layout uses appropriate styling for small screens
    assert "col-lg-3" in str(component_lg), "The layout does not use appropriate styling for large screens"  # Assert that the layout uses appropriate styling for large screens


def test_control_panel_with_no_data():
    """Tests that the control panel handles the case when no forecast data is available"""
    # Act
    component = create_control_panel(None, theme='light', viewport_size='md')  # Call create_control_panel with forecast_data=None

    # Assert
    status_component = next((child for child in component.children[0].children if getattr(child, 'id', None) == FORECAST_STATUS_ID), None)  # Find the forecast status component by ID (FORECAST_STATUS_ID)
    assert status_component is not None, "The component is still created successfully"  # Assert that the component is still created successfully
    assert "Loading..." in str(status_component.children[0].children), "The status shows a loading or unavailable indicator"  # Assert that the status shows a loading or unavailable indicator

    last_updated_component = next((child for child in component.children[0].children if getattr(child, 'id', None) == LAST_UPDATED_ID), None)  # Find the last updated info component by ID (LAST_UPDATED_ID)
    assert "Last updated:" in str(last_updated_component), "It shows a fallback timestamp or 'Not available' message"  # Assert that it shows a fallback timestamp or 'Not available' message


def test_control_panel_theme_support():
    """Tests that the control panel correctly applies different themes"""
    # Arrange
    forecast_data = create_sample_visualization_dataframe()  # Create a sample forecast dataframe

    # Act
    component_light = create_control_panel(forecast_data, theme='light', viewport_size='md')  # Call create_control_panel with theme='light'
    component_dark = create_control_panel(forecast_data, theme='dark', viewport_size='md')  # Call create_control_panel with theme='dark'

    # Assert
    assert "bg-light" in str(component_light), "The component uses light theme styling"  # Assert that the component uses light theme styling
    assert "bg-dark" in str(component_dark), "The component uses dark theme styling"  # Assert that the component uses dark theme styling


def find_component_by_id(component: dash.development.base_component.Component, component_id: str) -> dash.development.base_component.Component:
    """Helper function to find a component by ID in a Dash component tree"""
    if getattr(component, 'id', None) == component_id:  # Check if the current component has the target ID
        return component  # If it does, return the component

    if hasattr(component, 'children'):  # If the component has children, recursively search through them
        if isinstance(component.children, list):
            for child in component.children:
                found_component = find_component_by_id(child, component_id)
                if found_component:
                    return found_component
        else:
            found_component = find_component_by_id(component.children, component_id)
            if found_component:
                return found_component

    return None  # Return None if the component is not found