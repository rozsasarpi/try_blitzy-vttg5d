"""
Integration tests for the dashboard interaction functionality in the Electricity Market Price Forecasting System's web visualization interface.
This module tests the end-to-end user interactions with the dashboard, including product selection, date range changes, visualization options, and data refresh functionality.
"""

import pytest  # pytest: 7.0.0+
import dash  # dash: 2.9.0+
from dash import html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+
import pandas  # pandas: 2.0.0+
from unittest.mock import MagicMock  # standard library

from src.web.tests.conftest import app, test_client, mock_forecast_data, mock_fallback_data, mock_multi_product_data, dashboard_layout, dashboard_state, mock_callback_tester  # src/web/tests/conftest.py
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe, create_sample_fallback_dataframe, create_multi_product_forecast_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.component_fixtures import COMPONENT_IDS  # src/web/tests/fixtures/component_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_callback_context, mock_dashboard_state, CALLBACK_IDS  # src/web/tests/fixtures/callback_fixtures.py
from src.web.layouts.main_dashboard import create_main_dashboard  # src/web/layouts/main_dashboard.py
from src.web.layouts.main_dashboard import MAIN_DASHBOARD_ID  # src/web/layouts/main_dashboard.py
from src.web.callbacks.control_callbacks import register_control_callbacks  # src/web/callbacks/control_callbacks.py
from src.web.callbacks.visualization_callbacks import register_visualization_callbacks  # src/web/callbacks/visualization_callbacks.py
from src.web.callbacks.visualization_callbacks import DASHBOARD_STATE_STORE_ID  # src/web/callbacks/visualization_callbacks.py

TEST_TIMEOUT = 10


@pytest.mark.integration
def test_dashboard_renders_correctly(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container):
    """Tests that the dashboard renders correctly with all expected components"""
    # Arrange
    test_client.server.layout = dashboard_layout

    # Act
    test_client.visit_and_snapshot(
        url='/',
        assert_errors=False,
        timeout=TEST_TIMEOUT,
        snapshot_name='dashboard-render',
    )

    # Assert
    assert test_client.get_element(f'#{COMPONENT_IDS["control_panel"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["time_series"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["distribution"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["forecast_table"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["export_panel"]}')
    assert test_client.get_element(f'#{DASHBOARD_STATE_STORE_ID}')


@pytest.mark.integration
def test_product_selection_interaction(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container, mock_forecast_data: pandas.DataFrame):
    """Tests the interaction flow when selecting different products"""
    # Arrange
     test_client.server.layout = dashboard_layout

    # Act
    test_client.visit(url='/')
    test_client.wait_for_element_by_id(COMPONENT_IDS["control_panel"], timeout=TEST_TIMEOUT)

    # Select a different product from the dropdown
    test_client.select_dcc_dropdown(selector=f'#{CALLBACK_IDS["product_dropdown"]}', value="RTLMP")

    # Wait for the callback to complete
    test_client.wait_for_element_by_id(COMPONENT_IDS["time_series"], timeout=TEST_TIMEOUT)

    # Assert
    assert test_client.get_element(f'#{COMPONENT_IDS["time_series"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["distribution"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["forecast_table"]}')
    assert test_client.get_element(f'#{DASHBOARD_STATE_STORE_ID}')


@pytest.mark.integration
def test_date_range_interaction(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container, mock_forecast_data: pandas.DataFrame):
    """Tests the interaction flow when changing the date range"""
    # Arrange
    test_client.server.layout = dashboard_layout

    # Act
    test_client.visit(url='/')
    test_client.wait_for_element_by_id(COMPONENT_IDS["control_panel"], timeout=TEST_TIMEOUT)

    # Select a different date range
    test_client.select_date_range(selector=f'#{CALLBACK_IDS["date_range"]}', start_date="2023-01-05", end_date="2023-01-07")

    # Wait for the callback to complete
    test_client.wait_for_element_by_id(COMPONENT_IDS["time_series"], timeout=TEST_TIMEOUT)

    # Assert
    assert test_client.get_element(f'#{COMPONENT_IDS["time_series"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["distribution"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["forecast_table"]}')
    assert test_client.get_element(f'#{DASHBOARD_STATE_STORE_ID}')


@pytest.mark.integration
def test_visualization_options_interaction(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container, mock_forecast_data: pandas.DataFrame):
    """Tests the interaction flow when changing visualization options"""
    # Arrange
    test_client.server.layout = dashboard_layout

    # Act
    test_client.visit(url='/')
    test_client.wait_for_element_by_id(COMPONENT_IDS["control_panel"], timeout=TEST_TIMEOUT)

    # Toggle the uncertainty display option
    test_client.select_dcc_checklist(selector=f'#{CALLBACK_IDS["visualization_options"]}', values=[])

    # Wait for the callback to complete
    test_client.wait_for_element_by_id(COMPONENT_IDS["time_series"], timeout=TEST_TIMEOUT)

    # Toggle the historical data option
    test_client.select_dcc_checklist(selector=f'#{CALLBACK_IDS["visualization_options"]}', values=['historical'])

    # Wait for the callback to complete
    test_client.wait_for_element_by_id(COMPONENT_IDS["time_series"], timeout=TEST_TIMEOUT)

    # Assert
    assert test_client.get_element(f'#{COMPONENT_IDS["time_series"]}')
    assert test_client.get_element(f'#{DASHBOARD_STATE_STORE_ID}')


@pytest.mark.integration
def test_refresh_button_interaction(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container, mock_forecast_data: pandas.DataFrame):
    """Tests the interaction flow when clicking the refresh button"""
    # Arrange
    test_client.server.layout = dashboard_layout

    # Act
    test_client.visit(url='/')
    test_client.wait_for_element_by_id(COMPONENT_IDS["control_panel"], timeout=TEST_TIMEOUT)

    # Click the refresh button
    test_client.click_element(selector=f'#{CALLBACK_IDS["refresh_button"]}')

    # Wait for the callback to complete
    test_client.wait_for_element_by_id(COMPONENT_IDS["time_series"], timeout=TEST_TIMEOUT)

    # Assert
    assert test_client.get_element(f'#{COMPONENT_IDS["control_panel"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["time_series"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["distribution"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["forecast_table"]}')
    assert test_client.get_element(f'#{PRODUCT_COMPARISON_GRAPH_ID}')
    assert test_client.get_element(f'#{DASHBOARD_STATE_STORE_ID}')


@pytest.mark.integration
def test_time_series_click_interaction(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container, mock_forecast_data: pandas.DataFrame):
    """Tests the interaction flow when clicking on a point in the time series"""
    # Arrange
    test_client.server.layout = dashboard_layout

    # Act
    test_client.visit(url='/')
    test_client.wait_for_element_by_id(COMPONENT_IDS["time_series"], timeout=TEST_TIMEOUT)

    # Simulate a click on a specific data point
    test_client.click_element(selector=f'#{COMPONENT_IDS["time_series"]}')

    # Wait for the callback to complete
    test_client.wait_for_element_by_id(COMPONENT_IDS["distribution"], timeout=TEST_TIMEOUT)

    # Assert
    assert test_client.get_element(f'#{COMPONENT_IDS["distribution"]}')
    assert test_client.get_element(f'#{DASHBOARD_STATE_STORE_ID}')


@pytest.mark.integration
def test_fallback_indicator_display(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container, mock_fallback_data: pandas.DataFrame):
    """Tests that the fallback indicator is displayed when using fallback data"""
    # Arrange
    test_client.server.layout = dashboard_layout

    # Act
    test_client.visit(url='/')
    test_client.wait_for_element_by_id(COMPONENT_IDS["control_panel"], timeout=TEST_TIMEOUT)

    # Assert
    assert test_client.get_element(f'#{COMPONENT_IDS["control_panel"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["time_series"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["distribution"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["forecast_table"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["export_panel"]}')
    assert test_client.get_element(f'#{DASHBOARD_STATE_STORE_ID}')


@pytest.mark.integration
def test_product_comparison_interaction(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container, mock_multi_product_data: pandas.DataFrame):
    """Tests the interaction flow when using the product comparison view"""
    # Arrange
    test_client.server.layout = dashboard_layout

    # Act
    test_client.visit(url='/')
    test_client.wait_for_element_by_id(COMPONENT_IDS["control_panel"], timeout=TEST_TIMEOUT)

    # Click to navigate to the product comparison view
    test_client.click_element(selector=f'#{PRODUCT_COMPARISON_GRAPH_ID}')

    # Wait for the view to update
    test_client.wait_for_element_by_id(PRODUCT_COMPARISON_GRAPH_ID, timeout=TEST_TIMEOUT)

    # Assert
    assert test_client.get_element(f'#{PRODUCT_COMPARISON_GRAPH_ID}')
    assert test_client.get_element(f'#{DASHBOARD_STATE_STORE_ID}')


@pytest.mark.integration
def test_export_functionality(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container, mock_forecast_data: pandas.DataFrame):
    """Tests the data export functionality"""
    # Arrange
    test_client.server.layout = dashboard_layout

    # Act
    test_client.visit(url='/')
    test_client.wait_for_element_by_id(COMPONENT_IDS["export_panel"], timeout=TEST_TIMEOUT)

    # Click the CSV export button
    test_client.click_element(selector=f'#{COMPONENT_IDS["export_panel"]}')

    # Wait for the callback to complete
    test_client.wait_for_download(timeout=TEST_TIMEOUT)

    # Assert
    assert test_client.get_element(f'#{COMPONENT_IDS["export_panel"]}')


@pytest.mark.integration
def test_responsive_layout_changes(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container, mock_forecast_data: pandas.DataFrame):
    """Tests that the dashboard layout responds appropriately to viewport size changes"""
    # Arrange
    test_client.server.layout = dashboard_layout

    # Act
    test_client.visit(url='/')
    test_client.wait_for_element_by_id(COMPONENT_IDS["control_panel"], timeout=TEST_TIMEOUT)

    # Assert
    assert test_client.get_element(f'#{COMPONENT_IDS["control_panel"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["time_series"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["distribution"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["forecast_table"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["export_panel"]}')
    assert test_client.get_element(f'#{DASHBOARD_STATE_STORE_ID}')


@pytest.mark.integration
def test_end_to_end_user_flow(test_client: dash.testing.DashTestClient, dashboard_layout: dbc.Container, mock_forecast_data: pandas.DataFrame, mock_multi_product_data: pandas.DataFrame):
    """Tests a complete end-to-end user interaction flow"""
    # Arrange
    test_client.server.layout = dashboard_layout

    # Act
    test_client.visit(url='/')
    test_client.wait_for_element_by_id(COMPONENT_IDS["control_panel"], timeout=TEST_TIMEOUT)

    # Select a different product from the dropdown
    test_client.select_dcc_dropdown(selector=f'#{CALLBACK_IDS["product_dropdown"]}', value="RTLMP")

    # Wait for the visualizations to update
    test_client.wait_for_element_by_id(COMPONENT_IDS["time_series"], timeout=TEST_TIMEOUT)

    # Change the date range
    test_client.select_date_range(selector=f'#{CALLBACK_IDS["date_range"]}', start_date="2023-01-05", end_date="2023-01-07")

    # Wait for the visualizations to update
    test_client.wait_for_element_by_id(COMPONENT_IDS["time_series"], timeout=TEST_TIMEOUT)

    # Toggle visualization options
    test_client.select_dcc_checklist(selector=f'#{CALLBACK_IDS["visualization_options"]}', values=[])

    # Wait for the visualizations to update
    test_client.wait_for_element_by_id(COMPONENT_IDS["time_series"], timeout=TEST_TIMEOUT)

    # Click on a point in the time series
    test_client.click_element(selector=f'#{COMPONENT_IDS["time_series"]}')

    # Verify that the distribution plot updates
    test_client.wait_for_element_by_id(COMPONENT_IDS["distribution"], timeout=TEST_TIMEOUT)

    # Navigate to the product comparison view
    test_client.click_element(selector=f'#{PRODUCT_COMPARISON_GRAPH_ID}')

    # Verify that multiple products are displayed
    test_client.wait_for_element_by_id(PRODUCT_COMPARISON_GRAPH_ID, timeout=TEST_TIMEOUT)

    # Export the data in CSV format
    test_client.click_element(selector=f'#{COMPONENT_IDS["export_panel"]}')

    # Verify that the export was successful
    test_client.wait_for_download(timeout=TEST_TIMEOUT)

    # Click the refresh button
    test_client.click_element(selector=f'#{CALLBACK_IDS["refresh_button"]}')

    # Verify that all visualizations have been refreshed
    test_client.wait_for_element_by_id(COMPONENT_IDS["time_series"], timeout=TEST_TIMEOUT)

    # Assert
    assert test_client.get_element(f'#{COMPONENT_IDS["control_panel"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["time_series"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["distribution"]}')
    assert test_client.get_element(f'#{COMPONENT_IDS["forecast_table"]}')
    assert test_client.get_element(f'#{PRODUCT_COMPARISON_GRAPH_ID}')
    assert test_client.get_element(f'#{DASHBOARD_STATE_STORE_ID}')