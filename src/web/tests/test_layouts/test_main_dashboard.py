"""
Unit tests for the main dashboard layout of the Electricity Market Price Forecasting System's web visualization interface.
This module tests the creation, structure, and responsiveness of the main dashboard layout, ensuring it
correctly integrates all required components and adapts to different viewport sizes.
"""

# Standard library imports
import pytest  # pytest: 7.0.0+

# Third-party imports
import dash_bootstrap_components as dbc  # dash-bootstrap-components: 1.0.0+
import dash_html_components as html  # dash: 2.9.0+
import dash_core_components as dcc  # dash: 2.9.0+

# Internal imports
from src.web.layouts.main_dashboard import create_main_dashboard, get_initial_dashboard_state, update_dashboard_for_viewport, MAIN_DASHBOARD_ID, TIME_SERIES_SECTION_ID, DISTRIBUTION_SECTION_ID, TABLE_SECTION_ID, COMPARISON_SECTION_ID, EXPORT_SECTION_ID
from src.web.layouts.responsive import VIEWPORT_STORE_ID
from src.web.config.dashboard_config import DEFAULT_VIEWPORT, DASHBOARD_SECTIONS
from src.web.components.control_panel import CONTROL_PANEL_ID
from src.web.components.time_series import TIME_SERIES_GRAPH_ID
from src.web.components.probability_distribution import DISTRIBUTION_GRAPH_ID
from src.web.components.forecast_table import FORECAST_TABLE_ID
from src.web.components.product_comparison import PRODUCT_COMPARISON_GRAPH_ID
from src.web.components.export_panel import EXPORT_PANEL_ID
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe, create_sample_fallback_dataframe


def test_create_main_dashboard():
    """Tests that the main dashboard is created correctly with all required components"""
    # Step 1: Create sample forecast data using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Step 2: Call create_main_dashboard with the sample data
    dashboard = create_main_dashboard(sample_data)

    # Step 3: Assert that the returned object is a dbc.Container
    assert isinstance(dashboard, dbc.Container)

    # Step 4: Assert that the container has the correct ID (MAIN_DASHBOARD_ID)
    assert dashboard.id == MAIN_DASHBOARD_ID

    # Step 5: Assert that the container contains the viewport store component
    assert any(isinstance(child, dcc.Store) and child.id == VIEWPORT_STORE_ID for child in dashboard.children)

    # Step 6: Assert that all required sections are present in the dashboard
    section_ids = [TIME_SERIES_SECTION_ID, DISTRIBUTION_SECTION_ID, TABLE_SECTION_ID, COMPARISON_SECTION_ID, EXPORT_SECTION_ID]
    assert all(any(section_id in str(child) for child in dashboard.children) for section_id in section_ids)


def test_dashboard_contains_all_sections():
    """Tests that the dashboard contains all required sections"""
    # Step 1: Create sample forecast data using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Step 2: Call create_main_dashboard with the sample data
    dashboard = create_main_dashboard(sample_data)

    # Step 3: Assert that the dashboard contains the control panel section
    assert any(isinstance(child, dbc.Card) and CONTROL_PANEL_ID in str(child) for child in dashboard.children)

    # Step 4: Assert that the dashboard contains the time series section
    assert any(isinstance(child, dbc.Card) and TIME_SERIES_SECTION_ID in str(child) for child in dashboard.children)

    # Step 5: Assert that the dashboard contains the distribution section
    assert any(isinstance(child, dbc.Card) and DISTRIBUTION_SECTION_ID in str(child) for child in dashboard.children)

    # Step 6: Assert that the dashboard contains the table section
    assert any(isinstance(child, dbc.Card) and TABLE_SECTION_ID in str(child) for child in dashboard.children)

    # Step 7: Assert that the dashboard contains the comparison section
    assert any(isinstance(child, dbc.Card) and COMPARISON_SECTION_ID in str(child) for child in dashboard.children)

    # Step 8: Assert that the dashboard contains the export section
    assert any(isinstance(child, dbc.Card) and EXPORT_SECTION_ID in str(child) for child in dashboard.children)


def test_dashboard_sections_have_correct_ids():
    """Tests that each dashboard section has the correct ID"""
    # Step 1: Create sample forecast data using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Step 2: Call create_main_dashboard with the sample data
    dashboard = create_main_dashboard(sample_data)

    # Step 3: Assert that the time series section has ID TIME_SERIES_SECTION_ID
    assert any(isinstance(child, dbc.Card) and child.id == TIME_SERIES_SECTION_ID for child in dashboard.children)

    # Step 4: Assert that the distribution section has ID DISTRIBUTION_SECTION_ID
    assert any(isinstance(child, dbc.Card) and child.id == DISTRIBUTION_SECTION_ID for child in dashboard.children)

    # Step 5: Assert that the table section has ID TABLE_SECTION_ID
    assert any(isinstance(child, dbc.Card) and child.id == TABLE_SECTION_ID for child in dashboard.children)

    # Step 6: Assert that the comparison section has ID COMPARISON_SECTION_ID
    assert any(isinstance(child, dbc.Card) and child.id == COMPARISON_SECTION_ID for child in dashboard.children)

    # Step 7: Assert that the export section has ID EXPORT_SECTION_ID
    assert any(isinstance(child, dbc.Card) and child.id == EXPORT_SECTION_ID for child in dashboard.children)


def test_dashboard_components_have_correct_ids():
    """Tests that each dashboard component has the correct ID"""
    # Step 1: Create sample forecast data using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Step 2: Call create_main_dashboard with the sample data
    dashboard = create_main_dashboard(sample_data)

    # Step 3: Assert that the control panel component has ID CONTROL_PANEL_ID
    assert any(isinstance(child, dbc.Card) and CONTROL_PANEL_ID in str(child) for child in dashboard.children)

    # Step 4: Assert that the time series graph has ID TIME_SERIES_GRAPH_ID
    assert any(isinstance(child, dcc.Graph) and TIME_SERIES_GRAPH_ID == child.id for child in dashboard.children)

    # Step 5: Assert that the distribution graph has ID DISTRIBUTION_GRAPH_ID
    assert any(isinstance(child, dcc.Graph) and DISTRIBUTION_GRAPH_ID == child.id for child in dashboard.children)

    # Step 6: Assert that the forecast table has ID FORECAST_TABLE_ID
    assert any(isinstance(child, dash_table_components.DataTable) and FORECAST_TABLE_ID == child.id for child in dashboard.children)

    # Step 7: Assert that the product comparison graph has ID PRODUCT_COMPARISON_GRAPH_ID
    assert any(isinstance(child, dcc.Graph) and PRODUCT_COMPARISON_GRAPH_ID == child.id for child in dashboard.children)

    # Step 8: Assert that the export panel has ID EXPORT_PANEL_ID
    assert any(isinstance(child, dbc.Card) and EXPORT_PANEL_ID in str(child) for child in dashboard.children)


def test_get_initial_dashboard_state():
    """Tests that the initial dashboard state is correctly generated"""
    # Step 1: Call get_initial_dashboard_state()
    initial_state = get_initial_dashboard_state()

    # Step 2: Assert that the returned object is a dictionary
    assert isinstance(initial_state, dict)

    # Step 3: Assert that the dictionary contains 'product' key with a valid value
    assert 'product' in initial_state
    assert initial_state['product'] is not None

    # Step 4: Assert that the dictionary contains 'date_range' key with start and end dates
    assert 'start_date' in initial_state
    assert 'end_date' in initial_state

    # Step 5: Assert that the dictionary contains 'visualization_options' key with default options
    assert 'show_uncertainty' in initial_state
    assert 'show_historical' in initial_state


def test_update_dashboard_for_viewport():
    """Tests that the dashboard is correctly updated for different viewport sizes"""
    # Step 1: Create sample forecast data using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Step 2: Create initial dashboard with DEFAULT_VIEWPORT
    initial_dashboard = create_main_dashboard(sample_data, viewport_size=DEFAULT_VIEWPORT)

    # Step 3: Update dashboard for 'mobile' viewport
    mobile_dashboard = update_dashboard_for_viewport(initial_dashboard, 'mobile', sample_data)

    # Step 4: Assert that the updated dashboard is a dbc.Container
    assert isinstance(mobile_dashboard, dbc.Container)

    # Step 5: Assert that the updated dashboard has the correct ID
    assert mobile_dashboard.id == MAIN_DASHBOARD_ID

    # Step 6: Assert that the layout is appropriate for mobile viewport
    # Add assertions to check the layout for mobile viewport

    # Step 7: Update dashboard for 'tablet' viewport
    tablet_dashboard = update_dashboard_for_viewport(initial_dashboard, 'tablet', sample_data)

    # Step 8: Assert that the layout is appropriate for tablet viewport
    # Add assertions to check the layout for tablet viewport

    # Step 9: Update dashboard for 'desktop' viewport
    desktop_dashboard = update_dashboard_for_viewport(initial_dashboard, 'desktop', sample_data)

    # Step 10: Assert that the layout is appropriate for desktop viewport
    # Add assertions to check the layout for desktop viewport
    assert isinstance(desktop_dashboard, dbc.Container)
    assert desktop_dashboard.id == MAIN_DASHBOARD_ID


def test_dashboard_with_fallback_data():
    """Tests that the dashboard correctly handles fallback forecast data"""
    # Step 1: Create sample fallback data using create_sample_fallback_dataframe
    fallback_data = create_sample_fallback_dataframe()

    # Step 2: Call create_main_dashboard with the fallback data
    dashboard = create_main_dashboard(fallback_data)

    # Step 3: Assert that the dashboard contains a fallback indicator component
    assert any("Using fallback forecast" in str(child) for child in dashboard.children)

    # Step 4: Assert that all regular components are still present
    assert any(isinstance(child, dbc.Card) and CONTROL_PANEL_ID in str(child) for child in dashboard.children)
    assert any(isinstance(child, dcc.Graph) and TIME_SERIES_GRAPH_ID == child.id for child in dashboard.children)
    assert any(isinstance(child, dcc.Graph) and DISTRIBUTION_GRAPH_ID == child.id for child in dashboard.children)
    assert any(isinstance(child, dash_table_components.DataTable) and FORECAST_TABLE_ID == child.id for child in dashboard.children)
    assert any(isinstance(child, dcc.Graph) and PRODUCT_COMPARISON_GRAPH_ID == child.id for child in dashboard.children)
    assert any(isinstance(child, dbc.Card) and EXPORT_PANEL_ID in str(child) for child in dashboard.children)

    # Step 5: Assert that the fallback indicator shows the correct message
    assert any("Using fallback forecast" in str(child) for child in dashboard.children)


def test_dashboard_with_custom_theme():
    """Tests that the dashboard correctly applies a custom theme"""
    # Step 1: Create sample forecast data using create_sample_visualization_dataframe
    sample_data = create_sample_visualization_dataframe()

    # Step 2: Call create_main_dashboard with the sample data and a custom theme
    custom_theme = "darkly"
    dashboard = create_main_dashboard(sample_data, theme=custom_theme)

    # Step 3: Assert that the dashboard container has the custom theme class
    assert custom_theme in dashboard.className

    # Step 4: Assert that the theme is applied to all components
    # Add assertions to check if the theme is applied to all components
    assert isinstance(dashboard, dbc.Container)
    assert dashboard.id == MAIN_DASHBOARD_ID