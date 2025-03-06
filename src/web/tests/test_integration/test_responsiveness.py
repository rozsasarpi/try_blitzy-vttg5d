"""
Integration tests for the responsive design functionality of the Electricity Market Price Forecasting System's Dash-based visualization interface.
This module verifies that the dashboard correctly adapts to different viewport sizes (desktop, tablet, mobile) and ensures proper layout adjustments, component resizing, and overall user experience across devices.
"""

import pytest  # pytest: 7.0.0+
import dash  # dash: 2.9.0+
import dash.testing  # dash: 2.9.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+
import dash_html_components as html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+

from src.web.conftest import app, test_client, mock_forecast_data, dashboard_layout  # src/web/tests/conftest.py
from src.web.layouts.main_dashboard import create_main_dashboard  # src/web/layouts/main_dashboard.py
from src.web.layouts.main_dashboard import update_dashboard_for_viewport  # src/web/layouts/main_dashboard.py
from src.web.layouts.main_dashboard import MAIN_DASHBOARD_ID  # src/web/layouts/main_dashboard.py
from src.web.layouts.responsive import create_responsive_container  # src/web/layouts/responsive.py
from src.web.layouts.responsive import create_responsive_layout  # src/web/layouts/responsive.py
from src.web.layouts.responsive import create_viewport_detection  # src/web/layouts/responsive.py
from src.web.layouts.responsive import VIEWPORT_STORE_ID  # src/web/layouts/responsive.py
from src.web.utils.responsive_helpers import detect_viewport_size  # src/web/utils/responsive_helpers.py
from src.web.utils.responsive_helpers import get_responsive_style  # src/web/utils/responsive_helpers.py
from src.web.config.dashboard_config import VIEWPORT_BREAKPOINTS  # src/web/config/dashboard_config.py
from src.web.config.dashboard_config import DASHBOARD_LAYOUT  # src/web/config/dashboard_config.py

VIEWPORT_SIZES = ['desktop', 'tablet', 'mobile']
TEST_WIDTHS = {'desktop': 1200, 'tablet': 900, 'mobile': 600}


@pytest.mark.integration
def test_viewport_detection():
    """Tests that the viewport detection function correctly identifies viewport sizes based on screen width"""
    # Test that width below mobile breakpoint returns 'mobile'
    assert detect_viewport_size(TEST_WIDTHS['mobile'] - 1) == 'mobile'

    # Test that width between mobile and tablet breakpoints returns 'tablet'
    assert detect_viewport_size(TEST_WIDTHS['mobile'] + 1) == 'tablet'
    assert detect_viewport_size(TEST_WIDTHS['tablet'] - 1) == 'tablet'

    # Test that width above tablet breakpoint returns 'desktop'
    assert detect_viewport_size(TEST_WIDTHS['tablet'] + 1) == 'desktop'

    # Test edge cases at exact breakpoint values
    assert detect_viewport_size(VIEWPORT_BREAKPOINTS['mobile']) == 'mobile'
    assert detect_viewport_size(VIEWPORT_BREAKPOINTS['tablet']) == 'tablet'


@pytest.mark.integration
def test_responsive_style_generation():
    """Tests that responsive styles are correctly generated for different viewport sizes"""
    # Create base style dictionary with standard values
    base_style = {'margin': '10px', 'padding': '15px', 'font-size': '16px'}

    # Generate responsive styles for desktop viewport
    desktop_style = get_responsive_style('desktop', base_style)

    # Generate responsive styles for tablet viewport
    tablet_style = get_responsive_style('tablet', base_style)

    # Generate responsive styles for mobile viewport
    mobile_style = get_responsive_style('mobile', base_style)

    # Assert that each viewport size applies appropriate style adjustments
    assert desktop_style == {'margin': '10px', 'padding': '15px', 'font-size': '16px'}
    assert tablet_style == {'margin': '8px', 'padding': '12px', 'font-size': '14px'}
    assert mobile_style == {'margin': '5px', 'padding': '10px', 'font-size': '12px'}

    # Verify margin, padding, and font-size adjustments for each viewport
    assert desktop_style['margin'] == '10px'
    assert tablet_style['margin'] == '8px'
    assert mobile_style['margin'] == '5px'

    assert desktop_style['padding'] == '15px'
    assert tablet_style['padding'] == '12px'
    assert mobile_style['padding'] == '10px'

    assert desktop_style['font-size'] == '16px'
    assert tablet_style['font-size'] == '14px'
    assert mobile_style['font-size'] == '12px'


@pytest.mark.integration
def test_responsive_container_creation():
    """Tests that responsive containers are correctly created with appropriate styles for different viewport sizes"""
    # Create test content for container
    test_content = html.Div(id='test-content')

    # Create responsive container for desktop viewport
    desktop_container = create_responsive_container(children=test_content, viewport_size='desktop')

    # Create responsive container for tablet viewport
    tablet_container = create_responsive_container(children=test_content, viewport_size='tablet')

    # Create responsive container for mobile viewport
    mobile_container = create_responsive_container(children=test_content, viewport_size='mobile')

    # Assert that each container has appropriate responsive styles
    assert desktop_container.style == {'width': '100%', 'margin': '0 auto', 'padding': '15px'}
    assert tablet_container.style == {'width': '100%', 'margin': '0 auto', 'padding': '12px'}
    assert mobile_container.style == {'width': '100%', 'margin': '0 auto', 'padding': '10px'}

    # Verify that container children are preserved
    assert desktop_container.children == test_content
    assert tablet_container.children == test_content
    assert mobile_container.children == test_content

    # Check that container IDs and class names are correctly set
    assert desktop_container.id == 'responsive-container'
    assert tablet_container.className == 'viewport-tablet'
    assert mobile_container.className == 'viewport-mobile'


@pytest.mark.integration
def test_responsive_layout_creation():
    """Tests that responsive layouts are correctly created with appropriate structure for different viewport sizes"""
    # Create test content for layout
    test_content = html.Div(id='test-content')

    # Create responsive layout for desktop viewport
    desktop_layout = create_responsive_layout(children=test_content, viewport_size='desktop')

    # Create responsive layout for tablet viewport
    tablet_layout = create_responsive_layout(children=test_content, viewport_size='tablet')

    # Create responsive layout for mobile viewport
    mobile_layout = create_responsive_layout(children=test_content, viewport_size='mobile')

    # Assert that each layout has appropriate responsive styles
    assert desktop_layout.style == {'width': '100%', 'min-height': '100vh'}
    assert tablet_layout.style == {'width': '100%', 'min-height': '100vh'}
    assert mobile_layout.style == {'width': '100%', 'min-height': '100vh'}

    # Verify that layout children are preserved
    assert test_content in desktop_layout.children
    assert test_content in tablet_layout.children
    assert test_content in mobile_layout.children

    # Check that layout includes viewport detection components
    assert any(child.id == VIEWPORT_STORE_ID for child in desktop_layout.children)
    assert any(child.id == VIEWPORT_STORE_ID for child in tablet_layout.children)
    assert any(child.id == VIEWPORT_STORE_ID for child in mobile_layout.children)


@pytest.mark.integration
def test_dashboard_layout_adaptation(app, mock_forecast_data):
    """Tests that the main dashboard layout correctly adapts to different viewport sizes"""
    # Create dashboard layout for desktop viewport
    desktop_dashboard = create_main_dashboard(mock_forecast_data.to_dict(), viewport_size='desktop')

    # Create dashboard layout for tablet viewport
    tablet_dashboard = create_main_dashboard(mock_forecast_data.to_dict(), viewport_size='tablet')

    # Create dashboard layout for mobile viewport
    mobile_dashboard = create_main_dashboard(mock_forecast_data.to_dict(), viewport_size='mobile')

    # Verify that component arrangement follows DASHBOARD_LAYOUT configuration
    assert [col['name'] for row in DASHBOARD_LAYOUT['desktop']['rows'] for col in row['columns']] == \
           [child.id.split('-')[0] for child in desktop_dashboard.children[1:]]
    assert [col['name'] for row in DASHBOARD_LAYOUT['tablet']['rows'] for col in row['columns']] == \
           [child.id.split('-')[0] for child in tablet_dashboard.children[1:]]
    assert [col['name'] for row in DASHBOARD_LAYOUT['mobile']['rows'] for col in row['columns']] == \
           [child.id.split('-')[0] for child in mobile_dashboard.children[1:]]

    # Check that control panel is full width on mobile and tablet
    assert desktop_dashboard.children[1].children[0].children[0].width == 3
    assert tablet_dashboard.children[1].children[0].children[0].width == 12
    assert mobile_dashboard.children[1].children[0].children[0].width == 12

    # Verify that time series is below control panel on mobile and tablet
    assert desktop_dashboard.children[1].children[0].children[1].width == 9
    assert tablet_dashboard.children[1].children[1].children[0].width == 12
    assert mobile_dashboard.children[1].children[1].children[0].width == 12

    # Check that distribution and table are side by side on desktop and tablet
    assert desktop_dashboard.children[1].children[1].children[0].width == 6
    assert desktop_dashboard.children[1].children[1].children[1].width == 6
    assert tablet_dashboard.children[1].children[2].children[0].width == 6
    assert tablet_dashboard.children[1].children[2].children[1].width == 6

    # Verify that distribution and table are stacked on mobile
    assert mobile_dashboard.children[1].children[2].children[0].width == 12
    assert mobile_dashboard.children[1].children[3].children[0].width == 12


@pytest.mark.integration
def test_dashboard_update_for_viewport(app, mock_forecast_data):
    """Tests that the dashboard correctly updates when viewport size changes"""
    # Create initial dashboard with desktop viewport
    initial_dashboard = create_main_dashboard(mock_forecast_data.to_dict(), viewport_size='desktop')

    # Update dashboard for tablet viewport
    tablet_dashboard = update_dashboard_for_viewport(initial_dashboard, 'tablet', mock_forecast_data.to_dict())

    # Verify that layout structure changes appropriately
    assert [col['name'] for row in DASHBOARD_LAYOUT['tablet']['rows'] for col in row['columns']] == \
           [child.id.split('-')[0] for child in tablet_dashboard.children[1:]]

    # Update dashboard for mobile viewport
    mobile_dashboard = update_dashboard_for_viewport(initial_dashboard, 'mobile', mock_forecast_data.to_dict())

    # Verify that layout structure changes appropriately
    assert [col['name'] for row in DASHBOARD_LAYOUT['mobile']['rows'] for col in row['columns']] == \
           [child.id.split('-')[0] for child in mobile_dashboard.children[1:]]

    # Update back to desktop viewport
    desktop_dashboard = update_dashboard_for_viewport(initial_dashboard, 'desktop', mock_forecast_data.to_dict())

    # Verify that layout returns to original structure
    assert [col['name'] for row in DASHBOARD_LAYOUT['desktop']['rows'] for col in row['columns']] == \
           [child.id.split('-')[0] for child in desktop_dashboard.children[1:]]


@pytest.mark.integration
def test_component_resizing(app, mock_forecast_data):
    """Tests that individual components resize appropriately for different viewport sizes"""
    # Create dashboard layouts for different viewport sizes
    desktop_dashboard = create_main_dashboard(mock_forecast_data.to_dict(), viewport_size='desktop')
    tablet_dashboard = create_main_dashboard(mock_forecast_data.to_dict(), viewport_size='tablet')
    mobile_dashboard = create_main_dashboard(mock_forecast_data.to_dict(), viewport_size='mobile')

    # Extract time series component from each layout
    desktop_time_series = desktop_dashboard.children[1]
    tablet_time_series = tablet_dashboard.children[1]
    mobile_time_series = mobile_dashboard.children[1]

    # Verify that time series height adjusts appropriately
    assert desktop_time_series.style['height'] == 500
    assert tablet_time_series.style['height'] == 400
    assert mobile_time_series.style['height'] == 350

    # Extract distribution component from each layout
    desktop_distribution = desktop_dashboard.children[1]
    tablet_distribution = tablet_dashboard.children[1]
    mobile_distribution = mobile_dashboard.children[1]

    # Verify that distribution height adjusts appropriately
    assert desktop_distribution.style['height'] == 500
    assert tablet_distribution.style['height'] == 400
    assert mobile_distribution.style['height'] == 350

    # Extract table component from each layout
    desktop_table = desktop_dashboard.children[1]
    tablet_table = tablet_dashboard.children[1]
    mobile_table = mobile_dashboard.children[1]

    # Verify that table page size adjusts appropriately
    assert desktop_table.page_size == 12
    assert tablet_table.page_size == 12
    assert mobile_table.page_size == 12


@pytest.mark.integration
def test_viewport_store_creation():
    """Tests that the viewport store component is correctly created and initialized"""
    # Create viewport detection components
    viewport_components = create_viewport_detection()

    # Extract viewport store component
    viewport_store = viewport_components[0]

    # Verify that store has correct ID
    assert viewport_store.id == VIEWPORT_STORE_ID

    # Check that initial data contains width, height, and size properties
    assert 'width' in viewport_store.data
    assert 'height' in viewport_store.data
    assert 'size' in viewport_store.data

    # Verify that storage type is set to 'session'
    assert viewport_store.storage_type == 'session'


@pytest.mark.integration
def test_client_viewport_detection(test_client):
    """Tests that the client-side viewport detection works correctly using the test client"""
    # Set up test app with viewport detection
    app = dash.Dash(__name__)
    app.layout = html.Div(create_viewport_detection())

    # Load app with test_client at desktop width
    test_client.start_server(app, width=1200, height=800)

    # Verify that viewport store contains correct size
    assert test_client.get_local_data(VIEWPORT_STORE_ID) == {'width': 1200, 'height': 800, 'size': 'desktop'}

    # Resize browser to tablet width
    test_client.resize_window(width=900, height=600)

    # Verify that viewport store updates to 'tablet'
    assert test_client.get_local_data(VIEWPORT_STORE_ID)['size'] == 'tablet'

    # Resize browser to mobile width
    test_client.resize_window(width=600, height=400)

    # Verify that viewport store updates to 'mobile'
    assert test_client.get_local_data(VIEWPORT_STORE_ID)['size'] == 'mobile'


@pytest.mark.integration
def test_responsive_callback_registration(app):
    """Tests that responsive callbacks are correctly registered with the app"""
    # Create app with dashboard layout
    app.layout = html.Div(create_main_dashboard(mock_forecast_data.to_dict()))

    # Check that viewport callback is registered
    viewport_callback = next((cb for cb in app.callback_map.values() if VIEWPORT_STORE_ID in str(cb)), None)
    assert viewport_callback is not None

    # Verify that callback updates viewport store when window size changes
    assert any(VIEWPORT_STORE_ID in str(output) for output in viewport_callback.outputs)
    assert any('innerWidth' in str(input) for input in viewport_callback.inputs)

    # Check that dashboard update callback is registered
    dashboard_callback = next((cb for cb in app.callback_map.values() if MAIN_DASHBOARD_ID in str(cb)), None)
    assert dashboard_callback is not None

    # Verify that callback updates layout when viewport size changes
    assert any(MAIN_DASHBOARD_ID in str(output) for output in dashboard_callback.outputs)
    assert any(VIEWPORT_STORE_ID in str(input) for input in dashboard_callback.inputs)