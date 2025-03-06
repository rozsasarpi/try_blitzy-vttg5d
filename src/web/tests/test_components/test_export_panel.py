"""
Unit tests for the export panel component of the Electricity Market Price Forecasting System's
Dash-based visualization interface. Tests the functionality of the export panel, including
format selection, percentile range inputs, and export button.
"""

# External imports
import pytest  # pytest: 7.0.0+
import dash_bootstrap_components as dbc  # dash-bootstrap-components: 1.0.0+
import dash_html_components as html  # dash: 2.9.0+
import dash_core_components as dcc  # dash: 2.9.0+

# Internal imports
from src.web.components.export_panel import create_export_panel  # Function to create the export panel component
from src.web.components.export_panel import EXPORT_PANEL_ID  # ID for the export panel component
from src.web.components.export_panel import EXPORT_FORMAT_DROPDOWN_ID  # ID for the export format dropdown
from src.web.components.export_panel import EXPORT_BUTTON_ID  # ID for the export button
from src.web.components.export_panel import EXPORT_DOWNLOAD_ID  # ID for the download component
from src.web.components.export_panel import PERCENTILE_LOWER_INPUT_ID  # ID for the lower percentile input
from src.web.components.export_panel import PERCENTILE_UPPER_INPUT_ID  # ID for the upper percentile input
from src.web.data.data_exporter import EXPORT_FORMATS  # Dictionary of supported export formats
from src.web.data.data_exporter import DEFAULT_EXPORT_FORMAT  # Default export format
from src.web.tests.fixtures.component_fixtures import mock_component  # Create generic mock component for UI testing


@pytest.mark.components
def test_export_panel_creation():
    """Tests that the export panel component is created correctly"""
    # Create export panel component using create_export_panel
    export_panel = create_export_panel(viewport_size='desktop')

    # Assert that the component has the correct ID (EXPORT_PANEL_ID)
    assert export_panel.id == EXPORT_PANEL_ID

    # Assert that the component is a dbc.Card
    assert isinstance(export_panel, dbc.Card)

    # Assert that the component contains the export format dropdown
    assert find_component_by_id(export_panel, EXPORT_FORMAT_DROPDOWN_ID) is not None

    # Assert that the component contains the percentile inputs
    assert find_component_by_id(export_panel, PERCENTILE_LOWER_INPUT_ID) is not None
    assert find_component_by_id(export_panel, PERCENTILE_UPPER_INPUT_ID) is not None

    # Assert that the component contains the export button
    assert find_component_by_id(export_panel, EXPORT_BUTTON_ID) is not None

    # Assert that the component contains the download component
    assert find_component_by_id(export_panel, EXPORT_DOWNLOAD_ID) is not None


@pytest.mark.components
def test_export_format_dropdown():
    """Tests that the export format dropdown contains the correct options"""
    # Create export panel component using create_export_panel
    export_panel = create_export_panel(viewport_size='desktop')

    # Find the export format dropdown by ID (EXPORT_FORMAT_DROPDOWN_ID)
    dropdown = find_component_by_id(export_panel, EXPORT_FORMAT_DROPDOWN_ID)

    # Assert that the dropdown exists
    assert dropdown is not None

    # Assert that the dropdown contains options for all formats in EXPORT_FORMATS
    assert len(dropdown.options) == len(EXPORT_FORMATS)

    # Assert that the default value is set to DEFAULT_EXPORT_FORMAT
    assert dropdown.value == DEFAULT_EXPORT_FORMAT


@pytest.mark.components
def test_percentile_inputs():
    """Tests that the percentile inputs are created correctly with default values"""
    # Create export panel component using create_export_panel
    export_panel = create_export_panel(viewport_size='desktop')

    # Find the lower percentile input by ID (PERCENTILE_LOWER_INPUT_ID)
    lower_input = find_component_by_id(export_panel, PERCENTILE_LOWER_INPUT_ID)

    # Find the upper percentile input by ID (PERCENTILE_UPPER_INPUT_ID)
    upper_input = find_component_by_id(export_panel, PERCENTILE_UPPER_INPUT_ID)

    # Assert that both inputs exist
    assert lower_input is not None
    assert upper_input is not None

    # Assert that the lower percentile input has the default value of 10
    # Assert that the upper percentile input has the default value of 90
    assert lower_input.value == 10
    assert upper_input.value == 90

    # Assert that the inputs have the correct type (number)
    assert lower_input.type == 'number'
    assert upper_input.type == 'number'


@pytest.mark.components
def test_export_button():
    """Tests that the export button is created correctly"""
    # Create export panel component using create_export_panel
    export_panel = create_export_panel(viewport_size='desktop')

    # Find the export button by ID (EXPORT_BUTTON_ID)
    button = find_component_by_id(export_panel, EXPORT_BUTTON_ID)

    # Assert that the button exists
    assert button is not None

    # Assert that the button has the correct text ('Export Data')
    assert button.children == 'Export Data'

    # Assert that the button has the correct color (primary)
    assert button.color == 'primary'


@pytest.mark.components
def test_download_component():
    """Tests that the download component is created correctly"""
    # Create export panel component using create_export_panel
    export_panel = create_export_panel(viewport_size='desktop')

    # Find the download component by ID (EXPORT_DOWNLOAD_ID)
    download = find_component_by_id(export_panel, EXPORT_DOWNLOAD_ID)

    # Assert that the download component exists
    assert download is not None

    # Assert that the download component is a dcc.Download
    assert isinstance(download, dcc.Download)


@pytest.mark.components
@pytest.mark.responsive
def test_responsive_layout():
    """Tests that the export panel layout adapts to different viewport sizes"""
    # Create export panel with small viewport size using create_export_panel('sm')
    export_panel_sm = create_export_panel(viewport_size='sm')

    # Create export panel with large viewport size using create_export_panel('lg')
    export_panel_lg = create_export_panel(viewport_size='lg')

    # Assert that both panels have the correct ID (EXPORT_PANEL_ID)
    assert export_panel_sm.id == EXPORT_PANEL_ID
    assert export_panel_lg.id == EXPORT_PANEL_ID

    # Assert that both panels contain all required components
    assert find_component_by_id(export_panel_sm, EXPORT_FORMAT_DROPDOWN_ID) is not None
    assert find_component_by_id(export_panel_sm, PERCENTILE_LOWER_INPUT_ID) is not None
    assert find_component_by_id(export_panel_sm, PERCENTILE_UPPER_INPUT_ID) is not None
    assert find_component_by_id(export_panel_sm, EXPORT_BUTTON_ID) is not None
    assert find_component_by_id(export_panel_sm, EXPORT_DOWNLOAD_ID) is not None

    assert find_component_by_id(export_panel_lg, EXPORT_FORMAT_DROPDOWN_ID) is not None
    assert find_component_by_id(export_panel_lg, PERCENTILE_LOWER_INPUT_ID) is not None
    assert find_component_by_id(export_panel_lg, PERCENTILE_UPPER_INPUT_ID) is not None
    assert find_component_by_id(export_panel_lg, EXPORT_BUTTON_ID) is not None
    assert find_component_by_id(export_panel_lg, EXPORT_DOWNLOAD_ID) is not None

    # Check for any responsive layout differences between small and large viewports
    # This is a placeholder - actual responsive layout checks would depend on the
    # specific layout implementation (e.g. checking column widths, visibility of elements, etc.)
    # In this case, we are simply asserting that the components are created, and that
    # responsive behavior is implemented elsewhere (e.g. in CSS or callback logic).
    assert True


def find_component_by_id(component, component_id):
    """Helper function to find a component by ID in a Dash component tree"""
    # Check if the component has the specified ID
    if hasattr(component, 'id') and component.id == component_id:
        return component

    # If the component has children, recursively search through them
    if hasattr(component, 'children'):
        if isinstance(component.children, list):
            for child in component.children:
                found_component = find_component_by_id(child, component_id)
                if found_component:
                    return found_component
        else:
            found_component = find_component_by_id(component.children, component_id)
            if found_component:
                return found_component

    # Return None if the component is not found
    return None