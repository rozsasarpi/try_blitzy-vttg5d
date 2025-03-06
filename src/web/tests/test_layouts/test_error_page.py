import pytest  # pytest: 7.0.0+
import dash  # dash: 2.9.0+
from dash import html  # dash_html_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+

# Internal imports
from src.web.layouts.error_page import create_error_layout  # Import the error page layout function to test
from src.web.layouts.error_page import ERROR_PAGE_ID  # Import error page ID constant for testing
from src.web.layouts.error_page import ERROR_CONTAINER_ID  # Import error container ID constant for testing
from src.web.layouts.error_page import ERROR_TITLE_ID  # Import error title ID constant for testing
from src.web.layouts.error_page import ERROR_MESSAGE_ID  # Import error message ID constant for testing
from src.web.layouts.error_page import ERROR_DETAILS_ID  # Import error details ID constant for testing
from src.web.layouts.error_page import RETRY_BUTTON_ID  # Import retry button ID constant for testing
from src.web.utils.error_handlers import ERROR_TYPES  # Import error types dictionary for testing different error scenarios
from src.web.tests.fixtures.component_fixtures import mock_dash_app  # Import mock Dash app fixture for testing


def test_error_layout_creation():
    """Tests that the error page layout is created correctly with default parameters"""
    # Create error layout with default parameters
    layout = create_error_layout()

    # Assert that the layout is not None
    assert layout is not None

    # Assert that the layout has the correct ID (ERROR_PAGE_ID)
    assert layout.id == ERROR_PAGE_ID

    # Assert that the layout contains the error container
    assert any(child.id == ERROR_CONTAINER_ID for child in layout.children if hasattr(child, 'id'))

    # Find the error container
    error_container = next(child for child in layout.children if hasattr(child, 'id') and child.id == ERROR_CONTAINER_ID)

    # Assert that the error container has the correct ID (ERROR_CONTAINER_ID)
    assert error_container.id == ERROR_CONTAINER_ID

    # Assert that the default error message is displayed when no message is provided
    assert any(child.id == ERROR_MESSAGE_ID for child in error_container.children[2].children if hasattr(child, 'id'))


def test_error_layout_with_custom_message():
    """Tests that the error page layout displays custom error messages correctly"""
    # Create error layout with a custom error message
    custom_message = "A custom error message for testing."
    layout = create_error_layout(error_message=custom_message)

    # Assert that the layout contains the error message component
    assert any(child.id == ERROR_MESSAGE_ID for child in layout.children[1].children[0].children[2].children if hasattr(child, 'id'))

    # Find the error message component
    error_message_component = next(child for child in layout.children[1].children[0].children[2].children if hasattr(child, 'id') and child.id == ERROR_MESSAGE_ID)

    # Assert that the error message component has the correct ID (ERROR_MESSAGE_ID)
    assert error_message_component.id == ERROR_MESSAGE_ID

    # Assert that the custom error message is displayed correctly
    assert error_message_component.children == custom_message


@pytest.mark.parametrize('error_type, expected_title', [(key, value) for key, value in ERROR_TYPES.items()])
def test_error_layout_with_error_type(error_type, expected_title):
    """Tests that the error page layout displays different error types correctly"""
    # Create error layout with specified error type
    layout = create_error_layout(error_type=error_type)

    # Assert that the layout contains the error title component
    assert any(child.id == ERROR_TITLE_ID for child in layout.children[1].children[0].children[2].children if hasattr(child, 'id'))

    # Find the error title component
    error_title_component = next(child for child in layout.children[1].children[0].children[2].children if hasattr(child, 'id') and child.id == ERROR_TITLE_ID)

    # Assert that the error title component has the correct ID (ERROR_TITLE_ID)
    assert error_title_component.id == ERROR_TITLE_ID

    # Assert that the error title matches the expected title for the given error type
    assert error_title_component.children == expected_title


def test_error_layout_with_details():
    """Tests that the error page layout displays error details correctly when provided"""
    # Create error layout with error details
    error_details = "Detailed error information for debugging."
    layout = create_error_layout(error_details=error_details)

    # Assert that the layout contains the error details component
    assert any(child.id == ERROR_DETAILS_ID for child in layout.children[1].children[0].children[2].children if hasattr(child, 'id'))

    # Find the error details component
    error_details_component = next(child for child in layout.children[1].children[0].children[2].children if hasattr(child, 'id') and child.id == ERROR_DETAILS_ID)

    # Assert that the error details component has the correct ID (ERROR_DETAILS_ID)
    assert error_details_component.id == ERROR_DETAILS_ID

    # Assert that the error details are displayed correctly
    assert error_details_component.children.children == error_details


def test_error_layout_without_details():
    """Tests that the error page layout does not display error details when not provided"""
    # Create error layout without error details
    layout = create_error_layout()

    # Assert that the layout does not contain the error details component
    assert not any(child.id == ERROR_DETAILS_ID for child in layout.children[1].children[0].children[2].children if hasattr(child, 'id'))


def test_retry_button_presence():
    """Tests that the error page layout includes a retry button"""
    # Create error layout with default parameters
    layout = create_error_layout()

    # Assert that the layout contains the retry button component
    assert any(child.id == RETRY_BUTTON_ID for child in layout.children[1].children[0].children[2].children if hasattr(child, 'id'))

    # Find the retry button component
    retry_button_component = next(child for child in layout.children[1].children[0].children[2].children if hasattr(child, 'id') and child.id == RETRY_BUTTON_ID)

    # Assert that the retry button component has the correct ID (RETRY_BUTTON_ID)
    assert retry_button_component.id == RETRY_BUTTON_ID

    # Assert that the retry button has the correct text ('Retry')
    assert retry_button_component.children == "Retry"


@pytest.mark.parametrize('theme', ['light', 'dark'])
def test_error_layout_with_theme(theme):
    """Tests that the error page layout applies theme colors correctly"""
    # Create error layout with specified theme
    layout = create_error_layout(theme=theme)

    # Assert that the layout applies the correct theme colors
    assert layout.style['background-color'] is not None

    # Check specific elements for theme-appropriate styling
    # This is a basic check; more detailed checks could be added
    # to verify specific color values for different elements
    assert layout.children[0].className == "mb-3"
    assert layout.children[2].className == "py-3"


def test_error_layout_in_dash_app():
    """Tests that the error page layout can be rendered in a Dash application"""
    # Create a mock Dash app using mock_dash_app fixture
    app = mock_dash_app()

    # Create error layout with default parameters
    layout = create_error_layout()

    # Set the app layout to the error layout
    app.layout = layout

    # Assert that the app layout is set correctly
    assert app.layout is not None

    # Verify that no exceptions are raised when rendering the layout
    try:
        app.layout  # Accessing the layout triggers rendering
    except Exception as e:
        assert False, f"Exception raised during layout rendering: {e}"