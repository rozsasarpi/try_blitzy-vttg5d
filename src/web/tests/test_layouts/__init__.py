"""
Initialization module for the test_layouts package in the Electricity Market Price Forecasting System's web visualization component.
This module makes test fixtures and utilities available throughout the layout test suite and provides common test functionality for testing dashboard layouts.
"""

# Standard library imports
import pytest  # pytest: 7.0.0+

# Internal imports
from src.web.tests.test_layouts.test_main_dashboard import extract_component_ids  # Import helper function to extract component IDs from a Dash component tree
from src.web.tests.test_layouts.test_main_dashboard import find_component_by_id  # Import helper function to find a component by ID in a Dash component tree
from src.web.tests.fixtures.forecast_fixtures import sample_forecast_data  # Import sample forecast data fixture for layout testing
from src.web.tests.fixtures.forecast_fixtures import sample_fallback_forecast_data  # Import sample fallback forecast data fixture for layout testing
from src.web.tests.fixtures.component_fixtures import mock_dash_app  # Import mock Dash application fixture for layout testing


VIEWPORT_SIZES = ["mobile", "tablet", "desktop"]


def assert_component_exists(component: 'dash.development.base_component.Component', component_id: str) -> bool:
    """
    Helper function to assert that a component with a specific ID exists in a Dash component tree

    Args:
        component (dash.development.base_component.Component): The root component to search within
        component_id (str): The ID of the component to search for

    Returns:
        bool: True if component exists, raises AssertionError otherwise
    """
    found_component = find_component_by_id(component, component_id)
    assert found_component is not None, f"Component with ID '{component_id}' not found"
    return True


def assert_component_has_property(component: 'dash.development.base_component.Component', component_id: str, property_name: str, expected_value: 'any') -> bool:
    """
    Helper function to assert that a component has a specific property with an expected value

    Args:
        component (dash.development.base_component.Component): The root component to search within
        component_id (str): The ID of the component to search for
        property_name (str): The name of the property to check
        expected_value (any): The expected value of the property

    Returns:
        bool: True if property has expected value, raises AssertionError otherwise
    """
    found_component = find_component_by_id(component, component_id)
    assert found_component is not None, f"Component with ID '{component_id}' not found"
    assert hasattr(found_component, property_name), f"Component with ID '{component_id}' does not have property '{property_name}'"
    actual_value = getattr(found_component, property_name)
    assert actual_value == expected_value, f"Component with ID '{component_id}' has incorrect value for property '{property_name}'. Expected '{expected_value}', but got '{actual_value}'"
    return True


def assert_layout_structure(layout: 'dash.development.base_component.Component', viewport_size: str, expected_component_ids: list) -> bool:
    """
    Helper function to assert that a layout has the expected structure based on viewport size

    Args:
        layout (dash.development.base_component.Component): The layout to check
        viewport_size (str): The viewport size category ('mobile', 'tablet', 'desktop')
        expected_component_ids (list): List of expected component IDs in the layout

    Returns:
        bool: True if layout has expected structure, raises AssertionError otherwise
    """
    component_ids = extract_component_ids(layout)
    assert all(component_id in component_ids for component_id in expected_component_ids), f"Layout is missing expected component IDs. Expected: {expected_component_ids}, Actual: {component_ids}"

    # Add assertions to check the layout for mobile viewport
    # Add assertions to check the layout for tablet viewport
    # Add assertions to check the layout for desktop viewport
    return True


__all__ = [
    "extract_component_ids",
    "find_component_by_id",
    "assert_component_exists",
    "assert_component_has_property",
    "assert_layout_structure",
    "VIEWPORT_SIZES"
]