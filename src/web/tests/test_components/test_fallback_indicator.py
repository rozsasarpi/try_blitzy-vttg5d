"""
Unit tests for the fallback indicator component of the Electricity Market Price Forecasting System's
Dash-based visualization interface. This file tests the functionality of visual indicators that
notify users when they are viewing fallback forecast data (previous day's forecast) rather than
current forecast data.
"""

import pytest  # pytest: 7.0.0+
import dash  # dash: 2.9.0+
from dash import html  # dash_html_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+
import pandas  # pandas: 2.0.0+

from src.web.components.fallback_indicator import create_fallback_indicator, create_fallback_badge, is_using_fallback, FALLBACK_INDICATOR_ID, FALLBACK_BADGE_ID  # Import the fallback indicator component for testing
from src.web.utils.error_handlers import is_fallback_data, FALLBACK_MESSAGE  # Import the fallback data detection function for testing
from src.web.config.themes import get_status_color  # Import the status color function for testing
from src.web.tests.conftest import mock_forecast_data, mock_fallback_data  # Import the mock forecast data fixture


def test_is_using_fallback_with_none_data():
    """Tests that is_using_fallback returns False when data is None"""
    # Call is_using_fallback with None data
    result = is_using_fallback(None)
    # Assert that the result is False
    assert result is False


def test_is_using_fallback_with_normal_data(mock_forecast_data: pandas.DataFrame):
    """Tests that is_using_fallback returns False with normal forecast data"""
    # Call is_using_fallback with mock_forecast_data
    result = is_using_fallback(mock_forecast_data)
    # Assert that the result is False
    assert result is False


def test_is_using_fallback_with_fallback_data(mock_fallback_data: pandas.DataFrame):
    """Tests that is_using_fallback returns True with fallback forecast data"""
    # Call is_using_fallback with mock_fallback_data
    result = is_using_fallback(mock_fallback_data)
    # Assert that the result is True
    assert result is True


def test_create_fallback_indicator_with_none_data():
    """Tests that create_fallback_indicator returns an empty div when data is None"""
    # Call create_fallback_indicator with None data and 'light' theme
    indicator = create_fallback_indicator(None, 'light')
    # Assert that the result is an html.Div
    assert isinstance(indicator, html.Div)
    # Assert that the div has the correct ID (FALLBACK_INDICATOR_ID)
    assert indicator.id == FALLBACK_INDICATOR_ID
    # Assert that the div has no children (empty)
    assert indicator.children is None


def test_create_fallback_indicator_with_normal_data(mock_forecast_data: pandas.DataFrame):
    """Tests that create_fallback_indicator returns an empty div with normal forecast data"""
    # Call create_fallback_indicator with mock_forecast_data and 'light' theme
    indicator = create_fallback_indicator(mock_forecast_data, 'light')
    # Assert that the result is an html.Div
    assert isinstance(indicator, html.Div)
    # Assert that the div has the correct ID (FALLBACK_INDICATOR_ID)
    assert indicator.id == FALLBACK_INDICATOR_ID
    # Assert that the div has no children (empty)
    assert indicator.children is None


def test_create_fallback_indicator_with_fallback_data(mock_fallback_data: pandas.DataFrame):
    """Tests that create_fallback_indicator returns a warning alert with fallback forecast data"""
    # Call create_fallback_indicator with mock_fallback_data and 'light' theme
    indicator = create_fallback_indicator(mock_fallback_data, 'light')
    # Assert that the result is an html.Div
    assert isinstance(indicator, html.Div)
    # Assert that the div has the correct ID (FALLBACK_INDICATOR_ID)
    assert indicator.id == FALLBACK_INDICATOR_ID
    # Assert that the div has children (not empty)
    assert indicator.children is not None
    # Assert that the child is a dbc.Alert component
    assert isinstance(indicator.children, dbc.Alert)
    # Assert that the alert contains the FALLBACK_MESSAGE
    assert FALLBACK_MESSAGE in str(indicator.children.children)
    # Assert that the alert has color set to the warning color from get_status_color('fallback', 'light')
    assert indicator.children.style['backgroundColor'] == get_status_color('fallback', 'light')


@pytest.mark.parametrize('theme', ['light', 'dark', 'colorblind'])
def test_create_fallback_indicator_with_different_themes(mock_fallback_data: pandas.DataFrame, theme: str):
    """Tests that create_fallback_indicator uses the correct theme colors"""
    # Call create_fallback_indicator with mock_fallback_data and the parametrized theme
    indicator = create_fallback_indicator(mock_fallback_data, theme)
    # Get the expected color using get_status_color('fallback', theme)
    expected_color = get_status_color('fallback', theme)
    # Assert that the alert has color set to the expected color
    assert indicator.children.style['backgroundColor'] == expected_color


def test_create_fallback_badge_with_none_data():
    """Tests that create_fallback_badge returns None when data is None"""
    # Call create_fallback_badge with None data and 'light' theme
    badge = create_fallback_badge(None, 'light')
    # Assert that the result is None
    assert badge is None


def test_create_fallback_badge_with_normal_data(mock_forecast_data: pandas.DataFrame):
    """Tests that create_fallback_badge returns None with normal forecast data"""
    # Call create_fallback_badge with mock_forecast_data and 'light' theme
    badge = create_fallback_badge(mock_forecast_data, 'light')
    # Assert that the result is None
    assert badge is None


def test_create_fallback_badge_with_fallback_data(mock_fallback_data: pandas.DataFrame):
    """Tests that create_fallback_badge returns a badge with fallback forecast data"""
    # Call create_fallback_badge with mock_fallback_data and 'light' theme
    badge = create_fallback_badge(mock_fallback_data, 'light')
    # Assert that the result is a dbc.Badge
    assert isinstance(badge, dbc.Badge)
    # Assert that the badge has the correct ID (FALLBACK_BADGE_ID)
    assert badge.id == FALLBACK_BADGE_ID
    # Assert that the badge contains the text 'Fallback'
    assert badge.children == 'Fallback'
    # Assert that the badge has color set to the warning color from get_status_color('fallback', 'light')
    assert badge.style['backgroundColor'] == get_status_color('fallback', 'light')


@pytest.mark.parametrize('theme', ['light', 'dark', 'colorblind'])
def test_create_fallback_badge_with_different_themes(mock_fallback_data: pandas.DataFrame, theme: str):
    """Tests that create_fallback_badge uses the correct theme colors"""
    # Call create_fallback_badge with mock_fallback_data and the parametrized theme
    badge = create_fallback_badge(mock_fallback_data, theme)
    # Get the expected color using get_status_color('fallback', theme)
    expected_color = get_status_color('fallback', theme)
    # Assert that the badge has color set to the expected color
    assert badge.style['backgroundColor'] == expected_color