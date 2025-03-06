"""
Unit tests for the time series callbacks in the Electricity Market Price Forecasting Dashboard.
This module tests the callback functions that handle interactions with the time series visualization,
including uncertainty toggle, time point selection, and viewport changes.
"""
import pytest  # pytest: 7.0.0+
from unittest import mock  # standard library
import dash  # dash: 2.9.0+
from dash.dependencies import Input, Output, State  # dash: 2.9.0+
from dash.exceptions import PreventUpdate  # dash: 2.9.0+
import pandas  # pandas: 2.0.0+
import datetime  # standard library

from src.web.callbacks.time_series_callbacks import register_time_series_callbacks, handle_uncertainty_toggle, handle_time_series_click, handle_viewport_change  # src/web/callbacks/time_series_callbacks.py
from src.web.components.time_series import TIME_SERIES_GRAPH_ID, UNCERTAINTY_TOGGLE_ID  # src/web/components/time_series.py
from src.web.components.probability_distribution import DISTRIBUTION_GRAPH_ID  # src/web/components/probability_distribution.py
from src.web.layouts.responsive import VIEWPORT_STORE_ID  # src/web/layouts/responsive.py
from src.web.callbacks.visualization_callbacks import DASHBOARD_STATE_STORE_ID  # src/web/callbacks/visualization_callbacks.py
from src.web.utils.plot_helpers import extract_timestamp_from_click  # src/web/utils/plot_helpers.py
from src.web.tests.fixtures.callback_fixtures import mock_callback_context, mock_dashboard_state, sample_forecast_data, create_mock_callback_inputs, create_mock_callback_states  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.component_fixtures import mock_time_series, mock_distribution_plot  # src/web/tests/fixtures/component_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe  # src/web/tests/fixtures/forecast_fixtures.py


@pytest.mark.callback
def test_register_time_series_callbacks():
    """Tests that time series callbacks are correctly registered with the Dash app"""
    # Create a mock Dash app
    app = dash.Dash(__name__)

    # Call register_time_series_callbacks with the mock app
    register_time_series_callbacks(app)

    # Verify that the expected callbacks are registered
    expected_outputs = [
        Output(TIME_SERIES_GRAPH_ID, 'figure'),
        Output(DISTRIBUTION_GRAPH_ID, 'figure'),
        Output(TIME_SERIES_GRAPH_ID, 'figure')
    ]
    registered_outputs = [callback['outputs'] for callback in app.callback_map]
    assert all(output in registered_outputs for output in expected_outputs)

    # Check that the callback for uncertainty toggle is registered
    uncertainty_callback = next((cb for cb in app.callback_map if cb['outputs'] == Output(TIME_SERIES_GRAPH_ID, 'figure') and
                                 cb['inputs'] == Input(UNCERTAINTY_TOGGLE_ID, 'value')), None)
    assert uncertainty_callback is not None

    # Check that the callback for time series click is registered
    click_callback = next((cb for cb in app.callback_map if cb['outputs'] == Output(DISTRIBUTION_GRAPH_ID, 'figure') and
                           cb['inputs'] == Input(TIME_SERIES_GRAPH_ID, 'clickData')), None)
    assert click_callback is not None

    # Check that the callback for viewport change is registered
    viewport_callback = next((cb for cb in app.callback_map if cb['outputs'] == Output(TIME_SERIES_GRAPH_ID, 'figure') and
                              cb['inputs'] == Input(VIEWPORT_STORE_ID, 'data')), None)
    assert viewport_callback is not None


@pytest.mark.callback
def test_handle_uncertainty_toggle_show(mock_dashboard_state, sample_forecast_data):
    """Tests that the uncertainty toggle callback correctly shows uncertainty bands"""
    # Create mock time series figure without uncertainty bands
    mock_fig = mock_time_series(show_uncertainty=False).figure

    # Create mock dashboard state with sample forecast data
    dashboard_state = mock_dashboard_state
    dashboard_state['forecast_data'] = sample_forecast_data.to_dict('records')

    # Set up mock callback context with UNCERTAINTY_TOGGLE_ID as triggered
    mock_ctx = mock_callback_context(triggered_id=UNCERTAINTY_TOGGLE_ID)
    with mock.patch('dash.callback_context', mock_ctx):
        # Call handle_uncertainty_toggle with show_uncertainty=True
        updated_fig = handle_uncertainty_toggle(show_uncertainty=['show'], current_time_series=mock_fig, dashboard_state=dashboard_state)

        # Verify that the returned figure has uncertainty bands added
        assert len(updated_fig['data']) > 1

        # Check that the figure contains the expected traces for uncertainty
        trace_names = [trace['name'] for trace in updated_fig['data']]
        assert '10-90% Confidence Interval' in trace_names


@pytest.mark.callback
def test_handle_uncertainty_toggle_hide(mock_dashboard_state, sample_forecast_data):
    """Tests that the uncertainty toggle callback correctly hides uncertainty bands"""
    # Create mock time series figure with uncertainty bands
    mock_fig = mock_time_series(show_uncertainty=True).figure

    # Create mock dashboard state with sample forecast data
    dashboard_state = mock_dashboard_state
    dashboard_state['forecast_data'] = sample_forecast_data.to_dict('records')

    # Set up mock callback context with UNCERTAINTY_TOGGLE_ID as triggered
    mock_ctx = mock_callback_context(triggered_id=UNCERTAINTY_TOGGLE_ID)
    with mock.patch('dash.callback_context', mock_ctx):
        # Call handle_uncertainty_toggle with show_uncertainty=False
        updated_fig = handle_uncertainty_toggle(show_uncertainty=[], current_time_series=mock_fig, dashboard_state=dashboard_state)

        # Verify that the returned figure does not have uncertainty bands
        assert len(updated_fig['data']) == 1

        # Check that the figure contains only the main trace without uncertainty
        trace_names = [trace['name'] for trace in updated_fig['data']]
        assert '10-90% Confidence Interval' not in trace_names


@pytest.mark.callback
def test_handle_time_series_click(mock_dashboard_state, sample_forecast_data):
    """Tests that clicking on the time series updates the probability distribution"""
    # Create sample forecast data
    forecast_data = sample_forecast_data

    # Create mock dashboard state with the sample data
    dashboard_state = mock_dashboard_state
    dashboard_state['forecast_data'] = forecast_data.to_dict('records')

    # Create mock distribution plot
    mock_fig = mock_distribution_plot().figure

    # Create mock click data for a specific timestamp
    timestamp = forecast_data['timestamp'].iloc[0]
    click_data = {'points': [{'x': timestamp.isoformat()}]}

    # Set up mock callback context with TIME_SERIES_GRAPH_ID as triggered
    mock_ctx = mock_callback_context(triggered_id=TIME_SERIES_GRAPH_ID)
    with mock.patch('dash.callback_context', mock_ctx), \
            mock.patch('src.web.callbacks.time_series_callbacks.extract_timestamp_from_click', return_value=timestamp):
        # Call handle_time_series_click with the mock click data
        updated_fig = handle_time_series_click(click_data=click_data, current_distribution=mock_fig, dashboard_state=dashboard_state)

        # Verify that extract_timestamp_from_click was called with the click data
        extract_timestamp_from_click.assert_called_with(click_data)

        # Verify that the returned figure is updated for the selected timestamp
        assert updated_fig['layout']['title']['text'] == f"DALMP Price Distribution - {timestamp.strftime('%Y-%m-%d %H:%M:%S')}"


@pytest.mark.callback
def test_handle_time_series_click_no_click(mock_dashboard_state):
    """Tests that the callback handles the case when no click data is provided"""
    # Create mock dashboard state
    dashboard_state = mock_dashboard_state

    # Create mock distribution plot
    mock_fig = mock_distribution_plot().figure

    # Set up mock callback context with TIME_SERIES_GRAPH_ID as triggered
    mock_ctx = mock_callback_context(triggered_id=TIME_SERIES_GRAPH_ID)
    with mock.patch('dash.callback_context', mock_ctx):
        # Call handle_time_series_click with click_data=None
        with pytest.raises(PreventUpdate):
            handle_time_series_click(click_data=None, current_distribution=mock_fig, dashboard_state=dashboard_state)


@pytest.mark.callback
def test_handle_viewport_change(mock_dashboard_state, sample_forecast_data):
    """Tests that the viewport change callback correctly updates the time series visualization"""
    # Create mock time series figure
    mock_fig = mock_time_series().figure

    # Set up mock callback context with VIEWPORT_STORE_ID as triggered
    mock_ctx = mock_callback_context(triggered_id=VIEWPORT_STORE_ID)
    with mock.patch('dash.callback_context', mock_ctx):
        # Call handle_viewport_change with viewport_size='sm'
        updated_fig_sm = handle_viewport_change(viewport_size={'size': 'sm'}, current_time_series=mock_fig)

        # Verify that the returned figure has layout adjusted for small viewport
        assert updated_fig_sm['layout']['margin']['l'] < 50
        assert updated_fig_sm['layout']['font']['size'] < 12

        # Call handle_viewport_change with viewport_size='lg'
        updated_fig_lg = handle_viewport_change(viewport_size={'size': 'lg'}, current_time_series=mock_fig)

        # Verify that the returned figure has layout adjusted for large viewport
        assert updated_fig_lg['layout']['margin']['l'] == 50
        assert updated_fig_lg['layout']['font']['size'] == 12


def test_extract_timestamp_from_click():
    """Tests that timestamp is correctly extracted from click data"""
    # Create mock click data with x value as timestamp string
    timestamp_str = "2023-01-01T12:00:00"
    click_data = {'points': [{'x': timestamp_str}]}

    # Call extract_timestamp_from_click with the mock click data
    extracted_timestamp = extract_timestamp_from_click(click_data)

    # Verify that the returned timestamp matches the expected value
    assert extracted_timestamp == datetime.datetime.fromisoformat(timestamp_str)

    # Test with different timestamp formats
    timestamp_str = "2023-02-28 14:30:00"
    click_data = {'points': [{'x': timestamp_str}]}
    extracted_timestamp = extract_timestamp_from_click(click_data)
    assert extracted_timestamp == timestamp_str