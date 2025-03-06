"""
Unit tests for the control panel callback functions in the Electricity Market Price Forecasting System's Dash-based visualization interface.
Tests the functionality of callbacks that handle product selection, date range selection, visualization options, and data refresh.
"""
# Third-party imports
import pytest  # pytest: 7.0.0+
from unittest.mock import MagicMock  # standard library
import dash  # dash: 2.9.0+
from dash.dependencies import Input, Output, State  # dash: 2.9.0+
from dash.exceptions import PreventUpdate  # dash: 2.9.0+
import dash_html_components as html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+
import pandas  # pandas: 2.0.0+
import datetime  # standard library

# Internal imports
from src.web.callbacks.control_callbacks import register_control_callbacks  # src/web/callbacks/control_callbacks.py
from src.web.callbacks.control_callbacks import handle_product_selection  # src/web/callbacks/control_callbacks.py
from src.web.callbacks.control_callbacks import handle_date_range_selection  # src/web/callbacks/control_callbacks.py
from src.web.callbacks.control_callbacks import handle_visualization_options  # src/web/callbacks/control_callbacks.py
from src.web.callbacks.control_callbacks import handle_refresh_button  # src/web/callbacks/control_callbacks.py
from src.web.callbacks.control_callbacks import update_last_updated_info  # src/web/callbacks/control_callbacks.py
from src.web.callbacks.control_callbacks import update_forecast_status  # src/web/callbacks/control_callbacks.py
from src.web.callbacks.control_callbacks import load_forecast_data  # src/web/callbacks/control_callbacks.py
from src.web.components.control_panel import PRODUCT_DROPDOWN_ID  # src/web/components/control_panel.py
from src.web.components.control_panel import DATE_RANGE_PICKER_ID  # src/web/components/control_panel.py
from src.web.components.control_panel import VISUALIZATION_OPTIONS_ID  # src/web/components/control_panel.py
from src.web.components.control_panel import REFRESH_BUTTON_ID  # src/web/components/control_panel.py
from src.web.components.control_panel import LAST_UPDATED_ID  # src/web/components/control_panel.py
from src.web.components.control_panel import FORECAST_STATUS_ID  # src/web/components/control_panel.py
from src.web.components.time_series import TIME_SERIES_GRAPH_ID  # src/web/components/time_series.py
from src.web.components.probability_distribution import DISTRIBUTION_GRAPH_ID  # src/web/components/probability_distribution.py
from src.web.components.forecast_table import FORECAST_TABLE_ID  # src/web/components/forecast_table.py
from src.web.layouts.responsive import VIEWPORT_STORE_ID  # src/web/layouts/responsive.py
from src.web.config.product_config import PRODUCTS  # src/web/config/product_config.py
from src.web.config.product_config import DEFAULT_PRODUCT  # src/web/config/product_config.py
from src.web.utils.date_helpers import get_default_date_range  # src/web/utils/date_helpers.py
from src.web.utils.date_helpers import parse_date  # src/web/utils/date_helpers.py
from src.web.data.forecast_loader import forecast_loader  # src/web/data/forecast_loader.py
from src.web.tests.fixtures.callback_fixtures import mock_callback_context  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_product_selection_callback  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_date_range_callback  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_visualization_options_callback  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import mock_refresh_callback  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import create_mock_callback_inputs  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import create_mock_callback_states  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import sample_forecast_data  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_sample_fallback_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.component_fixtures import mock_time_series  # src/web/tests/fixtures/component_fixtures.py
from src.web.tests.fixtures.component_fixtures import mock_distribution_plot  # src/web/tests/fixtures/component_fixtures.py
from src.web.tests.fixtures.component_fixtures import mock_forecast_table  # src/web/tests/fixtures/component_fixtures.py
from src.web.tests.fixtures.component_fixtures import mock_dash_app  # src/web/tests/fixtures/component_fixtures.py


def test_register_control_callbacks(mock_dash_app):
    # Create a mock Dash app using mock_dash_app
    app = mock_dash_app

    # Call register_control_callbacks with the mock app
    register_control_callbacks(app)

    # Verify that the expected callbacks are registered in app.callback_map
    assert len(app.callback_map) == 4

    # Check that product selection callback is registered
    assert any(callback['outputs'] == [Output(TIME_SERIES_GRAPH_ID, 'figure'), Output(DISTRIBUTION_GRAPH_ID, 'figure'), Output(FORECAST_TABLE_ID, 'data'), Output(FORECAST_STATUS_ID, 'children'), Output(LAST_UPDATED_ID, 'children')] for callback in app.callback_map)

    # Check that date range selection callback is registered
    assert any(callback['outputs'] == [Output(TIME_SERIES_GRAPH_ID, 'figure'), Output(DISTRIBUTION_GRAPH_ID, 'figure'), Output(FORECAST_TABLE_ID, 'data'), Output(FORECAST_STATUS_ID, 'children'), Output(LAST_UPDATED_ID, 'children')] and len(callback['inputs']) == 2 for callback in app.callback_map)

    # Check that visualization options callback is registered
    assert any(callback['outputs'] == [Output(TIME_SERIES_GRAPH_ID, 'figure')] and len(callback['inputs']) == 1 for callback in app.callback_map)

    # Check that refresh button callback is registered
    assert any(callback['outputs'] == [Output(TIME_SERIES_GRAPH_ID, 'figure'), Output(DISTRIBUTION_GRAPH_ID, 'figure'), Output(FORECAST_TABLE_ID, 'data'), Output('product-comparison-graph', 'figure'), Output(FORECAST_STATUS_ID, 'children'), Output(LAST_UPDATED_ID, 'children')] for callback in app.callback_map)


def test_handle_product_selection_valid_product(mock_callback_context, mock_product_selection_callback):
    # Set up mock callback context with PRODUCT_DROPDOWN_ID as triggered_id
    mock_callback_context.triggered = [{'prop_id': PRODUCT_DROPDOWN_ID + '.value'}]

    # Create mock inputs with a valid product selection
    selected_product = 'DALMP'
    mock_inputs = create_mock_callback_inputs({PRODUCT_DROPDOWN_ID: selected_product})

    # Create mock states with current visualization state
    mock_states = create_mock_callback_states({
        DATE_RANGE_PICKER_ID: get_default_date_range(),
        VISUALIZATION_OPTIONS_ID: ['point_forecast', 'uncertainty'],
        VIEWPORT_STORE_ID: {'size': 'lg'}
    })

    # Mock the forecast_loader.load_forecast_by_date_range to return sample data
    forecast_loader.load_forecast_by_date_range = MagicMock(return_value=create_sample_visualization_dataframe())

    # Call handle_product_selection with the mock inputs and states
    time_series, distribution, table_data, forecast_status, last_updated = handle_product_selection(selected_product, mock_states[DATE_RANGE_PICKER_ID]['start_date'], mock_states[DATE_RANGE_PICKER_ID]['end_date'], mock_states[VISUALIZATION_OPTIONS_ID]['value'], mock_states[VIEWPORT_STORE_ID]['data'])

    # Verify that the returned components are correctly updated
    assert isinstance(time_series, dict)
    assert isinstance(distribution, dict)
    assert isinstance(table_data, list)
    assert isinstance(forecast_status, html.Div)
    assert isinstance(last_updated, html.Div)

    # Check that time series is updated with the selected product
    assert 'DALMP' in str(time_series)

    # Check that distribution plot is updated with the selected product
    assert 'DALMP' in str(distribution)

    # Check that forecast table is updated with the selected product
    assert 'DALMP' in str(table_data)

    # Check that last updated info is updated
    assert 'text-muted' in str(last_updated)

    # Check that forecast status is updated
    assert 'Normal' in str(forecast_status)


def test_handle_product_selection_default_product(mock_callback_context, mock_product_selection_callback):
    # Set up mock callback context with PRODUCT_DROPDOWN_ID as triggered_id
    mock_callback_context.triggered = [{'prop_id': PRODUCT_DROPDOWN_ID + '.value'}]

    # Create mock inputs with None as product selection
    mock_inputs = create_mock_callback_inputs({PRODUCT_DROPDOWN_ID: None})

    # Create mock states with current visualization state
    mock_states = create_mock_callback_states({
        DATE_RANGE_PICKER_ID: get_default_date_range(),
        VISUALIZATION_OPTIONS_ID: ['point_forecast', 'uncertainty'],
        VIEWPORT_STORE_ID: {'size': 'lg'}
    })

    # Mock the forecast_loader.load_forecast_by_date_range to return sample data
    forecast_loader.load_forecast_by_date_range = MagicMock(return_value=create_sample_visualization_dataframe())

    # Call handle_product_selection with the mock inputs and states
    time_series, distribution, table_data, forecast_status, last_updated = handle_product_selection(None, mock_states[DATE_RANGE_PICKER_ID]['start_date'], mock_states[DATE_RANGE_PICKER_ID]['end_date'], mock_states[VISUALIZATION_OPTIONS_ID]['value'], mock_states[VIEWPORT_STORE_ID]['data'])

    # Verify that the returned components use DEFAULT_PRODUCT
    assert 'DALMP' in str(time_series)
    assert 'DALMP' in str(distribution)
    assert 'DALMP' in str(table_data)


def test_handle_date_range_selection_valid_range(mock_callback_context, mock_date_range_callback):
    # Set up mock callback context with DATE_RANGE_PICKER_ID as triggered_id
    mock_callback_context.triggered = [{'prop_id': DATE_RANGE_PICKER_ID + '.start_date'}, {'prop_id': DATE_RANGE_PICKER_ID + '.end_date'}]

    # Create mock inputs with a valid date range
    start_date = '2023-06-01'
    end_date = '2023-06-03'
    mock_inputs = create_mock_callback_inputs({DATE_RANGE_PICKER_ID: [start_date, end_date]})

    # Create mock states with current visualization state
    mock_states = create_mock_callback_states({
        PRODUCT_DROPDOWN_ID: 'DALMP',
        VISUALIZATION_OPTIONS_ID: ['point_forecast', 'uncertainty'],
        VIEWPORT_STORE_ID: {'size': 'lg'}
    })

    # Mock the forecast_loader.load_forecast_by_date_range to return sample data
    forecast_loader.load_forecast_by_date_range = MagicMock(return_value=create_sample_visualization_dataframe())

    # Call handle_date_range_selection with the mock inputs and states
    time_series, distribution, table_data, forecast_status, last_updated = handle_date_range_selection(start_date, end_date, mock_states[PRODUCT_DROPDOWN_ID], mock_states[VISUALIZATION_OPTIONS_ID]['value'], mock_states[VIEWPORT_STORE_ID]['data'])

    # Verify that the returned components are correctly updated
    assert isinstance(time_series, dict)
    assert isinstance(distribution, dict)
    assert isinstance(table_data, list)
    assert isinstance(forecast_status, html.Div)
    assert isinstance(last_updated, html.Div)

    # Check that time series is updated with the date range
    assert start_date in str(time_series)
    assert end_date in str(time_series)

    # Check that distribution plot is updated with the date range
    assert start_date in str(distribution)
    assert end_date in str(distribution)

    # Check that forecast table is updated with the date range
    assert start_date in str(table_data)
    assert end_date in str(table_data)

    # Check that last updated info is updated
    assert 'text-muted' in str(last_updated)

    # Check that forecast status is updated
    assert 'Normal' in str(forecast_status)


def test_handle_date_range_selection_default_range(mock_callback_context, mock_date_range_callback):
    # Set up mock callback context with DATE_RANGE_PICKER_ID as triggered_id
    mock_callback_context.triggered = [{'prop_id': DATE_RANGE_PICKER_ID + '.start_date'}, {'prop_id': DATE_RANGE_PICKER_ID + '.end_date'}]

    # Create mock inputs with None as date range
    mock_inputs = create_mock_callback_inputs({DATE_RANGE_PICKER_ID: [None, None]})

    # Create mock states with current visualization state
    mock_states = create_mock_callback_states({
        PRODUCT_DROPDOWN_ID: 'DALMP',
        VISUALIZATION_OPTIONS_ID: ['point_forecast', 'uncertainty'],
        VIEWPORT_STORE_ID: {'size': 'lg'}
    })

    # Mock the forecast_loader.load_forecast_by_date_range to return sample data
    forecast_loader.load_forecast_by_date_range = MagicMock(return_value=create_sample_visualization_dataframe())

    # Call handle_date_range_selection with the mock inputs and states
    time_series, distribution, table_data, forecast_status, last_updated = handle_date_range_selection(None, None, mock_states[PRODUCT_DROPDOWN_ID], mock_states[VISUALIZATION_OPTIONS_ID]['value'], mock_states[VIEWPORT_STORE_ID]['data'])

    # Verify that the returned components use default date range
    default_start_date, default_end_date = get_default_date_range()
    assert default_start_date.strftime('%Y-%m-%d') in str(time_series)
    assert default_end_date.strftime('%Y-%m-%d') in str(time_series)
    assert default_start_date.strftime('%Y-%m-%d') in str(distribution)
    assert default_end_date.strftime('%Y-%m-%d') in str(distribution)
    assert default_start_date.strftime('%Y-%m-%d') in str(table_data)
    assert default_end_date.strftime('%Y-%m-%d') in str(table_data)


def test_handle_visualization_options_with_uncertainty(mock_callback_context, mock_visualization_options_callback):
    # Set up mock callback context with VISUALIZATION_OPTIONS_ID as triggered_id
    mock_callback_context.triggered = [{'prop_id': VISUALIZATION_OPTIONS_ID + '.value'}]

    # Create mock inputs with ['point_forecast', 'uncertainty'] as options
    mock_inputs = create_mock_callback_inputs({VISUALIZATION_OPTIONS_ID: ['point_forecast', 'uncertainty']})

    # Create mock states with current visualization state
    mock_states = create_mock_callback_states({
        PRODUCT_DROPDOWN_ID: 'DALMP',
        DATE_RANGE_PICKER_ID: get_default_date_range(),
        VIEWPORT_STORE_ID: {'size': 'lg'}
    })

    # Mock the forecast_loader.load_forecast_by_date_range to return sample data
    forecast_loader.load_forecast_by_date_range = MagicMock(return_value=create_sample_visualization_dataframe())

    # Call handle_visualization_options with the mock inputs and states
    time_series = handle_visualization_options(mock_inputs[VISUALIZATION_OPTIONS_ID], mock_states[PRODUCT_DROPDOWN_ID], mock_states[DATE_RANGE_PICKER_ID][0], mock_states[DATE_RANGE_PICKER_ID][1], mock_states[VIEWPORT_STORE_ID]['data'])

    # Verify that the returned time series component shows uncertainty bands
    assert isinstance(time_series, dict)

    # Check that the figure in the time series component has uncertainty traces
    assert 'Upper Bound' in str(time_series)
    assert 'Lower Bound' in str(time_series)


def test_handle_visualization_options_without_uncertainty(mock_callback_context, mock_visualization_options_callback):
    # Set up mock callback context with VISUALIZATION_OPTIONS_ID as triggered_id
    mock_callback_context.triggered = [{'prop_id': VISUALIZATION_OPTIONS_ID + '.value'}]

    # Create mock inputs with ['point_forecast'] as options (no uncertainty)
    mock_inputs = create_mock_callback_inputs({VISUALIZATION_OPTIONS_ID: ['point_forecast']})

    # Create mock states with current visualization state
    mock_states = create_mock_callback_states({
        PRODUCT_DROPDOWN_ID: 'DALMP',
        DATE_RANGE_PICKER_ID: get_default_date_range(),
        VIEWPORT_STORE_ID: {'size': 'lg'}
    })

    # Mock the forecast_loader.load_forecast_by_date_range to return sample data
    forecast_loader.load_forecast_by_date_range = MagicMock(return_value=create_sample_visualization_dataframe())

    # Call handle_visualization_options with the mock inputs and states
    time_series = handle_visualization_options(mock_inputs[VISUALIZATION_OPTIONS_ID], mock_states[PRODUCT_DROPDOWN_ID], mock_states[DATE_RANGE_PICKER_ID][0], mock_states[DATE_RANGE_PICKER_ID][1], mock_states[VIEWPORT_STORE_ID]['data'])

    # Verify that the returned time series component does not show uncertainty bands
    assert isinstance(time_series, dict)

    # Check that the figure in the time series component has only point forecast trace
    assert 'Upper Bound' not in str(time_series)
    assert 'Lower Bound' not in str(time_series)


def test_handle_refresh_button_click(mock_callback_context, mock_refresh_callback):
    # Set up mock callback context with REFRESH_BUTTON_ID as triggered_id
    mock_callback_context.triggered = [{'prop_id': REFRESH_BUTTON_ID + '.n_clicks'}]

    # Create mock inputs with n_clicks=1
    mock_inputs = create_mock_callback_inputs({REFRESH_BUTTON_ID: 1})

    # Create mock states with current visualization state
    mock_states = create_mock_callback_states({
        PRODUCT_DROPDOWN_ID: 'DALMP',
        DATE_RANGE_PICKER_ID: get_default_date_range(),
        VISUALIZATION_OPTIONS_ID: ['point_forecast', 'uncertainty'],
        VIEWPORT_STORE_ID: {'size': 'lg'}
    })

    # Mock the forecast_loader.clear_cache method
    forecast_loader.clear_cache = MagicMock()

    # Mock the forecast_loader.load_latest_forecast to return sample data
    forecast_loader.load_forecast_by_date_range = MagicMock(return_value=create_sample_visualization_dataframe())

    # Call handle_refresh_button with the mock inputs and states
    time_series, distribution, table_data, product_comparison, forecast_status, last_updated = handle_refresh_button(mock_inputs[REFRESH_BUTTON_ID], mock_states[PRODUCT_DROPDOWN_ID], mock_states[DATE_RANGE_PICKER_ID][0], mock_states[DATE_RANGE_PICKER_ID][1], mock_states[VISUALIZATION_OPTIONS_ID]['value'], mock_states[VIEWPORT_STORE_ID]['data'])

    # Verify that forecast_loader.clear_cache was called
    forecast_loader.clear_cache.assert_called_once()

    # Verify that forecast_loader.load_latest_forecast was called
    forecast_loader.load_forecast_by_date_range.assert_called_once()

    # Check that all visualization components are updated
    assert isinstance(time_series, dict)
    assert isinstance(distribution, dict)
    assert isinstance(table_data, list)
    assert isinstance(product_comparison, dict)
    assert isinstance(forecast_status, html.Div)
    assert isinstance(last_updated, html.Div)

    # Check that last updated info is updated
    assert 'text-muted' in str(last_updated)

    # Check that forecast status is updated
    assert 'Normal' in str(forecast_status)


def test_handle_refresh_button_no_click(mock_callback_context, mock_refresh_callback):
    # Set up mock callback context with REFRESH_BUTTON_ID as triggered_id
    mock_callback_context.triggered = [{'prop_id': REFRESH_BUTTON_ID + '.n_clicks'}]

    # Create mock inputs with n_clicks=None
    mock_inputs = create_mock_callback_inputs({REFRESH_BUTTON_ID: None})

    # Create mock states with current visualization state
    mock_states = create_mock_callback_states({
        PRODUCT_DROPDOWN_ID: 'DALMP',
        DATE_RANGE_PICKER_ID: get_default_date_range(),
        VISUALIZATION_OPTIONS_ID: ['point_forecast', 'uncertainty'],
        VIEWPORT_STORE_ID: {'size': 'lg'}
    })

    # Call handle_refresh_button with the mock inputs and states
    result = handle_refresh_button(mock_inputs[REFRESH_BUTTON_ID], mock_states[PRODUCT_DROPDOWN_ID], mock_states[DATE_RANGE_PICKER_ID][0], mock_states[DATE_RANGE_PICKER_ID][1], mock_states[VISUALIZATION_OPTIONS_ID]['value'], mock_states[VIEWPORT_STORE_ID]['data'])

    # Verify that PreventUpdate is raised or no update is returned
    assert result == (dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update)

    # Check that forecast_loader.clear_cache was not called
    forecast_loader.clear_cache.assert_not_called()

    # Check that forecast_loader.load_latest_forecast was not called
    forecast_loader.load_forecast_by_date_range.assert_not_called()


def test_update_last_updated_info():
    # Create a sample forecast dataframe with known generation timestamp
    generation_timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0)
    sample_df = create_sample_visualization_dataframe()
    sample_df['generation_timestamp'] = generation_timestamp

    # Call update_last_updated_info with the sample dataframe
    component = update_last_updated_info(sample_df)

    # Verify that the returned component contains the correct timestamp
    assert str(generation_timestamp.strftime('%Y-%m-%d %H:%M:%S')) in str(component)

    # Check that the component has the correct ID (LAST_UPDATED_ID)
    assert component.id == LAST_UPDATED_ID

    # Check that the component displays the timestamp in the expected format
    assert 'text-muted' in str(component)


def test_update_forecast_status_normal():
    # Create a sample forecast dataframe with is_fallback=False
    sample_df = create_sample_visualization_dataframe()

    # Call update_forecast_status with the sample dataframe
    component = update_forecast_status(sample_df, "light")

    # Verify that the returned component indicates normal status
    assert 'Normal' in str(component)

    # Check that the component has the correct ID (FORECAST_STATUS_ID)
    assert component.id == FORECAST_STATUS_ID

    # Check that the component has the appropriate style for normal status
    assert 'success' in str(component)


def test_update_forecast_status_fallback():
    # Create a sample fallback forecast dataframe with is_fallback=True
    sample_df = create_sample_fallback_dataframe()

    # Call update_forecast_status with the sample dataframe
    component = update_forecast_status(sample_df, "light")

    # Verify that the returned component indicates fallback status
    assert 'Fallback' in str(component)

    # Check that the component has the correct ID (FORECAST_STATUS_ID)
    assert component.id == FORECAST_STATUS_ID

    # Check that the component has the appropriate style for fallback status
    assert 'warning' in str(component)


def test_load_forecast_data():
    # Mock the forecast_loader.load_forecast_by_date_range to return sample data
    forecast_loader.load_forecast_by_date_range = MagicMock(return_value=create_sample_visualization_dataframe())

    # Call load_forecast_data with product and date range parameters
    product = 'DALMP'
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2023, 1, 3)
    result = load_forecast_data(product, start_date, end_date)

    # Verify that forecast_loader.load_forecast_by_date_range was called with correct parameters
    forecast_loader.load_forecast_by_date_range.assert_called_once_with(product=product, start_date=start_date, end_date=end_date)

    # Check that the returned dataframe matches the expected sample data
    assert isinstance(result, pandas.DataFrame)


def test_load_forecast_data_error_handling():
    # Mock the forecast_loader.load_forecast_by_date_range to raise an exception
    forecast_loader.load_forecast_by_date_range = MagicMock(side_effect=Exception("Test Exception"))

    # Call load_forecast_data with product and date range parameters
    product = 'DALMP'
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2023, 1, 3)
    result = load_forecast_data(product, start_date, end_date)

    # Verify that the function handles the exception gracefully
    assert isinstance(result, pandas.DataFrame)

    # Check that an empty dataframe is returned instead of propagating the exception
    assert result.empty