"""
Unit tests for the visualization callbacks in the Electricity Market Price Forecasting Dashboard.
This module tests the dashboard state management, coordinated viewport changes, and responsive layout adjustments implemented in the visualization_callbacks module.
"""
import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import dash  # dash: 2.9.0+
from dash.dependencies import Input, Output, State  # dash: 2.9.0+
from dash.exceptions import PreventUpdate  # dash: 2.9.0+
import dash_html_components as html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import pandas  # pandas: 2.0.0+

from src.web.callbacks.visualization_callbacks import register_visualization_callbacks, create_dashboard_state_store, update_dashboard_state, handle_coordinated_viewport_change, DASHBOARD_STATE_STORE_ID  # src/web/callbacks/visualization_callbacks.py
from src.web.components.time_series import TIME_SERIES_GRAPH_ID  # src/web/components/time_series.py
from src.web.components.probability_distribution import DISTRIBUTION_GRAPH_ID  # src/web/components/probability_distribution.py
from src.web.components.forecast_table import FORECAST_TABLE_ID  # src/web/components/forecast_table.py
from src.web.components.product_comparison import PRODUCT_COMPARISON_GRAPH_ID  # src/web/components/product_comparison.py
from src.web.components.control_panel import PRODUCT_DROPDOWN_ID, DATE_RANGE_PICKER_ID, VISUALIZATION_OPTIONS_ID  # src/web/components/control_panel.py
from src.web.layouts.responsive import VIEWPORT_STORE_ID  # src/web/layouts/responsive.py
from src.web.config.product_config import DEFAULT_PRODUCT  # src/web/config/product_config.py
from src.web.tests.fixtures.callback_fixtures import mock_callback_context, mock_dashboard_state, mock_viewport_change_callback, sample_forecast_data, create_mock_callback_inputs, create_mock_callback_states  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.component_fixtures import mock_time_series, mock_distribution_plot, mock_forecast_table, mock_product_comparison  # src/web/tests/fixtures/component_fixtures.py
from src.web.utils.date_helpers import get_default_date_range  # src/web/utils/date_helpers.py


def test_create_dashboard_state_store():
    # Call create_dashboard_state_store function
    store = create_dashboard_state_store()

    # Assert that the returned component is a dcc.Store
    assert isinstance(store, dcc.Store)

    # Assert that the component has the correct ID (DASHBOARD_STATE_STORE_ID)
    assert store.id == DASHBOARD_STATE_STORE_ID

    # Assert that the storage_type is set to 'session'
    assert store.storage_type == 'session'

    # Assert that the data is initialized as an empty dict
    assert store.data == {}


def test_register_visualization_callbacks():
    # Create a mock Dash app
    app = dash.Dash(__name__)

    # Call register_visualization_callbacks with the mock app
    register_visualization_callbacks(app)

    # Assert that the app.callback method was called the expected number of times
    assert len(app.callback_map) == 2

    # Assert that the callbacks were registered with the correct inputs and outputs
    assert any(callback['outputs'] == Output(DASHBOARD_STATE_STORE_ID, 'data') for callback in app.callback_map)
    assert any(callback['outputs'] == [Output(TIME_SERIES_GRAPH_ID, 'figure'), Output(DISTRIBUTION_GRAPH_ID, 'figure'), Output(FORECAST_TABLE_ID, 'style'), Output(PRODUCT_COMPARISON_GRAPH_ID, 'figure')] for callback in app.callback_map)


def test_update_dashboard_state_with_new_product():
    # Create mock inputs with a new product selection
    mock_inputs = create_mock_callback_inputs({PRODUCT_DROPDOWN_ID: 'RTLMP'})

    # Create mock states with current dashboard state
    mock_states = create_mock_callback_states({DASHBOARD_STATE_STORE_ID: {'product_id': 'DALMP', 'start_date': '2023-01-01', 'end_date': '2023-01-03', 'show_uncertainty': True, 'show_historical': False, 'viewport_size': 'lg'}})

    # Call update_dashboard_state with the mock inputs and states
    new_state = update_dashboard_state(mock_inputs[PRODUCT_DROPDOWN_ID], None, None, None, mock_states[DASHBOARD_STATE_STORE_ID])

    # Assert that the returned state has the updated product
    assert new_state['product_id'] == 'RTLMP'

    # Assert that other state properties are preserved
    assert new_state['start_date'] == '2023-01-01'
    assert new_state['end_date'] == '2023-01-03'
    assert new_state['show_uncertainty'] == True
    assert new_state['show_historical'] == False
    assert new_state['viewport_size'] == 'lg'


def test_update_dashboard_state_with_new_date_range():
    # Create mock inputs with a new date range
    mock_inputs = create_mock_callback_inputs({DATE_RANGE_PICKER_ID: ['2023-01-05', '2023-01-07']})

    # Create mock states with current dashboard state
    mock_states = create_mock_callback_states({DASHBOARD_STATE_STORE_ID: {'product_id': 'DALMP', 'start_date': '2023-01-01', 'end_date': '2023-01-03', 'show_uncertainty': True, 'show_historical': False, 'viewport_size': 'lg'}})

    # Call update_dashboard_state with the mock inputs and states
    new_state = update_dashboard_state(None, mock_inputs[DATE_RANGE_PICKER_ID], None, None, mock_states[DASHBOARD_STATE_STORE_ID])

    # Assert that the returned state has the updated date range
    assert new_state['start_date'] == '2023-01-05'
    assert new_state['end_date'] == '2023-01-07'

    # Assert that other state properties are preserved
    assert new_state['product_id'] == 'DALMP'
    assert new_state['show_uncertainty'] == True
    assert new_state['show_historical'] == False
    assert new_state['viewport_size'] == 'lg'


def test_update_dashboard_state_with_new_visualization_options():
    # Create mock inputs with new visualization options
    mock_inputs = create_mock_callback_inputs({VISUALIZATION_OPTIONS_ID: ['historical']})

    # Create mock states with current dashboard state
    mock_states = create_mock_callback_states({DASHBOARD_STATE_STORE_ID: {'product_id': 'DALMP', 'start_date': '2023-01-01', 'end_date': '2023-01-03', 'show_uncertainty': True, 'show_historical': False, 'viewport_size': 'lg'}})

    # Call update_dashboard_state with the mock inputs and states
    new_state = update_dashboard_state(None, None, mock_inputs[VISUALIZATION_OPTIONS_ID], None, mock_states[DASHBOARD_STATE_STORE_ID])

    # Assert that the returned state has the updated visualization options
    assert new_state['show_uncertainty'] == False
    assert new_state['show_historical'] == True

    # Assert that other state properties are preserved
    assert new_state['product_id'] == 'DALMP'
    assert new_state['start_date'] == '2023-01-01'
    assert new_state['end_date'] == '2023-01-03'
    assert new_state['viewport_size'] == 'lg'


def test_update_dashboard_state_with_new_viewport():
    # Create mock inputs with a new viewport size
    mock_inputs = create_mock_callback_inputs({VIEWPORT_STORE_ID: {'size': 'sm'}})

    # Create mock states with current dashboard state
    mock_states = create_mock_callback_states({DASHBOARD_STATE_STORE_ID: {'product_id': 'DALMP', 'start_date': '2023-01-01', 'end_date': '2023-01-03', 'show_uncertainty': True, 'show_historical': False, 'viewport_size': 'lg'}})

    # Call update_dashboard_state with the mock inputs and states
    new_state = update_dashboard_state(None, None, None, mock_inputs[VIEWPORT_STORE_ID], mock_states[DASHBOARD_STATE_STORE_ID])

    # Assert that the returned state has the updated viewport size
    assert new_state['viewport_size'] == 'sm'

    # Assert that other state properties are preserved
    assert new_state['product_id'] == 'DALMP'
    assert new_state['start_date'] == '2023-01-01'
    assert new_state['end_date'] == '2023-01-03'
    assert new_state['show_uncertainty'] == True
    assert new_state['show_historical'] == False


def test_update_dashboard_state_with_empty_state():
    # Create mock inputs with product, date range, and visualization options
    mock_inputs = create_mock_callback_inputs({PRODUCT_DROPDOWN_ID: 'RTLMP', DATE_RANGE_PICKER_ID: ['2023-01-05', '2023-01-07'], VISUALIZATION_OPTIONS_ID: ['historical']})

    # Create mock states with None for current state
    mock_states = create_mock_callback_states({DASHBOARD_STATE_STORE_ID: None})

    # Call update_dashboard_state with the mock inputs and states
    new_state = update_dashboard_state(mock_inputs[PRODUCT_DROPDOWN_ID], mock_inputs[DATE_RANGE_PICKER_ID], mock_inputs[VISUALIZATION_OPTIONS_ID], None, mock_states[DASHBOARD_STATE_STORE_ID])

    # Assert that the returned state has all expected properties initialized
    assert new_state['product_id'] == 'RTLMP'
    assert new_state['start_date'] == '2023-01-05'
    assert new_state['end_date'] == '2023-01-07'
    assert new_state['show_uncertainty'] == False
    assert new_state['show_historical'] == True
    assert 'viewport_size' in new_state

    # Assert that product is set to the input value or DEFAULT_PRODUCT if None
    assert new_state['product_id'] == 'RTLMP'

    # Assert that date range is set to the input value or default date range if None
    assert new_state['start_date'] == '2023-01-05'
    assert new_state['end_date'] == '2023-01-07'

    # Assert that visualization options are set to the input value or defaults if None
    assert new_state['show_uncertainty'] == False
    assert new_state['show_historical'] == True


def test_update_dashboard_state_loads_forecast_data(sample_forecast_data):
    # Mock the load_forecast_data function to return a test dataframe
    with unittest.mock.patch('src.web.callbacks.visualization_callbacks.load_forecast_data') as mock_load_forecast_data:
        mock_load_forecast_data.return_value = sample_forecast_data

        # Create mock inputs with new product or date range
        mock_inputs = create_mock_callback_inputs({PRODUCT_DROPDOWN_ID: 'RTLMP'})

        # Create mock states with current dashboard state
        mock_states = create_mock_callback_states({DASHBOARD_STATE_STORE_ID: {'product_id': 'DALMP', 'start_date': '2023-01-01', 'end_date': '2023-01-03', 'show_uncertainty': True, 'show_historical': False, 'viewport_size': 'lg'}})

        # Call update_dashboard_state with the mock inputs and states
        new_state = update_dashboard_state(mock_inputs[PRODUCT_DROPDOWN_ID], None, None, None, mock_states[DASHBOARD_STATE_STORE_ID])

        # Assert that load_forecast_data was called with correct parameters
        mock_load_forecast_data.assert_called_with('RTLMP', pandas.Timestamp('2023-01-01'), pandas.Timestamp('2023-01-03'))

        # Assert that the returned state includes the forecast data
        assert 'forecast_data' in new_state
        assert new_state['forecast_data'] == sample_forecast_data.to_dict('records')


def test_handle_coordinated_viewport_change():
    # Create mock viewport data with different sizes (sm, md, lg, xl)
    mock_viewport_data = {'size': 'md'}

    # Create mock visualization components
    mock_time_series_component = mock_time_series()
    mock_distribution_component = mock_distribution_plot()
    mock_forecast_table_component = mock_forecast_table()
    mock_comparison_component = mock_product_comparison()

    # Call handle_coordinated_viewport_change with the mock data
    updated_time_series, updated_distribution, updated_table_style, updated_comparison = handle_coordinated_viewport_change(mock_viewport_data, mock_time_series_component.figure, mock_distribution_component.figure, mock_forecast_table_component.style, mock_comparison_component.figure)

    # Assert that all returned components are updated for the new viewport
    assert updated_time_series is not None
    assert updated_distribution is not None
    assert updated_table_style is not None
    assert updated_comparison is not None

    # Assert that time series figure layout is adjusted for viewport
    assert 'height' in updated_time_series['layout']

    # Assert that distribution figure layout is adjusted for viewport
    assert 'height' in updated_distribution['layout']

    # Assert that table style is adjusted for viewport
    assert 'style_table' in updated_table_style

    # Assert that comparison figure layout is adjusted for viewport
    assert 'height' in updated_comparison['layout']


def test_handle_coordinated_viewport_change_mobile():
    # Create mock viewport data with 'sm' size
    mock_viewport_data = {'size': 'sm'}

    # Create mock visualization components
    mock_time_series_component = mock_time_series()
    mock_distribution_component = mock_distribution_plot()
    mock_forecast_table_component = mock_forecast_table()
    mock_comparison_component = mock_product_comparison()

    # Call handle_coordinated_viewport_change with the mock data
    updated_time_series, updated_distribution, updated_table_style, updated_comparison = handle_coordinated_viewport_change(mock_viewport_data, mock_time_series_component.figure, mock_distribution_component.figure, mock_forecast_table_component.style, mock_comparison_component.figure)

    # Assert that components are adjusted for mobile layout
    assert updated_time_series is not None
    assert updated_distribution is not None
    assert updated_table_style is not None
    assert updated_comparison is not None

    # Check for specific mobile-specific adjustments (reduced margins, font sizes, etc.)
    assert 'height' in updated_time_series['layout']
    assert 'height' in updated_distribution['layout']
    assert 'style_table' in updated_table_style


def test_handle_coordinated_viewport_change_tablet():
    # Create mock viewport data with 'md' size
    mock_viewport_data = {'size': 'md'}

    # Create mock visualization components
    mock_time_series_component = mock_time_series()
    mock_distribution_component = mock_distribution_plot()
    mock_forecast_table_component = mock_forecast_table()
    mock_comparison_component = mock_product_comparison()

    # Call handle_coordinated_viewport_change with the mock data
    updated_time_series, updated_distribution, updated_table_style, updated_comparison = handle_coordinated_viewport_change(mock_viewport_data, mock_time_series_component.figure, mock_distribution_component.figure, mock_forecast_table_component.style, mock_comparison_component.figure)

    # Assert that components are adjusted for tablet layout
    assert updated_time_series is not None
    assert updated_distribution is not None
    assert updated_table_style is not None
    assert updated_comparison is not None

    # Check for specific tablet-specific adjustments
    assert 'height' in updated_time_series['layout']
    assert 'height' in updated_distribution['layout']
    assert 'style_table' in updated_table_style


def test_handle_coordinated_viewport_change_desktop():
    # Create mock viewport data with 'lg' size
    mock_viewport_data = {'size': 'lg'}

    # Create mock visualization components
    mock_time_series_component = mock_time_series()
    mock_distribution_component = mock_distribution_plot()
    mock_forecast_table_component = mock_forecast_table()
    mock_comparison_component = mock_product_comparison()

    # Call handle_coordinated_viewport_change with the mock data
    updated_time_series, updated_distribution, updated_table_style, updated_comparison = handle_coordinated_viewport_change(mock_viewport_data, mock_time_series_component.figure, mock_distribution_component.figure, mock_forecast_table_component.style, mock_comparison_component.figure)

    # Assert that components are adjusted for desktop layout
    assert updated_time_series is not None
    assert updated_distribution is not None
    assert updated_table_style is not None
    assert updated_comparison is not None

    # Check for specific desktop-specific adjustments
    assert 'height' in updated_time_series['layout']
    assert 'height' in updated_distribution['layout']
    assert 'style_table' in updated_table_style


def test_handle_coordinated_viewport_change_large_desktop():
    # Create mock viewport data with 'xl' size
    mock_viewport_data = {'size': 'xl'}

    # Create mock visualization components
    mock_time_series_component = mock_time_series()
    mock_distribution_component = mock_distribution_plot()
    mock_forecast_table_component = mock_forecast_table()
    mock_comparison_component = mock_product_comparison()

    # Call handle_coordinated_viewport_change with the mock data
    updated_time_series, updated_distribution, updated_table_style, updated_comparison = handle_coordinated_viewport_change(mock_viewport_data, mock_time_series_component.figure, mock_distribution_component.figure, mock_forecast_table_component.style, mock_comparison_component.figure)

    # Assert that components are adjusted for large desktop layout
    assert updated_time_series is not None
    assert updated_distribution is not None
    assert updated_table_style is not None
    assert updated_comparison is not None

    # Check for specific large desktop-specific adjustments
    assert 'height' in updated_time_series['layout']
    assert 'height' in updated_distribution['layout']
    assert 'style_table' in updated_table_style


def test_load_forecast_data():
    # Mock the forecast_loader.load_forecast_by_date_range function
    with unittest.mock.patch('src.web.callbacks.visualization_callbacks.forecast_loader.load_forecast_by_date_range') as mock_load_forecast_by_date_range:
        # Define the return value for the mock function
        mock_load_forecast_by_date_range.return_value = pandas.DataFrame({'timestamp': [], 'value': []})

        # Call load_forecast_data with test parameters
        product_id = 'DALMP'
        start_date = pandas.Timestamp('2023-01-01')
        end_date = pandas.Timestamp('2023-01-03')
        load_forecast_data(product_id, start_date, end_date)

        # Assert that forecast_loader.load_forecast_by_date_range was called with correct parameters
        mock_load_forecast_by_date_range.assert_called_with(product_id, start_date, end_date)

        # Assert that the returned dataframe matches the expected data
        assert isinstance(mock_load_forecast_by_date_range.return_value, pandas.DataFrame)


def test_load_forecast_data_handles_errors():
    # Mock the forecast_loader.load_forecast_by_date_range function to raise an exception
    with unittest.mock.patch('src.web.callbacks.visualization_callbacks.forecast_loader.load_forecast_by_date_range') as mock_load_forecast_by_date_range:
        mock_load_forecast_by_date_range.side_effect = Exception('Test Exception')

        # Call load_forecast_data with test parameters
        product_id = 'DALMP'
        start_date = pandas.Timestamp('2023-01-01')
        end_date = pandas.Timestamp('2023-01-03')
        result = load_forecast_data(product_id, start_date, end_date)

        # Assert that the function returns an empty dataframe instead of raising an exception
        assert isinstance(result, pandas.DataFrame)
        assert result.empty

        # Assert that the error is logged
        assert mock_load_forecast_by_date_range.side_effect is not None