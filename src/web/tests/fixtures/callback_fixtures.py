"""
Provides test fixtures for testing Dash callbacks in the Electricity Market Price Forecasting Dashboard.
This module contains mock callback contexts, dashboard states, and helper functions for testing control panel, visualization, and data export callbacks.
"""
import pytest  # pytest: 7.0.0+
import dash  # dash: 2.9.0+
from dash.dependencies import Input, Output, State  # dash: 2.9.0+
from dash.exceptions import PreventUpdate  # dash: 2.9.0+
import dash_html_components as html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+
import pandas  # pandas: 2.0.0+
import datetime  # standard library
from unittest.mock import MagicMock  # standard library
from typing import Callable, Any, Dict, List, Union  # standard library

from src.web.components.control_panel import PRODUCT_DROPDOWN_ID, DATE_RANGE_PICKER_ID, VISUALIZATION_OPTIONS_ID, REFRESH_BUTTON_ID, LAST_UPDATED_ID, FORECAST_STATUS_ID  # src/web/components/control_panel.py
from src.web.components.time_series import TIME_SERIES_GRAPH_ID  # src/web/components/time_series.py
from src.web.components.probability_distribution import DISTRIBUTION_GRAPH_ID  # src/web/components/probability_distribution.py
from src.web.components.forecast_table import FORECAST_TABLE_ID  # src/web/components/forecast_table.py
from src.web.components.product_comparison import PRODUCT_COMPARISON_GRAPH_ID  # src/web/components/product_comparison.py
from src.web.components.export_panel import EXPORT_PANEL_ID  # src/web/components/export_panel.py
from src.web.layouts.responsive import VIEWPORT_STORE_ID  # src/web/layouts/responsive.py
from src.web.callbacks.visualization_callbacks import DASHBOARD_STATE_STORE_ID  # src/web/callbacks/visualization_callbacks.py
from src.web.config.product_config import PRODUCTS, DEFAULT_PRODUCT  # src/web/config/product_config.py
from src.web.utils.date_helpers import get_default_date_range  # src/web/utils/date_helpers.py
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe, create_sample_fallback_dataframe, create_multi_product_forecast_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.component_fixtures import mock_time_series, mock_distribution_plot, mock_forecast_table, mock_product_comparison  # src/web/tests/fixtures/component_fixtures.py


CALLBACK_IDS = {"product_dropdown": PRODUCT_DROPDOWN_ID, "date_range": DATE_RANGE_PICKER_ID, "visualization_options": VISUALIZATION_OPTIONS_ID, "refresh_button": REFRESH_BUTTON_ID, "last_updated": LAST_UPDATED_ID, "forecast_status": FORECAST_STATUS_ID, "time_series": TIME_SERIES_GRAPH_ID, "distribution": DISTRIBUTION_GRAPH_ID, "forecast_table": FORECAST_TABLE_ID, "product_comparison": PRODUCT_COMPARISON_GRAPH_ID, "export_panel": EXPORT_PANEL_ID, "viewport_store": VIEWPORT_STORE_ID, "dashboard_state": DASHBOARD_STATE_STORE_ID}
DEFAULT_DASHBOARD_STATE = {"product": DEFAULT_PRODUCT, "date_range": get_default_date_range(), "visualization_options": ["point_forecast", "uncertainty"], "viewport_size": "lg", "is_fallback": False, "generation_timestamp": datetime.datetime.now()}


@pytest.fixture
def mock_callback_context(triggered_id: str, inputs: Dict = None, states: Dict = None) -> 'MockCallbackContext':
    """Creates a mock dash.callback_context for testing callbacks"""
    mock_ctx = MockCallbackContext(triggered_id=triggered_id, inputs=inputs, states=states)
    return mock_ctx


@pytest.fixture
def mock_dashboard_state(product: str = None, date_range: List = None, visualization_options: List = None, viewport_size: str = None, is_fallback: bool = None, forecast_data: pandas.DataFrame = None) -> Dict:
    """Creates a mock dashboard state for testing callbacks"""
    state = DEFAULT_DASHBOARD_STATE.copy()
    if product is not None:
        state['product'] = product
    if date_range is not None:
        state['date_range'] = date_range
    if visualization_options is not None:
        state['visualization_options'] = visualization_options
    if viewport_size is not None:
        state['viewport_size'] = viewport_size
    if is_fallback is not None:
        state['is_fallback'] = is_fallback
    state['generation_timestamp'] = datetime.datetime.now()
    return state


@pytest.fixture
def mock_product_selection_callback(selected_product: str = None, date_range: List = None, visualization_options: List = None) -> Dict:
    """Creates mock data for testing product selection callbacks"""
    if selected_product is None:
        selected_product = DEFAULT_PRODUCT
    if date_range is None:
        date_range = get_default_date_range()
    if visualization_options is None:
        visualization_options = ['point_forecast', 'uncertainty']
    time_series_figure = mock_time_series().figure
    distribution_figure = mock_distribution_plot().figure
    forecast_table = mock_forecast_table()
    return {"time_series_figure": time_series_figure, "distribution_figure": distribution_figure, "forecast_table": forecast_table, "selected_product": selected_product, "date_range": date_range, "visualization_options": visualization_options}


@pytest.fixture
def mock_date_range_callback(date_range: List = None, selected_product: str = None, visualization_options: List = None) -> Dict:
    """Creates mock data for testing date range selection callbacks"""
    if date_range is None:
        date_range = get_default_date_range()
    if selected_product is None:
        selected_product = DEFAULT_PRODUCT
    if visualization_options is None:
        visualization_options = ['point_forecast', 'uncertainty']
    time_series_figure = mock_time_series().figure
    distribution_figure = mock_distribution_plot().figure
    forecast_table = mock_forecast_table()
    return {"time_series_figure": time_series_figure, "distribution_figure": distribution_figure, "forecast_table": forecast_table, "selected_product": selected_product, "date_range": date_range, "visualization_options": visualization_options}


@pytest.fixture
def mock_visualization_options_callback(visualization_options: List = None, selected_product: str = None, date_range: List = None) -> Dict:
    """Creates mock data for testing visualization options callbacks"""
    if visualization_options is None:
        visualization_options = ['point_forecast', 'uncertainty']
    if selected_product is None:
        selected_product = DEFAULT_PRODUCT
    if date_range is None:
        date_range = get_default_date_range()
    time_series_figure = mock_time_series().figure
    return {"time_series_figure": time_series_figure, "selected_product": selected_product, "date_range": date_range, "visualization_options": visualization_options}


@pytest.fixture
def mock_refresh_callback(n_clicks: int = None, selected_product: str = None, date_range: List = None, visualization_options: List = None) -> Dict:
    """Creates mock data for testing refresh button callbacks"""
    if n_clicks is None:
        n_clicks = 1
    if selected_product is None:
        selected_product = DEFAULT_PRODUCT
    if date_range is None:
        date_range = get_default_date_range()
    if visualization_options is None:
        visualization_options = ['point_forecast', 'uncertainty']
    time_series_figure = mock_time_series().figure
    distribution_figure = mock_distribution_plot().figure
    forecast_table = mock_forecast_table()
    product_comparison_figure = mock_product_comparison().figure
    return {"time_series_figure": time_series_figure, "distribution_figure": distribution_figure, "forecast_table": forecast_table, "product_comparison_figure": product_comparison_figure, "n_clicks": n_clicks, "selected_product": selected_product, "date_range": date_range, "visualization_options": visualization_options}


@pytest.fixture
def mock_viewport_change_callback(viewport_size: str = None) -> Dict:
    """Creates mock data for testing viewport change callbacks"""
    if viewport_size is None:
        viewport_size = 'lg'
    time_series_figure = mock_time_series().figure
    distribution_figure = mock_distribution_plot().figure
    forecast_table = mock_forecast_table()
    product_comparison_figure = mock_product_comparison().figure
    return {"time_series_figure": time_series_figure, "distribution_figure": distribution_figure, "forecast_table": forecast_table, "product_comparison_figure": product_comparison_figure, "viewport_size": viewport_size}


@pytest.fixture
def mock_export_callback(export_format: str = None, selected_product: str = None, date_range: List = None) -> Dict:
    """Creates mock data for testing export callbacks"""
    if export_format is None:
        export_format = 'csv'
    if selected_product is None:
        selected_product = DEFAULT_PRODUCT
    if date_range is None:
        date_range = get_default_date_range()
    sample_data = create_sample_visualization_dataframe()
    return {"sample_data": sample_data, "export_format": export_format, "selected_product": selected_product, "date_range": date_range}


@pytest.fixture
def sample_forecast_data(product: str = None, is_fallback: bool = None) -> pandas.DataFrame:
    """Provides sample forecast data for testing callbacks"""
    if product is None:
        product = DEFAULT_PRODUCT
    if is_fallback is None:
        is_fallback = False
    if is_fallback:
        return create_sample_fallback_dataframe(product=product)
    else:
        return create_sample_visualization_dataframe(product=product)


@pytest.fixture
def sample_multi_product_data(products: List[str] = None, is_fallback: bool = None) -> pandas.DataFrame:
    """Provides sample multi-product forecast data for testing callbacks"""
    if products is None:
        products = ['DALMP', 'RTLMP']
    if is_fallback is None:
        is_fallback = False
    return create_multi_product_forecast_dataframe(products=products, is_fallback=is_fallback)


def create_mock_callback_inputs(input_values: Dict) -> Dict:
    """Creates mock inputs for a Dash callback"""
    inputs = {}
    for key, value in input_values.items():
        inputs[key] = MagicMock(value=value)
    return inputs


def create_mock_callback_states(state_values: Dict) -> Dict:
    """Creates mock states for a Dash callback"""
    states = {}
    for key, value in state_values.items():
        mock_state = MagicMock()
        if isinstance(value, dict) and 'figure' in value:
            setattr(mock_state, 'figure', value['figure'])
        else:
            setattr(mock_state, 'value', value)
        states[key] = mock_state
    return states


def mock_callback_function(callback_function: Callable, return_value: Any) -> MagicMock:
    """Creates a mock for a callback function that returns predefined values"""
    mock = MagicMock()
    mock.return_value = return_value
    return mock


class MockCallbackContext:
    """Mock implementation of dash.callback_context for testing callbacks"""

    def __init__(self, triggered_id: str, inputs: Dict = None, states: Dict = None):
        self.triggered_id = triggered_id
        self.triggered = [{'prop_id': triggered_id + '.value'}]
        self.inputs = inputs if inputs is not None else {}
        self.states = states if states is not None else {}

    def get_triggered_id(self) -> str:
        return self.triggered_id


class MockDashApp:
    """Mock implementation of a Dash application for testing callbacks"""

    def __init__(self, layout: Dict = None):
        self.layout = layout if layout is not None else {}
        self.callback_map = []

    def callback(self, outputs: Union[Output, List], inputs: Union[Input, List], states: Union[State, List] = None) -> Callable:
        def decorator(func: Callable) -> Callable:
            self.callback_map.append({
                'outputs': outputs,
                'inputs': inputs,
                'states': states,
                'function': func
            })
            return func
        return decorator