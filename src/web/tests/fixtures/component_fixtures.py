"""
Provides test fixtures for Dash components used in the web visualization interface of the Electricity Market Price Forecasting System.
This module contains mock components, helper functions, and test data structures to facilitate unit testing of the dashboard's UI components.
"""

import pytest  # pytest: 7.0.0+
import dash  # dash: 2.9.0+
from dash import html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+
import pandas  # pandas: 2.0.0+
import plotly  # plotly: 5.14.0+
import typing  # standard library

from src.web.config.product_config import PRODUCTS, DEFAULT_PRODUCT  # List of valid electricity market products
from src.web.components.control_panel import PRODUCT_DROPDOWN_ID, DATE_RANGE_PICKER_ID, VISUALIZATION_OPTIONS_ID, TIME_SERIES_GRAPH_ID  # ID for the product dropdown component for callbacks
from src.web.tests.fixtures.forecast_fixtures import sample_forecast_data, sample_time_series_data, sample_distribution_data, sample_product_comparison_data, sample_hourly_table_data  # Import sample forecast data fixture

COMPONENT_IDS = {"control_panel": "control-panel", "time_series": "time-series-graph", "distribution": "distribution-graph", "forecast_table": "forecast-table", "product_comparison": "product-comparison-graph", "export_panel": "export-panel"}


@pytest.fixture
def mock_component(component_id: str, component_type: str, props: typing.Dict = None) -> dash.development.base_component.Component:
    """Creates a generic mock component for testing"""
    default_props = {'id': component_id}
    if props:
        default_props.update(props)

    if component_type == 'html':
        component = html.Div(**default_props)
    elif component_type == 'dcc':
        component = dcc.Graph(**default_props)
    elif component_type == 'dbc':
        component = dbc.Card(**default_props)
    else:
        component = html.Div(**default_props)
    return component


@pytest.fixture
def mock_control_panel(selected_product: str = None, selected_options: typing.List[str] = None, date_range: typing.Dict = None) -> dbc.Card:
    """Creates a mock control panel component for testing"""
    if selected_product is None:
        selected_product = DEFAULT_PRODUCT
    if selected_options is None:
        selected_options = ['point_forecast', 'uncertainty']
    if date_range is None:
        date_range = {'start_date': '2023-01-01', 'end_date': '2023-01-03'}

    product_dropdown = dcc.Dropdown(
        id=PRODUCT_DROPDOWN_ID,
        options=[{'label': p, 'value': p} for p in PRODUCTS],
        value=selected_product
    )
    date_picker_range = dcc.DatePickerRange(
        id=DATE_RANGE_PICKER_ID,
        start_date=date_range['start_date'],
        end_date=date_range['end_date']
    )
    visualization_options = dcc.Checklist(
        id=VISUALIZATION_OPTIONS_ID,
        options=[{'label': 'Point Forecast', 'value': 'point_forecast'},
                 {'label': 'Uncertainty', 'value': 'uncertainty'}],
        value=selected_options
    )
    refresh_button = html.Button('Refresh', id='refresh-button')
    last_updated_info = html.Div('Last Updated: 2023-01-01')
    forecast_status = html.Div('Normal')

    return dbc.Card(
        dbc.CardBody([
            product_dropdown,
            date_picker_range,
            visualization_options,
            refresh_button,
            last_updated_info,
            forecast_status
        ])
    )


@pytest.fixture
def mock_time_series(data: pandas.DataFrame = None, product_id: str = None, show_uncertainty: bool = None) -> dcc.Graph:
    """Creates a mock time series visualization component for testing"""
    if data is None:
        data = sample_time_series_data()
    if product_id is None:
        product_id = DEFAULT_PRODUCT
    if show_uncertainty is None:
        show_uncertainty = True

    fig = plotly.graph_objects.Figure(
        data=[{'type': 'scatter', 'x': data['timestamp'], 'y': data['value'], 'name': product_id}]
    )
    if show_uncertainty:
        fig.add_trace(
            {'type': 'scatter', 'x': data['timestamp'], 'y': data['value'] + 10, 'name': 'Upper Bound'}
        )
        fig.add_trace(
            {'type': 'scatter', 'x': data['timestamp'], 'y': data['value'] - 10, 'name': 'Lower Bound'}
        )

    return dcc.Graph(id=TIME_SERIES_GRAPH_ID, figure=fig)


@pytest.fixture
def mock_distribution_plot(data: pandas.DataFrame = None, product_id: str = None, timestamp: str = None) -> dcc.Graph:
    """Creates a mock probability distribution plot component for testing"""
    if data is None:
        data = sample_distribution_data()
    if product_id is None:
        product_id = DEFAULT_PRODUCT
    if timestamp is None:
        timestamp = data['timestamp'][0]

    fig = plotly.graph_objects.Figure(
        data=[{'type': 'histogram', 'x': data['value']}]
    )
    return dcc.Graph(id='distribution-graph', figure=fig)


@pytest.fixture
def mock_forecast_table(data: pandas.DataFrame = None, product_id: str = None) -> dash.dash_table.DataTable:
    """Creates a mock forecast table component for testing"""
    if data is None:
        data = sample_hourly_table_data()
    if product_id is None:
        product_id = DEFAULT_PRODUCT

    table_data = data.to_dict('records')
    columns = [{"name": i, "id": i} for i in data.columns]

    return dash.dash_table.DataTable(
        id='forecast-table',
        columns=columns,
        data=table_data,
        page_current=0,
        page_size=10,
        page_action='native',
        sort_action='native',
        style_cell={'textAlign': 'left'}
    )


@pytest.fixture
def mock_product_comparison(data: pandas.DataFrame = None, products: typing.List[str] = None) -> dcc.Graph:
    """Creates a mock product comparison visualization component for testing"""
    if data is None:
        data = sample_product_comparison_data()
    if products is None:
        products = ['DALMP', 'RTLMP']

    fig = plotly.graph_objects.Figure()
    for product in products:
        fig.add_trace(
            {'type': 'scatter', 'x': data['timestamp'], 'y': data[product], 'name': product}
        )
    return dcc.Graph(id='product-comparison-graph', figure=fig)


@pytest.fixture
def mock_export_panel() -> dbc.Card:
    """Creates a mock export panel component for testing"""
    csv_button = html.Button('Export CSV', id='csv-export-button')
    excel_button = html.Button('Export Excel', id='excel-export-button')

    return dbc.Card(
        dbc.CardBody([
            csv_button,
            excel_button
        ]),
        id='export-panel'
    )


@pytest.fixture
def mock_dash_app(components: typing.List = None) -> dash.Dash:
    """Creates a mock Dash application for component testing"""
    app = dash.Dash(__name__)
    if components is None:
        components = []
    app.layout = html.Div(components)
    return app


class MockTimeSeriesComponent:
    """Mock implementation of TimeSeriesComponent for testing"""

    def __init__(self, data: pandas.DataFrame = None, product_id: str = None, show_uncertainty: bool = None, component_id: str = None):
        self.data = data or sample_time_series_data()
        self.product_id = product_id or DEFAULT_PRODUCT
        self.show_uncertainty = show_uncertainty if show_uncertainty is not None else True
        self.component_id = component_id or TIME_SERIES_GRAPH_ID
        self.figure = None

    def render(self) -> dcc.Graph:
        """Renders the time series component"""
        fig = plotly.graph_objects.Figure(
            data=[{'type': 'scatter', 'x': self.data['timestamp'], 'y': self.data['value'], 'name': self.product_id}]
        )
        if self.show_uncertainty:
            fig.add_trace(
                {'type': 'scatter', 'x': self.data['timestamp'], 'y': self.data['value'] + 10, 'name': 'Upper Bound'}
            )
            fig.add_trace(
                {'type': 'scatter', 'x': self.data['timestamp'], 'y': self.data['value'] - 10, 'name': 'Lower Bound'}
            )
        self.figure = fig
        return dcc.Graph(id=self.component_id, figure=fig)

    def update(self, new_data: pandas.DataFrame = None, new_product_id: str = None) -> dcc.Graph:
        """Updates the component with new data"""
        if new_data is not None:
            self.data = new_data
        if new_product_id is not None:
            self.product_id = new_product_id
        return self.render()

    def toggle_uncertainty(self) -> dcc.Graph:
        """Toggles the display of uncertainty bands"""
        self.show_uncertainty = not self.show_uncertainty
        return self.render()