"""
Initialization file for the test fixtures package in the web visualization component of the Electricity Market Price Forecasting System.
This file makes test fixtures easily importable throughout the test suite by re-exporting fixtures from individual fixture modules.
"""

from .forecast_fixtures import (
    sample_forecast_data,
    sample_time_series_data,
    sample_distribution_data,
    sample_product_comparison_data,
    sample_hourly_table_data,
    sample_fallback_forecast_data,
    sample_incomplete_forecast_data,
    create_sample_forecast_dataframe,
    SAMPLE_PRODUCTS,
    BASE_PRICE_PATTERNS,
    VOLATILITY_FACTORS
)
from .component_fixtures import (
    mock_component,
    mock_control_panel,
    mock_time_series,
    mock_distribution_plot,
    mock_forecast_table,
    mock_product_comparison,
    mock_export_panel,
    mock_dash_app,
    MockTimeSeriesComponent,
    COMPONENT_IDS
)
from .callback_fixtures import (
    mock_callback_context,
    mock_dashboard_state,
    mock_product_selection_callback,
    mock_date_range_callback,
    mock_visualization_options_callback,
    mock_time_series_click_callback,
    create_mock_click_data,
    MockCallbackContext,
    CALLBACK_IDS
)

__all__ = [
    "sample_forecast_data",
    "sample_time_series_data",
    "sample_distribution_data",
    "sample_product_comparison_data",
    "sample_hourly_table_data",
    "sample_fallback_forecast_data",
    "sample_incomplete_forecast_data",
    "create_sample_forecast_dataframe",
    "SAMPLE_PRODUCTS",
    "BASE_PRICE_PATTERNS",
    "VOLATILITY_FACTORS",
    "mock_component",
    "mock_control_panel",
    "mock_time_series",
    "mock_distribution_plot",
    "mock_forecast_table",
    "mock_product_comparison",
    "mock_export_panel",
    "mock_dash_app",
    "MockTimeSeriesComponent",
    "COMPONENT_IDS",
    "mock_callback_context",
    "mock_dashboard_state",
    "mock_product_selection_callback",
    "mock_date_range_callback",
    "mock_visualization_options_callback",
    "mock_time_series_click_callback",
    "create_mock_click_data",
    "MockCallbackContext",
    "CALLBACK_IDS"
]