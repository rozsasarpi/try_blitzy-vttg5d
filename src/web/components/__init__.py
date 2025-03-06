"""Initialization module for the components package in the Electricity Market Price Forecasting System's web interface.
This module exports all the dashboard UI components, making them easily accessible when importing from the components package.
"""

# Import header component for re-export
from .header import create_header, HEADER_ID, TITLE_ID, SUBTITLE_ID, HELP_BUTTON_ID

# Import footer component for re-export
from .footer import create_footer, FOOTER_ID

# Import control panel component for re-export
from .control_panel import (
    create_control_panel,
    CONTROL_PANEL_ID,
    PRODUCT_DROPDOWN_ID,
    DATE_RANGE_PICKER_ID,
    VISUALIZATION_OPTIONS_ID,
    REFRESH_BUTTON_ID,
    get_initial_state
)

# Import time series component for re-export
from .time_series import (
    create_time_series_component,
    create_time_series_with_controls,
    create_multi_product_time_series,
    update_time_series,
    TimeSeriesComponent
)

# Import distribution component for re-export
from .probability_distribution import (
    create_distribution_component,
    create_distribution_with_stats,
    update_distribution,
    get_distribution_statistics,
    DistributionComponent
)

# Import fallback indicator component for re-export
from .fallback_indicator import (
    create_fallback_indicator,
    create_fallback_badge,
    is_using_fallback
)

__all__ = [
    "create_header",
    "HEADER_ID",
    "TITLE_ID",
    "SUBTITLE_ID",
    "HELP_BUTTON_ID",
    "create_footer",
    "FOOTER_ID",
    "create_control_panel",
    "CONTROL_PANEL_ID",
    "PRODUCT_DROPDOWN_ID",
    "DATE_RANGE_PICKER_ID",
    "VISUALIZATION_OPTIONS_ID",
    "REFRESH_BUTTON_ID",
    "get_initial_state",
    "create_time_series_component",
    "create_time_series_with_controls",
    "create_multi_product_time_series",
    "update_time_series",
    "TimeSeriesComponent",
    "create_distribution_component",
    "create_distribution_with_stats",
    "update_distribution",
    "get_distribution_statistics",
    "DistributionComponent",
    "create_fallback_indicator",
    "create_fallback_badge",
    "is_using_fallback"
]