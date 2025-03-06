"""
Component module that implements a tabular display of hourly forecast values for the 
Electricity Market Price Forecasting Dashboard. This module provides functions to create 
and update interactive tables showing point forecasts and percentile values for each hour 
in the forecast period.
"""

# External imports
import dash_table  # version 2.9.0+
import dash_html_components as html  # version 2.9.0+
import dash_core_components as dcc  # version 2.9.0+
import dash_bootstrap_components as dbc  # version 1.0.0+
import pandas as pd  # version 2.0.0+
import logging  # standard library
from typing import Dict, List, Any, Optional, Union  # standard library

# Internal imports
from ..data.data_processor import prepare_hourly_table_data
from ..utils.formatting import format_price, format_hour, format_range
from ..config.dashboard_config import get_table_config
from ..config.product_config import DEFAULT_PRODUCT, get_product_display_name
from ..utils.error_handlers import is_fallback_data

# Constants
FORECAST_TABLE_ID = "forecast-table"
PAGINATION_PAGE_ID = "forecast-table-pagination"
DEFAULT_PERCENTILES = [10, 90]

# Configure logger
logger = logging.getLogger(__name__)


def create_forecast_table(
    forecast_df: pd.DataFrame,
    product_id: str = None,
    viewport_size: Optional[str] = None,
    percentiles: Optional[List[int]] = None
) -> html.Div:
    """
    Creates a Dash DataTable component displaying hourly forecast values.
    
    Args:
        forecast_df: Forecast dataframe with hourly values
        product_id: Product identifier (e.g., 'DALMP', 'RTLMP')
        viewport_size: Viewport size category ('desktop', 'tablet', 'mobile')
        percentiles: List of percentiles to display (default: [10, 90])
        
    Returns:
        Container div with forecast table component
    """
    logger.info(f"Creating forecast table component for product: {product_id or DEFAULT_PRODUCT}")
    
    # Use default product if not provided
    if product_id is None:
        product_id = DEFAULT_PRODUCT
    
    # Use desktop as default viewport if not provided
    if viewport_size is None:
        viewport_size = 'desktop'
    
    # Use default percentiles if not provided
    if percentiles is None:
        percentiles = DEFAULT_PERCENTILES
    
    # Get table configuration based on viewport size
    table_config = get_table_config(viewport_size)
    
    # Check if forecast dataframe is valid
    if forecast_df is None or forecast_df.empty:
        return create_empty_table("No forecast data available", viewport_size)
    
    try:
        # Process forecast data for tabular display
        table_data = prepare_hourly_table_data(forecast_df, product_id, percentiles)
        
        if table_data is None or table_data.empty:
            return create_empty_table(f"No forecast data available for {get_product_display_name(product_id)}", viewport_size)
        
        # Create column definitions
        columns = create_table_columns(table_config, product_id)
        
        # Prepare data for dash_table format (list of dicts)
        table_records = table_data.to_dict('records')
        
        # Get styling from config
        style_config = create_table_style(table_config, viewport_size)
        
        # Create the DataTable component
        table = dash_table.DataTable(
            id=FORECAST_TABLE_ID,
            columns=columns,
            data=table_records,
            page_size=table_config.get('page_size', 12),
            page_current=0,
            page_action='native',
            sort_action='native' if table_config.get('sortable', True) else 'none',
            filter_action='native' if table_config.get('filterable', False) else 'none',
            style_table=style_config.get('style_table', {}),
            style_header=style_config.get('style_header', {}),
            style_cell=style_config.get('style_cell', {}),
            style_data=style_config.get('style_data', {}),
            style_data_conditional=style_config.get('style_data_conditional', []),
            style_cell_conditional=style_config.get('style_cell_conditional', []),
            css=[{"selector": ".dash-spreadsheet-container", "rule": "max-height: none;"}]
        )
        
        # Check if using fallback data
        fallback_indicator = None
        if is_fallback_data(forecast_df):
            fallback_indicator = html.Div(
                "Using fallback forecast (previous day's data)",
                className="fallback-indicator",
                style={
                    "color": "#856404",
                    "backgroundColor": "#fff3cd",
                    "padding": "0.5rem",
                    "borderRadius": "0.25rem",
                    "marginTop": "0.5rem",
                    "fontSize": "0.875rem"
                }
            )
        
        # Wrap table in container div
        container = html.Div(
            [
                table,
                fallback_indicator if fallback_indicator else None,
            ],
            id=f"{FORECAST_TABLE_ID}-container",
            className=f"forecast-table-container {viewport_size}"
        )
        
        return container
        
    except Exception as e:
        logger.error(f"Error creating forecast table: {str(e)}")
        return create_empty_table(f"Error displaying forecast table: {str(e)}", viewport_size)


def update_forecast_table(
    table_component: dash_table.DataTable,
    forecast_df: pd.DataFrame,
    product_id: str,
    percentiles: Optional[List[int]] = None
) -> dash_table.DataTable:
    """
    Updates an existing forecast table with new data.
    
    Args:
        table_component: Existing DataTable component to update
        forecast_df: New forecast dataframe with hourly values
        product_id: Product identifier (e.g., 'DALMP', 'RTLMP')
        percentiles: List of percentiles to display (default: [10, 90])
        
    Returns:
        Updated DataTable component
    """
    logger.info(f"Updating forecast table for product: {product_id}")
    
    # Use default percentiles if not provided
    if percentiles is None:
        percentiles = DEFAULT_PERCENTILES
    
    try:
        # Process forecast data for tabular display
        table_data = prepare_hourly_table_data(forecast_df, product_id, percentiles)
        
        if table_data is None or table_data.empty:
            # Keep existing table but clear data
            table_component.data = []
            return table_component
        
        # Update table data
        table_component.data = table_data.to_dict('records')
        
        return table_component
        
    except Exception as e:
        logger.error(f"Error updating forecast table: {str(e)}")
        # Keep existing table in case of error
        return table_component


def create_table_columns(table_config: dict, product_id: str) -> list:
    """
    Creates column definitions for the forecast table.
    
    Args:
        table_config: Configuration dictionary for the table
        product_id: Product identifier to use for formatting
        
    Returns:
        List of column definitions for DataTable
    """
    # Get column names from config
    column_names = table_config.get('columns', ["Hour", "Point Forecast", "10th Percentile", "90th Percentile", "Range"])
    
    # Create column definitions
    columns = []
    
    for col_name in column_names:
        # Determine column ID and type based on name
        if "Hour" in col_name:
            col_id = 'hour'
            col_type = 'text'
        elif "Point Forecast" in col_name:
            col_id = 'point_forecast_formatted'
            col_type = 'numeric'
        elif "Percentile" in col_name:
            # Extract percentile number from column name
            try:
                percentile = int(''.join(filter(str.isdigit, col_name)))
                col_id = f'percentile_{percentile}_formatted'
                col_type = 'numeric'
            except:
                # Fallback if can't extract percentile
                col_id = col_name.lower().replace(' ', '_')
                col_type = 'numeric'
        elif "Range" in col_name:
            col_id = 'range_formatted'
            col_type = 'text'
        else:
            # Generic column handling
            col_id = col_name.lower().replace(' ', '_')
            col_type = 'text'
        
        # Create column definition
        column_def = {
            'name': col_name,
            'id': col_id,
            'type': col_type
        }
        
        # Add sorting capabilities
        if table_config.get('sortable', True):
            if col_id == 'hour':
                # Use timestamp for sorting hour values
                column_def['sort_by'] = 'timestamp'
            elif col_id == 'point_forecast_formatted':
                # Use numeric value for sorting
                column_def['sort_by'] = 'point_forecast'
            elif 'percentile_' in col_id and '_formatted' in col_id:
                # Use numeric value for sorting percentiles
                column_def['sort_by'] = col_id.replace('_formatted', '')
        
        columns.append(column_def)
    
    return columns


def create_table_style(table_config: dict, viewport_size: str) -> dict:
    """
    Creates styling configuration for the forecast table.
    
    Args:
        table_config: Configuration dictionary for the table
        viewport_size: Viewport size category ('desktop', 'tablet', 'mobile')
        
    Returns:
        Dictionary of styling configurations
    """
    # Base styling for different viewport sizes
    if viewport_size == 'mobile':
        base_font_size = '0.75rem'
        cell_padding = '5px'
        header_height = '40px'
        row_height = '35px'
    elif viewport_size == 'tablet':
        base_font_size = '0.85rem'
        cell_padding = '8px'
        header_height = '45px'
        row_height = '40px'
    else:  # desktop
        base_font_size = '0.95rem'
        cell_padding = '10px'
        header_height = '50px'
        row_height = '45px'
    
    # Create style configuration
    style_config = {
        'style_table': {
            'overflowX': 'auto',
            'border': '1px solid #ddd',
            'borderRadius': '5px',
            'width': '100%'
        },
        'style_header': {
            'backgroundColor': '#f8f9fa',
            'color': '#343a40',
            'fontWeight': 'bold',
            'border': '1px solid #ddd',
            'height': header_height,
            'padding': cell_padding,
            'fontSize': base_font_size
        },
        'style_cell': {
            'padding': cell_padding,
            'border': '1px solid #ddd',
            'textAlign': 'center',
            'fontSize': base_font_size,
            'height': row_height,
            'minWidth': '60px',
            'maxWidth': '200px',
            'whiteSpace': 'normal',
            'overflow': 'hidden',
            'textOverflow': 'ellipsis'
        },
        'style_data': {
            'color': '#343a40'
        },
        'style_cell_conditional': [
            # Align hour column to left
            {
                'if': {'column_id': 'hour'},
                'textAlign': 'left',
                'minWidth': '100px'
            },
            # Right-align price columns
            {
                'if': {'column_type': 'numeric'},
                'textAlign': 'right'
            }
        ],
        'style_data_conditional': [
            # Alternate row colors
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': '#f8f9fa'
            },
            # Highlight hover row
            {
                'if': {'state': 'hover'},
                'backgroundColor': '#ebedef',
                'cursor': 'pointer'
            }
        ]
    }
    
    return style_config


def create_empty_table(message: str, viewport_size: Optional[str] = None) -> html.Div:
    """
    Creates an empty forecast table with a message.
    
    Args:
        message: Message to display in place of the table
        viewport_size: Viewport size category ('desktop', 'tablet', 'mobile')
        
    Returns:
        Container div with empty table message
    """
    # Use desktop as default viewport if not provided
    if viewport_size is None:
        viewport_size = 'desktop'
    
    # Get table configuration based on viewport size
    table_config = get_table_config(viewport_size)
    
    # Get table height from config or use default
    if viewport_size == 'mobile':
        height = '200px'
    elif viewport_size == 'tablet':
        height = '300px'
    else:  # desktop
        height = '400px'
    
    # Create empty table message
    empty_table = html.Div(
        message,
        style={
            'border': '1px solid #ddd',
            'borderRadius': '5px',
            'padding': '20px',
            'textAlign': 'center',
            'height': height,
            'display': 'flex',
            'alignItems': 'center',
            'justifyContent': 'center',
            'color': '#6c757d',
            'backgroundColor': '#f8f9fa',
            'width': '100%',
            'fontSize': '1rem'
        }
    )
    
    # Wrap in container div
    container = html.Div(
        empty_table,
        id=f"{FORECAST_TABLE_ID}-container",
        className=f"forecast-table-container {viewport_size}"
    )
    
    return container


def handle_viewport_change(
    table_component: dash_table.DataTable,
    new_viewport_size: str
) -> dash_table.DataTable:
    """
    Updates forecast table for a new viewport size.
    
    Args:
        table_component: Existing DataTable component to update
        new_viewport_size: New viewport size category ('desktop', 'tablet', 'mobile')
        
    Returns:
        Updated DataTable component for new viewport
    """
    # Get table configuration for new viewport size
    table_config = get_table_config(new_viewport_size)
    
    # Update page size based on viewport
    table_component.page_size = table_config.get('page_size', 12)
    
    # Update styling based on viewport size
    style_config = create_table_style(table_config, new_viewport_size)
    
    table_component.style_table = style_config.get('style_table', {})
    table_component.style_header = style_config.get('style_header', {})
    table_component.style_cell = style_config.get('style_cell', {})
    table_component.style_data = style_config.get('style_data', {})
    table_component.style_cell_conditional = style_config.get('style_cell_conditional', [])
    table_component.style_data_conditional = style_config.get('style_data_conditional', [])
    
    return table_component