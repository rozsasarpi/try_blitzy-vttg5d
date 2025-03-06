"""
Module that implements callback functions for the control panel components in the Electricity Market Price Forecasting System's Dash-based visualization interface. These callbacks handle user interactions with product selection, date range picking, visualization options, and data refresh functionality.
"""

# Standard library imports
import datetime
from typing import List, Tuple, Dict

# Third-party imports
import dash  # version 2.9.0+
from dash.dependencies import Input, Output, State  # version 2.9.0+
import dash_html_components as html  # version 2.0.0+
import dash_core_components as dcc  # version 2.0.0+
import dash_bootstrap_components as dbc  # version 1.0.0+
import pandas as pd  # version 2.0.0+

# Internal imports
from ..components.control_panel import (  # src/web/components/control_panel.py
    PRODUCT_DROPDOWN_ID,
    DATE_RANGE_PICKER_ID,
    VISUALIZATION_OPTIONS_ID,
    REFRESH_BUTTON_ID,
    LAST_UPDATED_ID,
    FORECAST_STATUS_ID
)
from ..components.time_series import (  # src/web/components/time_series.py
    TIME_SERIES_GRAPH_ID,
    update_time_series
)
from ..components.probability_distribution import (  # src/web/components/probability_distribution.py
    DISTRIBUTION_GRAPH_ID,
    update_distribution
)
from ..components.forecast_table import (  # src/web/components/forecast_table.py
    FORECAST_TABLE_ID,
    update_forecast_table
)
from ..components.product_comparison import (  # src/web/components/product_comparison.py
    PRODUCT_COMPARISON_GRAPH_ID,
    update_product_comparison
)
from ..components.export_panel import (  # src/web/components/export_panel.py
    EXPORT_PANEL_ID,
    update_export_panel
)
from ..data.forecast_loader import forecast_loader  # src/web/data/forecast_loader.py
from ..config.product_config import DEFAULT_PRODUCT, PRODUCTS  # src/web/config/product_config.py
from ..utils.date_helpers import (  # src/web/utils/date_helpers.py
    get_default_date_range,
    parse_date,
    format_datetime,
    get_current_time_cst
)
from ..components.fallback_indicator import create_fallback_badge  # src/web/components/fallback_indicator.py
from ..utils.error_handlers import handle_data_loading_error  # src/web/utils/error_handlers.py
from ..config.logging_config import get_logger  # src/web/config/logging_config.py
from ..layouts.responsive import VIEWPORT_STORE_ID  # src/web/layouts/responsive.py

# Initialize logger
logger = get_logger('control_callbacks')

def register_control_callbacks(app: dash.Dash) -> None:
    """
    Registers all callback functions for the control panel components with the Dash application.

    Args:
        app (dash.Dash): The Dash application instance.

    Returns:
        None: No return value.
    """
    logger.info("Starting control callbacks registration")

    # Register product selection callback
    @app.callback(
        [
            Output(TIME_SERIES_GRAPH_ID, 'figure'),
            Output(DISTRIBUTION_GRAPH_ID, 'figure'),
            Output(FORECAST_TABLE_ID, 'data'),
            Output(FORECAST_STATUS_ID, 'children'),
            Output(LAST_UPDATED_ID, 'children')
        ],
        [Input(PRODUCT_DROPDOWN_ID, 'value')],
        [
            State(DATE_RANGE_PICKER_ID, 'start_date'),
            State(DATE_RANGE_PICKER_ID, 'end_date'),
            State(VISUALIZATION_OPTIONS_ID, 'value'),
            State(VIEWPORT_STORE_ID, 'data')
        ]
    )
    def handle_product_selection(selected_product: str, start_date: str, end_date: str, visualization_options: List[str], viewport_data: Dict) -> Tuple[dict, dict, list, html.Div, html.Div]:
        """
        Callback function that handles product selection changes and updates visualizations.

        Args:
            selected_product (str): The selected product from the dropdown.
            start_date (str): The start date of the date range.
            end_date (str): The end date of the date range.
            visualization_options (List[str]): The selected visualization options.
            viewport_data (Dict): The viewport data.

        Returns:
            Tuple[dict, dict, list, html.Div, html.Div]: A tuple containing the updated time series figure, distribution figure, table data, forecast status component, and last updated info component.
        """
        logger.info(f"Product selection changed to: {selected_product}")
        
        # Use DEFAULT_PRODUCT if selected_product is None
        if selected_product is None:
            selected_product = DEFAULT_PRODUCT
        
        # Parse date range values
        start_date_parsed = parse_date(start_date)
        end_date_parsed = parse_date(end_date)
        
        # Determine if uncertainty should be shown based on visualization_options
        show_uncertainty = 'uncertainty' in visualization_options
        
        # Load forecast data for selected product and date range
        try:
            forecast_df = forecast_loader.load_forecast_by_date_range(
                product=selected_product,
                start_date=start_date_parsed,
                end_date=end_date_parsed
            )
        except Exception as e:
            logger.error(f"Error loading forecast data: {e}")
            return {}, {}, [], html.Div(), html.Div()
        
        # Update time series visualization with new data
        time_series_figure = update_time_series(
            graph_component=dcc.Graph(id=TIME_SERIES_GRAPH_ID),
            forecast_df=forecast_df,
            product_id=selected_product,
            show_uncertainty=show_uncertainty,
            viewport_size=viewport_data['size']
        ).figure
        
        # Update probability distribution visualization with new data
        distribution_figure = update_distribution(
            graph_component=dcc.Graph(id=DISTRIBUTION_GRAPH_ID),
            forecast_df=forecast_df,
            product_id=selected_product,
            timestamp=forecast_df['timestamp'].iloc[0] if not forecast_df.empty else None,
            viewport_size=viewport_data['size']
        ).figure
        
        # Update forecast table with new data
        table_data = update_forecast_table(
            table_component=dash_table.DataTable(id=FORECAST_TABLE_ID),
            forecast_df=forecast_df,
            product_id=selected_product
        ).data
        
        # Extract metadata from forecast data
        metadata = forecast_loader.get_forecast_metadata(forecast_df)
        
        # Create last updated info with timestamp from metadata
        last_updated_info = update_last_updated_info(forecast_df)
        
        # Create forecast status indicator (normal or fallback)
        forecast_status = update_forecast_status(forecast_df, "light")  # TODO: Get theme from app state
        
        # Return tuple of updated components
        return time_series_figure, distribution_figure, table_data, forecast_status, last_updated_info

    # Register date range selection callback
    @app.callback(
        [
            Output(TIME_SERIES_GRAPH_ID, 'figure'),
            Output(DISTRIBUTION_GRAPH_ID, 'figure'),
            Output(FORECAST_TABLE_ID, 'data'),
            Output(FORECAST_STATUS_ID, 'children'),
            Output(LAST_UPDATED_ID, 'children')
        ],
        [Input(DATE_RANGE_PICKER_ID, 'start_date'),
         Input(DATE_RANGE_PICKER_ID, 'end_date')],
        [
            State(PRODUCT_DROPDOWN_ID, 'value'),
            State(VISUALIZATION_OPTIONS_ID, 'value'),
            State(VIEWPORT_STORE_ID, 'data')
        ]
    )
    def handle_date_range_selection(start_date: str, end_date: str, selected_product: str, visualization_options: List[str], viewport_data: Dict) -> Tuple[dict, dict, list, html.Div, html.Div]:
        """
        Callback function that handles date range selection changes and updates visualizations.

        Args:
            start_date (str): The start date of the date range.
            end_date (str): The end date of the date range.
            selected_product (str): The selected product from the dropdown.
            visualization_options (List[str]): The selected visualization options.
            viewport_data (Dict): The viewport data.

        Returns:
            Tuple[dict, dict, list, html.Div, html.Div]: A tuple containing the updated time series figure, distribution figure, table data, forecast status component, and last updated info component.
        """
        logger.info(f"Date range selection changed to: {start_date} - {end_date}")
        
        # Use default date range if date_range contains None values
        if start_date is None or end_date is None:
            default_start_date, default_end_date = get_default_date_range()
            start_date = default_start_date.strftime('%Y-%m-%d')
            end_date = default_end_date.strftime('%Y-%m-%d')
        
        # Parse date range values
        start_date_parsed = parse_date(start_date)
        end_date_parsed = parse_date(end_date)
        
        # Use DEFAULT_PRODUCT if selected_product is None
        if selected_product is None:
            selected_product = DEFAULT_PRODUCT
        
        # Determine if uncertainty should be shown based on visualization_options
        show_uncertainty = 'uncertainty' in visualization_options
        
        # Load forecast data for selected product and date range
        try:
            forecast_df = forecast_loader.load_forecast_by_date_range(
                product=selected_product,
                start_date=start_date_parsed,
                end_date=end_date_parsed
            )
        except Exception as e:
            logger.error(f"Error loading forecast data: {e}")
            return {}, {}, [], html.Div(), html.Div()
        
        # Update time series visualization with new data
        time_series_figure = update_time_series(
            graph_component=dcc.Graph(id=TIME_SERIES_GRAPH_ID),
            forecast_df=forecast_df,
            product_id=selected_product,
            show_uncertainty=show_uncertainty,
            viewport_size=viewport_data['size']
        ).figure
        
        # Update probability distribution visualization with new data
        distribution_figure = update_distribution(
            graph_component=dcc.Graph(id=DISTRIBUTION_GRAPH_ID),
            forecast_df=forecast_df,
            product_id=selected_product,
            timestamp=forecast_df['timestamp'].iloc[0] if not forecast_df.empty else None,
            viewport_size=viewport_data['size']
        ).figure
        
        # Update forecast table with new data
        table_data = update_forecast_table(
            table_component=dash_table.DataTable(id=FORECAST_TABLE_ID),
            forecast_df=forecast_df,
            product_id=selected_product
        ).data
        
        # Extract metadata from forecast data
        metadata = forecast_loader.get_forecast_metadata(forecast_df)
        
        # Create last updated info with timestamp from metadata
        last_updated_info = update_last_updated_info(forecast_df)
        
        # Create forecast status indicator (normal or fallback)
        forecast_status = update_forecast_status(forecast_df, "light")  # TODO: Get theme from app state
        
        # Return tuple of updated components
        return time_series_figure, distribution_figure, table_data, forecast_status, last_updated_info

    # Register visualization options callback
    @app.callback(
        Output(TIME_SERIES_GRAPH_ID, 'figure'),
        [Input(VISUALIZATION_OPTIONS_ID, 'value')],
        [
            State(PRODUCT_DROPDOWN_ID, 'value'),
            State(DATE_RANGE_PICKER_ID, 'start_date'),
            State(DATE_RANGE_PICKER_ID, 'end_date'),
            State(VIEWPORT_STORE_ID, 'data')
        ]
    )
    def handle_visualization_options(visualization_options: List[str], selected_product: str, start_date: str, end_date: str, viewport_data: Dict) -> dict:
        """
        Callback function that handles visualization options changes and updates visualizations.

        Args:
            visualization_options (List[str]): The selected visualization options.
            selected_product (str): The selected product from the dropdown.
            start_date (str): The start date of the date range.
            end_date (str): The end date of the date range.
            viewport_data (Dict): The viewport data.

        Returns:
            dict: The updated time series figure.
        """
        logger.info(f"Visualization options changed to: {visualization_options}")
        
        # Use DEFAULT_PRODUCT if selected_product is None
        if selected_product is None:
            selected_product = DEFAULT_PRODUCT
        
        # Parse date range values
        start_date_parsed = parse_date(start_date)
        end_date_parsed = parse_date(end_date)
        
        # Determine if uncertainty should be shown based on visualization_options
        show_uncertainty = 'uncertainty' in visualization_options
        
        # Load forecast data for selected product and date range
        try:
            forecast_df = forecast_loader.load_forecast_by_date_range(
                product=selected_product,
                start_date=start_date_parsed,
                end_date=end_date_parsed
            )
        except Exception as e:
            logger.error(f"Error loading forecast data: {e}")
            return {}
        
        # Update time series visualization with new options
        time_series_figure = update_time_series(
            graph_component=dcc.Graph(id=TIME_SERIES_GRAPH_ID),
            forecast_df=forecast_df,
            product_id=selected_product,
            show_uncertainty=show_uncertainty,
            viewport_size=viewport_data['size']
        ).figure
        
        return time_series_figure

    # Register refresh button callback
    @app.callback(
        [
            Output(TIME_SERIES_GRAPH_ID, 'figure'),
            Output(DISTRIBUTION_GRAPH_ID, 'figure'),
            Output(FORECAST_TABLE_ID, 'data'),
            Output(PRODUCT_COMPARISON_GRAPH_ID, 'figure'),
            Output(FORECAST_STATUS_ID, 'children'),
            Output(LAST_UPDATED_ID, 'children')
        ],
        [Input(REFRESH_BUTTON_ID, 'n_clicks')],
        [
            State(PRODUCT_DROPDOWN_ID, 'value'),
            State(DATE_RANGE_PICKER_ID, 'start_date'),
            State(DATE_RANGE_PICKER_ID, 'end_date'),
            State(VISUALIZATION_OPTIONS_ID, 'value'),
            State(VIEWPORT_STORE_ID, 'data')
        ]
    )
    def handle_refresh_button(n_clicks: int, selected_product: str, start_date: str, end_date: str, visualization_options: List[str], viewport_data: Dict) -> Tuple[dict, dict, list, dict, html.Div, html.Div]:
        """
        Callback function that handles refresh button clicks and reloads forecast data.

        Args:
            n_clicks (int): The number of times the refresh button has been clicked.
            selected_product (str): The selected product from the dropdown.
            start_date (str): The start date of the date range.
            end_date (str): The end date of the date range.
            visualization_options (List[str]): The selected visualization options.
            viewport_data (Dict): The viewport data.

        Returns:
            Tuple[dict, dict, list, dict, html.Div, html.Div]: A tuple containing the updated time series figure, distribution figure, table data, product comparison figure, forecast status component, and last updated info component.
        """
        # Check if callback is triggered by button click (n_clicks > 0)
        if n_clicks is None or n_clicks == 0:
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
        
        logger.info("Refresh button clicked")
        
        # Clear forecast data cache
        forecast_loader.clear_cache()
        
        # Use DEFAULT_PRODUCT if selected_product is None
        if selected_product is None:
            selected_product = DEFAULT_PRODUCT
        
        # Parse date range values
        start_date_parsed = parse_date(start_date)
        end_date_parsed = parse_date(end_date)
        
        # Determine if uncertainty should be shown based on visualization_options
        show_uncertainty = 'uncertainty' in visualization_options
        
        # Load latest forecast data for selected product and date range
        try:
            forecast_df = forecast_loader.load_forecast_by_date_range(
                product=selected_product,
                start_date=start_date_parsed,
                end_date=end_date_parsed
            )
        except Exception as e:
            logger.error(f"Error loading forecast data: {e}")
            return {}, {}, [], {}, html.Div(), html.Div()
        
        # Update all visualizations with fresh data
        time_series_figure = update_time_series(
            graph_component=dcc.Graph(id=TIME_SERIES_GRAPH_ID),
            forecast_df=forecast_df,
            product_id=selected_product,
            show_uncertainty=show_uncertainty,
            viewport_size=viewport_data['size']
        ).figure
        
        distribution_figure = update_distribution(
            graph_component=dcc.Graph(id=DISTRIBUTION_GRAPH_ID),
            forecast_df=forecast_df,
            product_id=selected_product,
            timestamp=forecast_df['timestamp'].iloc[0] if not forecast_df.empty else None,
            viewport_size=viewport_data['size']
        ).figure
        
        table_data = update_forecast_table(
            table_component=dash_table.DataTable(id=FORECAST_TABLE_ID),
            forecast_df=forecast_df,
            product_id=selected_product
        ).data
        
        # TODO: Update product comparison graph
        product_comparison_figure = {}  # Placeholder
        
        # Extract metadata from forecast data
        metadata = forecast_loader.get_forecast_metadata(forecast_df)
        
        # Create last updated info with current timestamp
        last_updated_info = update_last_updated_info(forecast_df)
        
        # Create forecast status indicator (normal or fallback)
        forecast_status = update_forecast_status(forecast_df, "light")  # TODO: Get theme from app state
        
        # Return tuple of all updated components
        return time_series_figure, distribution_figure, table_data, product_comparison_figure, forecast_status, last_updated_info

    logger.info("Completed control callbacks registration")

def update_last_updated_info(forecast_df: pd.DataFrame) -> html.Div:
    """
    Updates the last updated information component with current timestamp.

    Args:
        forecast_df (pandas.DataFrame): The forecast dataframe.

    Returns:
        dash_html_components.Div: The updated last updated info component.
    """
    # Extract generation timestamp from forecast metadata
    metadata = forecast_loader.get_forecast_metadata(forecast_df)
    generation_timestamp = metadata.get('generation_timestamp')
    
    # If timestamp not available, use current time
    if generation_timestamp is None:
        generation_timestamp = get_current_time_cst()
    
    # Format timestamp for display
    formatted_timestamp = format_datetime(generation_timestamp)
    
    # Create HTML component with formatted timestamp
    last_updated_info = html.Div(
        formatted_timestamp,
        id=LAST_UPDATED_ID,
        className="text-muted mb-3"
    )
    
    # Return the component
    return last_updated_info

def update_forecast_status(forecast_df: pd.DataFrame, theme: str) -> html.Div:
    """
    Updates the forecast status indicator based on forecast data.

    Args:
        forecast_df (pandas.DataFrame): The forecast dataframe.
        theme (str): The current UI theme.

    Returns:
        dash_html_components.Div: The updated forecast status component.
    """
    # Check if forecast data is using fallback mechanism
    metadata = forecast_loader.get_forecast_metadata(forecast_df)
    is_fallback = metadata.get('is_fallback', False)
    
    # If using fallback, create fallback badge
    if is_fallback:
        status_content = create_fallback_badge(forecast_df, theme)
    else:
        # If normal forecast, create normal status badge
        status_content = dbc.Badge("Normal", color="success", className="ms-2")
    
    # Create the status component
    forecast_status = html.Div(
        status_content,
        id=FORECAST_STATUS_ID,
        className="mb-3"
    )
    
    # Return the component
    return forecast_status

def load_forecast_data(product: str, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    """
    Loads forecast data for the specified product and date range.

    Args:
        product (str): The product to load forecast data for.
        start_date (datetime.date): The start date of the date range.
        end_date (datetime.date): The end date of the date range.

    Returns:
        pandas.DataFrame: The loaded forecast data.
    """
    logger.info(f"Loading forecast data for product: {product}, date range: {start_date} - {end_date}")
    
    try:
        # Load forecast data using forecast_loader.load_forecast_by_date_range
        forecast_df = forecast_loader.load_forecast_by_date_range(
            product=product,
            start_date=start_date,
            end_date=end_date
        )
        
        # Return the forecast dataframe
        return forecast_df
    
    except Exception as e:
        # Handle error and return empty dataframe
        logger.error(f"Error loading forecast data: {e}")
        return pd.DataFrame()
    
    finally:
        logger.info(f"Completed loading data for product: {product}, date range: {start_date} - {end_date}")