"""
Component module that implements a product comparison visualization for the Electricity Market Price Forecasting Dashboard.
This component allows users to compare price forecasts across multiple electricity market products on the same timeline,
with appropriate styling and interactive features.
"""

import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import pandas as pd
import logging
from typing import Dict, List, Optional, Union

from ..utils.plot_helpers import (
    create_product_comparison_plot,
    add_fallback_indicator,
    apply_responsive_layout
)
from ..config.product_config import (
    PRODUCT_COMPARISON_DEFAULTS,
    MAX_COMPARISON_PRODUCTS,
    get_product_display_name,
    get_product_color,
    get_product_line_style
)
from ..data.forecast_loader import load_forecast_by_date_range
from ..utils.error_handlers import is_fallback_data, handle_data_loading_error

# Set up logger for this component
logger = logging.getLogger(__name__)

# Component IDs for callbacks
PRODUCT_COMPARISON_GRAPH_ID = "product-comparison-graph"
PRODUCT_SELECTOR_ID = "product-comparison-selector"
ADD_PRODUCT_BUTTON_ID = "add-product-button"
REMOVE_PRODUCT_BUTTON_ID = "remove-product-button"

# Default component settings
DEFAULT_GRAPH_HEIGHT = 500

def create_product_comparison(
    forecast_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    product_ids: Optional[List[str]] = None,
    viewport_size: str = "desktop"
) -> html.Div:
    """
    Creates a product comparison visualization component with controls.
    
    Args:
        forecast_dfs: Dictionary mapping product IDs to forecast DataFrames
        product_ids: List of product IDs to display
        viewport_size: Viewport size category for responsive layout
        
    Returns:
        Container with product comparison visualization and controls
    """
    logger.info("Creating product comparison component")
    
    # Use default products if none provided
    if product_ids is None:
        product_ids = PRODUCT_COMPARISON_DEFAULTS
    
    # Initialize empty dictionary if forecast_dfs is None
    if forecast_dfs is None:
        forecast_dfs = {}
    
    # Create graph component
    graph = create_product_comparison_graph(forecast_dfs, product_ids, viewport_size)
    
    # Create product selector dropdown
    product_selector = create_product_selector(product_ids)
    
    # Create add/remove buttons
    control_buttons = create_product_control_buttons()
    
    # Create responsive layout based on viewport size
    if viewport_size == "mobile":
        # Mobile: Stack controls and graph vertically
        component = html.Div([
            html.Div([
                html.Div([
                    html.Label("Add Product:"),
                    product_selector
                ], className="mb-2"),
                html.Div([
                    control_buttons
                ], className="mb-3")
            ]),
            graph
        ], className="product-comparison-container")
    else:
        # Tablet/Desktop: Controls on top, graph below
        component = html.Div([
            html.Div([
                dbc.Row([
                    dbc.Col([
                        html.Label("Add Product:"),
                        product_selector
                    ], width={"size": 6}),
                    dbc.Col([
                        control_buttons
                    ], width={"size": 6}, className="d-flex align-items-end justify-content-end")
                ], className="mb-3")
            ]),
            graph
        ], className="product-comparison-container")
    
    return component

def create_product_comparison_graph(
    forecast_dfs: Dict[str, pd.DataFrame],
    product_ids: List[str],
    viewport_size: str = "desktop"
) -> dcc.Graph:
    """
    Creates the graph component for product comparison visualization.
    
    Args:
        forecast_dfs: Dictionary mapping product IDs to forecast DataFrames
        product_ids: List of product IDs to display
        viewport_size: Viewport size category for responsive layout
        
    Returns:
        Graph component with product comparison visualization
    """
    # Check if we have data for any of the products
    have_data = False
    for product_id in product_ids:
        if product_id in forecast_dfs and not forecast_dfs[product_id].empty:
            have_data = True
            break
    
    if not have_data:
        # No data available for any product, show empty graph with message
        empty_fig = {
            "data": [],
            "layout": {
                "title": "No forecast data available for selected products",
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
                "annotations": [
                    {
                        "text": "No forecast data available for selected products. Please select different products or date range.",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {"size": 16}
                    }
                ]
            }
        }
        
        return dcc.Graph(
            id=PRODUCT_COMPARISON_GRAPH_ID,
            figure=empty_fig,
            style={"height": DEFAULT_GRAPH_HEIGHT},
            config={"displayModeBar": True}
        )
    
    # Create product comparison plot
    fig = create_product_comparison_plot(
        forecast_dfs=forecast_dfs,
        product_ids=product_ids,
        title="Product Comparison"
    )
    
    # Check if we're using fallback data for any product
    using_fallback = check_any_fallback_data(forecast_dfs)
    
    # Add fallback indicator if necessary
    if using_fallback:
        fig = add_fallback_indicator(fig, next(iter(forecast_dfs.values())))
    
    # Apply responsive layout adjustments
    fig = apply_responsive_layout(fig, viewport_size, DEFAULT_GRAPH_HEIGHT)
    
    # Create and return the graph component
    return dcc.Graph(
        id=PRODUCT_COMPARISON_GRAPH_ID,
        figure=fig,
        style={"height": "100%", "width": "100%"},
        config={
            "displayModeBar": True,
            "responsive": True,
            "displaylogo": False,
            "modeBarButtonsToRemove": ["lasso2d", "select2d"]
        }
    )

def update_product_comparison(
    graph_component: dcc.Graph,
    forecast_dfs: Dict[str, pd.DataFrame],
    product_ids: List[str],
    viewport_size: str = "desktop"
) -> dcc.Graph:
    """
    Updates an existing product comparison visualization with new data.
    
    Args:
        graph_component: The existing graph component to update
        forecast_dfs: Dictionary mapping product IDs to forecast DataFrames
        product_ids: List of product IDs to display
        viewport_size: Viewport size category for responsive layout
        
    Returns:
        Updated graph component
    """
    logger.info(f"Updating product comparison for products: {', '.join(product_ids)}")
    
    # Create new graph component
    new_graph = create_product_comparison_graph(forecast_dfs, product_ids, viewport_size)
    
    # Update existing graph component with new figure
    graph_component.figure = new_graph.figure
    
    return graph_component

def create_product_selector(current_products: List[str]) -> dcc.Dropdown:
    """
    Creates a dropdown selector for adding products to comparison.
    
    Args:
        current_products: List of currently selected product IDs
        
    Returns:
        Dropdown component for product selection
    """
    from ..config.product_config import PRODUCTS
    
    # Filter out products that are already selected
    available_products = [p for p in PRODUCTS if p not in current_products]
    
    # Create options list with display names
    options = [
        {"label": get_product_display_name(p), "value": p}
        for p in available_products
    ]
    
    # Create and return the dropdown component
    return dcc.Dropdown(
        id=PRODUCT_SELECTOR_ID,
        options=options,
        value=None,
        placeholder="Select a product to add",
        clearable=True,
        disabled=len(current_products) >= MAX_COMPARISON_PRODUCTS or len(available_products) == 0,
        style={"width": "100%"}
    )

def create_product_control_buttons() -> html.Div:
    """
    Creates add and remove buttons for product comparison.
    
    Returns:
        Container with add and remove buttons
    """
    add_button = dbc.Button(
        "Add Product",
        id=ADD_PRODUCT_BUTTON_ID,
        color="primary",
        className="me-2",
        disabled=False
    )
    
    remove_button = dbc.Button(
        "Remove Selected",
        id=REMOVE_PRODUCT_BUTTON_ID,
        color="secondary",
        className="ms-2",
        disabled=False
    )
    
    return html.Div(
        [add_button, remove_button],
        className="d-flex"
    )

def load_comparison_data(
    product_ids: List[str],
    start_date: str,
    end_date: str
) -> Dict[str, pd.DataFrame]:
    """
    Loads forecast data for multiple products for comparison.
    
    Args:
        product_ids: List of product IDs to load data for
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        Dictionary mapping product IDs to forecast dataframes
    """
    logger.info(f"Loading comparison data for products: {', '.join(product_ids)}")
    
    # Initialize dictionary to store dataframes
    forecast_dfs = {}
    
    # Load data for each product
    for product_id in product_ids:
        try:
            df = load_forecast_by_date_range(
                product=product_id,
                start_date=start_date,
                end_date=end_date
            )
            forecast_dfs[product_id] = df
        except Exception as e:
            logger.error(f"Error loading data for product {product_id}: {str(e)}")
            handle_data_loading_error(e, f"loading forecast data for {product_id}")
    
    return forecast_dfs

def check_any_fallback_data(forecast_dfs: Dict[str, pd.DataFrame]) -> bool:
    """
    Checks if any of the forecast dataframes use fallback data.
    
    Args:
        forecast_dfs: Dictionary mapping product IDs to forecast DataFrames
        
    Returns:
        True if any dataframe uses fallback data, False otherwise
    """
    for df in forecast_dfs.values():
        if is_fallback_data(df):
            return True
    return False