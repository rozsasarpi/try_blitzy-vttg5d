"""
Module that implements Dash callbacks for the product comparison visualization component in the 
Electricity Market Price Forecasting System. These callbacks handle user interactions with the 
product comparison view, including adding/removing products, updating the visualization when 
date ranges change, and responding to viewport size changes for responsive design.
"""

from typing import List, Dict, Any, Optional

import dash  # version 2.9.0+
from dash.dependencies import Input, Output, State  # version 2.9.0+
from dash.exceptions import PreventUpdate  # version 2.9.0+
from dash import no_update, html, dcc  # version 2.9.0+

from ..components.product_comparison import (
    create_product_comparison,
    update_product_comparison,
    load_comparison_data,
    PRODUCT_COMPARISON_GRAPH_ID,
    PRODUCT_SELECTOR_ID,
    ADD_PRODUCT_BUTTON_ID,
    REMOVE_PRODUCT_BUTTON_ID
)
from ..config.product_config import (
    PRODUCTS,
    MAX_COMPARISON_PRODUCTS,
    PRODUCT_COMPARISON_DEFAULTS,
    is_valid_product
)
from ..config.logging_config import get_logger
from ..data.forecast_loader import load_forecast_by_date_range
from ..utils.error_handlers import handle_data_loading_error
from ..config.dashboard_config import CONTENT_DIV_ID, VIEWPORT_STORE_ID

# Set up logger for this module
logger = get_logger('callbacks.product_comparison')

def register_product_comparison_callbacks(app: dash.Dash) -> None:
    """
    Registers all callbacks related to the product comparison visualization.
    
    Args:
        app: The Dash application instance
    """
    logger.info("Registering product comparison callbacks")
    
    # Register callback for initializing product comparison component
    @app.callback(
        Output('product-comparison-container', 'children'),
        Input('initialize-product-comparison', 'n_clicks'),
        State(VIEWPORT_STORE_ID, 'data')
    )
    def initialize_product_comparison(n_clicks, viewport_size):
        """
        Callback to initialize the product comparison component.
        
        Args:
            n_clicks: Number of times the initialization trigger has been clicked
            viewport_size: Current viewport size category
            
        Returns:
            Product comparison component
        """
        logger.info("Initializing product comparison component")
        
        try:
            # Load comparison data for default products
            forecast_dfs = load_comparison_data(
                PRODUCT_COMPARISON_DEFAULTS,
                start_date="2023-06-01",  # Default start date
                end_date="2023-06-03"     # Default end date
            )
            
            # Create the component
            component = create_product_comparison(
                forecast_dfs=forecast_dfs,
                product_ids=PRODUCT_COMPARISON_DEFAULTS,
                viewport_size=viewport_size or "desktop"
            )
            
            return component
        
        except Exception as e:
            logger.error(f"Error initializing product comparison: {str(e)}")
            return handle_data_loading_error(e, "initializing product comparison")
    
    # Register callback for adding a product to comparison
    @app.callback(
        [Output(PRODUCT_COMPARISON_GRAPH_ID, 'figure'), 
         Output('product-comparison-products', 'data')],
        Input(ADD_PRODUCT_BUTTON_ID, 'n_clicks'),
        State(PRODUCT_SELECTOR_ID, 'value'),
        State('product-comparison-products', 'data'),
        State('date-range-store', 'data'),
        State(VIEWPORT_STORE_ID, 'data')
    )
    def add_product_to_comparison(n_clicks, selected_product, current_products, date_range, viewport_size):
        """
        Callback to add a selected product to the comparison.
        
        Args:
            n_clicks: Number of times the add button has been clicked
            selected_product: The product selected to add
            current_products: List of currently displayed products
            date_range: Current date range
            viewport_size: Current viewport size category
            
        Returns:
            Tuple of (Updated figure, Updated product list)
        """
        # Check if callback was triggered by the add button
        ctx = dash.callback_context
        if not ctx.triggered or ctx.triggered[0]['prop_id'] != f"{ADD_PRODUCT_BUTTON_ID}.n_clicks":
            raise PreventUpdate
        
        # Check if we have a selected product
        if selected_product is None:
            raise PreventUpdate
        
        logger.info(f"Adding product to comparison: {selected_product}")
        
        try:
            # Initialize current_products if None
            if current_products is None:
                current_products = PRODUCT_COMPARISON_DEFAULTS.copy()
            else:
                current_products = list(current_products)  # Make a copy
            
            # Validate selected product
            if not is_valid_product(selected_product):
                logger.warning(f"Invalid product selected: {selected_product}")
                raise PreventUpdate
            
            # Check if product is already in comparison
            if selected_product in current_products:
                logger.info(f"Product {selected_product} already in comparison")
                raise PreventUpdate
            
            # Check if adding would exceed max
            if len(current_products) >= MAX_COMPARISON_PRODUCTS:
                logger.warning(f"Cannot add more products: max {MAX_COMPARISON_PRODUCTS} reached")
                raise PreventUpdate
            
            # Add product to list
            current_products.append(selected_product)
            
            # Extract date range
            start_date = date_range.get('start_date')
            end_date = date_range.get('end_date')
            
            if not start_date or not end_date:
                logger.warning("Missing date range information")
                start_date = "2023-06-01"  # Default start date
                end_date = "2023-06-03"    # Default end date
            
            # Load data for all products
            forecast_dfs = load_comparison_data(
                current_products,
                start_date,
                end_date
            )
            
            # Create updated figure
            from ..utils.plot_helpers import create_product_comparison_plot, apply_responsive_layout
            
            # Create the plot figure
            figure = create_product_comparison_plot(
                forecast_dfs=forecast_dfs,
                product_ids=current_products,
                title="Product Comparison"
            )
            
            # Apply responsive layout
            figure = apply_responsive_layout(figure, viewport_size or "desktop")
            
            return figure, current_products
        
        except Exception as e:
            logger.error(f"Error adding product to comparison: {str(e)}")
            return no_update, no_update
    
    # Register callback for removing a product from comparison
    @app.callback(
        [Output(PRODUCT_COMPARISON_GRAPH_ID, 'figure'), 
         Output('product-comparison-products', 'data')],
        Input(REMOVE_PRODUCT_BUTTON_ID, 'n_clicks'),
        State('product-comparison-products', 'data'),
        State('date-range-store', 'data'),
        State(VIEWPORT_STORE_ID, 'data')
    )
    def remove_product_from_comparison(n_clicks, current_products, date_range, viewport_size):
        """
        Callback to remove a product from the comparison.
        
        Args:
            n_clicks: Number of times the remove button has been clicked
            current_products: List of currently displayed products
            date_range: Current date range
            viewport_size: Current viewport size category
            
        Returns:
            Tuple of (Updated figure, Updated product list)
        """
        # Check if callback was triggered by the remove button
        ctx = dash.callback_context
        if not ctx.triggered or ctx.triggered[0]['prop_id'] != f"{REMOVE_PRODUCT_BUTTON_ID}.n_clicks":
            raise PreventUpdate
        
        logger.info("Removing product from comparison")
        
        try:
            # Validate current_products
            if current_products is None or len(current_products) <= 1:
                logger.warning("Cannot remove - need at least one product for comparison")
                raise PreventUpdate
            
            # Make a copy of current products list
            current_products = list(current_products)
            
            # Remove the last product
            removed_product = current_products.pop()
            logger.info(f"Removed product: {removed_product}")
            
            # Extract date range
            start_date = date_range.get('start_date')
            end_date = date_range.get('end_date')
            
            if not start_date or not end_date:
                logger.warning("Missing date range information")
                start_date = "2023-06-01"  # Default start date
                end_date = "2023-06-03"    # Default end date
            
            # Load data for remaining products
            forecast_dfs = load_comparison_data(
                current_products,
                start_date,
                end_date
            )
            
            # Create updated figure
            from ..utils.plot_helpers import create_product_comparison_plot, apply_responsive_layout
            
            # Create the plot figure
            figure = create_product_comparison_plot(
                forecast_dfs=forecast_dfs,
                product_ids=current_products,
                title="Product Comparison"
            )
            
            # Apply responsive layout
            figure = apply_responsive_layout(figure, viewport_size or "desktop")
            
            return figure, current_products
        
        except Exception as e:
            logger.error(f"Error removing product from comparison: {str(e)}")
            return no_update, no_update
    
    # Register callback for updating comparison when date range changes
    @app.callback(
        Output(PRODUCT_COMPARISON_GRAPH_ID, 'figure'),
        Input('date-range-store', 'data'),
        State('product-comparison-products', 'data'),
        State(VIEWPORT_STORE_ID, 'data')
    )
    def update_comparison_on_date_change(date_range, current_products, viewport_size):
        """
        Callback to update the comparison when date range changes.
        
        Args:
            date_range: New date range
            current_products: List of currently displayed products
            viewport_size: Current viewport size category
            
        Returns:
            Updated figure for the comparison graph
        """
        # Check if we have the needed data
        if not date_range or not current_products:
            raise PreventUpdate
        
        logger.info("Updating comparison based on date change")
        
        try:
            # Extract date range
            start_date = date_range.get('start_date')
            end_date = date_range.get('end_date')
            
            if not start_date or not end_date:
                logger.warning("Invalid date range")
                raise PreventUpdate
            
            logger.info(f"Date range: {start_date} to {end_date}")
            
            # Load data for all products with the new date range
            forecast_dfs = load_comparison_data(
                current_products,
                start_date,
                end_date
            )
            
            # Create updated figure
            from ..utils.plot_helpers import create_product_comparison_plot, apply_responsive_layout
            
            # Create the plot figure
            figure = create_product_comparison_plot(
                forecast_dfs=forecast_dfs,
                product_ids=current_products,
                title="Product Comparison"
            )
            
            # Apply responsive layout
            figure = apply_responsive_layout(figure, viewport_size or "desktop")
            
            return figure
        
        except Exception as e:
            logger.error(f"Error updating comparison on date change: {str(e)}")
            return no_update
    
    # Register callback for updating comparison when viewport size changes
    @app.callback(
        Output(PRODUCT_COMPARISON_GRAPH_ID, 'figure'),
        Input(VIEWPORT_STORE_ID, 'data'),
        State('product-comparison-products', 'data'),
        State('date-range-store', 'data'),
        State(PRODUCT_COMPARISON_GRAPH_ID, 'figure')
    )
    def update_comparison_on_viewport_change(viewport_size, current_products, date_range, current_figure):
        """
        Callback to update the comparison when viewport size changes.
        
        Args:
            viewport_size: New viewport size category
            current_products: List of currently displayed products
            date_range: Current date range
            current_figure: Current figure object
            
        Returns:
            Updated figure with responsive adjustments
        """
        # Check if we have the needed data
        if not viewport_size or not current_products or not current_figure:
            raise PreventUpdate
        
        # Check if this is an actual change in viewport size
        ctx = dash.callback_context
        if not ctx.triggered or ctx.triggered[0]['prop_id'] != f"{VIEWPORT_STORE_ID}.data":
            raise PreventUpdate
        
        logger.info(f"Updating comparison for viewport size: {viewport_size}")
        
        try:
            # If we just need to adjust layout for the new viewport size
            from ..utils.plot_helpers import apply_responsive_layout
            
            # Apply responsive layout to current figure
            updated_figure = apply_responsive_layout(current_figure, viewport_size)
            
            # If substantial viewport change, we might want to reload data and rebuild figure
            if current_figure is None:
                # Extract date range
                start_date = date_range.get('start_date')
                end_date = date_range.get('end_date')
                
                if not start_date or not end_date:
                    logger.warning("Invalid date range for viewport update")
                    start_date = "2023-06-01"  # Default start date
                    end_date = "2023-06-03"    # Default end date
                
                # Load data for all products
                forecast_dfs = load_comparison_data(
                    current_products,
                    start_date,
                    end_date
                )
                
                # Create updated figure
                from ..utils.plot_helpers import create_product_comparison_plot
                
                # Create the plot figure with new viewport size
                updated_figure = create_product_comparison_plot(
                    forecast_dfs=forecast_dfs,
                    product_ids=current_products,
                    title="Product Comparison"
                )
                
                # Apply responsive layout
                updated_figure = apply_responsive_layout(updated_figure, viewport_size)
            
            return updated_figure
        
        except Exception as e:
            logger.error(f"Error updating comparison on viewport change: {str(e)}")
            return no_update
    
    # Register callback for updating product selector dropdown options
    @app.callback(
        Output(PRODUCT_SELECTOR_ID, 'options'),
        Input('product-comparison-products', 'data')
    )
    def update_product_selector_options(current_products):
        """
        Callback to update the product selector dropdown options.
        
        Args:
            current_products: List of currently displayed products
            
        Returns:
            Updated dropdown options excluding already selected products
        """
        logger.debug("Updating product selector options")
        
        try:
            # Initialize current_products if None
            if current_products is None:
                current_products = PRODUCT_COMPARISON_DEFAULTS.copy()
            
            # Get all available products
            all_products = PRODUCTS.copy()
            
            # Filter out products that are already in the comparison
            available_products = [p for p in all_products if p not in current_products]
            
            # Create dropdown options
            from ..config.product_config import get_product_display_name
            options = [
                {"label": get_product_display_name(p), "value": p}
                for p in available_products
            ]
            
            logger.debug(f"Product selector has {len(options)} options")
            return options
        
        except Exception as e:
            logger.error(f"Error updating product selector options: {str(e)}")
            return no_update
    
    # Register callback for enabling/disabling add product button
    @app.callback(
        Output(ADD_PRODUCT_BUTTON_ID, 'disabled'),
        Input(PRODUCT_SELECTOR_ID, 'value'),
        State('product-comparison-products', 'data')
    )
    def update_add_button_state(selected_product, current_products):
        """
        Callback to enable/disable the add product button based on current state.
        
        Args:
            selected_product: Currently selected product in dropdown
            current_products: List of products already in comparison
            
        Returns:
            True if button should be disabled, False otherwise
        """
        try:
            # Initialize current_products if None
            if current_products is None:
                current_products = PRODUCT_COMPARISON_DEFAULTS.copy()
            
            # Disable button if no product is selected
            if selected_product is None:
                return True
            
            # Disable button if already at max products
            if len(current_products) >= MAX_COMPARISON_PRODUCTS:
                return True
            
            # Disable button if selected product is already in comparison
            if selected_product in current_products:
                return True
            
            # Otherwise, enable the button
            return False
        
        except Exception as e:
            logger.error(f"Error updating add button state: {str(e)}")
            return True  # Default to disabled if error
    
    # Register callback for enabling/disabling remove product button
    @app.callback(
        Output(REMOVE_PRODUCT_BUTTON_ID, 'disabled'),
        Input('product-comparison-products', 'data')
    )
    def update_remove_button_state(current_products):
        """
        Callback to enable/disable the remove product button based on current state.
        
        Args:
            current_products: List of products in comparison
            
        Returns:
            True if button should be disabled, False otherwise
        """
        try:
            # Disable button if current_products is None
            if current_products is None:
                return True
            
            # Disable button if only one product is left (minimum required)
            if len(current_products) <= 1:
                return True
            
            # Otherwise, enable the button
            return False
        
        except Exception as e:
            logger.error(f"Error updating remove button state: {str(e)}")
            return True  # Default to disabled if error
    
    logger.info("Product comparison callbacks registered")