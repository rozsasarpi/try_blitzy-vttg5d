"""
Module providing responsive layout components for the Electricity Market Price Forecasting System's Dash-based visualization interface. This module enables the dashboard to adapt to different screen sizes (desktop, tablet, mobile) by implementing responsive containers, layouts, and viewport detection mechanisms.
"""

import logging
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc  # version 1.0.0+

from ..config.settings import ENABLE_RESPONSIVE_UI
from ..config.dashboard_config import VIEWPORT_BREAKPOINTS, DEFAULT_VIEWPORT
from ..config.themes import RESPONSIVE_LAYOUTS
from ..utils.responsive_helpers import (
    get_responsive_style,
    create_responsive_style, 
    get_viewport_meta_tag,
    create_viewport_detection_script,
    detect_viewport_size
)

# Set up module logger
logger = logging.getLogger(__name__)

# Global IDs for viewport-related components
VIEWPORT_STORE_ID = 'viewport-store'
RESPONSIVE_CONTAINER_ID = 'responsive-container'
RESPONSIVE_LAYOUT_ID = 'responsive-layout'

# Default container styling
DEFAULT_CONTAINER_STYLE = {"width": "100%", "margin": "0 auto", "padding": "15px"}


def create_viewport_store():
    """
    Creates a hidden div to store the current viewport size information
    
    Returns:
        dash_core_components.Store: Store component for viewport information
    """
    return dcc.Store(
        id=VIEWPORT_STORE_ID,
        data={
            'width': None,
            'height': None,
            'size': DEFAULT_VIEWPORT
        },
        storage_type='session'
    )


def create_viewport_detection():
    """
    Creates components for detecting and updating viewport size
    
    Returns:
        list: List of components for viewport detection
    """
    # Create the store for viewport information
    viewport_store = create_viewport_store()
    
    # Create a hidden div with the viewport detection script
    viewport_detector = html.Div(
        id='viewport-detector',
        style={'display': 'none'},
        children=html.Script(create_viewport_detection_script())
    )
    
    return [viewport_store, viewport_detector]


def create_responsive_container(children=None, viewport_size='desktop', style=None, id=None, className=None):
    """
    Creates a container that adapts to different viewport sizes
    
    Args:
        children: List of child components
        viewport_size: The viewport size category ('desktop', 'tablet', 'mobile')
        style: Additional CSS styles to apply
        id: The component ID
        className: Additional CSS class names
        
    Returns:
        dash_bootstrap_components.Container: Responsive container component
    """
    # If responsive UI is disabled, return a standard container
    if not ENABLE_RESPONSIVE_UI:
        return dbc.Container(
            children=children,
            fluid=True,
            id=id or RESPONSIVE_CONTAINER_ID,
            className=className,
            style=style
        )
    
    # Create base style by combining default and provided styles
    base_style = dict(DEFAULT_CONTAINER_STYLE)
    if style:
        base_style.update(style)
    
    # Apply responsive adjustments based on viewport size
    adjusted_style = get_responsive_style(viewport_size, base_style)
    
    # Add viewport-specific class name
    viewport_class = f"viewport-{viewport_size}"
    if className:
        combined_class = f"{className} {viewport_class}"
    else:
        combined_class = viewport_class
    
    # Create the responsive container
    return dbc.Container(
        children=children,
        fluid=True,
        id=id or RESPONSIVE_CONTAINER_ID,
        className=combined_class,
        style=adjusted_style
    )


def create_responsive_layout(children=None, viewport_size='desktop', style=None, id=None, className=None):
    """
    Creates a layout that adapts to different viewport sizes
    
    Args:
        children: List of child components
        viewport_size: The viewport size category ('desktop', 'tablet', 'mobile')
        style: Additional CSS styles to apply
        id: The component ID
        className: Additional CSS class names
        
    Returns:
        dash_html_components.Div: Responsive layout component
    """
    logger.info(f"Creating responsive layout for viewport: {viewport_size}")
    
    # If responsive UI is disabled, return a standard layout
    if not ENABLE_RESPONSIVE_UI:
        return html.Div(
            children=children,
            id=id or RESPONSIVE_LAYOUT_ID,
            className=className,
            style=style
        )
    
    # Create base style
    base_style = {"width": "100%", "min-height": "100vh"}
    if style:
        base_style.update(style)
    
    # Apply responsive adjustments based on viewport size
    adjusted_style = get_responsive_style(viewport_size, base_style)
    
    # Add viewport-specific class name
    viewport_class = f"viewport-{viewport_size}"
    if className:
        combined_class = f"{className} {viewport_class}"
    else:
        combined_class = viewport_class
    
    # Ensure viewport detection components are included
    all_children = children or []
    
    # Check if viewport detection components are already included
    has_viewport_detection = any(
        getattr(child, 'id', None) == VIEWPORT_STORE_ID 
        for child in all_children if hasattr(child, 'id')
    )
    
    if not has_viewport_detection:
        all_children = create_viewport_detection() + (all_children if isinstance(all_children, list) else [all_children])
    
    # Create the responsive layout
    return html.Div(
        children=all_children,
        id=id or RESPONSIVE_LAYOUT_ID,
        className=combined_class,
        style=adjusted_style
    )


def apply_responsive_layout(component, viewport_size='desktop'):
    """
    Applies responsive layout adjustments to a component based on viewport size
    
    Args:
        component: Component to apply responsive adjustments to
        viewport_size: The viewport size category ('desktop', 'tablet', 'mobile')
        
    Returns:
        dash_html_components.Component: Component with responsive adjustments
    """
    # If responsive UI is disabled or component is None, return unchanged
    if not ENABLE_RESPONSIVE_UI or component is None:
        return component
    
    # Create a copy of the component to modify
    adjusted_component = component
    
    # Apply responsive style adjustments
    if hasattr(component, 'style') and component.style is not None:
        adjusted_style = get_responsive_style(viewport_size, component.style)
        adjusted_component = component.update(style=adjusted_style)
    
    # Add viewport-specific class
    if hasattr(component, 'className') and component.className:
        viewport_class = f"viewport-{viewport_size}"
        adjusted_component = adjusted_component.update(className=f"{component.className} {viewport_class}")
    
    # Recursively apply responsive layout to children
    if hasattr(component, 'children') and component.children:
        if isinstance(component.children, list):
            adjusted_children = [apply_responsive_layout(child, viewport_size) for child in component.children]
            adjusted_component = adjusted_component.update(children=adjusted_children)
        else:
            adjusted_child = apply_responsive_layout(component.children, viewport_size)
            adjusted_component = adjusted_component.update(children=adjusted_child)
    
    return adjusted_component


def get_viewport_callback():
    """
    Returns a callback function for updating viewport size information
    
    Returns:
        function: Callback function for viewport updates
    """
    def update_viewport(width, height):
        """
        Updates the viewport store with current dimensions and size category
        
        Args:
            width: Current window width in pixels
            height: Current window height in pixels
            
        Returns:
            dict: Updated viewport data
        """
        viewport_size = detect_viewport_size(width)
        return {
            'width': width,
            'height': height,
            'size': viewport_size
        }
    
    return update_viewport


def register_viewport_callback(app):
    """
    Registers the viewport detection callback with a Dash app
    
    Args:
        app: Dash application instance
    """
    viewport_callback = get_viewport_callback()
    
    app.clientside_callback(
        """
        function(width, height) {
            return {
                width: width,
                height: height,
                size: width <= BREAKPOINT_MOBILE ? 'mobile' :
                     width <= BREAKPOINT_TABLET ? 'tablet' : 'desktop'
            }
        }
        """.replace('BREAKPOINT_MOBILE', str(VIEWPORT_BREAKPOINTS['mobile']))
            .replace('BREAKPOINT_TABLET', str(VIEWPORT_BREAKPOINTS['tablet'])),
        dash.Output(VIEWPORT_STORE_ID, 'data'),
        [dash.Input('window', 'innerWidth'), dash.Input('window', 'innerHeight')]
    )
    
    logger.info("Viewport callback registered")


def create_responsive_row(columns, viewport_size='desktop', style=None, className=None):
    """
    Creates a responsive row with columns that adapt to viewport size
    
    Args:
        columns: List of column contents or components
        viewport_size: The viewport size category ('desktop', 'tablet', 'mobile')
        style: Additional CSS styles for the row
        className: Additional CSS class names
        
    Returns:
        dash_bootstrap_components.Row: Responsive row component
    """
    # If responsive UI is disabled, create a standard row with columns
    if not ENABLE_RESPONSIVE_UI:
        row_columns = []
        for col_content in columns:
            if isinstance(col_content, dict) and 'width' in col_content:
                width = col_content.pop('width')
                row_columns.append(dbc.Col(col_content, width=width))
            else:
                row_columns.append(dbc.Col(col_content))
        
        return dbc.Row(row_columns, style=style, className=className)
    
    # Apply responsive style adjustments to row style
    adjusted_style = get_responsive_style(viewport_size, style)
    
    # Process columns with responsive adjustments
    row_columns = []
    for col_content in columns:
        if isinstance(col_content, dict) and 'width' in col_content:
            # Extract content and width from dict
            width = col_content.pop('width')
            content = col_content
            
            # Apply responsive width adjustments based on viewport size
            if viewport_size == 'mobile':
                # On mobile, most columns should be full width
                adjusted_width = 12
            elif viewport_size == 'tablet':
                # On tablet, adjust column widths proportionally
                adjusted_width = min(12, int(width * 1.5))
            else:
                # On desktop, use specified width
                adjusted_width = width
            
            row_columns.append(dbc.Col(content, width=adjusted_width))
        else:
            # For columns without explicit width
            if viewport_size == 'mobile':
                # Full width on mobile
                row_columns.append(dbc.Col(col_content, width=12))
            else:
                # Auto width on tablet and desktop
                row_columns.append(dbc.Col(col_content))
    
    # Add viewport-specific class name
    viewport_class = f"viewport-{viewport_size}"
    if className:
        combined_class = f"{className} {viewport_class}"
    else:
        combined_class = viewport_class
    
    # Create the responsive row
    return dbc.Row(row_columns, style=adjusted_style, className=combined_class)