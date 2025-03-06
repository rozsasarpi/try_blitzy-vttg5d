"""
Configuration file defining electricity market products and their properties for the 
Electricity Market Price Forecasting System's visualization dashboard. This file provides
product definitions, display names, units, and default selections used throughout the 
dashboard components.
"""

import os
from typing import List, Dict, Any, Optional, Union

from .settings import CST_TIMEZONE
from .themes import PRODUCT_COLORS, DEFAULT_THEME

# List of all available product IDs for the forecasting system
PRODUCTS = ["DALMP", "RTLMP", "RegUp", "RegDown", "RRS", "NSRS"]

# Detailed information about each product
PRODUCT_DETAILS = {
    "DALMP": {
        "display_name": "Day-Ahead LMP",
        "description": "Day-Ahead Locational Marginal Price",
        "unit": "$/MWh",
        "can_be_negative": True,
        "category": "energy"
    },
    "RTLMP": {
        "display_name": "Real-Time LMP",
        "description": "Real-Time Locational Marginal Price",
        "unit": "$/MWh",
        "can_be_negative": True,
        "category": "energy"
    },
    "RegUp": {
        "display_name": "Regulation Up",
        "description": "Regulation Up Service",
        "unit": "$/MW",
        "can_be_negative": False,
        "category": "ancillary"
    },
    "RegDown": {
        "display_name": "Regulation Down",
        "description": "Regulation Down Service",
        "unit": "$/MW",
        "can_be_negative": False,
        "category": "ancillary"
    },
    "RRS": {
        "display_name": "Responsive Reserve",
        "description": "Responsive Reserve Service",
        "unit": "$/MW",
        "can_be_negative": False,
        "category": "ancillary"
    },
    "NSRS": {
        "display_name": "Non-Spinning Reserve",
        "description": "Non-Spinning Reserve Service",
        "unit": "$/MW",
        "can_be_negative": False,
        "category": "ancillary"
    }
}

# Default product to display when dashboard is first loaded
DEFAULT_PRODUCT = "DALMP"

# Categorization of products
PRODUCT_CATEGORIES = {
    "energy": ["DALMP", "RTLMP"],
    "ancillary": ["RegUp", "RegDown", "RRS", "NSRS"]
}

# Default products to show in comparison view
PRODUCT_COMPARISON_DEFAULTS = ["DALMP", "RTLMP"]

# Maximum number of products that can be compared simultaneously
MAX_COMPARISON_PRODUCTS = 6

# Line style definitions for each product in visualizations
PRODUCT_LINE_STYLES = {
    "DALMP": {"dash": "solid", "width": 2},
    "RTLMP": {"dash": "dot", "width": 2},
    "RegUp": {"dash": "dash", "width": 2},
    "RegDown": {"dash": "dashdot", "width": 2},
    "RRS": {"dash": "longdash", "width": 2},
    "NSRS": {"dash": "longdashdot", "width": 2}
}

# Product sort order for dropdowns and lists
PRODUCT_SORT_ORDER = {
    "DALMP": 1,
    "RTLMP": 2,
    "RegUp": 3,
    "RegDown": 4,
    "RRS": 5,
    "NSRS": 6
}


def get_product_display_name(product_id: str) -> str:
    """
    Returns the human-readable display name for a product.
    
    Args:
        product_id: Product identifier
        
    Returns:
        Display name for the product
    """
    if product_id in PRODUCT_DETAILS:
        return PRODUCT_DETAILS[product_id]["display_name"]
    return product_id


def get_product_description(product_id: str) -> str:
    """
    Returns the full description for a product.
    
    Args:
        product_id: Product identifier
        
    Returns:
        Description for the product
    """
    if product_id in PRODUCT_DETAILS:
        return PRODUCT_DETAILS[product_id]["description"]
    return ""


def get_product_unit(product_id: str) -> str:
    """
    Returns the unit of measurement for a product.
    
    Args:
        product_id: Product identifier
        
    Returns:
        Unit of measurement for the product
    """
    if product_id in PRODUCT_DETAILS:
        return PRODUCT_DETAILS[product_id]["unit"]
    return "$/MWh"  # Default unit if not found


def get_product_category(product_id: str) -> str:
    """
    Returns the category of a product (energy or ancillary).
    
    Args:
        product_id: Product identifier
        
    Returns:
        Category of the product
    """
    if product_id in PRODUCT_DETAILS:
        return PRODUCT_DETAILS[product_id]["category"]
    return "unknown"


def can_be_negative(product_id: str) -> bool:
    """
    Checks if a product price can be negative.
    
    Args:
        product_id: Product identifier
        
    Returns:
        True if product price can be negative, False otherwise
    """
    if product_id in PRODUCT_DETAILS:
        return PRODUCT_DETAILS[product_id]["can_be_negative"]
    return False


def get_products_by_category(category: str) -> List[str]:
    """
    Returns a list of product IDs in a specific category.
    
    Args:
        category: Product category (e.g., 'energy', 'ancillary')
        
    Returns:
        List of product IDs in the category
    """
    if category in PRODUCT_CATEGORIES:
        return PRODUCT_CATEGORIES[category]
    return []


def get_product_dropdown_options() -> List[Dict[str, str]]:
    """
    Returns formatted options for product dropdown component.
    
    Returns:
        List of dictionaries with label and value for each product
    """
    options = []
    for product_id in PRODUCTS:
        display_name = get_product_display_name(product_id)
        options.append({"label": display_name, "value": product_id})
    
    # Sort options according to product sort order
    options.sort(key=lambda x: PRODUCT_SORT_ORDER.get(x["value"], 999))
    
    return options


def get_product_color(product_id: str, theme: Optional[str] = None) -> str:
    """
    Returns the color for a specific product based on current theme.
    
    Args:
        product_id: Product identifier
        theme: Theme name (defaults to DEFAULT_THEME if not provided)
        
    Returns:
        Hex color code for the product
    """
    if theme is None:
        theme = DEFAULT_THEME
        
    if product_id in PRODUCT_COLORS:
        if theme in PRODUCT_COLORS[product_id]:
            return PRODUCT_COLORS[product_id][theme]
        return PRODUCT_COLORS[product_id][DEFAULT_THEME]
    
    # Default color if product not found
    return "#007bff"  # Default blue


def get_product_line_style(product_id: str) -> Dict[str, Any]:
    """
    Returns the line style for a specific product.
    
    Args:
        product_id: Product identifier
        
    Returns:
        Line style properties for the product
    """
    if product_id in PRODUCT_LINE_STYLES:
        return dict(PRODUCT_LINE_STYLES[product_id])  # Return a copy to prevent modifying the original
    
    # Default line style if product not found
    return {"dash": "solid", "width": 2}


def get_comparison_defaults() -> List[str]:
    """
    Returns the default products to show in comparison view.
    
    Returns:
        List of default product IDs for comparison
    """
    return list(PRODUCT_COMPARISON_DEFAULTS)  # Return a copy to prevent modifying the original


def is_valid_product(product_id: str) -> bool:
    """
    Checks if a product ID is valid.
    
    Args:
        product_id: Product identifier to check
        
    Returns:
        True if product ID is valid, False otherwise
    """
    return product_id in PRODUCTS