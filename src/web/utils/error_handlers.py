"""
Utility module that provides error handling functions and classes for the Electricity Market Price 
Forecasting System's Dash-based visualization interface. Implements centralized error handling,
user-friendly error message creation, exception formatting, and fallback data detection to ensure
a robust user experience when errors occur.
"""

import traceback
import datetime
import uuid
from typing import Dict, Any, Optional, Union, List, Tuple

import dash
import dash_html_components as html
import dash_bootstrap_components as dbc

from ..config.logging_config import get_logger
from ..config.settings import DEBUG
from ..config.themes import get_status_color

# Initialize logger
logger = get_logger('error_handlers')

# Dictionary mapping error types to user-friendly titles
ERROR_TYPES = {
    "data_loading": "Data Loading Error",
    "visualization": "Visualization Error",
    "api": "API Error",
    "processing": "Data Processing Error",
    "unknown": "Error Occurred"
}

# Registry to store errors with details
ERROR_REGISTRY = {}

# Message displayed when using fallback data
FALLBACK_MESSAGE = "Using fallback forecast (previous day's data)"


def handle_callback_error(error: Exception, callback_name: str) -> Dict[str, Any]:
    """
    Handles errors that occur during Dash callback execution.
    
    Args:
        error: The exception that was raised
        callback_name: Name of the callback where the error occurred
        
    Returns:
        A dictionary with error information including message, type, and details
    """
    logger.error(f"Error in callback {callback_name}: {format_exception(error)}")
    
    # Get detailed traceback
    error_traceback = traceback.format_exc()
    
    # Try to get callback context for additional information
    ctx = None
    try:
        ctx = dash.callback_context
    except:
        pass
    
    # Create error info dictionary
    error_info = {
        "type": "callback_error",
        "message": str(error),
        "details": f"Error occurred in {callback_name}",
        "callback_name": callback_name,
        "timestamp": datetime.datetime.now().isoformat()
    }
    
    # Include traceback in debug mode
    if DEBUG:
        error_info["traceback"] = error_traceback
        
    # Include callback context if available
    if ctx:
        try:
            error_info["context"] = {
                "triggered": ctx.triggered,
                "inputs": ctx.inputs,
                "states": ctx.states if hasattr(ctx, 'states') else None
            }
        except:
            pass
    
    return error_info


def create_error_message(message: str, error_type: str = "unknown", 
                        details: str = None, show_details: bool = False) -> html.Div:
    """
    Creates a user-friendly error message component for display.
    
    Args:
        message: The main error message to display
        error_type: Type of error (must be key in ERROR_TYPES or will use "unknown")
        details: Additional error details (optional)
        show_details: Whether to show detailed information (typically only in debug mode)
        
    Returns:
        A Dash HTML component with formatted error message
    """
    # Get appropriate error title
    error_title = ERROR_TYPES.get(error_type, ERROR_TYPES["unknown"])
    
    # Get appropriate color for error alert
    error_color = get_status_color("error", "light")
    
    # Create the main alert component
    alert = dbc.Alert(
        [
            html.H5(error_title, className="alert-heading"),
            html.P(message),
        ],
        color="danger",
        style={"backgroundColor": error_color, "border": "none"},
        className="mb-3"
    )
    
    # Create the details section if needed
    details_section = None
    if show_details and details:
        details_section = html.Div([
            html.Hr(),
            html.Details([
                html.Summary("Technical Details"),
                html.Pre(details, style={"whiteSpace": "pre-wrap", "fontSize": "0.8rem"})
            ])
        ])
        alert.children.append(details_section)
    
    # Return the complete error message component
    return html.Div([
        alert
    ], className="error-message-container")


def format_exception(error: Exception) -> str:
    """
    Formats an exception into a readable string.
    
    Args:
        error: The exception to format
        
    Returns:
        A formatted string representation of the exception
    """
    if error is None:
        return "Unknown error (None provided)"
    
    try:
        error_type = error.__class__.__name__
        error_message = str(error)
        return f"{error_type}: {error_message}"
    except:
        # Fallback if anything goes wrong during formatting
        return str(error)


def handle_data_loading_error(error: Exception, context: str) -> html.Div:
    """
    Handles errors that occur during data loading.
    
    Args:
        error: The exception that occurred
        context: Additional context about what was being loaded
        
    Returns:
        A Dash HTML component with an appropriate error message
    """
    logger.error(f"Data loading error in {context}: {format_exception(error)}")
    
    # Check if it's a connection error or timeout
    error_str = str(error).lower()
    if "connection" in error_str or "timeout" in error_str or "network" in error_str:
        message = f"Could not connect to the forecast data source. Please check your network connection and try again."
    elif "not found" in error_str or "404" in error_str:
        message = f"The requested forecast data could not be found. It may not be available yet."
    else:
        message = f"An error occurred while loading forecast data. {str(error) if DEBUG else 'Please try again later.'}"
    
    details = f"Context: {context}\nError: {format_exception(error)}"
    if DEBUG:
        details += f"\n\nTraceback:\n{traceback.format_exc()}"
    
    return create_error_message(
        message=message,
        error_type="data_loading",
        details=details,
        show_details=DEBUG
    )


def handle_visualization_error(error: Exception, component_name: str) -> html.Div:
    """
    Handles errors that occur during visualization rendering.
    
    Args:
        error: The exception that occurred
        component_name: Name of the visualization component that failed
        
    Returns:
        A Dash HTML component with an appropriate error message
    """
    logger.error(f"Visualization error in {component_name}: {format_exception(error)}")
    
    message = f"An error occurred while displaying the {component_name}. "
    if DEBUG:
        message += str(error)
    else:
        message += "Please try refreshing the page or contact support if the problem persists."
    
    details = f"Component: {component_name}\nError: {format_exception(error)}"
    if DEBUG:
        details += f"\n\nTraceback:\n{traceback.format_exc()}"
    
    return create_error_message(
        message=message,
        error_type="visualization",
        details=details,
        show_details=DEBUG
    )


def is_fallback_data(data: Dict) -> bool:
    """
    Checks if the forecast data is from the fallback mechanism.
    
    Args:
        data: Forecast data dictionary
        
    Returns:
        True if data is from fallback mechanism, False otherwise
    """
    if data is None:
        return False
    
    # Check if the data has the is_fallback flag
    return data.get('is_fallback', False)


def create_fallback_notice() -> html.Div:
    """
    Creates a notice component for fallback forecast data.
    
    Returns:
        A Dash HTML component with fallback notice
    """
    fallback_color = get_status_color("warning", "light")
    
    return html.Div([
        dbc.Alert(
            [
                html.I(className="fas fa-exclamation-triangle me-2"),
                FALLBACK_MESSAGE
            ],
            color="warning",
            style={"backgroundColor": fallback_color, "border": "none"},
            className="mb-3"
        )
    ], className="fallback-notice-container")


class ErrorHandler:
    """
    Class that provides centralized error handling functionality.
    """
    
    def __init__(self):
        """
        Initializes the error handler with an empty error registry.
        """
        self.error_registry = {}
        self.logger = get_logger('error_handler')
        self.logger.info("Initialized error handler")
    
    def register_error(self, error: Exception, context: str) -> str:
        """
        Registers an error in the error registry.
        
        Args:
            error: The exception that occurred
            context: Additional context about where/when the error occurred
            
        Returns:
            Error ID for reference
        """
        # Generate a unique ID for this error
        error_id = str(uuid.uuid4())
        
        # Format the error details
        error_details = {
            "error_type": error.__class__.__name__,
            "error_message": str(error),
            "context": context,
            "timestamp": datetime.datetime.now().isoformat(),
            "traceback": traceback.format_exc()
        }
        
        # Log the error
        self.logger.error(f"Error registered [{error_id}] in {context}: {format_exception(error)}")
        
        # Store in registry
        self.error_registry[error_id] = error_details
        
        return error_id
    
    def get_error_component(self, error_id: str, with_details: bool = False) -> html.Div:
        """
        Gets an error component for a registered error.
        
        Args:
            error_id: ID of the registered error
            with_details: Whether to include detailed information
            
        Returns:
            A Dash HTML component for displaying the error
        """
        # Check if the error exists in registry
        if error_id in self.error_registry:
            error_info = self.error_registry[error_id]
            
            message = f"{error_info['error_type']}: {error_info['error_message']}"
            
            details = None
            if with_details:
                details = f"Context: {error_info['context']}\n"
                details += f"Timestamp: {error_info['timestamp']}\n"
                details += f"Traceback:\n{error_info['traceback']}"
            
            # Determine error type for display
            error_type = "unknown"
            if "connection" in message.lower() or "timeout" in message.lower():
                error_type = "api"
            elif "data" in error_info['context'].lower() or "loading" in error_info['context'].lower():
                error_type = "data_loading"
            elif "visual" in error_info['context'].lower() or "chart" in error_info['context'].lower():
                error_type = "visualization"
            elif "process" in error_info['context'].lower():
                error_type = "processing"
            
            return create_error_message(
                message=message,
                error_type=error_type,
                details=details,
                show_details=with_details and DEBUG
            )
        else:
            # Error not found in registry
            self.logger.warning(f"Requested error component for unknown error ID: {error_id}")
            return create_error_message(
                message="An error occurred, but details are no longer available.",
                error_type="unknown",
                show_details=False
            )
    
    def clear_error(self, error_id: str) -> bool:
        """
        Removes an error from the registry.
        
        Args:
            error_id: ID of the error to remove
            
        Returns:
            True if error was cleared, False if not found
        """
        if error_id in self.error_registry:
            self.error_registry.pop(error_id)
            self.logger.info(f"Cleared error with ID: {error_id}")
            return True
        else:
            self.logger.warning(f"Attempted to clear non-existent error with ID: {error_id}")
            return False