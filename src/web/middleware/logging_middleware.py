"""
Middleware module that provides request logging and performance monitoring for the
Electricity Market Price Forecasting System's Dash-based visualization interface.

This module implements request timing, metrics collection, and structured logging
to support monitoring and observability requirements.
"""

import functools
import time
from typing import Callable, Dict, Any
import collections

import flask
from dash import Dash

from ..config.logging_config import get_logger
from ..config.settings import DEBUG

# Try to import VERBOSE_LOGGING, fallback to DEBUG if not available
try:
    from ..config.settings import VERBOSE_LOGGING
except ImportError:
    # Assume verbose logging follows debug mode if not explicitly defined
    VERBOSE_LOGGING = DEBUG

# Get logger for the middleware
logger = get_logger('logging_middleware')

# Dictionary to store metrics for each request endpoint
request_metrics = collections.defaultdict(lambda: {'count': 0, 'total_time': 0.0, 'min_time': float('inf'), 'max_time': 0.0})


def log_request_time(func: Callable) -> Callable:
    """
    Decorator that logs the execution time of a function and updates metrics.
    
    Args:
        func: The function to be decorated
        
    Returns:
        Wrapped function that logs execution time
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        
        # Update metrics for this function
        func_name = func.__name__
        request_metrics[func_name]['count'] += 1
        request_metrics[func_name]['total_time'] += execution_time
        request_metrics[func_name]['min_time'] = min(request_metrics[func_name]['min_time'], execution_time)
        request_metrics[func_name]['max_time'] = max(request_metrics[func_name]['max_time'], execution_time)
        
        # Log the execution time
        logger.info(f"Function {func_name} executed in {execution_time:.4f} seconds")
        
        return result
    return wrapper


def get_metrics_summary() -> Dict[str, Any]:
    """
    Returns a summary of request metrics for monitoring.
    
    Returns:
        Dictionary containing request metrics summary
    """
    summary = {
        'endpoints': {},
        'total_requests': 0,
        'slowest_endpoint': None,
        'slowest_time': 0.0
    }
    
    for endpoint, data in request_metrics.items():
        if data['count'] > 0:
            avg_time = data['total_time'] / data['count']
            summary['endpoints'][endpoint] = {
                'count': data['count'],
                'avg_time': avg_time,
                'min_time': data['min_time'],
                'max_time': data['max_time']
            }
            summary['total_requests'] += data['count']
            
            # Track slowest endpoint
            if data['max_time'] > summary['slowest_time']:
                summary['slowest_endpoint'] = endpoint
                summary['slowest_time'] = data['max_time']
    
    return summary


def reset_metrics() -> None:
    """
    Resets all collected request metrics.
    """
    request_metrics.clear()
    logger.info("Request metrics have been reset")


def format_request_log(request: flask.Request) -> str:
    """
    Formats request information for logging.
    
    Args:
        request: Flask request object
        
    Returns:
        Formatted request log string
    """
    # Extract basic request info
    method = request.method
    path = request.path
    query_string = request.query_string.decode('utf-8') if request.query_string else ""
    client_ip = request.remote_addr or "unknown"
    
    # Create log message
    log_message = f"{method} {path}"
    if query_string:
        log_message += f"?{query_string}"
    log_message += f" from {client_ip}"
    
    # Add request body summary for POST/PUT if verbose logging is enabled
    if VERBOSE_LOGGING and method in ('POST', 'PUT') and request.content_length:
        try:
            if request.is_json:
                body_summary = str(request.json)[:100]
                log_message += f" | Body: {body_summary}..."
            else:
                log_message += f" | Content-Type: {request.content_type}"
        except Exception as e:
            log_message += f" | Error reading body: {str(e)}"
    
    return log_message


class LoggingMiddleware:
    """
    Middleware class that provides request logging and performance monitoring for Dash applications.
    """
    
    def __init__(self, verbose: bool = False):
        """
        Initializes the logging middleware.
        
        Args:
            verbose: Whether to enable verbose logging
        """
        self.logger = get_logger('logging_middleware')
        self.verbose = verbose
        self.app = None
        self.logger.info("Logging middleware initialized")
    
    def apply(self, app: Dash) -> Dash:
        """
        Applies logging middleware to a Dash application.
        
        Args:
            app: Dash application to apply middleware to
            
        Returns:
            Dash application with logging middleware applied
        """
        self.app = app
        
        # Register request and response handlers
        app.server.before_request(self.log_request)
        app.server.after_request(self.log_response)
        
        # Patch callbacks with timing decorator
        self.patch_callbacks()
        
        self.logger.info("Logging middleware applied to Dash application")
        return app
    
    def log_request(self) -> None:
        """
        Logs information about incoming requests.
        """
        request = flask.request
        log_message = format_request_log(request)
        self.logger.info(f"Request: {log_message}")
        
        # Store request start time in Flask g object
        flask.g.start_time = time.time()
    
    def log_response(self, response: flask.Response) -> flask.Response:
        """
        Logs information about outgoing responses and calculates request duration.
        
        Args:
            response: Flask response object
            
        Returns:
            Unmodified response object
        """
        # Calculate request duration
        if hasattr(flask.g, 'start_time'):
            duration = time.time() - flask.g.start_time
            
            # Get endpoint for metrics
            endpoint = flask.request.endpoint or flask.request.path
            
            # Update metrics
            request_metrics[endpoint]['count'] += 1
            request_metrics[endpoint]['total_time'] += duration
            request_metrics[endpoint]['min_time'] = min(request_metrics[endpoint]['min_time'], duration)
            request_metrics[endpoint]['max_time'] = max(request_metrics[endpoint]['max_time'], duration)
            
            # Log response info
            self.logger.info(f"Response: {response.status_code} in {duration:.4f} seconds")
        else:
            self.logger.warning("Response received without request timing information")
        
        return response
    
    def patch_callbacks(self) -> None:
        """
        Patches the app.callback method to wrap all callbacks with timing decorator.
        """
        if not self.app:
            self.logger.error("Cannot patch callbacks: app not set")
            return
        
        # Store original callback method
        original_callback = self.app.callback
        
        # Define wrapper that applies timing decorator
        @functools.wraps(original_callback)
        def callback_wrapper(*args, **kwargs):
            callback_decorator = original_callback(*args, **kwargs)
            
            # Create a new function wrapper that applies our timing decorator
            @functools.wraps(callback_decorator)
            def function_wrapper(func):
                # Apply our timing decorator
                timed_func = log_request_time(func)
                # Apply the original wrapper
                return callback_decorator(timed_func)
            
            return function_wrapper
        
        # Replace the callback method
        self.app.callback = callback_wrapper
        self.logger.info("Dash callbacks patched with timing decorator")
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Returns current request metrics for monitoring.
        
        Returns:
            Dictionary containing request metrics
        """
        metrics = dict(request_metrics)
        metrics['summary'] = get_metrics_summary()
        return metrics