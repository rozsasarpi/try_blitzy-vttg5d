"""
Authentication middleware for the Dash-based visualization interface of the Electricity Market Price
Forecasting System. Implements basic authentication for administrative routes while allowing 
anonymous access to visualization features, following the security architecture requirements.
"""

import functools
import flask
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc

from ..config.settings import ENABLE_AUTH, DEBUG
from ..config.logging_config import get_logger
from ..services.authentication import AuthenticationService, is_protected_route

# Initialize logger
logger = get_logger('auth_middleware')

# Authentication service
auth_service = AuthenticationService()

# Routes
LOGIN_ROUTE = '/login'
LOGOUT_ROUTE = '/logout'

# Authentication cookie name
AUTH_COOKIE_NAME = 'auth_token'


def requires_auth(func):
    """
    Decorator function that enforces authentication for protected routes
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Skip authentication if disabled
        if not ENABLE_AUTH:
            return func(*args, **kwargs)
        
        # Get current request path
        path = flask.request.path
        
        # Check if this path requires authentication
        if not is_protected_route(path):
            return func(*args, **kwargs)
        
        # Get authentication token from cookies or headers
        auth_token = flask.request.cookies.get(AUTH_COOKIE_NAME)
        
        # Create headers dict for auth check
        headers = {}
        if auth_token:
            headers['Authorization'] = f"Bearer {auth_token}"
        elif 'Authorization' in flask.request.headers:
            headers['Authorization'] = flask.request.headers.get('Authorization')
        
        # Check if the request is authenticated
        auth_result = auth_service.check_auth(path, headers)
        if auth_result.get('authenticated', False):
            return func(*args, **kwargs)
        
        # Authentication failed
        if 'application/json' in flask.request.headers.get('Accept', ''):
            return flask.jsonify({"error": "Unauthorized", "redirect": LOGIN_ROUTE}), 401
        
        # Redirect to login page
        return flask.redirect(LOGIN_ROUTE)
    
    return wrapper


def create_login_form(error_message=None):
    """
    Creates a login form component for the dashboard
    """
    return html.Div([
        html.H2("Authentication Required", className="mb-4 text-center"),
        html.P("Please log in to access this feature.", className="mb-4 text-center"),
        html.Div([
            html.Label("Username", className="form-label"),
            dcc.Input(
                id="username-input",
                type="text",
                placeholder="Enter your username",
                className="form-control mb-3",
                style={"width": "100%", "padding": "8px"}
            ),
        ]),
        html.Div([
            html.Label("Password", className="form-label"),
            dcc.Input(
                id="password-input",
                type="password",
                placeholder="Enter your password",
                className="form-control mb-3",
                style={"width": "100%", "padding": "8px"}
            ),
        ]),
        dbc.Button(
            "Login", 
            id="login-button", 
            color="primary", 
            className="mb-3 w-100",
            n_clicks=0
        ),
        html.Div(
            error_message or "",
            id="login-error",
            style={"color": "red", "display": "block" if error_message else "none", "textAlign": "center"},
            className="mb-3"
        )
    ], className="login-form p-4 border rounded shadow", style={"maxWidth": "400px", "margin": "40px auto"})


def handle_login(username, password):
    """
    Handles login form submission
    """
    result = auth_service.login(username, password)
    
    if result["success"]:
        logger.info(f"Login successful for user: {username}")
        return {
            "success": True,
            "token": result["token"],
            "role": result.get("role", "viewer")
        }
    else:
        logger.warning(f"Login failed for user: {username}")
        return {
            "success": False,
            "error": result.get("error", "Invalid credentials")
        }


def handle_logout(token):
    """
    Handles user logout
    """
    result = auth_service.logout(token)
    logger.info("User logged out successfully")
    return {"success": True}


class AuthMiddleware:
    """
    Middleware class that implements authentication for the Dash application
    """
    
    def __init__(self, app):
        """
        Initializes the authentication middleware
        """
        self.app = app
        self.logger = get_logger('auth_middleware')
        self.auth_service = auth_service
        self.auth_enabled = ENABLE_AUTH
        self.logger.info(f"Initializing authentication middleware. Auth enabled: {self.auth_enabled}")
    
    def apply(self):
        """
        Applies authentication middleware to the Dash application
        """
        # Register authentication routes
        self.register_routes()
        
        # Add a before_request handler to check authentication
        self.app.server.before_request(self.check_auth)
        
        # Patch the app's callback decorator to include authentication checks
        self.patch_callback()
        
        self.logger.info("Authentication middleware applied to Dash application")
        return None
    
    def check_auth(self):
        """
        Checks if the current request is authenticated
        """
        # Skip authentication if disabled
        if not self.auth_enabled:
            return None
        
        # Skip auth check in debug mode if configured to do so
        if DEBUG and not self.auth_enabled:
            return None
        
        path = flask.request.path
        
        # Skip auth check for authentication-related routes
        if path == LOGIN_ROUTE or path == LOGOUT_ROUTE:
            return None
        
        # Skip auth check for non-protected routes
        if not is_protected_route(path):
            return None
        
        # Get authentication token
        auth_token = flask.request.cookies.get(AUTH_COOKIE_NAME)
        
        # Create headers for auth check
        headers = {}
        if auth_token:
            headers['Authorization'] = f"Bearer {auth_token}"
        elif 'Authorization' in flask.request.headers:
            headers['Authorization'] = flask.request.headers.get('Authorization')
        
        # Check authentication
        auth_result = self.auth_service.check_auth(path, headers)
        if auth_result.get('authenticated', False):
            self.logger.debug(f"Authentication successful for path: {path}")
            return None
        
        # Authentication failed
        self.logger.warning(f"Authentication failed for protected path: {path}")
        if 'application/json' in flask.request.headers.get('Accept', ''):
            return flask.jsonify({"error": "Unauthorized", "redirect": LOGIN_ROUTE}), 401
        
        # Redirect to login page
        return flask.redirect(LOGIN_ROUTE)
    
    def register_routes(self):
        """
        Registers authentication-related routes with the application
        """
        # Register the login route
        self.app.server.route(LOGIN_ROUTE, methods=['GET', 'POST'])(self.handle_login_request)
        
        # Register the logout route
        self.app.server.route(LOGOUT_ROUTE, methods=['GET', 'POST'])(self.handle_logout_request)
        
        self.logger.info(f"Registered authentication routes: {LOGIN_ROUTE}, {LOGOUT_ROUTE}")
        return None
    
    def handle_login_request(self):
        """
        Handles login requests
        """
        if flask.request.method == 'POST':
            username = flask.request.form.get('username')
            password = flask.request.form.get('password')
            
            if not username or not password:
                self.logger.warning("Login attempt with missing username or password")
                return flask.redirect(f"{LOGIN_ROUTE}?error=Username and password are required")
            
            result = handle_login(username, password)
            
            if result["success"]:
                response = flask.redirect('/')
                response.set_cookie(
                    AUTH_COOKIE_NAME,
                    result["token"],
                    max_age=3600,  # 1 hour
                    httponly=True,
                    secure=not DEBUG,  # Secure in production
                    samesite='Lax'
                )
                self.logger.info("Login successful, redirecting to dashboard")
                return response
            else:
                error_message = result.get("error", "Login failed")
                self.logger.warning(f"Login failed: {error_message}")
                return flask.redirect(f"{LOGIN_ROUTE}?error={error_message}")
        
        # Handle GET request - login form will be displayed by the Dash layout
        self.logger.debug("GET request to login route, redirecting to dashboard")
        return flask.redirect('/')
    
    def handle_logout_request(self):
        """
        Handles logout requests
        """
        auth_token = flask.request.cookies.get(AUTH_COOKIE_NAME)
        
        if auth_token:
            handle_logout(auth_token)
        
        response = flask.redirect(LOGIN_ROUTE)
        response.delete_cookie(AUTH_COOKIE_NAME)
        
        self.logger.info("User logged out successfully")
        return response
    
    def patch_callback(self):
        """
        Patches the app's callback decorator to include authentication checks
        """
        original_callback = self.app.callback
        
        # Create a new callback decorator that includes auth checks
        def auth_callback(*args, **kwargs):
            def decorator(callback_fn):
                # Apply the original callback
                wrapped = original_callback(*args, **kwargs)(callback_fn)
                return wrapped
            
            return decorator
        
        # Replace the app's callback method
        self.app.callback = auth_callback
        
        self.logger.info("Patched app callback decorator")
        return None