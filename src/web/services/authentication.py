"""
Authentication service for the Electricity Market Price Forecasting System's web visualization interface.
Implements basic authentication for administrative access while allowing anonymous access to
visualization features. Provides user authentication, token management, and route protection functionality.
"""

import os
import time
import base64
import hashlib
import hmac
import uuid
from typing import Dict, Tuple, Optional, Any

from ..config.settings import ENABLE_AUTH, SECRET_KEY, DEBUG
from ..config.logging_config import get_logger
from ..utils.error_handlers import format_exception

# Initialize logger
logger = get_logger('authentication')

# Default admin credentials (overridden by environment variables)
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'admin')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'password')

# Token expiry in seconds (default: 1 hour)
TOKEN_EXPIRY_SECONDS = int(os.getenv('TOKEN_EXPIRY_SECONDS', 3600))

# Routes that require authentication
PROTECTED_ROUTES = ['/admin', '/settings', '/maintenance', '/api/admin']

# Authentication header name and prefix
AUTH_HEADER_NAME = 'Authorization'
AUTH_TOKEN_PREFIX = 'Bearer '


def validate_credentials(username: str, password: str) -> bool:
    """
    Validates username and password against configured credentials.
    
    Args:
        username: The username to validate
        password: The password to validate
        
    Returns:
        True if credentials are valid, False otherwise
    """
    is_valid = username == ADMIN_USERNAME and password == ADMIN_PASSWORD
    logger.info(f"Authentication attempt for user '{username}': {'success' if is_valid else 'failed'}")
    return is_valid


def encode_credentials(username: str, password: str) -> str:
    """
    Encodes username and password for Basic Authentication.
    
    Args:
        username: The username to encode
        password: The password to encode
        
    Returns:
        Base64 encoded credentials string
    """
    credentials = f"{username}:{password}"
    encoded_bytes = base64.b64encode(credentials.encode('utf-8'))
    return encoded_bytes.decode('utf-8')


def decode_credentials(encoded_credentials: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Decodes Base64 encoded credentials from Basic Authentication.
    
    Args:
        encoded_credentials: The encoded credentials string
        
    Returns:
        Tuple of (username, password) or (None, None) if invalid
    """
    # Remove 'Basic ' prefix if present
    if encoded_credentials.startswith('Basic '):
        encoded_credentials = encoded_credentials[6:]
    
    try:
        decoded_bytes = base64.b64decode(encoded_credentials)
        decoded_str = decoded_bytes.decode('utf-8')
        username, password = decoded_str.split(':', 1)
        return username, password
    except Exception as e:
        logger.warning(f"Failed to decode credentials: {format_exception(e)}")
        return None, None


def is_protected_route(path: str) -> bool:
    """
    Determines if a route requires authentication.
    
    Args:
        path: The route path to check
        
    Returns:
        True if route requires authentication, False otherwise
    """
    return any(path.startswith(route) for route in PROTECTED_ROUTES)


def get_auth_header(token: str) -> Dict[str, str]:
    """
    Creates an authentication header with the given token.
    
    Args:
        token: The authentication token
        
    Returns:
        Dictionary with Authorization header
    """
    return {AUTH_HEADER_NAME: f"{AUTH_TOKEN_PREFIX}{token}"}


class AuthenticationService:
    """
    Service for handling user authentication and token management.
    """
    
    def __init__(self):
        """
        Initializes the authentication service.
        """
        self.auth_enabled = ENABLE_AUTH
        self.active_tokens: Dict[str, float] = {}  # token -> expiry_time
        self.logger = get_logger('authentication')
        self.logger.info(f"Initialized authentication service. Auth enabled: {self.auth_enabled}")
    
    def login(self, username: str, password: str) -> Dict[str, Any]:
        """
        Authenticates a user and issues a token.
        
        Args:
            username: The username to authenticate
            password: The password to authenticate
            
        Returns:
            Response with success status and token or error message
        """
        if not self.auth_enabled:
            self.logger.info("Authentication disabled, granting automatic access")
            return {
                "success": True,
                "token": "dummy-token-auth-disabled",
                "role": "admin"
            }
        
        if validate_credentials(username, password):
            token = self.generate_token(username)
            # Store token with expiry time
            expiry_time = time.time() + TOKEN_EXPIRY_SECONDS
            self.active_tokens[token] = expiry_time
            
            self.logger.info(f"Login successful for user '{username}', token issued")
            return {
                "success": True,
                "token": token,
                "role": self.get_user_role(username),
                "expires_in": TOKEN_EXPIRY_SECONDS
            }
        else:
            self.logger.warning(f"Login failed for user '{username}', invalid credentials")
            return {
                "success": False,
                "error": "Invalid credentials"
            }
    
    def logout(self, token: str) -> Dict[str, Any]:
        """
        Invalidates a user's authentication token.
        
        Args:
            token: The token to invalidate
            
        Returns:
            Response with logout status
        """
        if not self.auth_enabled:
            return {"success": True}
        
        if token in self.active_tokens:
            del self.active_tokens[token]
            self.logger.info(f"Logout successful, token invalidated")
        else:
            self.logger.info(f"Logout requested for unknown token (already expired or never existed)")
        
        return {"success": True}
    
    def check_auth(self, path: str, headers: Dict[str, str]) -> Dict[str, Any]:
        """
        Checks if a request is authenticated for a protected route.
        
        Args:
            path: The route path being accessed
            headers: Request headers containing authentication token
            
        Returns:
            Response with authentication status
        """
        if not self.auth_enabled:
            return {"authenticated": True, "role": "admin"}
        
        # Check if the path is a protected route
        if not is_protected_route(path):
            return {"authenticated": True, "role": "viewer"}
        
        # Extract token from Authorization header
        token = self.extract_token_from_headers(headers)
        
        if token and self.validate_token(token):
            self.logger.debug(f"Authentication successful for path: {path}")
            return {"authenticated": True, "role": "admin"}
        else:
            self.logger.warning(f"Authentication failed for protected path: {path}")
            return {"authenticated": False, "error": "Unauthorized access"}
    
    def generate_token(self, username: str) -> str:
        """
        Generates a secure authentication token.
        
        Args:
            username: The username to include in the token
            
        Returns:
            Secure authentication token
        """
        # Generate a random UUID
        random_id = str(uuid.uuid4())
        
        # Combine with username and current timestamp
        data = f"{random_id}:{username}:{time.time()}"
        
        # Create an HMAC signature using SECRET_KEY
        h = hmac.new(SECRET_KEY.encode('utf-8'), data.encode('utf-8'), hashlib.sha256)
        signature = h.hexdigest()
        
        # Encode the result with base64
        token_data = f"{data}:{signature}"
        token = base64.b64encode(token_data.encode('utf-8')).decode('utf-8')
        
        return token
    
    def validate_token(self, token: str) -> bool:
        """
        Validates an authentication token.
        
        Args:
            token: The token to validate
            
        Returns:
            True if token is valid, False otherwise
        """
        if token not in self.active_tokens:
            return False
        
        # Get token expiry time from active_tokens
        expiry_time = self.active_tokens[token]
        current_time = time.time()
        
        if current_time > expiry_time:
            # Token has expired, remove it
            del self.active_tokens[token]
            self.logger.debug(f"Token expired and removed")
            return False
        
        return True
    
    def get_user_role(self, username: str) -> str:
        """
        Gets the role of a user based on username.
        
        Args:
            username: The username to check
            
        Returns:
            User role (admin or viewer)
        """
        if username == ADMIN_USERNAME:
            return "admin"
        return "viewer"
    
    def maintenance(self) -> Dict[str, Any]:
        """
        Performs maintenance tasks like cleaning expired tokens.
        
        Args:
            None
            
        Returns:
            Maintenance operation results
        """
        current_time = time.time()
        expired_tokens = [token for token, expiry in self.active_tokens.items() if expiry < current_time]
        
        for token in expired_tokens:
            del self.active_tokens[token]
        
        cleaned_count = len(expired_tokens)
        self.logger.info(f"Maintenance completed: removed {cleaned_count} expired tokens")
        
        return {
            "success": True,
            "cleaned_tokens": cleaned_count,
            "active_tokens": len(self.active_tokens)
        }
    
    def extract_token_from_headers(self, headers: Dict[str, str]) -> Optional[str]:
        """
        Extracts authentication token from request headers.
        
        Args:
            headers: Request headers dictionary
            
        Returns:
            Extracted token or None if not found
        """
        if AUTH_HEADER_NAME not in headers:
            return None
            
        auth_header = headers[AUTH_HEADER_NAME]
        
        if not auth_header.startswith(AUTH_TOKEN_PREFIX):
            return None
            
        token = auth_header[len(AUTH_TOKEN_PREFIX):]
        return token