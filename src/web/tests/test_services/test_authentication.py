"""
Unit tests for the authentication service of the Electricity Market Price Forecasting System's web 
visualization interface. Tests the functionality of user authentication, token management, route 
protection, and credential validation.
"""

import pytest
import base64
import time
from unittest.mock import patch, MagicMock, Mock

from ...services.authentication import (
    AuthenticationService, 
    validate_credentials, 
    encode_credentials, 
    decode_credentials,
    is_protected_route,
    get_auth_header,
    ADMIN_USERNAME,
    ADMIN_PASSWORD,
    PROTECTED_ROUTES
)
from ...config.settings import ENABLE_AUTH

# Test constants
TEST_USERNAME = 'test_admin'
TEST_PASSWORD = 'test_password'
TEST_PROTECTED_ROUTE = '/admin/dashboard'
TEST_UNPROTECTED_ROUTE = '/forecast'


@pytest.mark.parametrize('username,password,expected', [
    (ADMIN_USERNAME, ADMIN_PASSWORD, True),
    ('wrong_user', ADMIN_PASSWORD, False),
    (ADMIN_USERNAME, 'wrong_pass', False)
])
def test_validate_credentials_valid(username, password, expected):
    """Tests that validate_credentials returns True for valid credentials"""
    # Act
    result = validate_credentials(username, password)
    
    # Assert
    assert result == expected


def test_encode_decode_credentials():
    """Tests that credentials can be encoded and then decoded correctly"""
    # Arrange
    username = TEST_USERNAME
    password = TEST_PASSWORD
    
    # Act
    encoded = encode_credentials(username, password)
    decoded_username, decoded_password = decode_credentials(encoded)
    
    # Assert
    assert decoded_username == username
    assert decoded_password == password


@pytest.mark.parametrize('encoded_str', ['invalid_base64', 'Basic invalid_base64', ''])
def test_decode_credentials_invalid(encoded_str):
    """Tests that decode_credentials handles invalid encoded strings"""
    # Act
    username, password = decode_credentials(encoded_str)
    
    # Assert
    assert username is None
    assert password is None


@pytest.mark.parametrize('route,expected', [
    ('/admin', True),
    ('/admin/users', True),
    ('/settings', True),
    ('/forecast', False),
    ('/', False)
])
def test_is_protected_route(route, expected):
    """Tests that is_protected_route correctly identifies protected routes"""
    # Act
    result = is_protected_route(route)
    
    # Assert
    assert result == expected


def test_get_auth_header():
    """Tests that get_auth_header creates the correct authorization header"""
    # Arrange
    token = "test_token_123"
    
    # Act
    headers = get_auth_header(token)
    
    # Assert
    assert "Authorization" in headers
    assert headers["Authorization"] == f"Bearer {token}"


def test_authentication_service_init():
    """Tests that AuthenticationService initializes correctly"""
    # Act
    auth_service = AuthenticationService()
    
    # Assert
    assert auth_service.auth_enabled == ENABLE_AUTH
    assert auth_service.active_tokens == {}
    assert auth_service.logger is not None


def test_login_auth_disabled():
    """Tests login behavior when authentication is disabled"""
    # Arrange
    with patch('src.web.services.authentication.ENABLE_AUTH', False):
        auth_service = AuthenticationService()
        auth_service.auth_enabled = False
        
        # Act
        result = auth_service.login(TEST_USERNAME, TEST_PASSWORD)
        
        # Assert
        assert result["success"] is True
        assert result["token"] == "dummy-token-auth-disabled"
        assert len(auth_service.active_tokens) == 0


def test_login_valid_credentials():
    """Tests login with valid credentials"""
    # Arrange
    auth_service = AuthenticationService()
    auth_service.auth_enabled = True
    
    with patch('src.web.services.authentication.validate_credentials', return_value=True):
        # Act
        result = auth_service.login(TEST_USERNAME, TEST_PASSWORD)
        
        # Assert
        assert result["success"] is True
        assert "token" in result
        assert result["token"] in auth_service.active_tokens


def test_login_invalid_credentials():
    """Tests login with invalid credentials"""
    # Arrange
    auth_service = AuthenticationService()
    auth_service.auth_enabled = True
    
    with patch('src.web.services.authentication.validate_credentials', return_value=False):
        # Act
        result = auth_service.login(TEST_USERNAME, TEST_PASSWORD)
        
        # Assert
        assert result["success"] is False
        assert "error" in result
        assert len(auth_service.active_tokens) == 0


def test_logout_auth_disabled():
    """Tests logout behavior when authentication is disabled"""
    # Arrange
    auth_service = AuthenticationService()
    auth_service.auth_enabled = False
    
    # Act
    result = auth_service.logout("test_token")
    
    # Assert
    assert result["success"] is True
    assert len(auth_service.active_tokens) == 0


def test_logout_valid_token():
    """Tests logout with a valid token"""
    # Arrange
    auth_service = AuthenticationService()
    auth_service.auth_enabled = True
    test_token = "test_token_123"
    auth_service.active_tokens[test_token] = time.time() + 3600
    
    # Act
    result = auth_service.logout(test_token)
    
    # Assert
    assert result["success"] is True
    assert test_token not in auth_service.active_tokens


def test_logout_invalid_token():
    """Tests logout with an invalid token"""
    # Arrange
    auth_service = AuthenticationService()
    auth_service.auth_enabled = True
    test_token = "test_token_123"
    # Do not add the token to active_tokens
    
    # Act
    result = auth_service.logout(test_token)
    
    # Assert
    assert result["success"] is True  # Logout is idempotent
    assert len(auth_service.active_tokens) == 0


def test_check_auth_disabled():
    """Tests check_auth behavior when authentication is disabled"""
    # Arrange
    auth_service = AuthenticationService()
    auth_service.auth_enabled = False
    
    # Act
    result = auth_service.check_auth(TEST_PROTECTED_ROUTE, {})
    
    # Assert
    assert result["authenticated"] is True
    assert result["role"] == "admin"


def test_check_auth_unprotected_route():
    """Tests check_auth with an unprotected route"""
    # Arrange
    auth_service = AuthenticationService()
    auth_service.auth_enabled = True
    
    with patch('src.web.services.authentication.is_protected_route', return_value=False):
        # Act
        result = auth_service.check_auth(TEST_UNPROTECTED_ROUTE, {})
        
        # Assert
        assert result["authenticated"] is True
        assert result["role"] == "viewer"


def test_check_auth_protected_route_no_token():
    """Tests check_auth with a protected route but no token"""
    # Arrange
    auth_service = AuthenticationService()
    auth_service.auth_enabled = True
    
    with patch('src.web.services.authentication.is_protected_route', return_value=True):
        with patch.object(auth_service, 'extract_token_from_headers', return_value=None):
            # Act
            result = auth_service.check_auth(TEST_PROTECTED_ROUTE, {})
            
            # Assert
            assert result["authenticated"] is False
            assert "error" in result


def test_check_auth_protected_route_invalid_token():
    """Tests check_auth with a protected route and invalid token"""
    # Arrange
    auth_service = AuthenticationService()
    auth_service.auth_enabled = True
    test_token = "test_token_123"
    
    with patch('src.web.services.authentication.is_protected_route', return_value=True):
        with patch.object(auth_service, 'extract_token_from_headers', return_value=test_token):
            with patch.object(auth_service, 'validate_token', return_value=False):
                # Act
                result = auth_service.check_auth(TEST_PROTECTED_ROUTE, {"Authorization": f"Bearer {test_token}"})
                
                # Assert
                assert result["authenticated"] is False
                assert "error" in result


def test_check_auth_protected_route_valid_token():
    """Tests check_auth with a protected route and valid token"""
    # Arrange
    auth_service = AuthenticationService()
    auth_service.auth_enabled = True
    test_token = "test_token_123"
    
    with patch('src.web.services.authentication.is_protected_route', return_value=True):
        with patch.object(auth_service, 'extract_token_from_headers', return_value=test_token):
            with patch.object(auth_service, 'validate_token', return_value=True):
                # Act
                result = auth_service.check_auth(TEST_PROTECTED_ROUTE, {"Authorization": f"Bearer {test_token}"})
                
                # Assert
                assert result["authenticated"] is True
                assert result["role"] == "admin"


def test_generate_token():
    """Tests that generate_token creates unique tokens"""
    # Arrange
    auth_service = AuthenticationService()
    
    # Act
    token1 = auth_service.generate_token(TEST_USERNAME)
    token2 = auth_service.generate_token(TEST_USERNAME)
    
    # Assert
    assert token1 != ""
    assert token2 != ""
    assert token1 != token2  # Tokens should be unique even for the same username


def test_validate_token_nonexistent():
    """Tests validate_token with a token that doesn't exist"""
    # Arrange
    auth_service = AuthenticationService()
    test_token = "nonexistent_token"
    
    # Act
    result = auth_service.validate_token(test_token)
    
    # Assert
    assert result is False


def test_validate_token_expired():
    """Tests validate_token with an expired token"""
    # Arrange
    auth_service = AuthenticationService()
    test_token = "expired_token"
    auth_service.active_tokens[test_token] = time.time() - 3600  # Expired 1 hour ago
    
    # Act
    result = auth_service.validate_token(test_token)
    
    # Assert
    assert result is False
    assert test_token not in auth_service.active_tokens  # Token should be removed


def test_validate_token_valid():
    """Tests validate_token with a valid token"""
    # Arrange
    auth_service = AuthenticationService()
    test_token = "valid_token"
    auth_service.active_tokens[test_token] = time.time() + 3600  # Expires in 1 hour
    
    # Act
    result = auth_service.validate_token(test_token)
    
    # Assert
    assert result is True
    assert test_token in auth_service.active_tokens


@pytest.mark.parametrize('username,expected_role', [
    (ADMIN_USERNAME, 'admin'),
    ('regular_user', 'viewer')
])
def test_get_user_role(username, expected_role):
    """Tests that get_user_role returns the correct role"""
    # Arrange
    auth_service = AuthenticationService()
    
    # Act
    role = auth_service.get_user_role(username)
    
    # Assert
    assert role == expected_role


def test_maintenance_expired_tokens():
    """Tests that maintenance removes expired tokens"""
    # Arrange
    auth_service = AuthenticationService()
    current_time = time.time()
    
    # Add some expired tokens
    auth_service.active_tokens["expired_token1"] = current_time - 3600  # Expired 1 hour ago
    auth_service.active_tokens["expired_token2"] = current_time - 1800  # Expired 30 minutes ago
    
    # Add some valid tokens
    auth_service.active_tokens["valid_token1"] = current_time + 3600  # Expires in 1 hour
    auth_service.active_tokens["valid_token2"] = current_time + 7200  # Expires in 2 hours
    
    # Act
    result = auth_service.maintenance()
    
    # Assert
    assert result["success"] is True
    assert result["cleaned_tokens"] == 2
    assert result["active_tokens"] == 2
    assert "expired_token1" not in auth_service.active_tokens
    assert "expired_token2" not in auth_service.active_tokens
    assert "valid_token1" in auth_service.active_tokens
    assert "valid_token2" in auth_service.active_tokens


def test_extract_token_from_headers_missing():
    """Tests extract_token_from_headers with missing Authorization header"""
    # Arrange
    auth_service = AuthenticationService()
    headers = {}
    
    # Act
    token = auth_service.extract_token_from_headers(headers)
    
    # Assert
    assert token is None


@pytest.mark.parametrize('header_value', ['Token abc123', 'abc123', 'Basic abc123'])
def test_extract_token_from_headers_invalid_format(header_value):
    """Tests extract_token_from_headers with invalid header format"""
    # Arrange
    auth_service = AuthenticationService()
    headers = {"Authorization": header_value}
    
    # Act
    token = auth_service.extract_token_from_headers(headers)
    
    # Assert
    assert token is None


def test_extract_token_from_headers_valid():
    """Tests extract_token_from_headers with valid header format"""
    # Arrange
    auth_service = AuthenticationService()
    test_token = "valid_token_123"
    headers = {"Authorization": f"Bearer {test_token}"}
    
    # Act
    token = auth_service.extract_token_from_headers(headers)
    
    # Assert
    assert token == test_token