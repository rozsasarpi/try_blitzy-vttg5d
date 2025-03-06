import pytest
from unittest.mock import patch
import sys
import os
from datetime import datetime

from flask import Flask

# Internal imports
from src.backend.main import main, create_app, parse_arguments, run_forecast
from src.backend.pipeline.pipeline_executor import execute_with_default_config
from src.backend.scheduler.forecast_scheduler import run_forecast_now
from src.backend.api.routes import api_blueprint


def test_parse_arguments_forecast_command():
    """Test that the argument parser correctly handles the forecast command"""
    # Mock sys.argv with forecast command arguments
    with patch('sys.argv', ['main.py', 'run', '--target_date', '2023-12-31']):
        # Call parse_arguments()
        args = parse_arguments()

        # Assert that the returned namespace has the expected command and parameters
        assert args.command == 'run'
        assert args.target_date == '2023-12-31'
        # Verify date argument is correctly parsed


def test_parse_arguments_scheduler_command():
    """Test that the argument parser correctly handles the scheduler command"""
    # Mock sys.argv with scheduler command arguments
    with patch('sys.argv', ['main.py', 'schedule']):
        # Call parse_arguments()
        args = parse_arguments()

        # Assert that the returned namespace has the expected command
        assert args.command == 'schedule'
        # Verify no additional parameters are required for scheduler command


def test_parse_arguments_api_command():
    """Test that the argument parser correctly handles the api command"""
    # Mock sys.argv with api command arguments
    with patch('sys.argv', ['main.py', 'serve', '--port', '6000']):
        # Call parse_arguments()
        args = parse_arguments()

        # Assert that the returned namespace has the expected command
        assert args.command == 'serve'
        # Verify port parameter is correctly parsed
        assert args.port == 6000


def test_create_app():
    """Test that the create_app function returns a properly configured Flask application"""
    # Call create_app()
    app = create_app()

    # Assert that the returned object is a Flask application
    assert isinstance(app, Flask)
    # Verify that the api_blueprint is registered with the app
    assert 'api' in app.blueprints
    # Check that error handlers are configured
    assert app.error_handler_spec
    # Verify debug mode is set according to environment
    assert app.debug == ('True' == os.getenv('DEBUG', 'True'))


def test_run_forecast_with_scheduler():
    """Test that run_forecast correctly uses the scheduler when use_scheduler is True"""
    # Mock run_forecast_now to return a test result
    with patch('src.backend.main.run_forecast_now', return_value={'test': 'success'}) as mock_run_forecast_now:
        # Call run_forecast with use_scheduler=True
        with patch('sys.argv', ['main.py', 'run', '--target_date', '2023-12-31']) as mock_sys_argv:
            args = parse_arguments()
            result = run_forecast(args)

            # Assert that run_forecast_now was called with the correct parameters
            mock_run_forecast_now.assert_called_once()
            # Verify that the function returns the expected result
            assert result == 0


def test_run_forecast_without_scheduler():
    """Test that run_forecast correctly uses execute_with_default_config when use_scheduler is False"""
    # Mock execute_with_default_config to return a test result
    with patch('src.backend.main.execute_forecasting_pipeline', return_value={'test': 'success'}) as mock_execute:
        # Call run_forecast with use_scheduler=False
        with patch('sys.argv', ['main.py', 'run', '--target_date', '2023-12-31']) as mock_sys_argv:
            args = parse_arguments()
            result = run_forecast(args)

            # Assert that execute_with_default_config was called with the correct parameters
            mock_execute.assert_called_once()
            # Verify that the function returns the expected result
            assert result == 0


def test_main_forecast_command():
    """Test that the main function correctly handles the forecast command"""
    # Mock sys.argv with forecast command arguments
    with patch('sys.argv', ['main.py', 'run', '--target_date', '2023-12-31']):
        # Mock run_forecast to return a test result
        with patch('src.backend.main.run_forecast', return_value=0) as mock_run_forecast:
            # Call main()
            result = main()

            # Assert that run_forecast was called with the correct parameters
            mock_run_forecast.assert_called_once()
            # Verify that the function returns the expected exit code
            assert result == 0


def test_main_scheduler_command():
    """Test that the main function correctly handles the scheduler command"""
    # Mock sys.argv with scheduler command arguments
    with patch('sys.argv', ['main.py', 'schedule']):
        # Mock run_scheduler_service to prevent actual execution
        with patch('src.backend.main.start_scheduler_service', return_value=0) as mock_run_scheduler_service:
            # Call main()
            result = main()

            # Assert that run_scheduler_service was called
            mock_run_scheduler_service.assert_called_once()
            # Verify that the function returns the expected exit code
            assert result == 0


def test_main_api_command():
    """Test that the main function correctly handles the api command"""
    # Mock sys.argv with api command arguments
    with patch('sys.argv', ['main.py', 'serve', '--port', '6000']):
        # Mock run_api_server to prevent actual server start
        with patch('src.backend.main.start_api_server', return_value=0) as mock_run_api_server:
            # Call main()
            result = main()

            # Assert that run_api_server was called with the correct parameters
            mock_run_api_server.assert_called_once()
            # Verify that the function returns the expected exit code
            assert result == 0


def test_main_error_handling():
    """Test that the main function correctly handles exceptions"""
    # Mock sys.argv with valid command arguments
    with patch('sys.argv', ['main.py', 'run', '--target_date', '2023-12-31']):
        # Mock parse_arguments to raise an exception
        with patch('src.backend.main.parse_args', side_effect=Exception('Test exception')):
            # Call main()
            result = main()

            # Verify that the function returns a non-zero exit code
            assert result == 1