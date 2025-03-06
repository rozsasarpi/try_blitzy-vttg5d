"""
Unit tests for the data export callback functions in the Electricity Market Price Forecasting System's
Dash-based visualization interface. Tests the functionality of callbacks that handle exporting forecast
data in various formats (CSV, Excel, JSON) with customizable options.
"""

# External imports
import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import dash  # dash: 2.9.0+
from dash import no_update  # dash: 2.9.0+
from dash.dependencies import Input, Output, State  # dash: 2.9.0+
import datetime  # standard library

# Internal imports
from src.web.callbacks.data_export_callbacks import register_data_export_callbacks, handle_export_button_click, parse_percentile_value
from src.web.components.export_panel import EXPORT_FORMAT_DROPDOWN_ID, EXPORT_BUTTON_ID, EXPORT_DOWNLOAD_ID, PERCENTILE_LOWER_INPUT_ID, PERCENTILE_UPPER_INPUT_ID
from src.web.components.control_panel import PRODUCT_DROPDOWN_ID, DATE_RANGE_PICKER_ID
from src.web.data.data_exporter import export_forecast_by_date_range, EXPORT_FORMATS, DEFAULT_EXPORT_FORMAT
from src.web.config.product_config import DEFAULT_PRODUCT
from src.web.utils.date_helpers import parse_date, get_default_date_range
from src.web.tests.fixtures.callback_fixtures import mock_callback_context, mock_export_callback, create_mock_callback_inputs, create_mock_callback_states
from src.web.tests.fixtures.forecast_fixtures import sample_forecast_data
from src.web.tests.fixtures.component_fixtures import mock_dash_app


def test_register_data_export_callbacks(mock_dash_app):
    """Tests that data export callbacks are correctly registered with the Dash app"""
    # Create a mock Dash app using mock_dash_app
    app = mock_dash_app()
    
    # Call register_data_export_callbacks with the mock app
    register_data_export_callbacks(app)
    
    # Verify that the expected callbacks are registered in app.callback_map
    assert any(callback['outputs'] == Output(EXPORT_DOWNLOAD_ID, "data") for callback in app.callback_map)
    
    # Check that export button click callback is registered with correct inputs and outputs
    export_callback = next((callback for callback in app.callback_map if callback['outputs'] == Output(EXPORT_DOWNLOAD_ID, "data")), None)
    assert export_callback is not None
    assert all(input in export_callback['inputs'] for input in [Input(EXPORT_BUTTON_ID, "n_clicks")])


def test_handle_export_button_click_valid_inputs(mock_callback_context, mock_export_callback):
    """Tests the export button click callback with valid inputs"""
    # Set up mock callback context with EXPORT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=EXPORT_BUTTON_ID)
    
    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({'n_clicks': 1})
    
    # Create mock states with export_format='csv', selected_product='DALMP', date_range=['2023-06-01', '2023-06-03'], percentile_lower='10', percentile_upper='90'
    states = create_mock_callback_states({
        EXPORT_FORMAT_DROPDOWN_ID: 'csv',
        PRODUCT_DROPDOWN_ID: 'DALMP',
        DATE_RANGE_PICKER_ID: ['2023-06-01', '2023-06-03'],
        PERCENTILE_LOWER_INPUT_ID: '10',
        PERCENTILE_UPPER_INPUT_ID: '90'
    })
    
    # Mock the export_forecast_by_date_range function to return a sample export data dictionary
    with unittest.mock.patch('src.web.callbacks.data_export_callbacks.export_forecast_by_date_range', return_value={'content': 'test_content', 'filename': 'test_filename.csv', 'mime_type': 'text/csv'}) as mock_export:
        # Call handle_export_button_click with the mock inputs and states
        result = handle_export_button_click(inputs['n_clicks'], states[EXPORT_FORMAT_DROPDOWN_ID], states[PRODUCT_DROPDOWN_ID], states[DATE_RANGE_PICKER_ID][0], states[DATE_RANGE_PICKER_ID][1], states[PERCENTILE_LOWER_INPUT_ID], states[PERCENTILE_UPPER_INPUT_ID])
        
        # Verify that export_forecast_by_date_range was called with correct parameters
        mock_export.assert_called_once()
        args, kwargs = mock_export.call_args
        assert kwargs['product'] == 'DALMP'
        assert kwargs['start_date'] == parse_date('2023-06-01')
        assert kwargs['end_date'] == parse_date('2023-06-03')
        assert kwargs['export_format'] == 'csv'
        assert kwargs['percentiles'] == [10, 90]
        
        # Check that the returned download data matches the expected format
        assert result['content'] == 'test_content'
        
        # Verify that the content, filename, and mime type are correctly set in the download data
        assert result['filename'] == 'test_filename.csv'
        assert result['mime_type'] == 'text/csv'


def test_handle_export_button_click_no_click(mock_callback_context):
    """Tests the export button click callback when button is not clicked"""
    # Set up mock callback context with EXPORT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=EXPORT_BUTTON_ID)
    
    # Create mock inputs with n_clicks=None
    inputs = create_mock_callback_inputs({'n_clicks': None})
    
    # Create mock states with default values
    states = create_mock_callback_states({
        EXPORT_FORMAT_DROPDOWN_ID: DEFAULT_EXPORT_FORMAT,
        PRODUCT_DROPDOWN_ID: DEFAULT_PRODUCT,
        DATE_RANGE_PICKER_ID: get_default_date_range(),
        PERCENTILE_LOWER_INPUT_ID: '10',
        PERCENTILE_UPPER_INPUT_ID: '90'
    })
    
    # Call handle_export_button_click with the mock inputs and states
    result = handle_export_button_click(inputs['n_clicks'], states[EXPORT_FORMAT_DROPDOWN_ID], states[PRODUCT_DROPDOWN_ID], states[DATE_RANGE_PICKER_ID][0], states[DATE_RANGE_PICKER_ID][1], states[PERCENTILE_LOWER_INPUT_ID], states[PERCENTILE_UPPER_INPUT_ID])
    
    # Verify that no_update is returned
    assert result == no_update
    
    # Check that export_forecast_by_date_range was not called
    with unittest.mock.patch('src.web.callbacks.data_export_callbacks.export_forecast_by_date_range') as mock_export:
        handle_export_button_click(inputs['n_clicks'], states[EXPORT_FORMAT_DROPDOWN_ID], states[PRODUCT_DROPDOWN_ID], states[DATE_RANGE_PICKER_ID][0], states[DATE_RANGE_PICKER_ID][1], states[PERCENTILE_LOWER_INPUT_ID], states[PERCENTILE_UPPER_INPUT_ID])
        mock_export.assert_not_called()


def test_handle_export_button_click_default_product(mock_callback_context, mock_export_callback):
    """Tests the export button click callback when no product is selected"""
    # Set up mock callback context with EXPORT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=EXPORT_BUTTON_ID)
    
    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({'n_clicks': 1})
    
    # Create mock states with export_format='csv', selected_product=None, date_range=['2023-06-01', '2023-06-03'], percentile_lower='10', percentile_upper='90'
    states = create_mock_callback_states({
        EXPORT_FORMAT_DROPDOWN_ID: 'csv',
        PRODUCT_DROPDOWN_ID: None,
        DATE_RANGE_PICKER_ID: ['2023-06-01', '2023-06-03'],
        PERCENTILE_LOWER_INPUT_ID: '10',
        PERCENTILE_UPPER_INPUT_ID: '90'
    })
    
    # Mock the export_forecast_by_date_range function to return a sample export data dictionary
    with unittest.mock.patch('src.web.callbacks.data_export_callbacks.export_forecast_by_date_range', return_value={'content': 'test_content', 'filename': 'test_filename.csv', 'mime_type': 'text/csv'}) as mock_export:
        # Call handle_export_button_click with the mock inputs and states
        result = handle_export_button_click(inputs['n_clicks'], states[EXPORT_FORMAT_DROPDOWN_ID], states[PRODUCT_DROPDOWN_ID], states[DATE_RANGE_PICKER_ID][0], states[DATE_RANGE_PICKER_ID][1], states[PERCENTILE_LOWER_INPUT_ID], states[PERCENTILE_UPPER_INPUT_ID])
        
        # Verify that export_forecast_by_date_range was called with DEFAULT_PRODUCT
        mock_export.assert_called_once()
        args, kwargs = mock_export.call_args
        assert kwargs['product'] == DEFAULT_PRODUCT
        
        # Check that the returned download data matches the expected format
        assert result['content'] == 'test_content'


def test_handle_export_button_click_default_date_range(mock_callback_context, mock_export_callback):
    """Tests the export button click callback when no date range is selected"""
    # Set up mock callback context with EXPORT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=EXPORT_BUTTON_ID)
    
    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({'n_clicks': 1})
    
    # Create mock states with export_format='csv', selected_product='DALMP', date_range=[None, None], percentile_lower='10', percentile_upper='90'
    states = create_mock_callback_states({
        EXPORT_FORMAT_DROPDOWN_ID: 'csv',
        PRODUCT_DROPDOWN_ID: 'DALMP',
        DATE_RANGE_PICKER_ID: [None, None],
        PERCENTILE_LOWER_INPUT_ID: '10',
        PERCENTILE_UPPER_INPUT_ID: '90'
    })
    
    # Mock the export_forecast_by_date_range function to return a sample export data dictionary
    with unittest.mock.patch('src.web.callbacks.data_export_callbacks.export_forecast_by_date_range', return_value={'content': 'test_content', 'filename': 'test_filename.csv', 'mime_type': 'text/csv'}) as mock_export, \
            unittest.mock.patch('src.web.callbacks.data_export_callbacks.get_default_date_range', return_value=(parse_date('2023-06-01'), parse_date('2023-06-03'))) as mock_default_date_range:
        # Call handle_export_button_click with the mock inputs and states
        result = handle_export_button_click(inputs['n_clicks'], states[EXPORT_FORMAT_DROPDOWN_ID], states[PRODUCT_DROPDOWN_ID], states[DATE_RANGE_PICKER_ID][0], states[DATE_RANGE_PICKER_ID][1], states[PERCENTILE_LOWER_INPUT_ID], states[PERCENTILE_UPPER_INPUT_ID])
        
        # Verify that get_default_date_range was called
        mock_default_date_range.assert_called_once()
        
        # Verify that export_forecast_by_date_range was called with the default date range
        mock_export.assert_called_once()
        args, kwargs = mock_export.call_args
        assert kwargs['start_date'] == parse_date('2023-06-01')
        assert kwargs['end_date'] == parse_date('2023-06-03')
        
        # Check that the returned download data matches the expected format
        assert result['content'] == 'test_content'


@pytest.mark.parametrize('export_format', ['csv', 'excel', 'json'])
def test_handle_export_button_click_different_formats(export_format):
    """Tests the export button click callback with different export formats"""
    # Set up mock callback context with EXPORT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=EXPORT_BUTTON_ID)
    
    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({'n_clicks': 1})
    
    # Create mock states with the specified export_format, selected_product='DALMP', date_range=['2023-06-01', '2023-06-03'], percentile_lower='10', percentile_upper='90'
    states = create_mock_callback_states({
        EXPORT_FORMAT_DROPDOWN_ID: export_format,
        PRODUCT_DROPDOWN_ID: 'DALMP',
        DATE_RANGE_PICKER_ID: ['2023-06-01', '2023-06-03'],
        PERCENTILE_LOWER_INPUT_ID: '10',
        PERCENTILE_UPPER_INPUT_ID: '90'
    })
    
    # Mock the export_forecast_by_date_range function to return a sample export data dictionary
    with unittest.mock.patch('src.web.callbacks.data_export_callbacks.export_forecast_by_date_range', return_value={'content': 'test_content', 'filename': f'test_filename.{export_format}', 'mime_type': EXPORT_FORMATS[export_format]['mime']}) as mock_export:
        # Call handle_export_button_click with the mock inputs and states
        result = handle_export_button_click(inputs['n_clicks'], states[EXPORT_FORMAT_DROPDOWN_ID], states[PRODUCT_DROPDOWN_ID], states[DATE_RANGE_PICKER_ID][0], states[DATE_RANGE_PICKER_ID][1], states[PERCENTILE_LOWER_INPUT_ID], states[PERCENTILE_UPPER_INPUT_ID])
        
        # Verify that export_forecast_by_date_range was called with the correct format
        mock_export.assert_called_once()
        args, kwargs = mock_export.call_args
        assert kwargs['export_format'] == export_format
        
        # Check that the returned download data has the correct mime type for the format
        assert result['mime_type'] == EXPORT_FORMATS[export_format]['mime']
        
        # Verify that the filename has the correct extension for the format
        assert result['filename'] == f'test_filename.{export_format}'


@pytest.mark.parametrize('lower,upper,expected_lower,expected_upper', [
    ('10', '90', 10, 90),
    ('0', '100', 0, 100),
    ('-10', '110', 0, 100),
    ('invalid', 'invalid', 10, 90)
])
def test_handle_export_button_click_percentile_validation(lower, upper, expected_lower, expected_upper):
    """Tests the export button click callback with various percentile inputs"""
    # Set up mock callback context with EXPORT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=EXPORT_BUTTON_ID)
    
    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({'n_clicks': 1})
    
    # Create mock states with export_format='csv', selected_product='DALMP', date_range=['2023-06-01', '2023-06-03'], percentile_lower=lower, percentile_upper=upper
    states = create_mock_callback_states({
        EXPORT_FORMAT_DROPDOWN_ID: 'csv',
        PRODUCT_DROPDOWN_ID: 'DALMP',
        DATE_RANGE_PICKER_ID: ['2023-06-01', '2023-06-03'],
        PERCENTILE_LOWER_INPUT_ID: lower,
        PERCENTILE_UPPER_INPUT_ID: upper
    })
    
    # Mock the export_forecast_by_date_range function to return a sample export data dictionary
    with unittest.mock.patch('src.web.callbacks.data_export_callbacks.export_forecast_by_date_range', return_value={'content': 'test_content', 'filename': 'test_filename.csv', 'mime_type': 'text/csv'}) as mock_export:
        # Call handle_export_button_click with the mock inputs and states
        handle_export_button_click(inputs['n_clicks'], states[EXPORT_FORMAT_DROPDOWN_ID], states[PRODUCT_DROPDOWN_ID], states[DATE_RANGE_PICKER_ID][0], states[DATE_RANGE_PICKER_ID][1], states[PERCENTILE_LOWER_INPUT_ID], states[PERCENTILE_UPPER_INPUT_ID])
        
        # Verify that export_forecast_by_date_range was called with the expected_lower and expected_upper percentiles
        mock_export.assert_called_once()
        args, kwargs = mock_export.call_args
        assert kwargs['percentiles'] == [expected_lower, expected_upper]
        
        # Check that the returned download data matches the expected format
        # (This part is skipped as the focus is on percentile validation)


def test_handle_export_button_click_error_handling(mock_callback_context):
    """Tests the error handling in the export button click callback"""
    # Set up mock callback context with EXPORT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=EXPORT_BUTTON_ID)
    
    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({'n_clicks': 1})
    
    # Create mock states with valid export parameters
    states = create_mock_callback_states({
        EXPORT_FORMAT_DROPDOWN_ID: 'csv',
        PRODUCT_DROPDOWN_ID: 'DALMP',
        DATE_RANGE_PICKER_ID: ['2023-06-01', '2023-06-03'],
        PERCENTILE_LOWER_INPUT_ID: '10',
        PERCENTILE_UPPER_INPUT_ID: '90'
    })
    
    # Mock the export_forecast_by_date_range function to raise an exception
    with unittest.mock.patch('src.web.callbacks.data_export_callbacks.export_forecast_by_date_range', side_effect=Exception('Test exception')) as mock_export:
        # Call handle_export_button_click with the mock inputs and states
        result = handle_export_button_click(inputs['n_clicks'], states[EXPORT_FORMAT_DROPDOWN_ID], states[PRODUCT_DROPDOWN_ID], states[DATE_RANGE_PICKER_ID][0], states[DATE_RANGE_PICKER_ID][1], states[PERCENTILE_LOWER_INPUT_ID], states[PERCENTILE_UPPER_INPUT_ID])
        
        # Verify that the function handles the exception gracefully
        assert result['filename'] == 'error.txt'
        assert result['mime_type'] == 'text/plain'
        assert 'Export failed' in result['content']
        
        # Check that an appropriate error message is returned instead of propagating the exception
        assert 'Test exception' in result['content']


@pytest.mark.parametrize('value,default,expected', [
    ('10', 5, 10),
    ('0', 5, 0),
    ('100', 5, 100)
])
def test_parse_percentile_value_valid_inputs(value, default, expected):
    """Tests the parse_percentile_value function with valid inputs"""
    # Call parse_percentile_value with the specified value and default
    result = parse_percentile_value(value, default)
    
    # Verify that the returned value matches the expected result
    assert result == expected


@pytest.mark.parametrize('value,default,expected', [
    ('invalid', 5, 5),
    ('-10', 5, 0),
    ('110', 5, 100)
])
def test_parse_percentile_value_invalid_inputs(value, default, expected):
    """Tests the parse_percentile_value function with invalid inputs"""
    # Call parse_percentile_value with the specified value and default
    result = parse_percentile_value(value, default)
    
    # Verify that the returned value matches the expected result
    assert result == expected
    
    # For invalid string, check that default is returned
    if value == 'invalid':
        assert result == default
    
    # For out-of-range values, check that they are clamped to valid range (0-100)
    if value == '-10':
        assert result == 0
    if value == '110':
        assert result == 100