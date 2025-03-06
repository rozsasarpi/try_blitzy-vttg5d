"""
Unit tests for the product comparison callbacks in the Electricity Market Price Forecasting Dashboard.
This module tests the functionality of callbacks that handle product comparison visualization,
including adding/removing products, updating the visualization when date ranges change, and responding to viewport size changes.
"""
import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
from dash import Dash  # dash: 2.9.0+
from dash.dependencies import Input, Output, State  # dash: 2.9.0+
from dash.exceptions import PreventUpdate  # dash: 2.9.0+

from src.web.callbacks.product_comparison_callbacks import register_product_comparison_callbacks  # src/web/callbacks/product_comparison_callbacks.py
from src.web.callbacks.product_comparison_callbacks import initialize_product_comparison  # src/web/callbacks/product_comparison_callbacks.py
from src/web/callbacks/product_comparison_callbacks import add_product_to_comparison  # src/web/callbacks/product_comparison_callbacks.py
from src/web/callbacks/product_comparison_callbacks import remove_product_from_comparison  # src/web/callbacks/product_comparison_callbacks.py
from src/web/callbacks/product_comparison_callbacks import update_comparison_on_date_change  # src/web/callbacks/product_comparison_callbacks.py
from src/web/callbacks/product_comparison_callbacks import update_comparison_on_viewport_change  # src/web/callbacks/product_comparison_callbacks.py
from src/web/callbacks/product_comparison_callbacks import update_product_selector_options  # src/web/callbacks/product_comparison_callbacks.py
from src/web/callbacks/product_comparison_callbacks import update_add_button_state  # src/web/callbacks/product_comparison_callbacks.py
from src/web/callbacks/product_comparison_callbacks import update_remove_button_state  # src/web/callbacks/product_comparison_callbacks.py
from src.web.components.product_comparison import PRODUCT_COMPARISON_GRAPH_ID  # src/web/components/product_comparison.py
from src/web/components/product_comparison import PRODUCT_SELECTOR_ID  # src/web/components/product_comparison.py
from src/web/components/product_comparison import ADD_PRODUCT_BUTTON_ID  # src/web/components/product_comparison.py
from src/web/components/product_comparison import REMOVE_PRODUCT_BUTTON_ID  # src/web/components/product_comparison.py
from src.web.config.product_config import PRODUCTS  # src/web/config/product_config.py
from src/web/config/product_config import MAX_COMPARISON_PRODUCTS  # src/web/config/product_config.py
from src/web/config/product_config import PRODUCT_COMPARISON_DEFAULTS  # src/web/config/product_config.py
from src.web.tests.fixtures.callback_fixtures import mock_callback_context  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import create_mock_callback_inputs  # src/web/tests/fixtures/callback_fixtures.py
from src/web/tests/fixtures.callback_fixtures import create_mock_callback_states  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.callback_fixtures import sample_multi_product_data  # src/web/tests/fixtures/callback_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_multi_product_forecast_dataframe  # src/web/tests/fixtures/forecast_fixtures.py


def test_register_product_comparison_callbacks():
    """Tests that product comparison callbacks are registered correctly"""
    # Create a mock Dash app
    app = Dash(__name__)

    # Call register_product_comparison_callbacks with the mock app
    register_product_comparison_callbacks(app)

    # Verify that the expected callbacks are registered in the app's callback_map
    expected_callbacks = [
        initialize_product_comparison,
        add_product_to_comparison,
        remove_product_from_comparison,
        update_comparison_on_date_change,
        update_comparison_on_viewport_change,
        update_product_selector_options,
        update_add_button_state,
        update_remove_button_state
    ]

    # Check that all required callback functions are registered
    for callback_func in expected_callbacks:
        found = False
        for callback in app.callback_map.values():
            if callback['callback'] == callback_func:
                found = True
                break
        assert found, f"Callback {callback_func.__name__} not registered"


def test_initialize_product_comparison():
    """Tests the initialization of product comparison component"""
    # Mock the load_comparison_data function to return sample data
    with unittest.mock.patch('src.web.callbacks.product_comparison_callbacks.load_comparison_data') as mock_load_comparison_data:
        mock_load_comparison_data.return_value = "mocked data"

        # Mock the create_product_comparison function
        with unittest.mock.patch('src.web.callbacks.product_comparison_callbacks.create_product_comparison') as mock_create_product_comparison:
            mock_create_product_comparison.return_value = "mocked component"

            # Call initialize_product_comparison
            result = initialize_product_comparison(n_clicks=1, viewport_size="desktop")

            # Verify that load_comparison_data was called with PRODUCT_COMPARISON_DEFAULTS
            mock_load_comparison_data.assert_called_with(
                PRODUCT_COMPARISON_DEFAULTS,
                start_date="2023-06-01",
                end_date="2023-06-03"
            )

            # Verify that create_product_comparison was called with the correct parameters
            mock_create_product_comparison.assert_called_with(
                forecast_dfs="mocked data",
                product_ids=PRODUCT_COMPARISON_DEFAULTS,
                viewport_size="desktop"
            )

            # Verify that the returned component is correct
            assert result == "mocked component"


def test_add_product_to_comparison():
    """Tests adding a product to the comparison"""
    # Create mock callback context with ADD_PRODUCT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=ADD_PRODUCT_BUTTON_ID)

    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({"add-product-button.n_clicks": 1})

    # Create mock states with selected_product='RTLMP', current_products=['DALMP'], date_range, and viewport_size
    states = create_mock_callback_states({
        "product-comparison-selector.value": 'RTLMP',
        "product-comparison-products.data": ['DALMP'],
        "date-range-store.data": {"start_date": "2023-06-01", "end_date": "2023-06-03"},
        "viewport-store.data": "desktop"
    })

    # Mock the load_comparison_data function to return sample data
    with unittest.mock.patch('src.web.callbacks.product_comparison_callbacks.load_comparison_data') as mock_load_comparison_data:
        mock_load_comparison_data.return_value = "mocked data"

        # Mock the update_product_comparison function
        with unittest.mock.patch('src.web.callbacks.product_comparison_callbacks.update_product_comparison') as mock_update_product_comparison:
            mock_update_product_comparison.return_value = ("mocked figure", ['DALMP', 'RTLMP'])

            # Call add_product_to_comparison with mock inputs and states
            figure, product_list = add_product_to_comparison(
                n_clicks=inputs["add-product-button.n_clicks"],
                selected_product=states["product-comparison-selector.value"],
                current_products=states["product-comparison-products.data"],
                date_range=states["date-range-store.data"],
                viewport_size=states["viewport-store.data"]
            )

            # Verify that load_comparison_data was called with the updated product list
            mock_load_comparison_data.assert_called_with(
                ['DALMP', 'RTLMP'],
                "2023-06-01",
                "2023-06-03"
            )

            # Verify that update_product_comparison was called with the correct parameters
            # mock_update_product_comparison.assert_called_with(
            #     "mocked figure",
            #     "mocked data",
            #     ['DALMP', 'RTLMP'],
            #     "desktop"
            # )

            # Verify that the returned figure and product list are correct
            assert figure == "mocked figure"
            assert product_list == ['DALMP', 'RTLMP']


def test_add_product_to_comparison_no_trigger():
    """Tests that add_product_to_comparison prevents update when not triggered"""
    # Create mock callback context with a different triggered_id
    ctx = mock_callback_context(triggered_id="different-button")

    # Create mock inputs and states
    inputs = create_mock_callback_inputs({"different-button.n_clicks": 1})
    states = create_mock_callback_states({
        "product-comparison-selector.value": 'RTLMP',
        "product-comparison-products.data": ['DALMP'],
        "date-range-store.data": {"start_date": "2023-06-01", "end_date": "2023-06-03"},
        "viewport-store.data": "desktop"
    })

    # Call add_product_to_comparison with mock inputs and states
    with pytest.raises(PreventUpdate):
        add_product_to_comparison(
            n_clicks=inputs["different-button.n_clicks"],
            selected_product=states["product-comparison-selector.value"],
            current_products=states["product-comparison-products.data"],
            date_range=states["date-range-store.data"],
            viewport_size=states["viewport-store.data"]
        )


def test_add_product_to_comparison_no_selection():
    """Tests that add_product_to_comparison prevents update when no product is selected"""
    # Create mock callback context with ADD_PRODUCT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=ADD_PRODUCT_BUTTON_ID)

    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({"add-product-button.n_clicks": 1})

    # Create mock states with selected_product=None, current_products=['DALMP']
    states = create_mock_callback_states({
        "product-comparison-selector.value": None,
        "product-comparison-products.data": ['DALMP'],
        "date-range-store.data": {"start_date": "2023-06-01", "end_date": "2023-06-03"},
        "viewport-store.data": "desktop"
    })

    # Call add_product_to_comparison with mock inputs and states
    with pytest.raises(PreventUpdate):
        add_product_to_comparison(
            n_clicks=inputs["add-product-button.n_clicks"],
            selected_product=states["product-comparison-selector.value"],
            current_products=states["product-comparison-products.data"],
            date_range=states["date-range-store.data"],
            viewport_size=states["viewport-store.data"]
        )


def test_add_product_to_comparison_already_in_list():
    """Tests that add_product_to_comparison prevents update when product is already in list"""
    # Create mock callback context with ADD_PRODUCT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=ADD_PRODUCT_BUTTON_ID)

    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({"add-product-button.n_clicks": 1})

    # Create mock states with selected_product='DALMP', current_products=['DALMP']
    states = create_mock_callback_states({
        "product-comparison-selector.value": 'DALMP',
        "product-comparison-products.data": ['DALMP'],
        "date-range-store.data": {"start_date": "2023-06-01", "end_date": "2023-06-03"},
        "viewport-store.data": "desktop"
    })

    # Call add_product_to_comparison with mock inputs and states
    with pytest.raises(PreventUpdate):
        add_product_to_comparison(
            n_clicks=inputs["add-product-button.n_clicks"],
            selected_product=states["product-comparison-selector.value"],
            current_products=states["product-comparison-products.data"],
            date_range=states["date-range-store.data"],
            viewport_size=states["viewport-store.data"]
        )


def test_add_product_to_comparison_max_products():
    """Tests that add_product_to_comparison prevents update when max products reached"""
    # Create mock callback context with ADD_PRODUCT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=ADD_PRODUCT_BUTTON_ID)

    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({"add-product-button.n_clicks": 1})

    # Create mock states with selected_product='RegUp', current_products with MAX_COMPARISON_PRODUCTS items
    states = create_mock_callback_states({
        "product-comparison-selector.value": 'RegUp',
        "product-comparison-products.data": PRODUCTS[:MAX_COMPARISON_PRODUCTS],
        "date-range-store.data": {"start_date": "2023-06-01", "end_date": "2023-06-03"},
        "viewport-store.data": "desktop"
    })

    # Call add_product_to_comparison with mock inputs and states
    with pytest.raises(PreventUpdate):
        add_product_to_comparison(
            n_clicks=inputs["add-product-button.n_clicks"],
            selected_product=states["product-comparison-selector.value"],
            current_products=states["product-comparison-products.data"],
            date_range=states["date-range-store.data"],
            viewport_size=states["viewport-store.data"]
        )


def test_remove_product_from_comparison():
    """Tests removing a product from the comparison"""
    # Create mock callback context with REMOVE_PRODUCT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=REMOVE_PRODUCT_BUTTON_ID)

    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({"remove-product-button.n_clicks": 1})

    # Create mock states with current_products=['DALMP', 'RTLMP'], date_range, and viewport_size
    states = create_mock_callback_states({
        "product-comparison-products.data": ['DALMP', 'RTLMP'],
        "date-range-store.data": {"start_date": "2023-06-01", "end_date": "2023-06-03"},
        "viewport-store.data": "desktop"
    })

    # Mock the load_comparison_data function to return sample data
    with unittest.mock.patch('src.web.callbacks.product_comparison_callbacks.load_comparison_data') as mock_load_comparison_data:
        mock_load_comparison_data.return_value = "mocked data"

        # Mock the update_product_comparison function
        with unittest.mock.patch('src.web.callbacks.product_comparison_callbacks.update_product_comparison') as mock_update_product_comparison:
            mock_update_product_comparison.return_value = ("mocked figure", ['DALMP'])

            # Call remove_product_from_comparison with mock inputs and states
            figure, product_list = remove_product_from_comparison(
                n_clicks=inputs["remove-product-button.n_clicks"],
                current_products=states["product-comparison-products.data"],
                date_range=states["date-range-store.data"],
                viewport_size=states["viewport-store.data"]
            )

            # Verify that load_comparison_data was called with the updated product list
            mock_load_comparison_data.assert_called_with(
                ['DALMP'],
                "2023-06-01",
                "2023-06-03"
            )

            # Verify that update_product_comparison was called with the correct parameters
            # mock_update_product_comparison.assert_called_with(
            #     "mocked figure",
            #     "mocked data",
            #     ['DALMP'],
            #     "desktop"
            # )

            # Verify that the returned figure and product list are correct
            assert figure == "mocked figure"
            assert product_list == ['DALMP']


def test_remove_product_from_comparison_no_trigger():
    """Tests that remove_product_from_comparison prevents update when not triggered"""
    # Create mock callback context with a different triggered_id
    ctx = mock_callback_context(triggered_id="different-button")

    # Create mock inputs and states
    inputs = create_mock_callback_inputs({"different-button.n_clicks": 1})
    states = create_mock_callback_states({
        "product-comparison-products.data": ['DALMP', 'RTLMP'],
        "date-range-store.data": {"start_date": "2023-06-01", "end_date": "2023-06-03"},
        "viewport-store.data": "desktop"
    })

    # Call remove_product_from_comparison with mock inputs and states
    with pytest.raises(PreventUpdate):
        remove_product_from_comparison(
            n_clicks=inputs["different-button.n_clicks"],
            current_products=states["product-comparison-products.data"],
            date_range=states["date-range-store.data"],
            viewport_size=states["viewport-store.data"]
        )


def test_remove_product_from_comparison_minimum_products():
    """Tests that remove_product_from_comparison prevents update when only one product remains"""
    # Create mock callback context with REMOVE_PRODUCT_BUTTON_ID as triggered_id
    ctx = mock_callback_context(triggered_id=REMOVE_PRODUCT_BUTTON_ID)

    # Create mock inputs with n_clicks=1
    inputs = create_mock_callback_inputs({"remove-product-button.n_clicks": 1})

    # Create mock states with current_products=['DALMP']
    states = create_mock_callback_states({
        "product-comparison-products.data": ['DALMP'],
        "date-range-store.data": {"start_date": "2023-06-01", "end_date": "2023-06-03"},
        "viewport-store.data": "desktop"
    })

    # Call remove_product_from_comparison with mock inputs and states
    with pytest.raises(PreventUpdate):
        remove_product_from_comparison(
            n_clicks=inputs["remove-product-button.n_clicks"],
            current_products=states["product-comparison-products.data"],
            date_range=states["date-range-store.data"],
            viewport_size=states["viewport-store.data"]
        )


def test_update_comparison_on_date_change():
    """Tests updating the comparison when date range changes"""
    # Create mock inputs with date_range
    inputs = create_mock_callback_inputs({"date-range-store.data": {"start_date": "2023-06-05", "end_date": "2023-06-07"}})

    # Create mock states with current_products=['DALMP', 'RTLMP'] and viewport_size
    states = create_mock_callback_states({
        "product-comparison-products.data": ['DALMP', 'RTLMP'],
        "viewport-store.data": "desktop"
    })

    # Mock the load_comparison_data function to return sample data
    with unittest.mock.patch('src.web.callbacks.product_comparison_callbacks.load_comparison_data') as mock_load_comparison_data:
        mock_load_comparison_data.return_value = "mocked data"

        # Mock the update_product_comparison function
        with unittest.mock.patch('src.web.callbacks.product_comparison_callbacks.update_product_comparison') as mock_update_product_comparison:
            mock_update_product_comparison.return_value = "mocked figure"

            # Call update_comparison_on_date_change with mock inputs and states
            figure = update_comparison_on_date_change(
                date_range=inputs["date-range-store.data"],
                current_products=states["product-comparison-products.data"],
                viewport_size=states["viewport-store.data"]
            )

            # Verify that load_comparison_data was called with the correct parameters
            mock_load_comparison_data.assert_called_with(
                ['DALMP', 'RTLMP'],
                "2023-06-05",
                "2023-06-07"
            )

            # Verify that update_product_comparison was called with the correct parameters
            # mock_update_product_comparison.assert_called_with(
            #     "mocked figure",
            #     "mocked data",
            #     ['DALMP', 'RTLMP'],
            #     "desktop"
            # )

            # Verify that the returned figure is correct
            assert figure == "mocked figure"


def test_update_comparison_on_date_change_no_data():
    """Tests that update_comparison_on_date_change prevents update when no data available"""
    # Create mock inputs with date_range=None
    inputs = create_mock_callback_inputs({"date-range-store.data": None})

    # Create mock states with current_products=None
    states = create_mock_callback_states({
        "product-comparison-products.data": None,
        "viewport-store.data": "desktop"
    })

    # Call update_comparison_on_date_change with mock inputs and states
    with pytest.raises(PreventUpdate):
        update_comparison_on_date_change(
            date_range=inputs["date-range-store.data"],
            current_products=states["product-comparison-products.data"],
            viewport_size=states["viewport-store.data"]
        )


def test_update_comparison_on_viewport_change():
    """Tests updating the comparison when viewport size changes"""
    # Create mock inputs with viewport_size='sm'
    inputs = create_mock_callback_inputs({"viewport-store.data": 'sm'})

    # Create mock states with current_products=['DALMP', 'RTLMP'], date_range, and current_figure
    states = create_mock_callback_states({
        "product-comparison-products.data": ['DALMP', 'RTLMP'],
        "date-range-store.data": {"start_date": "2023-06-01", "end_date": "2023-06-03"},
        "product-comparison-graph.figure": "mocked figure"
    })

    # Mock the load_comparison_data function to return sample data
    with unittest.mock.patch('src.web.callbacks.product_comparison_callbacks.load_comparison_data') as mock_load_comparison_data:
        mock_load_comparison_data.return_value = "mocked data"

        # Mock the update_product_comparison function
        with unittest.mock.patch('src.web.callbacks.product_comparison_callbacks.update_product_comparison') as mock_update_product_comparison:
            mock_update_product_comparison.return_value = "mocked figure"

            # Call update_comparison_on_viewport_change with mock inputs and states
            figure = update_comparison_on_viewport_change(
                viewport_size=inputs["viewport-store.data"],
                current_products=states["product-comparison-products.data"],
                date_range=states["date-range-store.data"],
                current_figure=states["product-comparison-graph.figure"]
            )

            # Verify that update_product_comparison was called with the correct parameters
            # mock_update_product_comparison.assert_called_with(
            #     "mocked figure",
            #     "mocked data",
            #     ['DALMP', 'RTLMP'],
            #     "sm"
            # )

            # Verify that the returned figure is correct
            assert figure == "mocked figure"


def test_update_comparison_on_viewport_change_no_data():
    """Tests that update_comparison_on_viewport_change prevents update when no data available"""
    # Create mock inputs with viewport_size=None
    inputs = create_mock_callback_inputs({"viewport-store.data": None})

    # Create mock states with current_products=None, current_figure=None
    states = create_mock_callback_states({
        "product-comparison-products.data": None,
        "date-range-store.data": {"start_date": "2023-06-01", "end_date": "2023-06-03"},
        "product-comparison-graph.figure": None
    })

    # Call update_comparison_on_viewport_change with mock inputs and states
    with pytest.raises(PreventUpdate):
        update_comparison_on_viewport_change(
            viewport_size=inputs["viewport-store.data"],
            current_products=states["product-comparison-products.data"],
            date_range=states["date-range-store.data"],
            current_figure=states["product-comparison-graph.figure"]
        )


def test_update_product_selector_options():
    """Tests updating the product selector dropdown options"""
    # Create mock inputs with current_products=['DALMP']
    inputs = create_mock_callback_inputs({"product-comparison-products.data": ['DALMP']})

    # Call update_product_selector_options with mock inputs
    options = update_product_selector_options(current_products=inputs["product-comparison-products.data"])

    # Verify that the returned options exclude 'DALMP'
    assert all(option['value'] != 'DALMP' for option in options)

    # Verify that the returned options include all other products from PRODUCTS
    remaining_products = [p for p in PRODUCTS if p != 'DALMP']
    assert all(any(option['value'] == p for option in options) for p in remaining_products)


def test_update_add_button_state():
    """Tests enabling/disabling the add product button"""
    # Create mock inputs with selected_product='RTLMP'
    inputs = create_mock_callback_inputs({"product-comparison-selector.value": 'RTLMP'})

    # Create mock states with current_products=['DALMP']
    states = create_mock_callback_states({"product-comparison-products.data": ['DALMP']})

    # Call update_add_button_state with mock inputs and states
    disabled = update_add_button_state(selected_product=inputs["product-comparison-selector.value"], current_products=states["product-comparison-products.data"])

    # Verify that the returned value is False (button enabled)
    assert disabled is False

    # Create mock inputs with selected_product=None
    inputs = create_mock_callback_inputs({"product-comparison-selector.value": None})

    # Call update_add_button_state with mock inputs and states
    disabled = update_add_button_state(selected_product=inputs["product-comparison-selector.value"], current_products=states["product-comparison-products.data"])

    # Verify that the returned value is True (button disabled)
    assert disabled is True

    # Create mock states with current_products containing MAX_COMPARISON_PRODUCTS items
    states = create_mock_callback_states({"product-comparison-products.data": PRODUCTS[:MAX_COMPARISON_PRODUCTS]})

    # Call update_add_button_state with mock inputs and states
    disabled = update_add_button_state(selected_product=inputs["product-comparison-selector.value"], current_products=states["product-comparison-products.data"])

    # Verify that the returned value is True (button disabled)
    assert disabled is True


def test_update_remove_button_state():
    """Tests enabling/disabling the remove product button"""
    # Create mock inputs with current_products=['DALMP', 'RTLMP']
    inputs = create_mock_callback_inputs({"product-comparison-products.data": ['DALMP', 'RTLMP']})

    # Call update_remove_button_state with mock inputs
    disabled = update_remove_button_state(current_products=inputs["product-comparison-products.data"])

    # Verify that the returned value is False (button enabled)
    assert disabled is False

    # Create mock inputs with current_products=['DALMP']
    inputs = create_mock_callback_inputs({"product-comparison-products.data": ['DALMP']})

    # Call update_remove_button_state with mock inputs
    disabled = update_remove_button_state(current_products=inputs["product-comparison-products.data"])

    # Verify that the returned value is True (button disabled)
    assert disabled is True