import pytest  # pytest: 7.0.0+
import dash  # dash: 2.9.0+
from dash import html  # dash_html_components: 2.0.0+
import dash_core_components as dcc  # dash_core_components: 2.0.0+
import dash_bootstrap_components as dbc  # dash_bootstrap_components: 1.0.0+
import unittest.mock  # standard library

from src.web.layouts.responsive import (
    create_viewport_store,
    create_viewport_detection,
    create_responsive_container,
    create_responsive_layout,
    apply_responsive_layout,
    get_viewport_callback,
    register_viewport_callback,
    create_responsive_row,
    VIEWPORT_STORE_ID,
    RESPONSIVE_CONTAINER_ID,
    RESPONSIVE_LAYOUT_ID
)
from src.web.utils.responsive_helpers import (
    detect_viewport_size,
    get_responsive_style,
    create_responsive_style,
    get_viewport_meta_tag
)
from src.web.config.dashboard_config import VIEWPORT_BREAKPOINTS, DEFAULT_VIEWPORT
from src.web.config.themes import RESPONSIVE_LAYOUTS
from src.web.config.settings import ENABLE_RESPONSIVE_UI
from src.web.tests.fixtures.component_fixtures import mock_component

TEST_COMPONENT_ID = "test-component"
TEST_CONTAINER_ID = "test-container"
TEST_LAYOUT_ID = "test-layout"


class TestResponsiveHelpers:
    def setup_method(self, method):
        """Set up test environment before each test method"""
        self.original_enable_responsive_ui = ENABLE_RESPONSIVE_UI
        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = True
        self.test_base_styles = {"margin": "10px", "padding": "15px", "font-size": "16px"}

    def teardown_method(self, method):
        """Clean up test environment after each test method"""
        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = self.original_enable_responsive_ui

    def test_detect_viewport_size(self):
        """Test viewport size detection based on width"""
        assert detect_viewport_size(VIEWPORT_BREAKPOINTS["mobile"] - 1) == "mobile"
        assert detect_viewport_size(
            VIEWPORT_BREAKPOINTS["mobile"] + 1
        ) == "tablet"
        assert detect_viewport_size(VIEWPORT_BREAKPOINTS["tablet"] - 1) == "tablet"
        assert (
            detect_viewport_size(VIEWPORT_BREAKPOINTS["tablet"] + 1) == "desktop"
        )
        assert detect_viewport_size(1500) == "desktop"
        assert detect_viewport_size(VIEWPORT_BREAKPOINTS["mobile"]) == "tablet"
        assert detect_viewport_size(VIEWPORT_BREAKPOINTS["tablet"]) == "desktop"

    def test_get_responsive_style(self):
        """Test generation of responsive styles"""
        mobile_styles = get_responsive_style("mobile", self.test_base_styles)
        tablet_styles = get_responsive_style("tablet", self.test_base_styles)
        desktop_styles = get_responsive_style("desktop", self.test_base_styles)

        assert mobile_styles["margin"] == "5px"
        assert tablet_styles["padding"] == "12px"
        assert desktop_styles["font-size"] == "16px"
        assert get_responsive_style("mobile", {"color": "red"})["color"] == "red"
        assert get_responsive_style("mobile", {"margin": "20px", "color": "red"})[
            "margin"
        ] == "5px"

    def test_create_responsive_style(self):
        """Test creation of complete responsive styles"""
        style = create_responsive_style(base_style={"color": "blue"}, viewport_size="mobile")
        assert style["color"] == "blue"
        assert style["margin"] == "5px"

        style = create_responsive_style(
            base_style={"color": "blue"},
            viewport_size="mobile",
            overrides={"color": "red"},
        )
        assert style["color"] == "red"
        assert style["margin"] == "5px"

        assert create_responsive_style() == get_responsive_style("desktop")
        assert create_responsive_style(base_style=None) == get_responsive_style(
            "desktop", {}
        )

    def test_get_viewport_meta_tag(self):
        """Test generation of viewport meta tag"""
        meta_tag = get_viewport_meta_tag()
        assert meta_tag["name"] == "viewport"
        assert (
            meta_tag["content"]
            == "width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"
        )


class TestResponsiveComponents:
    def setup_method(self, method):
        """Set up test environment before each test method"""
        self.original_enable_responsive_ui = ENABLE_RESPONSIVE_UI
        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = True
        self.test_children = [
            html.Div(id="child1"),
            dcc.Graph(id="child2"),
            dbc.Button(id="child3"),
        ]

    def teardown_method(self, method):
        """Clean up test environment after each test method"""
        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = self.original_enable_responsive_ui

    def test_create_viewport_store(self):
        """Test creation of viewport store component"""
        store = create_viewport_store()
        assert isinstance(store, dcc.Store)
        assert store.id == VIEWPORT_STORE_ID
        assert "width" in store.data
        assert "height" in store.data
        assert "size" in store.data
        assert store.data["size"] == DEFAULT_VIEWPORT

    def test_create_viewport_detection(self):
        """Test creation of viewport detection components"""
        components = create_viewport_detection()
        assert isinstance(components, list)
        assert any(c.id == VIEWPORT_STORE_ID for c in components)
        assert any(c.id == "viewport-detector" for c in components)
        assert any(
            isinstance(c, html.Script) for c in components if c.id == "viewport-detector"
        )

    def test_create_responsive_container(self):
        """Test creation of responsive container"""
        container = create_responsive_container(children=self.test_children)
        assert isinstance(container, dbc.Container)
        assert container.fluid is True
        assert container.id == RESPONSIVE_CONTAINER_ID
        assert container.children == self.test_children

        container = create_responsive_container(viewport_size="mobile")
        assert "viewport-mobile" in container.className

        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = False
        container = create_responsive_container()
        assert "viewport-" not in container.className
        ENABLE_RESPONSIVE_UI = True

    def test_create_responsive_layout(self):
        """Test creation of responsive layout"""
        layout = create_responsive_layout(children=self.test_children)
        assert isinstance(layout, html.Div)
        assert layout.id == RESPONSIVE_LAYOUT_ID
        assert layout.children == self.test_children

        layout = create_responsive_layout(viewport_size="tablet")
        assert "viewport-tablet" in layout.className
        assert any(c.id == VIEWPORT_STORE_ID for c in layout.children)

        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = False
        layout = create_responsive_layout()
        assert "viewport-" not in layout.className
        ENABLE_RESPONSIVE_UI = True

    def test_apply_responsive_layout(self):
        """Test applying responsive layout to components"""
        component = mock_component(TEST_COMPONENT_ID, "html")
        adjusted = apply_responsive_layout(component, viewport_size="mobile")
        assert adjusted.id == TEST_COMPONENT_ID
        assert "viewport-mobile" in adjusted.className

        component = mock_component(TEST_COMPONENT_ID, "html", {"children": self.test_children})
        adjusted = apply_responsive_layout(component, viewport_size="mobile")
        assert len(adjusted.children) == len(self.test_children)

        assert apply_responsive_layout(None) is None

        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = False
        component = mock_component(TEST_COMPONENT_ID, "html")
        adjusted = apply_responsive_layout(component, viewport_size="mobile")
        assert "viewport-" not in adjusted.className
        ENABLE_RESPONSIVE_UI = True

    def test_create_responsive_row(self):
        """Test creation of responsive row with columns"""
        columns = [{"content": "Column 1", "width": 6}, {"content": "Column 2", "width": 6}]
        row = create_responsive_row(columns)
        assert isinstance(row, dbc.Row)
        assert len(row.children) == 2
        assert row.children[0].width == 6
        assert row.children[1].width == 6

        row = create_responsive_row(columns, viewport_size="mobile")
        assert row.children[0].width == 12
        assert row.children[1].width == 12

        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = False
        row = create_responsive_row(columns)
        assert row.children[0].width == 6
        assert row.children[1].width == 6
        ENABLE_RESPONSIVE_UI = True


class TestViewportCallbacks:
    def setup_method(self, method):
        """Set up test environment before each test method"""
        self.mock_app = dash.Dash(__name__)
        self.mock_clientside_callback = unittest.mock.MagicMock()
        self.mock_app.clientside_callback = self.mock_clientside_callback

    def test_get_viewport_callback(self):
        """Test generation of viewport callback function"""
        callback = get_viewport_callback()
        assert callable(callback)
        result = callback(800, 600)
        assert isinstance(result, dict)
        assert "width" in result
        assert "height" in result
        assert "size" in result
        assert result["size"] == "tablet"

    def test_register_viewport_callback(self):
        """Test registration of viewport callback with app"""
        register_viewport_callback(self.mock_app)
        self.mock_clientside_callback.assert_called_once()
        args, kwargs = self.mock_clientside_callback.call_args
        assert kwargs["output"] == dash.Output(VIEWPORT_STORE_ID, "data")
        assert kwargs["inputs"] == [
            dash.Input("window", "innerWidth"),
            dash.Input("window", "innerHeight"),
        ]


class TestResponsiveIntegration:
    def setup_method(self, method):
        """Set up test environment before each test method"""
        self.original_enable_responsive_ui = ENABLE_RESPONSIVE_UI
        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = True
        self.test_components = [
            create_viewport_store(),
            create_responsive_container(
                children=[
                    create_responsive_row(
                        [
                            {"content": html.Div(id="col1"), "width": 6},
                            {"content": html.Div(id="col2"), "width": 6},
                        ]
                    ),
                    html.Div(id="content"),
                ]
            ),
        ]

    def teardown_method(self, method):
        """Clean up test environment after each test method"""
        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = self.original_enable_responsive_ui

    def test_responsive_layout_integration(self):
        """Test integration of responsive components in a layout"""
        layout = create_responsive_layout(children=self.test_components)
        assert any(c.id == VIEWPORT_STORE_ID for c in layout.children)
        assert "viewport-desktop" in layout.className

    def test_responsive_disabled_behavior(self):
        """Test behavior when responsive UI is disabled"""
        global ENABLE_RESPONSIVE_UI
        ENABLE_RESPONSIVE_UI = False

        container = create_responsive_container()
        assert "viewport-" not in container.className

        layout = create_responsive_layout()
        assert "viewport-" not in layout.className

        component = mock_component(TEST_COMPONENT_ID, "html")
        adjusted = apply_responsive_layout(component, viewport_size="mobile")
        assert adjusted.id == TEST_COMPONENT_ID
        assert "viewport-" not in adjusted.className