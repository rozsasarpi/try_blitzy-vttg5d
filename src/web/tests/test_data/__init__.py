"""
Initialization file for the test_data package in the web tests directory.
This file makes test fixtures and utilities available for testing the data handling components of the web visualization system, including schema validation, data loading, processing, and exporting functionality.
"""

from pathlib import Path  # pathlib: any

from src.web.tests.fixtures.forecast_fixtures import create_sample_forecast_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_sample_visualization_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_multi_product_forecast_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_sample_fallback_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_incomplete_visualization_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_invalid_visualization_dataframe  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import create_forecast_with_extreme_values  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import DEFAULT_PERCENTILES  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import SAMPLE_COUNT  # src/web/tests/fixtures/forecast_fixtures.py
from src.web.tests.fixtures.forecast_fixtures import FORECAST_HORIZON_HOURS  # src/web/tests/fixtures/forecast_fixtures.py

TEST_DATA_DIR = Path(__file__).parent
TEST_FIXTURES_DIR = TEST_DATA_DIR.parent / 'fixtures'

__all__ = [
    'create_sample_forecast_dataframe',
    'create_sample_visualization_dataframe',
    'create_multi_product_forecast_dataframe',
    'create_sample_fallback_dataframe',
    'create_incomplete_visualization_dataframe',
    'create_invalid_visualization_dataframe',
    'create_forecast_with_extreme_values',
    'DEFAULT_PERCENTILES',
    'SAMPLE_COUNT',
    'FORECAST_HORIZON_HOURS',
    'TEST_DATA_DIR',
    'TEST_FIXTURES_DIR'
]