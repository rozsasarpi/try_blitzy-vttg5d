"""
Initialization file for the test_storage package in the Electricity Market Price Forecasting System.

This package contains tests for the forecast storage components, which are responsible for
storing timestamped forecast data as pandas dataframes and implementing pandera schema validation.

The storage tests ensure the reliability and correctness of forecast data storage, retrieval,
schema validation, and index management functionalities.
"""

# List of test modules in the storage test package
# This list can be used for test discovery and organization
STORAGE_TEST_MODULES = [
    "test_schema_definitions",
    "test_path_resolver",
    "test_index_manager",
    "test_dataframe_store",
    "test_storage_manager"
]