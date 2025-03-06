"""
Initialization file for the data ingestion test package in the
Electricity Market Price Forecasting System.

This module defines package-level constants and utilities for testing 
data ingestion components including required column definitions for 
validating test data structure.
"""

# Version of the test package - update this when making significant changes
# to ensure test compatibility with the main codebase
TEST_DATA_INGESTION_VERSION = "1.0.0"

# Required columns for data validation in tests
# These match the schema definitions in the main application
REQUIRED_LOAD_FORECAST_COLUMNS = ["timestamp", "load_mw", "region"]

REQUIRED_HISTORICAL_PRICE_COLUMNS = ["timestamp", "product", "price", "node"]

REQUIRED_GENERATION_FORECAST_COLUMNS = ["timestamp", "fuel_type", "generation_mw", "region"]