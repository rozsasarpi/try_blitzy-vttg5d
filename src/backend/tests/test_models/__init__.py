"""
Initialization file for the test_models package in the Electricity Market Price Forecasting System.
This file makes the test_models package importable and defines test-specific constants and utilities
for testing data models, validation models, and forecast models.
"""

from ..__init__ import TEST_PACKAGE_VERSION  # Import test package version for tracking test compatibility

TEST_MODELS_VERSION = "1.0.0"
"""Version of the test_models package for tracking test compatibility"""

TEST_DATA_MODELS_ENABLED = True
"""Flag to enable/disable data model tests"""

TEST_VALIDATION_MODELS_ENABLED = True
"""Flag to enable/disable validation model tests"""

TEST_FORECAST_MODELS_ENABLED = True
"""Flag to enable/disable forecast model tests"""

__all__ = [
    "TEST_MODELS_VERSION",
    "TEST_DATA_MODELS_ENABLED",
    "TEST_VALIDATION_MODELS_ENABLED",
    "TEST_FORECAST_MODELS_ENABLED"
]