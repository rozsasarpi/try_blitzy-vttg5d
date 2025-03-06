"""
Initialization file for the test package of the Electricity Market Price Forecasting System.
Defines test package version, constants, and exports necessary components for the test suite.
This file serves as the entry point for the testing infrastructure, making the test package importable and establishing common test utilities.
"""

import pathlib

import pytest  # version: 7.0.0+
from . import fixtures

TEST_PACKAGE_VERSION = "0.1.0"
TEST_DATA_DIR = pathlib.Path(__file__).parent / 'test_data'

__all__ = [
    "TEST_PACKAGE_VERSION",
    "TEST_DATA_DIR",
]