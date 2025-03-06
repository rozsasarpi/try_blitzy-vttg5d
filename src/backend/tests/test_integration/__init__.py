"""
Integration Tests Package for the Electricity Market Price Forecasting System.

This package contains integration tests that verify the correct interaction
between different components of the system. Integration tests focus on ensuring
that data flows correctly through the pipeline and that components work together
as expected.

The tests in this package cover:
- Component Integration: Testing inter-component communication
- Pipeline Integration: Verifying end-to-end data flow
- External Integration: Testing data source connectivity with mocked external APIs
"""

import pytest  # pytest 7.0.0+

# Version of the integration test package for tracking test compatibility
INTEGRATION_TEST_VERSION = "1.0.0"

# Default timeout in seconds for integration tests
# This is higher than unit tests due to the complex nature of integration tests
INTEGRATION_TEST_TIMEOUT = 300