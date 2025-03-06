"""
Test module for the forecasting engine components.

This module contains tests for the linear model-based forecasting engine that 
generates probabilistic price forecasts for electricity market products.
Tests in this module verify the correct implementation of:
- Linear model selection for each product/hour combination
- Probabilistic forecast generation with appropriate uncertainty estimation
- 72-hour forecast horizon with hourly granularity
- Forecast validation and quality checks
"""

# Version of the forecasting engine test module for tracking test compatibility
TEST_MODULE_VERSION = "1.0.0"