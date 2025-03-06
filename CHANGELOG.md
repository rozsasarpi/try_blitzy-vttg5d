# Changelog

All notable changes to the Electricity Market Price Forecasting System will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial development of the forecasting pipeline
- Implementation of linear models for each product/hour combination
- Fallback mechanism for system reliability
- Dash-based visualization interface

## [1.0.0] - YYYY-MM-DD

### Added
- Core forecasting engine with linear models for each product/hour combination
- Data ingestion pipeline for load forecasts, historical prices, and generation forecasts
- Feature engineering pipeline with product/hour specific features
- Forecast validation with completeness, plausibility, and consistency checks
- Storage system for forecast dataframes with pandera schema validation
- Fallback mechanism using previous day's forecast when current generation fails
- Dash-based visualization showing time vs. price forecasts
- Scheduled execution at 7 AM CST daily
- Comprehensive test suite with unit, integration, and end-to-end tests
- Docker containerization for consistent deployment
- Monitoring and alerting system for operational reliability

[Unreleased]: https://github.com/username/electricity-market-forecasting/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/username/electricity-market-forecasting/releases/tag/v1.0.0