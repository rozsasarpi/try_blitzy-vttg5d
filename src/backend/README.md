# Electricity Market Price Forecasting System - Backend

![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![Poetry](https://img.shields.io/badge/dependency%20management-poetry-blue)
![pytest](https://img.shields.io/badge/testing-pytest-green)
![black](https://img.shields.io/badge/code%20style-black-black)
![mypy](https://img.shields.io/badge/type%20checking-mypy-blue)

## Overview

Backend component of the Electricity Market Price Forecasting System that generates high-accuracy day-ahead market price forecasts for electricity traders and automated trading systems. The system produces probabilistic forecasts for Day-Ahead Locational Marginal Prices (DALMP), Real-Time Locational Marginal Prices (RTLMP), and all ancillary service products with hourly granularity over a 72-hour horizon.

## Features

- Probabilistic price forecasts for electricity market products
- Linear models tailored to specific product/hour combinations
- 72-hour forecast horizon with hourly granularity
- Scheduled execution at 7 AM CST daily
- Fallback mechanism for reliability
- Pandera schema validation for data quality
- API for forecast data access

## Installation

1. Clone the repository
2. Navigate to the backend directory
3. Install dependencies using Poetry
4. Configure environment variables
5. Run setup scripts

```bash
git clone https://github.com/organization/electricity-market-price-forecasting.git
cd electricity-market-price-forecasting/src/backend
poetry install
cp .env.example .env
# Edit .env with your configuration
```

## Usage

The system can be run in three modes: forecast generation, scheduler service, or API server.

### Generate Forecast

Run a forecast generation immediately for a specific date.

```bash
python main.py forecast --date 2023-06-01
```

### Run Scheduler

Start the scheduler service that will automatically run forecasts at 7 AM CST daily.

```bash
python main.py scheduler
```

### Start API Server

Start the API server to provide access to forecast data.

```bash
python main.py api --port 5000
```

## Architecture

The system follows a functional pipeline architecture with the following components:

| Component | Description |
|-----------|-------------|
| Data Ingestion | Collects data from external sources including load forecasts, historical prices, and generation forecasts. |
| Feature Engineering | Transforms raw data into feature vectors for each product/hour combination. |
| Forecasting Engine | Generates probabilistic price forecasts using linear models specific to each product/hour. |
| Forecast Validation | Ensures forecasts meet quality standards and conform to the pandera schema. |
| Storage System | Saves forecasts as pandas dataframes with appropriate timestamps and metadata. |
| Fallback Mechanism | Provides previous day's forecast when current generation fails. |
| Scheduler | Triggers forecast generation at 7 AM CST daily. |
| API | Provides access to forecast data for visualization and downstream systems. |

## Configuration

The system is configured through environment variables and settings files:

| File | Description |
|------|-------------|
| .env | Environment-specific configuration (copied from .env.example) |
| config/settings.py | Central configuration module with constants and parameters |
| config/logging_config.py | Logging configuration |
| config/schema_config.py | Pandera schema definitions for data validation |

## Development

This project uses Poetry for dependency management and includes several development tools:

| Tool | Description |
|------|-------------|
| pytest | Testing framework with coverage reporting |
| black | Code formatter with 100 character line length |
| isort | Import sorter configured to work with black |
| mypy | Static type checker with strict settings |
| flake8 | Linter with bugbear plugin |
| pre-commit | Pre-commit hooks for code quality |

Common development commands:

```bash
# Run tests
poetry run pytest

# Run tests with coverage
poetry run pytest --cov=.

# Format code
poetry run black .

# Sort imports
poetry run isort .

# Type checking
poetry run mypy .

# Lint code
poetry run flake8

# Install pre-commit hooks
poetry run pre-commit install
```

## Project Structure

The backend code is organized into the following directories:

| Directory | Description |
|-----------|-------------|
| config/ | Configuration modules and settings |
| models/ | Data models and class definitions |
| utils/ | Utility functions and helpers |
| storage/ | Forecast storage and retrieval |
| data_ingestion/ | External data source integration |
| feature_engineering/ | Feature creation and transformation |
| forecasting_engine/ | Linear models and probabilistic forecasting |
| forecast_validation/ | Forecast quality validation |
| fallback/ | Fallback mechanism implementation |
| scheduler/ | Scheduled execution |
| pipeline/ | End-to-end forecasting pipeline |
| api/ | API server and endpoints |
| scripts/ | Utility scripts |
| tests/ | Test suite organized by component |

## Testing

The system includes comprehensive tests organized by component:

| Test Type | Description |
|-----------|-------------|
| Unit Tests | Tests for individual functions and classes |
| Integration Tests | Tests for component interactions |
| End-to-End Tests | Tests for complete pipeline execution |

Run tests with:

```bash
poetry run pytest
```

## Docker

The system can be run in Docker using the provided Dockerfile and docker-compose.yml:

```bash
# Build Docker image
docker build -t electricity-market-forecasting-backend .

# Run with Docker Compose
docker-compose up -d
```

## License

This project is licensed under the terms specified in the LICENSE file.