# Contributing to the Electricity Market Price Forecasting System

Thank you for your interest in contributing to the Electricity Market Price Forecasting System! This document provides guidelines and instructions for contributing to this project.

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Environment Setup](#development-environment-setup)
- [Coding Standards](#coding-standards)
- [Functional Programming Approach](#functional-programming-approach)
- [Testing Requirements](#testing-requirements)
- [Pull Request Process](#pull-request-process)
- [Issue Reporting](#issue-reporting)
- [Feature Requests](#feature-requests)
- [Documentation](#documentation)
- [Branch Strategy](#branch-strategy)
- [Release Process](#release-process)
- [Community](#community)

## Code of Conduct

This project adheres to a Code of Conduct that all contributors are expected to follow. By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

### Our Pledge

We pledge to make participation in our project a harassment-free experience for everyone, regardless of age, body size, disability, ethnicity, gender identity and expression, level of experience, nationality, personal appearance, race, religion, or sexual identity and orientation.

### Our Standards

- Using welcoming and inclusive language
- Being respectful of differing viewpoints and experiences
- Gracefully accepting constructive criticism
- Focusing on what is best for the community
- Showing empathy towards other community members

## Getting Started

Before you begin contributing, please:

1. Read the [README.md](README.md) to understand the project's purpose and architecture
2. Check the issue tracker for open issues that might need attention
3. For new features or significant changes, open an issue first to discuss your proposed changes

## Development Environment Setup

Follow these steps to set up your development environment:

### Prerequisites

- Python 3.10+
- Docker and Docker Compose (for containerized development)
- Git

### Installation Steps

1. Fork the repository on GitHub
2. Clone your fork locally
```bash
git clone https://github.com/your-username/electricity-market-price-forecasting.git
cd electricity-market-price-forecasting
```
3. Add the original repository as upstream
```bash
git remote add upstream https://github.com/original-owner/electricity-market-price-forecasting.git
```
4. Create a virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```
5. Install development dependencies
```bash
# For backend development
cd src/backend
pip install -r requirements.txt
pip install -e .

# For web development
cd src/web
pip install -r requirements.txt
pip install -e .
```

### Development Tools

We use the following tools for development:

| Tool | Purpose | Installation |
|------|---------|-------------|
| Poetry | Dependency management | `pip install poetry` |
| pytest | Testing framework | `pip install pytest pytest-cov` |
| black | Code formatting | `pip install black` |
| isort | Import sorting | `pip install isort` |
| mypy | Static type checking | `pip install mypy` |
| flake8 | Linting | `pip install flake8 flake8-bugbear` |
| pre-commit | Pre-commit hooks | `pip install pre-commit && pre-commit install` |

### Setting Up Pre-commit Hooks

We use pre-commit hooks to ensure code quality before commits:

```bash
pre-commit install
```

This will install hooks that run black, isort, flake8, and mypy before each commit.

## Coding Standards

Please adhere to the following coding standards when contributing to this project:

### Python Style Guide

- Follow PEP 8 style guide for Python code
- Use black with a line length of 100 characters for code formatting
- Use isort with black profile for import sorting
- Use type hints for all function parameters and return values
- Document all functions, classes, and modules using docstrings
- Keep functions small and focused on a single responsibility

### Code Formatting

Run the following commands before submitting your code:

```bash
# Format code with black
black --line-length 100 .

# Sort imports with isort
isort --profile black --line-length 100 .

# Check for linting issues
flake8 .

# Run type checking
mypy .
```

### Naming Conventions

- Use snake_case for variables, functions, and file names
- Use PascalCase for class names
- Use UPPER_CASE for constants
- Use descriptive names that reflect the purpose of the variable, function, or class
- Prefix private functions and variables with a single underscore (_)

### Comments and Documentation

- Write clear, concise comments that explain why, not what
- Use docstrings for all public functions, classes, and modules
- Follow Google style docstrings format
- Keep documentation up-to-date with code changes

## Functional Programming Approach

This project follows a functional programming approach as specified in the requirements. Please adhere to these principles when contributing:

- **Pure Functions**: Write functions that have no side effects and return the same output for the same input
- **Immutability**: Avoid modifying data structures in place; create new ones instead
- **Function Composition**: Build complex operations by composing simpler functions
- **Avoid State**: Minimize the use of state and global variables
- **Type Hints**: Use type hints to document function signatures and enable static type checking

```python
# Prefer this (functional approach)
def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    """Transform input data for feature engineering.
    
    Args:
        data: Input DataFrame with raw data
        
    Returns:
        Transformed DataFrame
    """
    # Create a new DataFrame instead of modifying in place
    transformed = data.copy()
    transformed['new_column'] = transformed['existing_column'] * 2
    return transformed

# Avoid this (non-functional approach)
def transform_data_bad(data: pd.DataFrame) -> None:
    """Transform input data for feature engineering.
    
    Args:
        data: Input DataFrame with raw data to be modified in place
    """
    # Modifies data in place (side effect)
    data['new_column'] = data['existing_column'] * 2
```

## Testing Requirements

All code contributions must include appropriate tests. We use pytest as our testing framework.

### Test Coverage

- Backend code must maintain at least 90% test coverage
- Web visualization code must maintain at least 80% test coverage
- All new features must include tests
- All bug fixes must include tests that verify the fix

### Test Categories

Write tests for the following categories as appropriate:

- **Unit Tests**: Test individual functions and classes in isolation (`@pytest.mark.unit`)
- **Integration Tests**: Test interactions between components (`@pytest.mark.integration`)
- **End-to-End Tests**: Test complete workflows from input to output (`@pytest.mark.e2e`)

### Running Tests

Run tests using the following commands:

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=. --cov-report=term

# Run specific test categories
pytest -m unit
pytest -m integration
pytest -m e2e
```

### Test Structure

Organize tests to mirror the structure of the code being tested. For example:

```
src/backend/data_ingestion/load_forecast.py -> src/backend/tests/test_data_ingestion/test_load_forecast.py
```

### Test Fixtures

Use pytest fixtures for test setup and teardown. Place common fixtures in the appropriate fixtures directory:

```
src/backend/tests/fixtures/
src/web/tests/fixtures/
```

## Pull Request Process

Follow these steps when submitting a pull request:

1. Ensure your code follows the coding standards and passes all tests
2. Update documentation as needed
3. Create a feature branch for your changes (`git checkout -b feature/your-feature-name`)
4. Commit your changes with clear, descriptive commit messages
5. Push your branch to your fork (`git push origin feature/your-feature-name`)
6. Submit a pull request to the `develop` branch of the original repository
7. Fill out the pull request template with all required information
8. Respond to any feedback or requested changes from reviewers

Pull requests will be reviewed by maintainers. CI checks must pass before a pull request can be merged.

## Issue Reporting

When reporting issues, please include:

- A clear, descriptive title
- A detailed description of the issue
- Steps to reproduce the problem
- Expected behavior
- Actual behavior
- Screenshots or logs if applicable
- Environment information (OS, Python version, etc.)

## Feature Requests

We welcome feature requests! Please use the feature request template when submitting new ideas. Include:

- A clear description of the feature
- The problem it solves
- The business value it provides
- Any implementation considerations

## Documentation

Documentation is a crucial part of this project. Please follow these guidelines when updating documentation:

- Keep README.md up-to-date with any changes to installation or usage instructions
- Update docstrings when changing function signatures or behavior
- Add comments to explain complex logic or algorithms
- Update architectural documentation when making significant changes to the system design

## Branch Strategy

This project follows a simplified GitFlow workflow:

- **main**: Production-ready code. Only merged from develop or hotfix branches.
- **develop**: Integration branch for features and bug fixes.
- **feature/\***: New features or enhancements. Created from and merged back to develop.
- **bugfix/\***: Bug fixes for issues in develop. Created from and merged back to develop.
- **hotfix/\***: Critical fixes for production issues. Created from main and merged to both main and develop.

## Release Process

Releases are managed by the core team following these steps:

1. Features and fixes are accumulated in the develop branch
2. When ready for release, a release branch is created from develop
3. Testing and final adjustments are made in the release branch
4. The release branch is merged to main and tagged with a version number
5. The release branch is also merged back to develop

## Community

We value community contributions and aim to foster an inclusive and collaborative environment. Feel free to reach out to the maintainers with questions or suggestions for improving the contribution process.