"""
Path resolution utilities for forecast storage in the Electricity Market Price Forecasting System.

This module provides functions to generate standardized paths for forecast files, index files,
and latest forecast links, ensuring consistent storage structure and enabling efficient retrieval
of forecast data. It implements a hierarchical storage structure with year/month/day organization
and maintains symbolic links to the latest forecasts for quick access.
"""

import os
import pathlib
import datetime
from typing import Union

# Internal imports
from ..config.settings import (
    STORAGE_ROOT_DIR,
    STORAGE_LATEST_DIR,
    STORAGE_INDEX_FILE,
    FORECAST_PRODUCTS
)
from .exceptions import StoragePathError
from ..utils.file_utils import ensure_directory_exists
from ..utils.logging_utils import get_logger, log_exceptions

# Configure logger
logger = get_logger(__name__)

# Default file format
DEFAULT_FORMAT = 'parquet'


@log_exceptions
def get_base_storage_path() -> pathlib.Path:
    """
    Returns the base storage path for forecasts.
    
    Returns:
        pathlib.Path: Path to the base storage directory
    """
    base_path = pathlib.Path(STORAGE_ROOT_DIR)
    ensure_directory_exists(base_path)
    return base_path


@log_exceptions
def get_year_month_path(forecast_date: datetime.datetime) -> pathlib.Path:
    """
    Gets the year/month directory path for a specific date.
    
    Args:
        forecast_date: Date to get the directory for
        
    Returns:
        pathlib.Path: Path to the year/month directory
    """
    year = forecast_date.year
    month = forecast_date.month
    
    # Construct path using year and month
    path = pathlib.Path(STORAGE_ROOT_DIR) / str(year) / f"{month:02d}"
    
    # Ensure the directory exists
    ensure_directory_exists(path)
    
    return path


@log_exceptions
def get_forecast_file_path(
    forecast_date: datetime.datetime,
    product: str,
    format: str = DEFAULT_FORMAT
) -> pathlib.Path:
    """
    Generates a file path for a specific forecast.
    
    Args:
        forecast_date: Date of the forecast
        product: Price product identifier
        format: File format (default: 'parquet')
        
    Returns:
        pathlib.Path: Path to the forecast file
        
    Raises:
        StoragePathError: If product is invalid
    """
    # Validate product
    validate_product(product)
    
    # Get the year/month directory path
    dir_path = get_year_month_path(forecast_date)
    
    # Extract day for filename
    day = forecast_date.day
    
    # Construct filename: day_product.format
    filename = f"{day:02d}_{product}.{format}"
    
    # Join directory path with filename
    return dir_path / filename


@log_exceptions
def get_latest_file_path(
    product: str,
    format: str = DEFAULT_FORMAT
) -> pathlib.Path:
    """
    Gets the path to the latest forecast file for a product.
    
    Args:
        product: Price product identifier
        format: File format (default: 'parquet')
        
    Returns:
        pathlib.Path: Path to the latest forecast file
        
    Raises:
        StoragePathError: If product is invalid
    """
    # Validate product
    validate_product(product)
    
    # Ensure latest directory exists
    latest_dir = pathlib.Path(STORAGE_LATEST_DIR)
    ensure_directory_exists(latest_dir)
    
    # Construct path to latest file
    return latest_dir / f"{product}.{format}"


@log_exceptions
def get_index_file_path() -> pathlib.Path:
    """
    Gets the path to the forecast index file.
    
    Returns:
        pathlib.Path: Path to the index file
    """
    index_path = pathlib.Path(STORAGE_INDEX_FILE)
    
    # Ensure parent directory exists
    ensure_directory_exists(index_path.parent)
    
    return index_path


@log_exceptions
def create_backup_path(file_path: pathlib.Path) -> pathlib.Path:
    """
    Creates a backup path for a file.
    
    Args:
        file_path: Path to the original file
        
    Returns:
        pathlib.Path: Path to the backup file
    """
    # Get current timestamp in a format suitable for filenames
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Construct backup path by appending timestamp
    return file_path.with_name(f"{file_path.stem}_{timestamp}{file_path.suffix}")


@log_exceptions
def validate_product(product: str) -> bool:
    """
    Validates that a product name is in the list of valid products.
    
    Args:
        product: Product name to validate
        
    Returns:
        bool: True if valid, raises exception if invalid
        
    Raises:
        StoragePathError: If product is not in the list of valid products
    """
    if product not in FORECAST_PRODUCTS:
        raise StoragePathError(
            f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}",
            product
        )
    return True


@log_exceptions
def resolve_relative_path(relative_path: Union[str, pathlib.Path]) -> pathlib.Path:
    """
    Resolves a relative path against the base storage path.
    
    Args:
        relative_path: Relative path to resolve
        
    Returns:
        pathlib.Path: Absolute path resolved against base storage
    """
    base_path = get_base_storage_path()
    
    # Convert relative_path to Path if it's a string
    if isinstance(relative_path, str):
        relative_path = pathlib.Path(relative_path)
    
    # Join the base path with the relative path
    return base_path / relative_path


@log_exceptions
def get_relative_storage_path(absolute_path: pathlib.Path) -> pathlib.Path:
    """
    Gets a path relative to the base storage path.
    
    Args:
        absolute_path: Absolute path to convert to relative
        
    Returns:
        pathlib.Path: Path relative to base storage
    """
    base_path = get_base_storage_path()
    
    # Calculate the relative path
    return absolute_path.relative_to(base_path)