"""
Utility module providing file operation functions for the Electricity Market Price Forecasting System.

This module implements directory management, file path generation, dataframe serialization/deserialization,
and forecast file management with a functional programming approach.
"""

import os
import pathlib
from pathlib import Path
import shutil
import datetime
from typing import Optional, List, Union
import pandas as pd  # version: 2.0.0+

# Internal imports
from ..config.settings import (
    STORAGE_ROOT_DIR,
    STORAGE_LATEST_DIR, 
    FORECAST_PRODUCTS
)
from .logging_utils import get_logger, log_execution_time, log_exceptions
from .date_utils import get_previous_day_date

# Configure logger
logger = get_logger(__name__)

# Default file format
DEFAULT_FORMAT = 'parquet'


@log_exceptions
def ensure_directory_exists(directory_path: Union[str, pathlib.Path]) -> pathlib.Path:
    """
    Ensures a directory exists, creating it if necessary.
    
    Args:
        directory_path: Path to the directory to ensure exists
        
    Returns:
        Path object pointing to the directory
        
    Raises:
        OSError: If directory creation fails
    """
    # Convert to Path object if it's a string
    path = Path(directory_path)
    
    # Create directory if it doesn't exist
    if not path.exists():
        logger.info(f"Creating directory: {path}")
        path.mkdir(parents=True, exist_ok=True)
    
    return path


@log_exceptions
def get_forecast_directory(forecast_date: datetime.datetime) -> pathlib.Path:
    """
    Gets the directory for storing forecasts based on date.
    
    Args:
        forecast_date: Date for which to get the forecast directory
        
    Returns:
        Path to the forecast directory (year/month structure)
        
    Raises:
        TypeError: If forecast_date is not a datetime object
    """
    if not isinstance(forecast_date, datetime.datetime):
        raise TypeError("forecast_date must be a datetime object")
    
    # Extract year and month for directory structure
    year = forecast_date.strftime('%Y')
    month = forecast_date.strftime('%m')
    
    # Construct path
    directory_path = Path(STORAGE_ROOT_DIR) / year / month
    
    # Ensure directory exists
    return ensure_directory_exists(directory_path)


@log_exceptions
def get_forecast_file_path(
    forecast_date: datetime.datetime, 
    product: str, 
    format: str = DEFAULT_FORMAT
) -> pathlib.Path:
    """
    Generates a file path for a forecast file.
    
    Args:
        forecast_date: Date of the forecast
        product: Forecast product identifier
        format: File format extension (default: 'parquet')
        
    Returns:
        Path to the forecast file
        
    Raises:
        ValueError: If product is not in the list of valid forecast products
        TypeError: If forecast_date is not a datetime object
    """
    if not isinstance(forecast_date, datetime.datetime):
        raise TypeError("forecast_date must be a datetime object")
    
    # Validate product
    if product not in FORECAST_PRODUCTS:
        raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")
    
    # Get directory for this forecast date
    directory = get_forecast_directory(forecast_date)
    
    # Extract day for filename
    day = forecast_date.strftime('%d')
    
    # Construct filename: day_product.format
    filename = f"{day}_{product}.{format}"
    
    # Return complete path
    return directory / filename


@log_execution_time
@log_exceptions
def save_dataframe(
    df: pd.DataFrame, 
    file_path: Union[str, pathlib.Path],
    format: str = DEFAULT_FORMAT
) -> bool:
    """
    Saves a pandas DataFrame to a file.
    
    Args:
        df: DataFrame to save
        file_path: Path where the file should be saved
        format: File format (default: 'parquet')
        
    Returns:
        True if successful, False otherwise
        
    Raises:
        TypeError: If df is not a pandas DataFrame
        OSError: If directory creation or file writing fails
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")
    
    # Convert to Path object if it's a string
    path = Path(file_path)
    
    # Ensure parent directory exists
    ensure_directory_exists(path.parent)
    
    try:
        # Save in the appropriate format
        if format.lower() == 'parquet':
            df.to_parquet(path, index=False)
        elif format.lower() == 'csv':
            df.to_csv(path, index=False)
        else:
            logger.error(f"Unsupported file format: {format}")
            return False
        
        logger.info(f"Successfully saved DataFrame to {path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save DataFrame to {path}: {str(e)}")
        raise
    

@log_execution_time
@log_exceptions
def load_dataframe(
    file_path: Union[str, pathlib.Path],
    format: str = DEFAULT_FORMAT
) -> Optional[pd.DataFrame]:
    """
    Loads a pandas DataFrame from a file.
    
    Args:
        file_path: Path to the file to load
        format: File format (default: 'parquet')
        
    Returns:
        Loaded DataFrame or None if file doesn't exist or format is invalid
        
    Raises:
        ValueError: If file format is not supported
    """
    # Convert to Path object if it's a string
    path = Path(file_path)
    
    # Check if file exists
    if not path.exists():
        logger.warning(f"File not found: {path}")
        return None
    
    try:
        # Load in the appropriate format
        if format.lower() == 'parquet':
            df = pd.read_parquet(path)
        elif format.lower() == 'csv':
            df = pd.read_csv(path)
        else:
            logger.error(f"Unsupported file format: {format}")
            return None
        
        logger.info(f"Successfully loaded DataFrame from {path}")
        return df
    except Exception as e:
        logger.error(f"Failed to load DataFrame from {path}: {str(e)}")
        raise


@log_exceptions
def list_forecast_files(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    product: Optional[str] = None,
    format: str = DEFAULT_FORMAT
) -> List[pathlib.Path]:
    """
    Lists all forecast files for a date range.
    
    Args:
        start_date: Start date for file listing
        end_date: End date for file listing
        product: Optional product filter
        format: File format to filter by (default: 'parquet')
        
    Returns:
        List of file paths matching the criteria
        
    Raises:
        TypeError: If start_date or end_date is not a datetime object
        ValueError: If start_date is after end_date
    """
    if not isinstance(start_date, datetime.datetime) or not isinstance(end_date, datetime.datetime):
        raise TypeError("Both start_date and end_date must be datetime objects")
    
    if start_date > end_date:
        raise ValueError("start_date must be before or equal to end_date")
    
    result_files = []
    
    # Convert to date objects for comparison
    current_date = start_date.date()
    end_date_day = end_date.date()
    
    # Iterate through each day in the range
    while current_date <= end_date_day:
        # Convert to datetime for directory functions
        current_datetime = datetime.datetime.combine(
            current_date, datetime.time(0, 0, 0)
        )
        
        try:
            directory = get_forecast_directory(current_datetime)
            
            # If directory exists, look for matching files
            if directory.exists():
                # Get day string for filename matching
                day = current_date.strftime('%d')
                
                # Get all files in the directory
                for file_path in directory.glob(f"{day}_*.{format}"):
                    file_name = file_path.name
                    
                    # If product filter is applied, check if it matches
                    if product:
                        if file_name == f"{day}_{product}.{format}":
                            result_files.append(file_path)
                    else:
                        # No product filter, add all matching files
                        result_files.append(file_path)
        
        except Exception as e:
            logger.warning(f"Error listing forecast files for {current_date}: {str(e)}")
        
        # Move to next day
        current_date += datetime.timedelta(days=1)
    
    logger.info(f"Found {len(result_files)} forecast files for period {start_date.date()} to {end_date.date()}")
    return result_files


@log_exceptions
def get_latest_forecast_file(
    product: str,
    format: str = DEFAULT_FORMAT
) -> Optional[pathlib.Path]:
    """
    Gets the most recent forecast file for a product.
    
    Args:
        product: Forecast product identifier
        format: File format (default: 'parquet')
        
    Returns:
        Path to the latest forecast file or None if not found
        
    Raises:
        ValueError: If product is not in the list of valid forecast products
    """
    if product not in FORECAST_PRODUCTS:
        raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")
    
    # Check if latest link exists
    latest_link = Path(STORAGE_LATEST_DIR) / f"{product}.{format}"
    if latest_link.exists():
        try:
            # Resolve the symbolic link to get the actual file
            actual_file = latest_link.resolve()
            if actual_file.exists():
                logger.debug(f"Found latest forecast for {product} via symbolic link: {actual_file}")
                return actual_file
        except Exception as e:
            logger.warning(f"Error resolving latest link for {product}: {str(e)}")
    
    # Fallback: search by date
    logger.info(f"No latest link found for {product}, searching by date")
    current_date = datetime.datetime.now()
    
    # Look back up to 7 days for a forecast file
    for days_back in range(7):
        search_date = current_date - datetime.timedelta(days=days_back)
        try:
            file_path = get_forecast_file_path(search_date, product, format)
            if file_path.exists():
                logger.info(f"Found latest forecast for {product} by date search: {file_path}")
                return file_path
        except Exception as e:
            logger.debug(f"Error checking file for {search_date.date()}: {str(e)}")
    
    logger.warning(f"No forecast file found for {product} in the last 7 days")
    return None


@log_exceptions
def update_latest_link(
    file_path: Union[str, pathlib.Path],
    product: str
) -> bool:
    """
    Updates the 'latest' symbolic link for a product to point to the given file.
    
    Args:
        file_path: Path to the forecast file
        product: Forecast product identifier
        
    Returns:
        True if successful, False otherwise
        
    Raises:
        ValueError: If product is not in the list of valid forecast products
        OSError: If symbolic link creation fails
    """
    if product not in FORECAST_PRODUCTS:
        raise ValueError(f"Invalid product: {product}. Must be one of {FORECAST_PRODUCTS}")
    
    # Convert to Path object if it's a string
    source_path = Path(file_path)
    
    # Check if source file exists
    if not source_path.exists():
        logger.error(f"Source file does not exist: {source_path}")
        return False
    
    # Ensure latest directory exists
    latest_dir = ensure_directory_exists(STORAGE_LATEST_DIR)
    
    # Determine file format from source path
    format = source_path.suffix.lstrip('.')
    
    # Construct the link path
    link_path = latest_dir / f"{product}.{format}"
    
    try:
        # Remove existing link if it exists
        if link_path.exists():
            if link_path.is_symlink():
                link_path.unlink()
            else:
                logger.warning(f"Expected symlink but found file at {link_path}, removing")
                os.remove(link_path)
        
        # Create new symbolic link
        # We need to use os.symlink and relative_to for cross-platform compatibility
        # Using relative path for the link to handle directory movements
        try:
            # Try to create a relative link
            source_path_abs = source_path.absolute()
            link_path_abs = link_path.absolute()
            
            # Create relative path
            relative_path = os.path.relpath(source_path_abs, link_path_abs.parent)
            
            # Create the symbolic link
            os.symlink(relative_path, link_path)
        except (ValueError, OSError):
            # Fallback to absolute path if relative path fails
            os.symlink(source_path.absolute(), link_path)
        
        logger.info(f"Updated latest link for {product} to point to {source_path}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to update latest link for {product}: {str(e)}")
        return False


@log_execution_time
@log_exceptions
def clean_old_forecasts(retention_days: int) -> int:
    """
    Removes forecast files older than a retention period.
    
    Args:
        retention_days: Number of days to keep forecasts
        
    Returns:
        Number of files removed
        
    Raises:
        ValueError: If retention_days is not a positive integer
    """
    if not isinstance(retention_days, int) or retention_days <= 0:
        raise ValueError("retention_days must be a positive integer")
    
    # Calculate cutoff date
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=retention_days)
    cutoff_timestamp = cutoff_date.timestamp()
    
    # Files removed counter
    removed_count = 0
    
    # Get the root storage path
    root_path = Path(STORAGE_ROOT_DIR)
    
    # Skip the 'latest' directory
    latest_dir = Path(STORAGE_LATEST_DIR)
    
    logger.info(f"Cleaning forecast files older than {cutoff_date.date()}")
    
    # Walk through the directory tree
    for year_dir in root_path.glob('[0-9][0-9][0-9][0-9]'):
        if not year_dir.is_dir():
            continue
            
        for month_dir in year_dir.glob('[0-9][0-9]'):
            if not month_dir.is_dir():
                continue
                
            # Check all forecast files in this month directory
            for file_path in month_dir.glob('*.*'):
                # Skip directories and non-files
                if not file_path.is_file() or file_path.is_symlink():
                    continue
                
                # Skip files in 'latest' directory
                if latest_dir in file_path.parents:
                    continue
                
                try:
                    # Check file modification time
                    mod_time = file_path.stat().st_mtime
                    
                    # Remove file if older than cutoff
                    if mod_time < cutoff_timestamp:
                        file_path.unlink()
                        removed_count += 1
                        logger.debug(f"Removed old forecast file: {file_path}")
                
                except Exception as e:
                    logger.warning(f"Error processing file {file_path}: {str(e)}")
            
            # Check if month directory is empty after file removal
            remaining_files = list(month_dir.glob('*'))
            if not remaining_files:
                try:
                    month_dir.rmdir()
                    logger.debug(f"Removed empty month directory: {month_dir}")
                except Exception as e:
                    logger.warning(f"Failed to remove empty directory {month_dir}: {str(e)}")
        
        # Check if year directory is empty after month directories removal
        remaining_dirs = list(year_dir.glob('*'))
        if not remaining_dirs:
            try:
                year_dir.rmdir()
                logger.debug(f"Removed empty year directory: {year_dir}")
            except Exception as e:
                logger.warning(f"Failed to remove empty directory {year_dir}: {str(e)}")
    
    logger.info(f"Cleaned {removed_count} forecast files older than {retention_days} days")
    return removed_count