"""
Core implementation for storing and retrieving forecast dataframes in the Electricity Market Price Forecasting System.

This module provides a functional interface for saving, loading, and managing forecast data with
schema validation, metadata handling, and fallback mechanisms. It ensures that stored forecasts
meet quality requirements and can be efficiently retrieved for visualization and downstream use.
"""

import os
import pathlib
import datetime
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd  # version: 2.0.0
import pandera as pa  # version: 0.16.0

# Internal imports
from .path_resolver import (
    get_forecast_file_path,
    get_latest_file_path,
    validate_product
)
from .schema_definitions import (
    validate_forecast_schema,
    add_storage_metadata,
    check_storage_integrity,
    extract_storage_metadata,
    upgrade_schema_if_needed
)
from .index_manager import (
    add_forecast_to_index,
    remove_forecast_from_index,
    update_latest_links,
    query_index_by_date,
    get_forecast_file_paths
)
from ..utils.file_utils import save_dataframe, load_dataframe
from ..utils.logging_utils import get_logger, log_execution_time, log_exceptions
from .exceptions import (
    StorageError,
    SchemaValidationError,
    FileOperationError,
    DataFrameNotFoundError,
    DataIntegrityError
)

# Configure logger
logger = get_logger(__name__)

# Default file format
DEFAULT_FORMAT = 'parquet'


@log_execution_time
@log_exceptions
def store_forecast(
    df: pd.DataFrame,
    forecast_timestamp: datetime.datetime,
    product: str,
    is_fallback: bool = False,
    format: str = DEFAULT_FORMAT
) -> pathlib.Path:
    """
    Stores a forecast dataframe with validation and indexing.
    
    Args:
        df: DataFrame to store
        forecast_timestamp: Timestamp of the forecast
        product: Price product identifier
        is_fallback: Whether this is a fallback forecast
        format: File format (default: 'parquet')
        
    Returns:
        Path to the stored forecast file
        
    Raises:
        SchemaValidationError: If dataframe fails schema validation
        FileOperationError: If file operation fails
        StorageError: For other storage-related errors
    """
    # Validate the product name
    validate_product(product)
    
    # Validate the forecast dataframe against schema
    is_valid, validation_errors = validate_forecast_schema(df)
    if not is_valid:
        logger.error(f"Schema validation failed for {product} forecast at {forecast_timestamp}")
        raise SchemaValidationError("Forecast dataframe failed schema validation", validation_errors)
    
    # Add storage metadata
    df_with_metadata = add_storage_metadata(df)
    
    # Get the file path for the forecast
    file_path = get_forecast_file_path(forecast_timestamp, product, format)
    
    # Save the dataframe to the file
    try:
        success = save_dataframe(df_with_metadata, file_path, format)
        if not success:
            raise FileOperationError(f"Failed to save dataframe", file_path, "write")
    except Exception as e:
        logger.error(f"Failed to save dataframe to {file_path}: {str(e)}")
        raise FileOperationError(f"Failed to save dataframe: {str(e)}", file_path, "write")
    
    # Add the forecast to the index
    generation_timestamp = df["generation_timestamp"].iloc[0] if "generation_timestamp" in df.columns else datetime.datetime.now()
    
    add_forecast_to_index(
        file_path,
        forecast_timestamp,
        product,
        generation_timestamp,
        is_fallback
    )
    
    # Update the latest links
    update_latest_links()
    
    logger.info(f"Successfully stored {product} forecast for {forecast_timestamp} at {file_path}")
    return file_path


@log_execution_time
@log_exceptions
def load_forecast(
    forecast_timestamp: datetime.datetime,
    product: str,
    format: str = DEFAULT_FORMAT
) -> pd.DataFrame:
    """
    Loads a forecast dataframe by timestamp and product.
    
    Args:
        forecast_timestamp: Timestamp of the forecast
        product: Price product identifier
        format: File format (default: 'parquet')
        
    Returns:
        Loaded forecast dataframe
        
    Raises:
        DataFrameNotFoundError: If forecast file does not exist
        DataIntegrityError: If forecast data fails integrity check
        FileOperationError: If file operation fails
    """
    # Validate the product name
    validate_product(product)
    
    # Get the file path for the forecast
    file_path = get_forecast_file_path(forecast_timestamp, product, format)
    
    # Check if the file exists
    if not file_path.exists():
        logger.error(f"Forecast file not found: {file_path}")
        raise DataFrameNotFoundError(f"Forecast not found for {product} at {forecast_timestamp}", product, forecast_timestamp)
    
    # Load the dataframe from the file
    try:
        df = load_dataframe(file_path, format)
        if df is None:
            raise FileOperationError(f"Failed to load dataframe", file_path, "read")
    except Exception as e:
        logger.error(f"Failed to load dataframe from {file_path}: {str(e)}")
        raise FileOperationError(f"Failed to load dataframe: {str(e)}", file_path, "read")
    
    # Check storage integrity
    is_valid, integrity_issues = check_storage_integrity(df)
    if not is_valid:
        logger.warning(f"Integrity check failed for {product} forecast at {forecast_timestamp}")
        raise DataIntegrityError("Forecast data failed integrity check", file_path, integrity_issues)
    
    # Upgrade schema if needed
    df = upgrade_schema_if_needed(df)
    
    logger.info(f"Successfully loaded {product} forecast for {forecast_timestamp}")
    return df


@log_execution_time
@log_exceptions
def load_latest_forecast(
    product: str,
    format: str = DEFAULT_FORMAT
) -> pd.DataFrame:
    """
    Loads the latest forecast for a product.
    
    Args:
        product: Price product identifier
        format: File format (default: 'parquet')
        
    Returns:
        Latest forecast dataframe
        
    Raises:
        DataFrameNotFoundError: If latest forecast file does not exist
        DataIntegrityError: If forecast data fails integrity check
        FileOperationError: If file operation fails
    """
    # Validate the product name
    validate_product(product)
    
    # Get the latest file path
    latest_path = get_latest_file_path(product, format)
    
    # Check if the file exists
    if not latest_path.exists():
        logger.error(f"Latest forecast file not found: {latest_path}")
        raise DataFrameNotFoundError(f"Latest forecast not found for {product}", product, datetime.datetime.now())
    
    # Load the dataframe from the file
    try:
        df = load_dataframe(latest_path, format)
        if df is None:
            raise FileOperationError(f"Failed to load dataframe", latest_path, "read")
    except Exception as e:
        logger.error(f"Failed to load dataframe from {latest_path}: {str(e)}")
        raise FileOperationError(f"Failed to load dataframe: {str(e)}", latest_path, "read")
    
    # Check storage integrity
    is_valid, integrity_issues = check_storage_integrity(df)
    if not is_valid:
        logger.warning(f"Integrity check failed for latest {product} forecast")
        raise DataIntegrityError("Forecast data failed integrity check", latest_path, integrity_issues)
    
    # Upgrade schema if needed
    df = upgrade_schema_if_needed(df)
    
    logger.info(f"Successfully loaded latest {product} forecast")
    return df


@log_execution_time
@log_exceptions
def delete_forecast(
    forecast_timestamp: datetime.datetime,
    product: str
) -> bool:
    """
    Deletes a forecast from storage and index.
    
    Args:
        forecast_timestamp: Timestamp of the forecast to delete
        product: Price product identifier
        
    Returns:
        True if successful, False if forecast not found
    """
    # Validate the product name
    validate_product(product)
    
    # Get the file path for the forecast
    file_path = get_forecast_file_path(forecast_timestamp, product)
    
    # Check if the file exists
    if not file_path.exists():
        logger.warning(f"Cannot delete forecast - file not found: {file_path}")
        return False
    
    # Remove the file
    try:
        os.remove(file_path)
    except Exception as e:
        logger.error(f"Failed to delete file {file_path}: {str(e)}")
        raise FileOperationError(f"Failed to delete file: {str(e)}", file_path, "delete")
    
    # Remove the forecast from the index
    remove_forecast_from_index(forecast_timestamp, product)
    
    # Update the latest links
    update_latest_links()
    
    logger.info(f"Successfully deleted {product} forecast for {forecast_timestamp}")
    return True


@log_execution_time
@log_exceptions
def get_forecasts_by_date_range(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    product: Optional[str] = None
) -> Dict[datetime.datetime, pd.DataFrame]:
    """
    Retrieves forecasts within a date range.
    
    Args:
        start_date: Start date for the query
        end_date: End date for the query
        product: Optional product filter
        
    Returns:
        Dictionary mapping timestamps to forecast dataframes
    """
    # If product is provided, validate it
    if product is not None:
        validate_product(product)
    
    # Query the index for forecasts in the date range
    index_results = query_index_by_date(start_date, end_date, product)
    
    # Get file paths for the matching forecasts
    file_paths = get_forecast_file_paths(index_results)
    
    # Initialize results dictionary
    results = {}
    
    # Load each forecast dataframe
    for timestamp, path in file_paths.items():
        try:
            # Extract format from file extension
            format = path.suffix.lstrip('.')
            
            # Load the dataframe
            df = load_dataframe(path, format)
            
            # Check integrity
            is_valid, _ = check_storage_integrity(df)
            
            if is_valid:
                # Upgrade schema if needed
                df = upgrade_schema_if_needed(df)
                results[timestamp] = df
            else:
                logger.warning(f"Skipping forecast at {timestamp} due to integrity issues")
        except Exception as e:
            logger.warning(f"Failed to load forecast at {timestamp} from {path}: {str(e)}")
    
    logger.info(f"Retrieved {len(results)} forecasts for date range {start_date} to {end_date}")
    return results


@log_exceptions
def get_forecast_metadata(
    forecast_timestamp: datetime.datetime,
    product: str
) -> Dict:
    """
    Extracts metadata from a stored forecast.
    
    Args:
        forecast_timestamp: Timestamp of the forecast
        product: Price product identifier
        
    Returns:
        Dictionary of forecast metadata
        
    Raises:
        DataFrameNotFoundError: If forecast file does not exist
        FileOperationError: If file operation fails
    """
    # Validate the product name
    validate_product(product)
    
    # Get the file path for the forecast
    file_path = get_forecast_file_path(forecast_timestamp, product)
    
    # Check if the file exists
    if not file_path.exists():
        logger.error(f"Forecast file not found: {file_path}")
        raise DataFrameNotFoundError(f"Forecast not found for {product} at {forecast_timestamp}", product, forecast_timestamp)
    
    # Load the dataframe from the file
    try:
        df = load_dataframe(file_path)
        if df is None:
            raise FileOperationError(f"Failed to load dataframe", file_path, "read")
    except Exception as e:
        logger.error(f"Failed to load dataframe from {file_path}: {str(e)}")
        raise FileOperationError(f"Failed to load dataframe: {str(e)}", file_path, "read")
    
    # Extract storage metadata
    metadata = extract_storage_metadata(df)
    
    # Add forecast timestamp and product to metadata
    metadata["forecast_timestamp"] = forecast_timestamp
    metadata["product"] = product
    
    logger.debug(f"Extracted metadata for {product} forecast at {forecast_timestamp}")
    return metadata


@log_exceptions
def check_forecast_exists(
    forecast_timestamp: datetime.datetime,
    product: str
) -> bool:
    """
    Checks if a forecast exists in storage.
    
    Args:
        forecast_timestamp: Timestamp of the forecast
        product: Price product identifier
        
    Returns:
        True if forecast exists, False otherwise
    """
    # Validate the product name
    validate_product(product)
    
    # Get the file path for the forecast
    file_path = get_forecast_file_path(forecast_timestamp, product)
    
    # Check if the file exists
    exists = file_path.exists()
    
    logger.debug(f"Forecast existence check for {product} at {forecast_timestamp}: {exists}")
    return exists


@log_execution_time
@log_exceptions
def copy_forecast(
    source_timestamp: datetime.datetime,
    target_timestamp: datetime.datetime,
    product: str,
    mark_as_fallback: bool = True
) -> pathlib.Path:
    """
    Creates a copy of a forecast with a new timestamp.
    
    Args:
        source_timestamp: Timestamp of the source forecast
        target_timestamp: Timestamp for the new forecast
        product: Price product identifier
        mark_as_fallback: Whether to mark the copy as a fallback
        
    Returns:
        Path to the new forecast file
        
    Raises:
        DataFrameNotFoundError: If source forecast does not exist
    """
    # Validate the product name
    validate_product(product)
    
    # Load the source forecast
    df = load_forecast(source_timestamp, product)
    
    # Adjust timestamps in the dataframe
    time_shift = target_timestamp - source_timestamp
    
    # If there's a timestamp column, shift it
    if "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"] + pd.Timedelta(time_shift)
    
    # If mark_as_fallback is True, set the is_fallback flag
    if mark_as_fallback and "is_fallback" in df.columns:
        df["is_fallback"] = True
    
    # Store the modified forecast
    file_path = store_forecast(df, target_timestamp, product, mark_as_fallback)
    
    logger.info(f"Successfully copied {product} forecast from {source_timestamp} to {target_timestamp}")
    return file_path


@log_execution_time
@log_exceptions
def get_storage_statistics() -> Dict:
    """
    Calculates statistics about stored forecasts.
    
    Returns:
        Dictionary with storage statistics
    """
    from .index_manager import get_index_statistics
    import shutil
    
    # Get index statistics
    index_stats = get_index_statistics()
    
    # Calculate storage space usage
    storage_path = pathlib.Path(get_forecast_file_path(datetime.datetime.now(), "DALMP")).parent.parent
    
    try:
        total_size = sum(f.stat().st_size for f in storage_path.glob('**/*') if f.is_file())
        total_size_mb = total_size / (1024 * 1024)
    except Exception as e:
        logger.warning(f"Failed to calculate storage size: {str(e)}")
        total_size_mb = 0
    
    # Compile statistics
    stats = {
        "total_forecasts": index_stats.get("total_entries", 0),
        "forecasts_by_product": index_stats.get("entries_by_product", {}),
        "forecasts_by_fallback": index_stats.get("entries_by_fallback", {}),
        "forecast_date_range": index_stats.get("date_range", {"min_date": "N/A", "max_date": "N/A"}),
        "storage_space_mb": round(total_size_mb, 2),
        "last_update": index_stats.get("most_recent_generation", "N/A")
    }
    
    logger.info(f"Calculated storage statistics: {stats['total_forecasts']} forecasts using {stats['storage_space_mb']} MB")
    return stats