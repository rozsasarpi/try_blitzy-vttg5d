"""
Main interface for the forecast storage system in the Electricity Market Price Forecasting System.

Provides a unified API for storing, retrieving, and managing forecast data with schema validation,
indexing, and fallback mechanisms. Coordinates between various storage components to ensure
data integrity and availability.
"""

import os
import pathlib
import datetime
from typing import Dict, List, Optional, Union

import pandas as pd  # version: 2.0.0

# Internal imports
from .dataframe_store import (
    store_forecast, 
    load_forecast, 
    load_latest_forecast,
    delete_forecast,
    get_forecasts_by_date_range,
    get_forecast_metadata,
    check_forecast_exists,
    copy_forecast,
    get_storage_statistics
)
from .path_resolver import (
    validate_product,
    get_base_storage_path,
    get_index_file_path
)
from .index_manager import (
    clean_index,
    rebuild_index,
    get_index_statistics,
    get_latest_forecast_metadata
)
from .schema_definitions import (
    get_schema_info
)
from .exceptions import (
    StorageError,
    DataFrameNotFoundError
)
from ..utils.file_utils import clean_old_forecasts
from ..utils.logging_utils import get_logger, log_execution_time, log_exceptions
from ..config.settings import (
    FORECAST_PRODUCTS,
    STORAGE_ROOT_DIR
)

# Configure logger
logger = get_logger(__name__)

# Default retention period for forecast data (90 days)
DEFAULT_RETENTION_DAYS = 90


@log_execution_time
@log_exceptions
def save_forecast(df: pd.DataFrame, 
                  forecast_timestamp: datetime.datetime, 
                  product: str,
                  is_fallback: bool = False) -> pathlib.Path:
    """
    Saves a forecast dataframe to storage with validation.
    
    Args:
        df: DataFrame to store
        forecast_timestamp: Timestamp of the forecast
        product: Forecast product identifier
        is_fallback: Whether this is a fallback forecast
        
    Returns:
        Path to the stored forecast file
        
    Raises:
        StorageError: If storage operation fails
    """
    logger.info(f"Saving {product} forecast for {forecast_timestamp}")
    
    # Validate inputs
    validate_product(product)
    
    # Delegate to dataframe_store implementation
    file_path = store_forecast(df, forecast_timestamp, product, is_fallback)
    
    logger.info(f"Successfully saved {product} forecast for {forecast_timestamp}")
    return file_path


@log_execution_time
@log_exceptions
def get_forecast(forecast_timestamp: datetime.datetime, product: str) -> pd.DataFrame:
    """
    Retrieves a forecast dataframe by timestamp and product.
    
    Args:
        forecast_timestamp: Timestamp of the forecast
        product: Forecast product identifier
        
    Returns:
        Forecast dataframe
        
    Raises:
        DataFrameNotFoundError: If forecast does not exist
        StorageError: If retrieval operation fails
    """
    logger.info(f"Retrieving {product} forecast for {forecast_timestamp}")
    
    # Validate inputs
    validate_product(product)
    
    try:
        # Delegate to dataframe_store implementation
        df = load_forecast(forecast_timestamp, product)
        
        logger.info(f"Successfully retrieved {product} forecast for {forecast_timestamp}")
        return df
    except DataFrameNotFoundError:
        logger.error(f"Forecast not found for {product} at {forecast_timestamp}")
        raise


@log_execution_time
@log_exceptions
def get_latest_forecast(product: str) -> pd.DataFrame:
    """
    Retrieves the latest forecast for a product.
    
    Args:
        product: Forecast product identifier
        
    Returns:
        Latest forecast dataframe
        
    Raises:
        DataFrameNotFoundError: If no forecast exists for the product
        StorageError: If retrieval operation fails
    """
    logger.info(f"Retrieving latest forecast for {product}")
    
    # Validate inputs
    validate_product(product)
    
    try:
        # Delegate to dataframe_store implementation
        df = load_latest_forecast(product)
        
        logger.info(f"Successfully retrieved latest {product} forecast")
        return df
    except DataFrameNotFoundError:
        logger.error(f"No forecast found for {product}")
        raise


@log_execution_time
@log_exceptions
def get_forecasts_for_period(start_date: datetime.datetime, 
                             end_date: datetime.datetime,
                             product: Optional[str] = None) -> Dict[datetime.datetime, pd.DataFrame]:
    """
    Retrieves forecasts for a specific time period.
    
    Args:
        start_date: Start date for the query
        end_date: End date for the query
        product: Optional product filter
        
    Returns:
        Dictionary mapping timestamps to forecast dataframes
    """
    logger.info(f"Retrieving forecasts from {start_date} to {end_date}" + 
                (f" for {product}" if product else " for all products"))
    
    # If product is provided, validate it
    if product is not None:
        validate_product(product)
    
    # Delegate to dataframe_store implementation
    result = get_forecasts_by_date_range(start_date, end_date, product)
    
    logger.info(f"Retrieved {len(result)} forecasts for the specified period")
    return result


@log_execution_time
@log_exceptions
def remove_forecast(forecast_timestamp: datetime.datetime, product: str) -> bool:
    """
    Removes a forecast from storage.
    
    Args:
        forecast_timestamp: Timestamp of the forecast
        product: Forecast product identifier
        
    Returns:
        True if successful, False if forecast not found
    """
    logger.info(f"Removing {product} forecast for {forecast_timestamp}")
    
    # Validate inputs
    validate_product(product)
    
    # Delegate to dataframe_store implementation
    result = delete_forecast(forecast_timestamp, product)
    
    if result:
        logger.info(f"Successfully removed {product} forecast for {forecast_timestamp}")
    else:
        logger.warning(f"Forecast not found for {product} at {forecast_timestamp}")
    
    return result


@log_exceptions
def check_forecast_availability(forecast_timestamp: datetime.datetime, product: str) -> bool:
    """
    Checks if a forecast is available in storage.
    
    Args:
        forecast_timestamp: Timestamp of the forecast
        product: Forecast product identifier
        
    Returns:
        True if forecast exists, False otherwise
    """
    # Validate inputs
    validate_product(product)
    
    # Delegate to dataframe_store implementation
    return check_forecast_exists(forecast_timestamp, product)


@log_execution_time
@log_exceptions
def duplicate_forecast(source_timestamp: datetime.datetime,
                      target_timestamp: datetime.datetime,
                      product: str,
                      mark_as_fallback: bool = True) -> pathlib.Path:
    """
    Creates a copy of a forecast with a new timestamp.
    
    Args:
        source_timestamp: Timestamp of the source forecast
        target_timestamp: Timestamp for the new forecast
        product: Forecast product identifier
        mark_as_fallback: Whether to mark the copy as a fallback
        
    Returns:
        Path to the new forecast file
    """
    logger.info(f"Duplicating {product} forecast from {source_timestamp} to {target_timestamp}")
    
    # Validate inputs
    validate_product(product)
    
    # Delegate to dataframe_store implementation
    file_path = copy_forecast(source_timestamp, target_timestamp, product, mark_as_fallback)
    
    logger.info(f"Successfully duplicated forecast to {target_timestamp}")
    return file_path


@log_exceptions
def get_forecast_info(forecast_timestamp: datetime.datetime, product: str) -> Dict:
    """
    Retrieves metadata about a specific forecast.
    
    Args:
        forecast_timestamp: Timestamp of the forecast
        product: Forecast product identifier
        
    Returns:
        Dictionary of forecast metadata
    """
    # Validate inputs
    validate_product(product)
    
    # Delegate to dataframe_store implementation
    return get_forecast_metadata(forecast_timestamp, product)


@log_exceptions
def get_latest_forecasts_info() -> Dict:
    """
    Retrieves metadata about the latest forecasts for all products.
    
    Returns:
        Dictionary mapping products to their latest forecast metadata
    """
    # Delegate to index_manager implementation
    return get_latest_forecast_metadata()


@log_execution_time
@log_exceptions
def maintain_storage(retention_days: Optional[int] = None) -> Dict:
    """
    Performs maintenance operations on the storage system.
    
    Args:
        retention_days: Days to keep forecasts (defaults to DEFAULT_RETENTION_DAYS)
        
    Returns:
        Dictionary with maintenance statistics
    """
    logger.info("Starting storage maintenance")
    
    # Use default if not provided
    if retention_days is None:
        retention_days = DEFAULT_RETENTION_DAYS
    
    # Clean old forecast files
    removed_count = clean_old_forecasts(retention_days)
    
    # Clean index to remove entries for deleted files
    index_stats = clean_index()
    
    # Compile statistics
    stats = {
        "removed_files": removed_count,
        "removed_index_entries": index_stats.get("removed_entries", 0),
        "remaining_index_entries": index_stats.get("remaining_entries", 0),
        "retention_days": retention_days
    }
    
    logger.info(f"Maintenance complete: removed {removed_count} files and {stats['removed_index_entries']} index entries")
    return stats


@log_execution_time
@log_exceptions
def rebuild_storage_index() -> Dict:
    """
    Rebuilds the storage index from scratch.
    
    Returns:
        Dictionary with rebuild statistics
    """
    logger.info("Starting storage index rebuild")
    
    # Delegate to index_manager implementation
    stats = rebuild_index()
    
    logger.info(f"Rebuild complete: indexed {stats.get('files_processed', 0)} files")
    return stats


@log_execution_time
@log_exceptions
def get_storage_info() -> Dict:
    """
    Retrieves information about the storage system.
    
    Returns:
        Dictionary with storage information
    """
    logger.info("Gathering storage system information")
    
    # Get storage statistics
    storage_stats = get_storage_statistics()
    
    # Get index statistics
    index_stats = get_index_statistics()
    
    # Get schema information
    schema_info = get_schema_info()
    
    # Get storage paths
    storage_paths = {
        "root_dir": str(get_base_storage_path()),
        "index_file": str(get_index_file_path())
    }
    
    # Compile information
    info = {
        "storage_stats": storage_stats,
        "index_stats": index_stats,
        "schema_info": schema_info,
        "storage_paths": storage_paths,
        "products": FORECAST_PRODUCTS
    }
    
    logger.info("Storage information gathered successfully")
    return info


@log_execution_time
@log_exceptions
def initialize_storage() -> bool:
    """
    Initializes the storage system if needed.
    
    Returns:
        True if initialization was performed, False if already initialized
    """
    logger.info("Checking storage system initialization")
    
    storage_root = pathlib.Path(STORAGE_ROOT_DIR)
    index_path = get_index_file_path()
    
    initialized = True
    
    # Check if storage root exists
    if not storage_root.exists():
        logger.info(f"Creating storage root directory: {storage_root}")
        storage_root.mkdir(parents=True, exist_ok=True)
        initialized = False
    
    # Check if index exists
    if not index_path.exists():
        logger.info(f"Index file not found, rebuilding: {index_path}")
        rebuild_storage_index()
        initialized = False
    
    if initialized:
        logger.info("Storage system already initialized")
    else:
        logger.info("Storage system initialization complete")
    
    return not initialized