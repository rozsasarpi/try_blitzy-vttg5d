"""
Index management module for the Electricity Market Price Forecasting System.

This module provides functionality to create, update, query, and maintain an index
of stored forecasts. The index enables efficient retrieval of forecasts by date,
product, and other criteria while maintaining links to the latest forecasts.
"""

import os
import pathlib
import datetime
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd  # version: 2.0.0
import shutil  # standard library

# Internal imports
from .path_resolver import (
    get_index_file_path,
    get_latest_file_path,
    get_base_storage_path,
    validate_product,
    create_backup_path
)
from .exceptions import IndexUpdateError, StorageError
from ..utils.file_utils import save_dataframe, load_dataframe, update_latest_link
from ..utils.logging_utils import get_logger, log_execution_time, log_exceptions
from ..config.settings import FORECAST_PRODUCTS, STORAGE_INDEX_FILE

# Set up logger
logger = get_logger(__name__)

# Define the schema for the index DataFrame
INDEX_SCHEMA = {
    "timestamp": "datetime64[ns]",
    "product": "str",
    "file_path": "str",
    "generation_timestamp": "datetime64[ns]",
    "is_fallback": "bool"
}

@log_exceptions
def initialize_index() -> bool:
    """
    Creates a new empty index if it doesn't exist.
    
    Returns:
        bool: True if index was created, False if it already existed
    """
    index_path = get_index_file_path()
    
    # Check if index already exists
    if index_path.exists():
        logger.debug(f"Index already exists at {index_path}")
        return False
    
    # Create an empty DataFrame with the defined schema
    empty_df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in INDEX_SCHEMA.items()})
    
    # Save the empty DataFrame as the new index
    save_dataframe(empty_df, index_path)
    
    logger.info(f"Created new empty forecast index at {index_path}")
    return True

@log_exceptions
def load_index() -> pd.DataFrame:
    """
    Loads the forecast index from disk.
    
    Returns:
        pandas.DataFrame: DataFrame containing the forecast index
    """
    index_path = get_index_file_path()
    
    # Check if index exists, if not initialize it
    if not index_path.exists():
        logger.info(f"Index file not found at {index_path}, initializing new index")
        initialize_index()
    
    # Load the index DataFrame
    index_df = load_dataframe(index_path)
    
    # Ensure timestamp columns are datetime type
    if "timestamp" in index_df.columns:
        index_df["timestamp"] = pd.to_datetime(index_df["timestamp"])
    
    if "generation_timestamp" in index_df.columns:
        index_df["generation_timestamp"] = pd.to_datetime(index_df["generation_timestamp"])
    
    logger.debug(f"Loaded forecast index with {len(index_df)} entries")
    return index_df

@log_exceptions
def save_index(index_df: pd.DataFrame) -> bool:
    """
    Saves the forecast index to disk.
    
    Args:
        index_df: DataFrame containing the index to save
        
    Returns:
        bool: True if successful, False otherwise
    """
    index_path = get_index_file_path()
    
    # Create a backup of the existing index if it exists
    if index_path.exists():
        backup_path = create_backup_path(index_path)
        try:
            shutil.copy2(index_path, backup_path)
            logger.debug(f"Created backup of index at {backup_path}")
        except Exception as e:
            logger.warning(f"Failed to create backup of index: {str(e)}")
    
    # Save the updated index
    success = save_dataframe(index_df, index_path)
    
    if success:
        logger.info(f"Successfully saved forecast index with {len(index_df)} entries")
    else:
        logger.error(f"Failed to save forecast index")
        
    return success

@log_execution_time
@log_exceptions
def add_forecast_to_index(
    file_path: pathlib.Path,
    timestamp: datetime.datetime,
    product: str,
    generation_timestamp: datetime.datetime,
    is_fallback: bool
) -> bool:
    """
    Adds a forecast to the index.
    
    Args:
        file_path: Path to the forecast file
        timestamp: Timestamp of the forecast
        product: Product identifier
        generation_timestamp: When the forecast was generated
        is_fallback: Whether this is a fallback forecast
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Validate product
    validate_product(product)
    
    # Load the current index
    index_df = load_index()
    
    # Create a new entry
    new_entry = {
        "timestamp": timestamp,
        "product": product,
        "file_path": str(file_path),
        "generation_timestamp": generation_timestamp,
        "is_fallback": is_fallback
    }
    
    # Check if an entry for this timestamp and product already exists
    mask = (index_df["timestamp"] == timestamp) & (index_df["product"] == product)
    
    if mask.any():
        # Update existing entry
        logger.debug(f"Updating existing index entry for {product} at {timestamp}")
        for key, value in new_entry.items():
            index_df.loc[mask, key] = value
    else:
        # Add new entry
        logger.debug(f"Adding new index entry for {product} at {timestamp}")
        index_df = pd.concat([index_df, pd.DataFrame([new_entry])], ignore_index=True)
    
    # Save the updated index
    success = save_index(index_df)
    
    if success:
        logger.info(f"Added forecast to index: {product} at {timestamp}")
    
    return success

@log_exceptions
def remove_forecast_from_index(timestamp: datetime.datetime, product: str) -> bool:
    """
    Removes a forecast from the index.
    
    Args:
        timestamp: Timestamp of the forecast to remove
        product: Product identifier
        
    Returns:
        bool: True if successful, False if forecast not found
    """
    # Validate product
    validate_product(product)
    
    # Load the current index
    index_df = load_index()
    
    # Check if an entry for this timestamp and product exists
    mask = (index_df["timestamp"] == timestamp) & (index_df["product"] == product)
    
    if not mask.any():
        logger.warning(f"No index entry found for {product} at {timestamp}")
        return False
    
    # Remove the entry
    index_df = index_df[~mask]
    
    # Save the updated index
    success = save_index(index_df)
    
    if success:
        logger.info(f"Removed forecast from index: {product} at {timestamp}")
    
    return success

@log_exceptions
def query_index_by_date(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    product: Optional[str] = None
) -> pd.DataFrame:
    """
    Queries the index for forecasts within a date range.
    
    Args:
        start_date: Start date for the query range
        end_date: End date for the query range
        product: Optional product to filter by
        
    Returns:
        pandas.DataFrame: DataFrame with matching forecast entries
    """
    # Validate product if provided
    if product is not None:
        validate_product(product)
    
    # Load the current index
    index_df = load_index()
    
    # Filter by date range
    mask = (index_df["timestamp"] >= start_date) & (index_df["timestamp"] <= end_date)
    
    # Add product filter if specified
    if product is not None:
        mask = mask & (index_df["product"] == product)
    
    # Apply the filter
    result_df = index_df[mask]
    
    logger.debug(
        f"Query returned {len(result_df)} forecasts between {start_date} and {end_date}"
        + (f" for product {product}" if product else "")
    )
    
    return result_df

@log_exceptions
def get_forecast_file_paths(query_result: pd.DataFrame) -> Dict[datetime.datetime, pathlib.Path]:
    """
    Gets file paths for forecasts matching criteria.
    
    Args:
        query_result: DataFrame with query results from query_index_by_date
        
    Returns:
        dict: Dictionary mapping timestamps to file paths
    """
    result = {}
    
    for _, row in query_result.iterrows():
        timestamp = row["timestamp"]
        file_path_str = row["file_path"]
        
        # Convert string path to Path object
        file_path = pathlib.Path(file_path_str)
        
        # Add to results dictionary
        result[timestamp] = file_path
    
    logger.debug(f"Extracted {len(result)} file paths from query result")
    return result

@log_execution_time
@log_exceptions
def update_latest_links() -> Dict[str, pathlib.Path]:
    """
    Updates symbolic links to the latest forecasts.
    
    Returns:
        dict: Dictionary of products and their latest forecast paths
    """
    # Load the current index
    index_df = load_index()
    
    result = {}
    
    # Process each product
    for product in FORECAST_PRODUCTS:
        # Filter index for this product
        product_df = index_df[index_df["product"] == product]
        
        if product_df.empty:
            logger.warning(f"No forecasts found for product {product}")
            continue
        
        # Get the most recent forecast by generation time
        latest = product_df.loc[product_df["generation_timestamp"].idxmax()]
        
        # Get the file path
        file_path_str = latest["file_path"]
        file_path = pathlib.Path(file_path_str)
        
        # Update the symbolic link
        if update_latest_link(file_path, product):
            result[product] = file_path
            logger.debug(f"Updated latest link for {product} to {file_path}")
        
    logger.info(f"Updated latest links for {len(result)} products")
    return result

@log_exceptions
def get_latest_forecast_metadata() -> Dict[str, Dict]:
    """
    Gets metadata for the latest forecasts.
    
    Returns:
        dict: Dictionary of products and their latest forecast metadata
    """
    # Load the current index
    index_df = load_index()
    
    result = {}
    
    # Process each product
    for product in FORECAST_PRODUCTS:
        # Filter index for this product
        product_df = index_df[index_df["product"] == product]
        
        if product_df.empty:
            logger.warning(f"No forecasts found for product {product}")
            continue
        
        # Get the most recent forecast by generation time
        latest = product_df.loc[product_df["generation_timestamp"].idxmax()]
        
        # Extract metadata
        result[product] = {
            "timestamp": latest["timestamp"],
            "generation_timestamp": latest["generation_timestamp"],
            "is_fallback": latest["is_fallback"]
        }
    
    logger.debug(f"Retrieved metadata for {len(result)} latest forecasts")
    return result

@log_execution_time
@log_exceptions
def clean_index() -> Dict[str, int]:
    """
    Removes entries from the index that point to non-existent files.
    
    Returns:
        dict: Dictionary with cleaning statistics
    """
    # Load the current index
    index_df = load_index()
    
    # Initialize counters
    total_entries = len(index_df)
    removed_entries = 0
    
    # Create a mask for rows to keep
    rows_to_remove = []
    
    # Check each row
    for idx, row in index_df.iterrows():
        file_path_str = row["file_path"]
        file_path = pathlib.Path(file_path_str)
        
        # Check if file exists
        if not file_path.exists():
            rows_to_remove.append(idx)
            logger.debug(f"Marking for removal: {file_path} (not found)")
    
    # Remove the rows
    if rows_to_remove:
        index_df = index_df.drop(rows_to_remove)
        removed_entries = len(rows_to_remove)
        
        # Save the updated index
        save_index(index_df)
        
        # Update latest links
        update_latest_links()
        
        logger.info(f"Removed {removed_entries} entries pointing to non-existent files")
    else:
        logger.info("No invalid entries found in index")
    
    # Compile statistics
    stats = {
        "total_entries": total_entries,
        "removed_entries": removed_entries,
        "remaining_entries": total_entries - removed_entries
    }
    
    return stats

@log_execution_time
@log_exceptions
def rebuild_index() -> Dict[str, int]:
    """
    Rebuilds the entire index by scanning the storage directory.
    
    Returns:
        dict: Dictionary with rebuild statistics
    """
    # Get base storage path
    base_path = get_base_storage_path()
    
    # Create a backup of the existing index if it exists
    index_path = get_index_file_path()
    if index_path.exists():
        backup_path = create_backup_path(index_path)
        try:
            shutil.copy2(index_path, backup_path)
            logger.info(f"Created backup of index at {backup_path}")
        except Exception as e:
            logger.warning(f"Failed to create backup of index: {str(e)}")
    
    # Initialize counters
    files_found = 0
    files_processed = 0
    files_skipped = 0
    
    # Create a new empty DataFrame for the index
    new_index = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in INDEX_SCHEMA.items()})
    
    # Walk through the directory structure
    for year_dir in base_path.glob('[0-9][0-9][0-9][0-9]'):
        if not year_dir.is_dir():
            continue
            
        for month_dir in year_dir.glob('[0-9][0-9]'):
            if not month_dir.is_dir():
                continue
                
            # Look for forecast files
            for file_path in month_dir.glob('*.*'):
                if not file_path.is_file() or file_path.is_symlink():
                    continue
                
                files_found += 1
                
                try:
                    # Parse the filename to get day and product
                    filename = file_path.name
                    if '_' not in filename:
                        logger.warning(f"Skipping file with unexpected name format: {filename}")
                        files_skipped += 1
                        continue
                    
                    # Expected format: day_product.extension
                    day_str, rest = filename.split('_', 1)
                    if '.' not in rest:
                        logger.warning(f"Skipping file with unexpected name format: {filename}")
                        files_skipped += 1
                        continue
                    
                    product, extension = rest.rsplit('.', 1)
                    
                    # Validate product
                    try:
                        validate_product(product)
                    except StorageError:
                        logger.warning(f"Skipping file with invalid product: {filename}")
                        files_skipped += 1
                        continue
                    
                    # Extract year, month, day
                    year = year_dir.name
                    month = month_dir.name
                    day = day_str
                    
                    # Create timestamp
                    try:
                        timestamp = datetime.datetime.strptime(f"{year}-{month}-{day} 00:00:00", "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        logger.warning(f"Skipping file with invalid date: {filename}")
                        files_skipped += 1
                        continue
                    
                    # Load the forecast file to extract metadata
                    forecast_df = load_dataframe(file_path)
                    
                    if forecast_df is None:
                        logger.warning(f"Skipping file that could not be loaded: {file_path}")
                        files_skipped += 1
                        continue
                    
                    # Extract generation timestamp and fallback status
                    if "generation_timestamp" in forecast_df.columns:
                        generation_timestamp = forecast_df["generation_timestamp"].iloc[0]
                    else:
                        # Use file modification time as fallback
                        mod_time = file_path.stat().st_mtime
                        generation_timestamp = datetime.datetime.fromtimestamp(mod_time)
                    
                    is_fallback = False
                    if "is_fallback" in forecast_df.columns:
                        is_fallback = forecast_df["is_fallback"].iloc[0]
                    
                    # Add to the new index
                    new_entry = {
                        "timestamp": timestamp,
                        "product": product,
                        "file_path": str(file_path),
                        "generation_timestamp": generation_timestamp,
                        "is_fallback": is_fallback
                    }
                    
                    new_index = pd.concat([new_index, pd.DataFrame([new_entry])], ignore_index=True)
                    files_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {str(e)}")
                    files_skipped += 1
    
    # Save the rebuilt index
    save_index(new_index)
    
    # Update latest links
    update_latest_links()
    
    # Compile statistics
    stats = {
        "files_found": files_found,
        "files_processed": files_processed,
        "files_skipped": files_skipped,
        "index_entries": len(new_index)
    }
    
    logger.info(f"Rebuilt index with {stats['index_entries']} entries from {stats['files_processed']} files")
    return stats

@log_exceptions
def get_index_statistics() -> Dict[str, Union[int, str, Dict]]:
    """
    Calculates statistics about the index.
    
    Returns:
        dict: Dictionary with index statistics
    """
    # Load the current index
    index_df = load_index()
    
    # Initialize the statistics dictionary
    stats = {}
    
    # Total number of entries
    stats["total_entries"] = len(index_df)
    
    # Entries per product
    if not index_df.empty:
        product_counts = index_df["product"].value_counts().to_dict()
        stats["entries_by_product"] = product_counts
    else:
        stats["entries_by_product"] = {}
    
    # Entries by fallback status
    if not index_df.empty and "is_fallback" in index_df.columns:
        fallback_counts = index_df["is_fallback"].value_counts().to_dict()
        stats["entries_by_fallback"] = fallback_counts
    else:
        stats["entries_by_fallback"] = {}
    
    # Date range
    if not index_df.empty and "timestamp" in index_df.columns:
        min_date = index_df["timestamp"].min()
        max_date = index_df["timestamp"].max()
        stats["date_range"] = {
            "min_date": min_date.strftime("%Y-%m-%d") if min_date is not pd.NaT else "N/A",
            "max_date": max_date.strftime("%Y-%m-%d") if max_date is not pd.NaT else "N/A"
        }
    else:
        stats["date_range"] = {"min_date": "N/A", "max_date": "N/A"}
    
    # Most recent generation
    if not index_df.empty and "generation_timestamp" in index_df.columns:
        most_recent = index_df["generation_timestamp"].max()
        stats["most_recent_generation"] = most_recent.strftime("%Y-%m-%d %H:%M:%S") if most_recent is not pd.NaT else "N/A"
    else:
        stats["most_recent_generation"] = "N/A"
    
    logger.debug(f"Calculated index statistics: {len(index_df)} total entries")
    return stats