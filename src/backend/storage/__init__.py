"""
Storage module for the Electricity Market Price Forecasting System.

This module provides a unified API for storing, retrieving, and managing forecast
dataframes with schema validation, indexing, and fallback mechanisms. It ensures
that stored forecasts meet quality requirements and can be efficiently retrieved
for visualization and downstream use.

Forecast data is stored as pandas dataframes with timestamps, following a
consistent schema enforced by pandera validation. The module includes
functionality for handling fallback scenarios when forecast generation fails.
"""

# Re-export storage manager functions
from .storage_manager import (
    save_forecast,
    get_forecast,
    get_latest_forecast,
    get_forecasts_for_period,
    remove_forecast,
    check_forecast_availability,
    duplicate_forecast,
    get_forecast_info,
    get_latest_forecasts_info,
    maintain_storage,
    rebuild_storage_index,
    get_storage_info,
    initialize_storage
)

# Re-export exception classes
from .exceptions import (
    StorageError,
    SchemaValidationError,
    FileOperationError,
    DataFrameNotFoundError,
    IndexUpdateError,
    StoragePathError,
    DataIntegrityError
)

# Re-export schema validation functions and constants
from .schema_definitions import (
    validate_forecast_schema,
    STORAGE_METADATA_FIELDS
)

# Initialize the storage system when the module is imported
# This ensures the storage directories and index exist
try:
    initialize_storage()
except Exception as e:
    import logging
    logging.getLogger(__name__).warning(f"Failed to initialize storage: {e}")