"""
Implements data transformation functionality for the data ingestion pipeline of the Electricity Market Price 
Forecasting System. This module transforms raw data from external sources into standardized formats suitable 
for the forecasting engine, handling data cleaning, normalization, and conversion between different representations.
"""

# Standard library imports
import datetime
from typing import Dict, List, Any, Optional

# External imports
import numpy as np  # version: 1.24.0+
import pandas as pd  # version: 2.0.0+

# Internal imports
from .exceptions import DataTransformationError
from ..models.data_models import (
    LoadForecast,
    HistoricalPrice,
    GenerationForecast
)
from ..models.validation_models import ValidationResult
from ..utils.date_utils import localize_to_cst
from ..utils.logging_utils import get_logger, log_execution_time
from ..config.settings import FORECAST_PRODUCTS

# Configure logger
logger = get_logger(__name__)


@log_execution_time
def normalize_load_forecast_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes load forecast data to a standard format.
    
    Args:
        df: Input DataFrame with load forecast data
        
    Returns:
        Normalized load forecast dataframe
    """
    logger.info("Normalizing load forecast data")
    
    # Check if dataframe is None or empty
    if df is None or df.empty:
        logger.warning("Empty load forecast dataframe provided")
        return pd.DataFrame()
    
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Ensure column names are standardized (lowercase, no spaces)
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Ensure required columns exist
    required_columns = ['timestamp', 'load_mw', 'region']
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Required column '{col}' not found in load forecast data")
            raise DataTransformationError(
                "load_forecast", 
                "column_normalization", 
                ValueError(f"Missing required column: {col}")
            )
    
    # Ensure timestamp column is in datetime format
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    except Exception as e:
        logger.error(f"Failed to convert timestamp column to datetime: {str(e)}")
        raise DataTransformationError(
            "load_forecast", 
            "timestamp_conversion", 
            e
        )
    
    # Convert timestamps to CST timezone
    df['timestamp'] = df['timestamp'].apply(localize_to_cst)
    
    # Ensure load_mw values are numeric (float)
    try:
        df['load_mw'] = pd.to_numeric(df['load_mw'], errors='coerce')
    except Exception as e:
        logger.error(f"Failed to convert load_mw to numeric: {str(e)}")
        raise DataTransformationError(
            "load_forecast", 
            "numeric_conversion", 
            e
        )
    
    # Drop rows with NaN load_mw values
    if df['load_mw'].isna().any():
        logger.warning(f"Dropping {df['load_mw'].isna().sum()} rows with missing load values")
        df = df.dropna(subset=['load_mw'])
    
    # Ensure region values are uppercase strings
    df['region'] = df['region'].astype(str).str.upper()
    
    # Sort dataframe by timestamp
    df = df.sort_values('timestamp')
    
    logger.info(f"Normalized load forecast data with {len(df)} rows")
    return df


@log_execution_time
def normalize_historical_prices_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes historical price data to a standard format.
    
    Args:
        df: Input DataFrame with historical price data
        
    Returns:
        Normalized historical price dataframe
    """
    logger.info("Normalizing historical price data")
    
    # Check if dataframe is None or empty
    if df is None or df.empty:
        logger.warning("Empty historical price dataframe provided")
        return pd.DataFrame()
    
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Ensure column names are standardized (lowercase, no spaces)
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Ensure required columns exist
    required_columns = ['timestamp', 'product', 'price', 'node']
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Required column '{col}' not found in historical price data")
            raise DataTransformationError(
                "historical_prices", 
                "column_normalization", 
                ValueError(f"Missing required column: {col}")
            )
    
    # Ensure timestamp column is in datetime format
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    except Exception as e:
        logger.error(f"Failed to convert timestamp column to datetime: {str(e)}")
        raise DataTransformationError(
            "historical_prices", 
            "timestamp_conversion", 
            e
        )
    
    # Convert timestamps to CST timezone
    df['timestamp'] = df['timestamp'].apply(localize_to_cst)
    
    # Ensure price values are numeric (float)
    try:
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
    except Exception as e:
        logger.error(f"Failed to convert price to numeric: {str(e)}")
        raise DataTransformationError(
            "historical_prices", 
            "numeric_conversion", 
            e
        )
    
    # Drop rows with NaN price values
    if df['price'].isna().any():
        logger.warning(f"Dropping {df['price'].isna().sum()} rows with missing price values")
        df = df.dropna(subset=['price'])
    
    # Ensure product values are uppercase strings
    df['product'] = df['product'].astype(str).str.upper()
    
    # Filter to include only products in FORECAST_PRODUCTS
    valid_products_mask = df['product'].isin(FORECAST_PRODUCTS)
    if not valid_products_mask.all():
        invalid_products = df.loc[~valid_products_mask, 'product'].unique()
        logger.warning(f"Filtering out invalid products: {invalid_products}")
        df = df[valid_products_mask]
    
    # Ensure node values are uppercase strings
    df['node'] = df['node'].astype(str).str.upper()
    
    # Sort dataframe by timestamp and product
    df = df.sort_values(['timestamp', 'product'])
    
    logger.info(f"Normalized historical price data with {len(df)} rows")
    return df


@log_execution_time
def normalize_generation_forecast_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes generation forecast data to a standard format.
    
    Args:
        df: Input DataFrame with generation forecast data
        
    Returns:
        Normalized generation forecast dataframe
    """
    logger.info("Normalizing generation forecast data")
    
    # Check if dataframe is None or empty
    if df is None or df.empty:
        logger.warning("Empty generation forecast dataframe provided")
        return pd.DataFrame()
    
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Ensure column names are standardized (lowercase, no spaces)
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Ensure required columns exist
    required_columns = ['timestamp', 'fuel_type', 'generation_mw', 'region']
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Required column '{col}' not found in generation forecast data")
            raise DataTransformationError(
                "generation_forecast", 
                "column_normalization", 
                ValueError(f"Missing required column: {col}")
            )
    
    # Ensure timestamp column is in datetime format
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    except Exception as e:
        logger.error(f"Failed to convert timestamp column to datetime: {str(e)}")
        raise DataTransformationError(
            "generation_forecast", 
            "timestamp_conversion", 
            e
        )
    
    # Convert timestamps to CST timezone
    df['timestamp'] = df['timestamp'].apply(localize_to_cst)
    
    # Ensure generation_mw values are numeric (float)
    try:
        df['generation_mw'] = pd.to_numeric(df['generation_mw'], errors='coerce')
    except Exception as e:
        logger.error(f"Failed to convert generation_mw to numeric: {str(e)}")
        raise DataTransformationError(
            "generation_forecast", 
            "numeric_conversion", 
            e
        )
    
    # Drop rows with NaN generation_mw values
    if df['generation_mw'].isna().any():
        logger.warning(f"Dropping {df['generation_mw'].isna().sum()} rows with missing generation values")
        df = df.dropna(subset=['generation_mw'])
    
    # Ensure fuel_type values are lowercase strings (by convention)
    df['fuel_type'] = df['fuel_type'].astype(str).str.lower()
    
    # Ensure region values are uppercase strings
    df['region'] = df['region'].astype(str).str.upper()
    
    # Sort dataframe by timestamp and fuel_type
    df = df.sort_values(['timestamp', 'fuel_type'])
    
    logger.info(f"Normalized generation forecast data with {len(df)} rows")
    return df


@log_execution_time
def models_to_dataframe(models: List, model_type: str) -> pd.DataFrame:
    """
    Converts a list of data model instances to a pandas DataFrame.
    
    Args:
        models: List of data model instances
        model_type: Type of model ('load_forecast', 'historical_price', 'generation_forecast')
        
    Returns:
        DataFrame created from model instances
    """
    logger.info(f"Converting {len(models)} {model_type} models to dataframe")
    
    if not models:
        return pd.DataFrame()
    
    # Create list of dictionaries
    rows = [model.to_dataframe_row() for model in models]
    
    # Convert to DataFrame
    df = pd.DataFrame(rows)
    
    logger.info(f"Created dataframe with {len(df)} rows from {model_type} models")
    return df


@log_execution_time
def dataframe_to_models(df: pd.DataFrame, model_type: str) -> List:
    """
    Converts a pandas DataFrame to a list of data model instances.
    
    Args:
        df: Input DataFrame
        model_type: Type of model to create ('load_forecast', 'historical_price', 'generation_forecast')
        
    Returns:
        List of data model instances
    """
    logger.info(f"Converting dataframe with {len(df)} rows to {model_type} models")
    
    if df.empty:
        return []
    
    # Determine appropriate model class
    if model_type == 'load_forecast':
        model_class = LoadForecast
    elif model_type == 'historical_price':
        model_class = HistoricalPrice
    elif model_type == 'generation_forecast':
        model_class = GenerationForecast
    else:
        logger.error(f"Unknown model type: {model_type}")
        raise ValueError(f"Unknown model type: {model_type}")
    
    # Create model instances for each row
    models = []
    for _, row in df.iterrows():
        try:
            model = model_class(**row.to_dict())
            models.append(model)
        except Exception as e:
            logger.warning(f"Failed to create {model_type} model from row: {str(e)}")
            # Continue with other rows
    
    logger.info(f"Created {len(models)} {model_type} models from dataframe")
    return models


@log_execution_time
def resample_time_series(
    df: pd.DataFrame, 
    timestamp_column: str, 
    freq: str, 
    aggregation_rules: dict
) -> pd.DataFrame:
    """
    Resamples time series data to a specified frequency.
    
    Args:
        df: Input DataFrame with time series data
        timestamp_column: Name of the timestamp column
        freq: Frequency string (e.g., 'H' for hourly)
        aggregation_rules: Dictionary mapping column names to aggregation functions
        
    Returns:
        Resampled dataframe
    """
    logger.info(f"Resampling time series data to {freq} frequency")
    
    if df.empty:
        return df.copy()
    
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Set timestamp column as index if not already
    if df.index.name != timestamp_column:
        df = df.set_index(timestamp_column)
    
    # Ensure index is datetime type and sorted
    if not isinstance(df.index, pd.DatetimeIndex):
        logger.warning("Converting index to DatetimeIndex for resampling")
        df.index = pd.to_datetime(df.index)
    
    # Sort index
    df = df.sort_index()
    
    # Apply resampling with specified aggregation rules
    try:
        resampled_df = df.resample(freq).agg(aggregation_rules)
    except Exception as e:
        logger.error(f"Failed to resample dataframe: {str(e)}")
        raise DataTransformationError(
            "time_series", 
            "resample_operation", 
            e
        )
    
    # Reset index to make timestamp a column again
    resampled_df = resampled_df.reset_index()
    
    logger.info(f"Resampled dataframe from {len(df)} to {len(resampled_df)} rows")
    return resampled_df


@log_execution_time
def align_timestamps(
    dataframes: Dict[str, pd.DataFrame], 
    timestamp_column: str, 
    freq: str
) -> Dict[str, pd.DataFrame]:
    """
    Aligns timestamps across multiple dataframes to ensure consistent time points.
    
    Args:
        dataframes: Dictionary mapping names to DataFrames
        timestamp_column: Name of the timestamp column
        freq: Frequency string (e.g., 'H' for hourly)
        
    Returns:
        Dictionary of aligned dataframes
    """
    logger.info(f"Aligning timestamps across {len(dataframes)} dataframes")
    
    if not dataframes:
        return {}
    
    # Extract unique timestamps from all dataframes
    all_timestamps = set()
    for name, df in dataframes.items():
        if df is not None and not df.empty and timestamp_column in df.columns:
            all_timestamps.update(df[timestamp_column].tolist())
    
    # Create a common DatetimeIndex
    if not all_timestamps:
        logger.warning("No timestamps found in any dataframe")
        return {name: df.copy() for name, df in dataframes.items()}
    
    # Convert to list and sort
    all_timestamps = sorted(all_timestamps)
    
    # Create regular frequency range covering all timestamps
    min_timestamp = min(all_timestamps)
    max_timestamp = max(all_timestamps)
    common_index = pd.date_range(start=min_timestamp, end=max_timestamp, freq=freq)
    
    aligned_dataframes = {}
    
    for name, df in dataframes.items():
        if df is None or df.empty:
            aligned_dataframes[name] = df.copy() if df is not None else pd.DataFrame()
            continue
        
        df_copy = df.copy()
        
        # Set timestamp as index
        if timestamp_column in df_copy.columns:
            df_copy = df_copy.set_index(timestamp_column)
            
            # Reindex to common index
            aligned_df = df_copy.reindex(common_index)
            
            # Handle missing values according to data type
            for column in aligned_df.columns:
                if pd.api.types.is_numeric_dtype(aligned_df[column]):
                    # Interpolate numeric columns
                    aligned_df[column] = aligned_df[column].interpolate(method='time')
                else:
                    # Forward fill non-numeric columns
                    aligned_df[column] = aligned_df[column].ffill()
            
            # Reset index to make timestamp a column again
            aligned_df = aligned_df.reset_index()
            aligned_df = aligned_df.rename(columns={'index': timestamp_column})
            
            aligned_dataframes[name] = aligned_df
        else:
            logger.warning(f"DataFrame '{name}' does not have column '{timestamp_column}', skipping alignment")
            aligned_dataframes[name] = df_copy
    
    logger.info(f"Aligned {len(aligned_dataframes)} dataframes to {len(common_index)} timestamps")
    return aligned_dataframes


@log_execution_time
def merge_dataframes(
    dataframes: List[pd.DataFrame], 
    suffixes: List[str], 
    timestamp_column: str = 'timestamp', 
    how: str = 'outer'
) -> pd.DataFrame:
    """
    Merges multiple dataframes on a common timestamp column.
    
    Args:
        dataframes: List of DataFrames to merge
        suffixes: List of suffixes to apply to overlapping columns (must be same length as dataframes - 1)
        timestamp_column: Name of the timestamp column to merge on
        how: Type of merge to perform ('left', 'right', 'outer', 'inner')
        
    Returns:
        Merged dataframe
    """
    logger.info(f"Merging {len(dataframes)} dataframes")
    
    if not dataframes:
        return pd.DataFrame()
    
    if len(dataframes) == 1:
        return dataframes[0].copy()
    
    if len(suffixes) != len(dataframes) - 1:
        logger.error(f"Number of suffixes ({len(suffixes)}) must be one less than number of dataframes ({len(dataframes)})")
        raise ValueError("Incorrect number of suffixes provided")
    
    # Start with the first dataframe
    result = dataframes[0].copy()
    
    # Iteratively merge with remaining dataframes
    for i, df in enumerate(dataframes[1:]):
        if df is None or df.empty:
            logger.warning(f"Skipping empty dataframe at index {i+1}")
            continue
            
        # Check that timestamp column exists in both dataframes
        if timestamp_column not in result.columns or timestamp_column not in df.columns:
            logger.error(f"Timestamp column '{timestamp_column}' not found in all dataframes")
            raise ValueError(f"Timestamp column '{timestamp_column}' missing from one or more dataframes")
        
        # Merge with current result
        try:
            result = pd.merge(
                result, 
                df, 
                on=timestamp_column, 
                how=how, 
                suffixes=('', f'_{suffixes[i]}')
            )
        except Exception as e:
            logger.error(f"Failed to merge dataframe at index {i+1}: {str(e)}")
            raise DataTransformationError(
                "merge_operation", 
                "dataframe_merge", 
                e
            )
    
    logger.info(f"Merged dataframe has {len(result)} rows and {len(result.columns)} columns")
    return result


@log_execution_time
def pivot_generation_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots generation forecast data to create columns for each fuel type.
    
    Args:
        df: DataFrame with generation forecast data
        
    Returns:
        Pivoted dataframe with fuel types as columns
    """
    logger.info("Pivoting generation forecast data")
    
    if df.empty or 'fuel_type' not in df.columns or 'generation_mw' not in df.columns:
        logger.warning("Cannot pivot empty dataframe or one without required columns")
        return df.copy()
    
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Ensure timestamp column exists
    if 'timestamp' not in df.columns:
        logger.error("Cannot pivot generation data without timestamp column")
        raise ValueError("Timestamp column missing from generation dataframe")
    
    try:
        # Pivot the dataframe
        pivoted = df.pivot_table(
            index='timestamp', 
            columns='fuel_type', 
            values='generation_mw', 
            aggfunc='sum'
        )
        
        # Handle missing values with zeros
        pivoted = pivoted.fillna(0)
        
        # Reset index to make timestamp a column again
        pivoted = pivoted.reset_index()
        
        # Rename columns to include 'generation_' prefix
        column_mapping = {
            col: f'generation_{col}' for col in pivoted.columns if col != 'timestamp'
        }
        pivoted = pivoted.rename(columns=column_mapping)
        
        logger.info(f"Pivoted generation data to {len(pivoted.columns) - 1} fuel type columns")
        return pivoted
        
    except Exception as e:
        logger.error(f"Failed to pivot generation data: {str(e)}")
        raise DataTransformationError(
            "generation_forecast", 
            "pivot_operation", 
            e
        )


class DataTransformer:
    """
    Main class for transforming data in the ingestion pipeline.
    
    This class provides methods to transform various types of input data into standardized
    formats suitable for the forecasting engine.
    """
    
    def __init__(self):
        """
        Initializes the data transformer.
        """
        self.logger = logger
    
    def transform_load_forecast(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms load forecast data to the standard format.
        
        Args:
            df: Input DataFrame with load forecast data
            
        Returns:
            Transformed load forecast dataframe
        """
        self.logger.info("Starting load forecast transformation")
        
        try:
            # Normalize the data
            transformed_df = normalize_load_forecast_data(df)
            return transformed_df
        except Exception as e:
            self.logger.error(f"Failed to transform load forecast data: {str(e)}")
            if isinstance(e, DataTransformationError):
                raise
            raise DataTransformationError("load_forecast", "transformation", e)
    
    def transform_historical_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms historical price data to the standard format.
        
        Args:
            df: Input DataFrame with historical price data
            
        Returns:
            Transformed historical price dataframe
        """
        self.logger.info("Starting historical price transformation")
        
        try:
            # Normalize the data
            transformed_df = normalize_historical_prices_data(df)
            return transformed_df
        except Exception as e:
            self.logger.error(f"Failed to transform historical price data: {str(e)}")
            if isinstance(e, DataTransformationError):
                raise
            raise DataTransformationError("historical_prices", "transformation", e)
    
    def transform_generation_forecast(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms generation forecast data to the standard format.
        
        Args:
            df: Input DataFrame with generation forecast data
            
        Returns:
            Transformed generation forecast dataframe
        """
        self.logger.info("Starting generation forecast transformation")
        
        try:
            # Normalize the data
            normalized_df = normalize_generation_forecast_data(df)
            
            # Pivot the data to create columns for each fuel type
            transformed_df = pivot_generation_data(normalized_df)
            
            return transformed_df
        except Exception as e:
            self.logger.error(f"Failed to transform generation forecast data: {str(e)}")
            if isinstance(e, DataTransformationError):
                raise
            raise DataTransformationError("generation_forecast", "transformation", e)
    
    def prepare_combined_dataset(
        self, 
        load_df: pd.DataFrame, 
        price_df: pd.DataFrame, 
        generation_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Prepares a combined dataset from all data sources for the forecasting engine.
        
        Args:
            load_df: Load forecast DataFrame
            price_df: Historical price DataFrame
            generation_df: Generation forecast DataFrame
            
        Returns:
            Combined dataset ready for feature engineering
        """
        self.logger.info("Preparing combined dataset from all data sources")
        
        try:
            # Align timestamps across all dataframes
            aligned_dfs = align_timestamps(
                {
                    'load': load_df,
                    'price': price_df,
                    'generation': generation_df
                },
                'timestamp',
                'H'  # Hourly frequency
            )
            
            # Pivot generation data if not already pivoted
            if 'generation' in aligned_dfs and 'fuel_type' in aligned_dfs['generation'].columns:
                aligned_dfs['generation'] = pivot_generation_data(aligned_dfs['generation'])
            
            # Merge aligned dataframes
            combined_df = merge_dataframes(
                [aligned_dfs['load'], aligned_dfs['price'], aligned_dfs['generation']],
                ['price', 'gen'],
                'timestamp',
                'outer'
            )
            
            # Perform final data cleaning and preparation
            # Drop any rows with all missing values
            combined_df = combined_df.dropna(how='all')
            
            self.logger.info(f"Combined dataset prepared with {len(combined_df)} rows and {len(combined_df.columns)} columns")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"Failed to prepare combined dataset: {str(e)}")
            if isinstance(e, DataTransformationError):
                raise
            raise DataTransformationError("combined_dataset", "preparation", e)
    
    def convert_to_models(self, df: pd.DataFrame, model_type: str) -> List:
        """
        Converts a dataframe to a list of data model instances.
        
        Args:
            df: Input DataFrame
            model_type: Type of model to create ('load_forecast', 'historical_price', 'generation_forecast')
            
        Returns:
            List of data model instances
        """
        self.logger.info(f"Converting dataframe to {model_type} models")
        
        try:
            return dataframe_to_models(df, model_type)
        except Exception as e:
            self.logger.error(f"Failed to convert dataframe to {model_type} models: {str(e)}")
            if isinstance(e, DataTransformationError):
                raise
            raise DataTransformationError(model_type, "model_conversion", e)
    
    def convert_from_models(self, models: List, model_type: str) -> pd.DataFrame:
        """
        Converts a list of data model instances to a dataframe.
        
        Args:
            models: List of data model instances
            model_type: Type of model ('load_forecast', 'historical_price', 'generation_forecast')
            
        Returns:
            DataFrame created from model instances
        """
        self.logger.info(f"Converting {model_type} models to dataframe")
        
        try:
            return models_to_dataframe(models, model_type)
        except Exception as e:
            self.logger.error(f"Failed to convert {model_type} models to dataframe: {str(e)}")
            if isinstance(e, DataTransformationError):
                raise
            raise DataTransformationError(model_type, "dataframe_conversion", e)