"""
Module for generating lagged features from time series data in the Electricity Market Price Forecasting System.

This module implements functionality to create time-lagged variables from historical data, 
which are essential for capturing temporal patterns in electricity prices. It follows 
a functional programming approach with clear separation between the class implementation 
and exported functions.
"""

import pandas as pd  # pandas 2.0.0+
import numpy as np   # numpy 1.24.0+
from typing import List, Optional, Union, Dict  # standard library
import functools  # standard library

# Internal imports
from ..utils.logging_utils import get_logger
from .exceptions import LaggedFeatureError
from ..utils.validation_utils import validate_dataframe, check_required_columns

# Initialize logger
logger = get_logger(__name__)

# Default lag periods to use if none specified
DEFAULT_LAG_PERIODS = [1, 2, 3, 6, 12, 24, 48, 72, 168]


class LaggedFeatureGenerator:
    """Class for generating lagged features from time series data"""
    
    def __init__(self, df: pd.DataFrame, timestamp_column: str, lag_periods: List[int] = None):
        """
        Initialize the LaggedFeatureGenerator with a dataframe and configuration
        
        Args:
            df: Input dataframe containing time series data
            timestamp_column: Name of the column containing timestamps
            lag_periods: List of lag periods to use for feature generation (default: DEFAULT_LAG_PERIODS)
        """
        self._df = df.copy()
        self._timestamp_column = timestamp_column
        self._lag_periods = lag_periods if lag_periods is not None else DEFAULT_LAG_PERIODS
        self._feature_columns = []
        
        # Validate that the timestamp column exists
        if self._timestamp_column not in self._df.columns:
            raise LaggedFeatureError(
                f"Timestamp column not found in dataframe",
                [], 
                self._lag_periods
            )
        
        # Ensure the dataframe is sorted by timestamp
        self._df = self._df.sort_values(by=self._timestamp_column)
        
        logger.info(f"Initialized LaggedFeatureGenerator with {len(self._df)} rows and timestamp column '{self._timestamp_column}'")
        
    def add_feature_columns(self, columns: List[str]) -> None:
        """
        Add columns to be used for generating lagged features
        
        Args:
            columns: List of column names to generate lagged features for
        """
        # Validate that all columns exist in the dataframe
        missing_columns = [col for col in columns if col not in self._df.columns]
        if missing_columns:
            raise LaggedFeatureError(
                f"Columns not found in dataframe",
                columns,
                self._lag_periods
            )
        
        self._feature_columns.extend(columns)
        logger.info(f"Added feature columns for lag generation: {columns}")
        
    def set_lag_periods(self, lag_periods: List[int]) -> None:
        """
        Set the lag periods to use for feature generation
        
        Args:
            lag_periods: List of lag periods to use
        """
        # Validate that lag_periods is a list of integers
        if not all(isinstance(lag, int) and lag > 0 for lag in lag_periods):
            raise LaggedFeatureError(
                "Lag periods must be positive integers",
                self._feature_columns,
                lag_periods
            )
            
        self._lag_periods = lag_periods
        logger.info(f"Set lag periods: {lag_periods}")
        
    def generate_category_lagged_features(self, column: str) -> None:
        """
        Generate lagged features for a specific category or column
        
        Args:
            column: Column name to generate lagged features for
        """
        # Validate that the column exists in the dataframe
        if column not in self._df.columns:
            raise LaggedFeatureError(
                f"Column not found in dataframe",
                [column],
                self._lag_periods
            )
            
        # Generate lagged features for each lag period
        for lag_period in self._lag_periods:
            lag_column_name = create_lag_column_name(column, lag_period)
            self._df[lag_column_name] = self._df[column].shift(lag_period)
            
        logger.info(f"Generated lagged features for column '{column}' with lag periods {self._lag_periods}")
        
    def generate_all_lagged_features(self) -> pd.DataFrame:
        """
        Generate lagged features for all specified feature columns
        
        Returns:
            DataFrame with added lagged features
        """
        # Validate that feature columns have been added
        if not self._feature_columns:
            raise LaggedFeatureError(
                "No feature columns specified for lag generation",
                [],
                self._lag_periods
            )
            
        # Generate lagged features for each column
        for column in self._feature_columns:
            self.generate_category_lagged_features(column)
            
        return self._df
        
    def get_feature_names(self) -> List[str]:
        """
        Get the names of all generated lagged feature columns
        
        Returns:
            List of lagged feature column names
        """
        return get_lag_feature_names(self._feature_columns, self._lag_periods)


def create_lag_column_name(column_name: str, lag_period: int) -> str:
    """
    Creates a standardized column name for a lagged feature
    
    Args:
        column_name: Original column name
        lag_period: Lag period value
        
    Returns:
        Formatted lag column name
    """
    return f"{column_name}_lag_{lag_period}"
    

def get_lag_feature_names(columns: List[str], lag_periods: List[int]) -> List[str]:
    """
    Generates a list of lag feature column names for given columns and lag periods
    
    Args:
        columns: List of column names
        lag_periods: List of lag periods
        
    Returns:
        List of lag feature column names
    """
    lag_feature_names = []
    for column in columns:
        for lag_period in lag_periods:
            lag_feature_names.append(create_lag_column_name(column, lag_period))
    
    return lag_feature_names


def generate_lagged_features(
    df: pd.DataFrame, 
    columns: List[str], 
    lag_periods: List[int] = None, 
    timestamp_column: str = 'timestamp'
) -> pd.DataFrame:
    """
    Creates lagged features for specified columns in a dataframe
    
    Args:
        df: Input dataframe containing time series data
        columns: List of columns to create lagged features for
        lag_periods: List of lag periods to use (default: DEFAULT_LAG_PERIODS)
        timestamp_column: Name of the column containing timestamps (default: 'timestamp')
        
    Returns:
        DataFrame with original and lagged features
    """
    try:
        # Validate input dataframe has required columns
        validation_result = check_required_columns(df, [timestamp_column] + columns)
        if not validation_result.is_valid:
            raise LaggedFeatureError(
                f"Input dataframe missing required columns",
                columns,
                lag_periods or DEFAULT_LAG_PERIODS
            )
        
        # Set default lag periods if not provided
        if lag_periods is None:
            lag_periods = DEFAULT_LAG_PERIODS
            logger.info(f"Using default lag periods: {lag_periods}")
        
        # Create a copy of the input dataframe to avoid modifying the original
        df_copy = df.copy()
        
        # Sort the dataframe by timestamp_column to ensure correct lag calculation
        df_sorted = df_copy.sort_values(by=timestamp_column)
        
        # Create a LaggedFeatureGenerator instance
        generator = LaggedFeatureGenerator(df_sorted, timestamp_column, lag_periods)
        
        # Generate lagged features for all specified columns
        generator.add_feature_columns(columns)
        result_df = generator.generate_all_lagged_features()
        
        logger.info(f"Successfully generated {len(generator.get_feature_names())} lagged features")
        
        return result_df
        
    except Exception as e:
        if not isinstance(e, LaggedFeatureError):
            # Wrap any other exceptions in a LaggedFeatureError
            e = LaggedFeatureError(
                f"Error generating lagged features",
                columns,
                lag_periods or DEFAULT_LAG_PERIODS,
                e
            )
        
        logger.error(f"Failed to generate lagged features: {str(e)}")
        raise e