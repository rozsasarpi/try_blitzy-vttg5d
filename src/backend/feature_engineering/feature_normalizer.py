"""
Module implementing feature normalization functionality for the Electricity Market Price Forecasting System.
Provides standardization of feature values for model input with persistence capabilities to ensure
consistent normalization across training and inference.
"""

import os
import pickle
import pathlib
from typing import Dict, List, Optional, Tuple, Union, Any

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler

# Internal imports
from ..utils.logging_utils import get_logger
from ..utils.decorators import log_exceptions, timing_decorator
from ..utils.file_utils import save_dataframe, load_dataframe, ensure_directory_exists
from .exceptions import FeatureNormalizationError
from ..config.settings import STORAGE_ROOT_DIR

# Configure logger
logger = get_logger(__name__)

# Path for storing normalizers
NORMALIZER_DIR = os.path.join(STORAGE_ROOT_DIR, 'normalizers')

# Dictionary mapping normalization method names to their implementations
NORMALIZATION_METHODS = {
    'standard': StandardScaler,
    'minmax': MinMaxScaler,
    'robust': RobustScaler,
    'none': None
}


class FeatureNormalizer:
    """Class for consistent feature normalization with persistence capabilities."""
    
    def __init__(self, method: str, normalizer_id: Optional[str] = None):
        """
        Initialize the feature normalizer with specified method.
        
        Args:
            method: Normalization method ('standard', 'minmax', 'robust', or 'none')
            normalizer_id: Optional identifier for this normalizer
        
        Raises:
            ValueError: If method is not a supported normalization method
        """
        if method not in NORMALIZATION_METHODS:
            raise ValueError(f"Unsupported normalization method: {method}. Must be one of {list(NORMALIZATION_METHODS.keys())}")
        
        self.method = method
        self.scalers = {}  # Dict to store fitted scalers for each column
        self.feature_stats = {}  # Dict to store feature statistics
        self.normalizer_id = normalizer_id
        
        # If normalizer_id is provided, try to load existing normalizer
        if normalizer_id:
            try:
                self.load_scalers(normalizer_id)
            except Exception as e:
                logger.warning(f"Could not load existing normalizer '{normalizer_id}': {e}")
    
    def fit(self, df: pd.DataFrame, columns: Optional[List[str]] = None) -> 'FeatureNormalizer':
        """
        Fit normalizers on the data for each column.
        
        Args:
            df: DataFrame containing features to normalize
            columns: List of columns to normalize. If None, uses all numeric columns.
        
        Returns:
            Self for method chaining
        
        Raises:
            FeatureNormalizationError: If fitting fails
        """
        # If method is 'none', do nothing
        if self.method == 'none':
            logger.info("Normalization method is 'none', no fitting performed")
            return self
        
        try:
            # If columns is None, use all numeric columns
            if columns is None:
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                columns = numeric_cols
                logger.info(f"Using all numeric columns for normalization: {columns}")
            
            # Create and fit a scaler for each column
            for col in columns:
                if col not in df.columns:
                    logger.warning(f"Column '{col}' not found in DataFrame, skipping")
                    continue
                
                # Skip if column has non-numeric data
                if not pd.api.types.is_numeric_dtype(df[col]):
                    logger.warning(f"Column '{col}' is not numeric, skipping")
                    continue
                
                # Skip columns with all NaN values
                if df[col].isna().all():
                    logger.warning(f"Column '{col}' contains only NaN values, skipping")
                    continue
                
                # Get the scaler class for the specified method
                scaler_class = NORMALIZATION_METHODS[self.method]
                
                # Create and fit the scaler
                scaler = scaler_class()
                col_data = df[col].values.reshape(-1, 1)  # Reshape for scikit-learn
                scaler.fit(col_data)
                
                # Store the fitted scaler
                self.scalers[col] = scaler
                
                # Store feature statistics
                self.feature_stats[col] = {
                    'mean': float(np.mean(df[col])),
                    'std': float(np.std(df[col])),
                    'min': float(np.min(df[col])),
                    'max': float(np.max(df[col])),
                }
                
                logger.debug(f"Fitted normalizer for column '{col}' using method '{self.method}'")
            
            return self
            
        except Exception as e:
            error_msg = f"Error fitting normalizer: {str(e)}"
            logger.error(error_msg)
            raise FeatureNormalizationError(error_msg, e)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform data using fitted normalizers.
        
        Args:
            df: DataFrame to transform
        
        Returns:
            Transformed DataFrame
        
        Raises:
            FeatureNormalizationError: If transformation fails
        """
        # If method is 'none', return the original DataFrame
        if self.method == 'none':
            logger.info("Normalization method is 'none', returning original DataFrame")
            return df
        
        # Create a copy to avoid modifying the original
        df_normalized = df.copy()
        
        try:
            # Transform each column that has a fitted scaler
            for col, scaler in self.scalers.items():
                if col not in df.columns:
                    logger.warning(f"Column '{col}' not found in DataFrame, skipping")
                    continue
                
                # Skip if column has non-numeric data
                if not pd.api.types.is_numeric_dtype(df[col]):
                    logger.warning(f"Column '{col}' is not numeric, skipping")
                    continue
                
                # Transform the column
                col_data = df[col].values.reshape(-1, 1)  # Reshape for scikit-learn
                df_normalized[col] = scaler.transform(col_data).flatten()
                
                logger.debug(f"Transformed column '{col}' using normalizer")
            
            return df_normalized
            
        except Exception as e:
            error_msg = f"Error transforming data: {str(e)}"
            logger.error(error_msg)
            raise FeatureNormalizationError(error_msg, e)
    
    def fit_transform(self, df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Fit normalizers and transform data in one step.
        
        Args:
            df: DataFrame containing features to normalize
            columns: List of columns to normalize. If None, uses all numeric columns.
        
        Returns:
            Normalized DataFrame
        
        Raises:
            FeatureNormalizationError: If operation fails
        """
        return self.fit(df, columns).transform(df)
    
    def get_feature_stats(self) -> Dict[str, Dict[str, float]]:
        """
        Get statistics for normalized features.
        
        Returns:
            Dictionary of feature statistics
        """
        return self.feature_stats
    
    def save_scalers(self, normalizer_id: Optional[str] = None) -> bool:
        """
        Save normalizer state to disk.
        
        Args:
            normalizer_id: Identifier for the normalizer. If None, uses self.normalizer_id.
        
        Returns:
            True if successful, False otherwise
        
        Raises:
            ValueError: If no normalizer_id is provided or set
        """
        # Use provided ID or fall back to instance ID
        normalizer_id = normalizer_id or self.normalizer_id
        
        if not normalizer_id:
            raise ValueError("No normalizer_id provided for saving")
        
        try:
            # Get the file path
            normalizer_path = get_normalizer_path(normalizer_id)
            
            # Prepare data to save
            data_to_save = {
                'method': self.method,
                'scalers': self.scalers,
                'feature_stats': self.feature_stats
            }
            
            # Save using pickle
            with open(normalizer_path, 'wb') as f:
                pickle.dump(data_to_save, f)
            
            logger.info(f"Saved normalizer '{normalizer_id}' to {normalizer_path}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to save normalizer: {str(e)}"
            logger.error(error_msg)
            return False
    
    def load_scalers(self, normalizer_id: Optional[str] = None) -> bool:
        """
        Load normalizer state from disk.
        
        Args:
            normalizer_id: Identifier for the normalizer. If None, uses self.normalizer_id.
        
        Returns:
            True if successful, False otherwise
        
        Raises:
            ValueError: If no normalizer_id is provided or set
        """
        # Use provided ID or fall back to instance ID
        normalizer_id = normalizer_id or self.normalizer_id
        
        if not normalizer_id:
            raise ValueError("No normalizer_id provided for loading")
        
        try:
            # Get the file path
            normalizer_path = get_normalizer_path(normalizer_id)
            
            # Check if file exists
            if not normalizer_path.exists():
                logger.warning(f"Normalizer file not found: {normalizer_path}")
                return False
            
            # Load using pickle
            with open(normalizer_path, 'rb') as f:
                data = pickle.load(f)
            
            # Update instance attributes
            self.method = data['method']
            self.scalers = data['scalers']
            self.feature_stats = data['feature_stats']
            
            logger.info(f"Loaded normalizer '{normalizer_id}' from {normalizer_path}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to load normalizer: {str(e)}"
            logger.error(error_msg)
            return False
    
    def inverse_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reverse the normalization transformation.
        
        Args:
            df: DataFrame with normalized values
        
        Returns:
            DataFrame with original scale
        
        Raises:
            FeatureNormalizationError: If inverse transformation fails
        """
        # If method is 'none', return the original DataFrame
        if self.method == 'none':
            logger.info("Normalization method is 'none', returning original DataFrame")
            return df
        
        # Create a copy to avoid modifying the original
        df_original = df.copy()
        
        try:
            # Inverse transform each column that has a fitted scaler
            for col, scaler in self.scalers.items():
                if col not in df.columns:
                    logger.warning(f"Column '{col}' not found in DataFrame, skipping")
                    continue
                
                # Skip if column has non-numeric data
                if not pd.api.types.is_numeric_dtype(df[col]):
                    logger.warning(f"Column '{col}' is not numeric, skipping")
                    continue
                
                # Inverse transform the column
                col_data = df[col].values.reshape(-1, 1)  # Reshape for scikit-learn
                df_original[col] = scaler.inverse_transform(col_data).flatten()
                
                logger.debug(f"Inverse transformed column '{col}' using normalizer")
            
            return df_original
            
        except Exception as e:
            error_msg = f"Error performing inverse transform: {str(e)}"
            logger.error(error_msg)
            raise FeatureNormalizationError(error_msg, e)


@timing_decorator
@log_exceptions
def normalize_features(
    df: pd.DataFrame,
    method: str = 'standard',
    columns: Optional[List[str]] = None,
    fit: bool = True,
    normalizer_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Normalize features using the specified method.
    
    Args:
        df: DataFrame containing features to normalize
        method: Normalization method ('standard', 'minmax', 'robust', or 'none')
        columns: List of columns to normalize. If None, uses all numeric columns.
        fit: Whether to fit the normalizer on this data. If False, loads existing normalizer.
        normalizer_id: Identifier for saving/loading this normalizer
    
    Returns:
        Normalized DataFrame
    
    Raises:
        FeatureNormalizationError: If normalization fails
        ValueError: If method is not supported
    """
    # Validate the normalization method
    if method not in NORMALIZATION_METHODS:
        raise ValueError(f"Unsupported normalization method: {method}. Must be one of {list(NORMALIZATION_METHODS.keys())}")
    
    # If method is 'none', return the original DataFrame
    if method == 'none':
        return df
    
    # If columns is None, use all numeric columns
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
        logger.info(f"Using all numeric columns for normalization: {columns}")
    
    try:
        # Create a FeatureNormalizer instance
        normalizer = FeatureNormalizer(method, normalizer_id)
        
        # Fit or transform based on the fit flag
        if fit:
            logger.info(f"Fitting and transforming data using method '{method}'")
            result_df = normalizer.fit_transform(df, columns)
            
            # Save the normalizer if an ID is provided
            if normalizer_id:
                normalizer.save_scalers(normalizer_id)
        else:
            # If not fitting, we must have a normalizer_id to load
            if not normalizer_id:
                raise ValueError("Must provide normalizer_id when fit=False")
            
            # Load the normalizer and transform
            logger.info(f"Loading normalizer '{normalizer_id}' and transforming data")
            if not normalizer.load_scalers(normalizer_id):
                raise FeatureNormalizationError(f"Failed to load normalizer '{normalizer_id}'")
            
            result_df = normalizer.transform(df)
        
        return result_df
        
    except Exception as e:
        if isinstance(e, FeatureNormalizationError):
            raise e
        else:
            error_msg = f"Error normalizing features: {str(e)}"
            logger.error(error_msg)
            raise FeatureNormalizationError(error_msg, e)


@log_exceptions
def get_normalizer_path(normalizer_id: str) -> pathlib.Path:
    """
    Get the file path for a normalizer based on its ID.
    
    Args:
        normalizer_id: Identifier for the normalizer
    
    Returns:
        Path to the normalizer file
    
    Raises:
        FeatureNormalizationError: If path creation fails
    """
    try:
        # Ensure the normalizer directory exists
        normalizer_dir = ensure_directory_exists(NORMALIZER_DIR)
        
        # Construct the file path
        normalizer_path = normalizer_dir / f"{normalizer_id}.pkl"
        
        return normalizer_path
        
    except Exception as e:
        error_msg = f"Error getting normalizer path: {str(e)}"
        logger.error(error_msg)
        raise FeatureNormalizationError(error_msg, e)