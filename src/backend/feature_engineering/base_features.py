"""
Implements the base feature creation functionality for the Electricity Market Price Forecasting System.

This module is responsible for creating fundamental features from raw input data sources
(load forecasts, historical prices, generation forecasts) that will be used as inputs for
derived features and ultimately for the forecasting models. It follows a functional
programming approach with clear separation of concerns.
"""

import pandas as pd  # version: 2.0.0
import numpy as np  # version: 1.24.0
import datetime
from typing import Dict, List, Any, Optional, Union

# Internal imports
from ..utils.logging_utils import get_logger, log_execution_time
from .exceptions import FeatureCreationError, MissingFeatureError
from ..models.data_models import LoadForecast, HistoricalPrice, GenerationForecast
from ..data_ingestion import DataIngestionManager
from ..config.settings import FORECAST_PRODUCTS, FORECAST_HORIZON_HOURS

# Configure logger
logger = get_logger(__name__)

# Constants
REQUIRED_DATA_SOURCES = ["load_forecast", "historical_prices", "generation_forecast"]
TEMPORAL_FEATURES = ["hour", "day_of_week", "month", "is_weekend", "is_holiday"]

@log_execution_time
def fetch_base_feature_data(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    products: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Fetches all required data for base feature creation from external sources.
    
    Args:
        start_date: Start date for the data range
        end_date: End date for the data range
        products: List of price products to fetch (defaults to all FORECAST_PRODUCTS)
        
    Returns:
        Dictionary containing all collected data
        
    Raises:
        FeatureCreationError: If there's an error fetching or processing the data
    """
    try:
        # Initialize data ingestion manager
        data_manager = DataIngestionManager()
        
        # Set default products if not provided
        if products is None:
            products = FORECAST_PRODUCTS
            
        logger.info(f"Fetching base feature data from {start_date} to {end_date} for products: {products}")
        
        # Fetch all required data
        data_dict = data_manager.get_all_data(start_date, end_date, products)
        
        # Validate that all required data sources are present
        missing_sources = [source for source in REQUIRED_DATA_SOURCES if source not in data_dict]
        if missing_sources:
            raise MissingFeatureError(f"Missing required data sources: {missing_sources}", missing_sources)
        
        logger.info(f"Successfully fetched data. Shapes - Load: {data_dict['load_forecast'].shape}, "
                   f"Price: {data_dict['historical_prices'].shape}, "
                   f"Generation: {data_dict['generation_forecast'].shape}")
        
        return data_dict
    
    except Exception as e:
        error_message = f"Failed to fetch base feature data: {str(e)}"
        logger.error(error_message)
        raise FeatureCreationError(error_message, e)

def create_temporal_features(df: pd.DataFrame, timestamp_column: str) -> pd.DataFrame:
    """
    Creates temporal features from timestamp information.
    
    Args:
        df: DataFrame containing a timestamp column
        timestamp_column: Name of the timestamp column
        
    Returns:
        DataFrame with added temporal features
        
    Raises:
        ValueError: If timestamp_column doesn't exist in the DataFrame
    """
    if timestamp_column not in df.columns:
        raise ValueError(f"Column '{timestamp_column}' not found in DataFrame")
    
    # Create a copy to avoid modifying the original DataFrame
    result_df = df.copy()
    
    # Extract temporal features
    result_df['hour'] = result_df[timestamp_column].dt.hour
    result_df['day_of_week'] = result_df[timestamp_column].dt.dayofweek
    result_df['month'] = result_df[timestamp_column].dt.month
    
    # Create weekend indicator (0 for weekday, 1 for weekend)
    result_df['is_weekend'] = (result_df['day_of_week'] >= 5).astype(int)
    
    # Create holiday indicator (simplified - would need a holiday calendar in production)
    # This is a placeholder - in a real implementation, you would use a holiday calendar
    result_df['is_holiday'] = 0
    
    return result_df

def create_load_features(load_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates features from load forecast data.
    
    Args:
        load_df: DataFrame containing load forecast data
        
    Returns:
        DataFrame with load-based features
        
    Raises:
        ValueError: If required columns are missing
    """
    # Validate required columns
    required_columns = ['timestamp', 'load_mw']
    for col in required_columns:
        if col not in load_df.columns:
            raise ValueError(f"Required column '{col}' not found in load_df")
    
    # Create a copy to avoid modifying the original DataFrame
    result_df = load_df.copy()
    
    # Create load rate of change (percentage change from previous hour)
    result_df['load_rate_of_change'] = result_df['load_mw'].pct_change() * 100
    
    # Calculate daily peak and average load for relative measures
    result_df['date'] = result_df['timestamp'].dt.date
    daily_max = result_df.groupby('date')['load_mw'].transform('max')
    daily_avg = result_df.groupby('date')['load_mw'].transform('mean')
    
    # Create relative load metrics
    result_df['load_daily_peak'] = result_df['load_mw'] / daily_max
    result_df['load_daily_average'] = result_df['load_mw'] / daily_avg
    
    # Drop the temporary date column
    result_df = result_df.drop('date', axis=1)
    
    # Fill NaN values that could be created by pct_change or division
    result_df = result_df.fillna(method='bfill').fillna(method='ffill')
    
    return result_df

def create_generation_features(generation_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates features from generation forecast data.
    
    Args:
        generation_df: DataFrame containing generation forecast data
        
    Returns:
        DataFrame with generation-based features
        
    Raises:
        ValueError: If required columns are missing
    """
    # Validate that we have the right data format
    if 'timestamp' not in generation_df.columns:
        raise ValueError("Required column 'timestamp' not found in generation_df")
    
    # Check if the data is already pivoted (with generation_* columns)
    # or if it's in a long format with fuel_type and generation_mw columns
    if 'fuel_type' in generation_df.columns and 'generation_mw' in generation_df.columns:
        # Need to pivot the data first
        pivoted_df = generation_df.pivot_table(
            index='timestamp',
            columns='fuel_type',
            values='generation_mw',
            aggfunc='sum'
        ).reset_index()
        # Rename columns to include 'generation_' prefix
        new_columns = {col: f'generation_{col}' for col in pivoted_df.columns if col != 'timestamp'}
        pivoted_df = pivoted_df.rename(columns=new_columns)
        result_df = pivoted_df
    else:
        # Data is already in pivoted format
        result_df = generation_df.copy()
    
    # Find generation columns (those starting with 'generation_')
    generation_columns = [col for col in result_df.columns if col.startswith('generation_')]
    
    if not generation_columns:
        raise ValueError("No generation columns found in generation_df")
    
    # Calculate total generation
    result_df['total_generation'] = result_df[generation_columns].sum(axis=1)
    
    # Calculate renewable ratio (assuming columns with 'wind', 'solar', 'hydro' are renewable)
    renewable_columns = [col for col in generation_columns if 
                         any(r in col.lower() for r in ['wind', 'solar', 'hydro'])]
    
    if renewable_columns:
        renewable_gen = result_df[renewable_columns].sum(axis=1)
        result_df['renewable_ratio'] = renewable_gen / result_df['total_generation']
    
    # Calculate fuel mix diversity using normalized entropy
    # Higher values indicate a more diverse generation mix
    def normalized_entropy(row, columns):
        values = row[columns].values
        total = values.sum()
        if total <= 0:
            return 0
        proportions = values / total
        # Filter out zeros to avoid log(0)
        proportions = proportions[proportions > 0]
        if len(proportions) <= 1:
            return 0
        entropy = -np.sum(proportions * np.log(proportions))
        max_entropy = np.log(len(columns))  # Maximum possible entropy
        return entropy / max_entropy if max_entropy > 0 else 0
    
    result_df['fuel_mix_diversity'] = result_df.apply(
        lambda row: normalized_entropy(row, generation_columns), axis=1
    )
    
    # Fill NaN values
    result_df = result_df.fillna(method='bfill').fillna(method='ffill').fillna(0)
    
    return result_df

def create_price_features(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates features from historical price data.
    
    Args:
        price_df: DataFrame containing historical price data
        
    Returns:
        DataFrame with price-based features
        
    Raises:
        ValueError: If required columns are missing
    """
    # Validate required columns
    if 'timestamp' not in price_df.columns:
        raise ValueError("Required column 'timestamp' not found in price_df")
    
    # Check if the data is already pivoted (with price_* columns)
    # or if it's in a long format with product and price columns
    if 'product' in price_df.columns and 'price' in price_df.columns:
        # Need to pivot the data first
        pivoted_df = price_df.pivot_table(
            index='timestamp',
            columns='product',
            values='price',
            aggfunc='mean'
        ).reset_index()
        # Rename columns to include 'price_' prefix
        new_columns = {col: f'price_{col}' for col in pivoted_df.columns if col != 'timestamp'}
        pivoted_df = pivoted_df.rename(columns=new_columns)
        result_df = pivoted_df
    else:
        # Data is already in pivoted format
        result_df = price_df.copy()
    
    # Find price columns (those starting with 'price_')
    price_columns = [col for col in result_df.columns if col.startswith('price_')]
    
    if not price_columns:
        raise ValueError("No price columns found in price_df")
    
    # Calculate price volatility features (standard deviation over recent windows)
    for col in price_columns:
        # Set timestamp as index for window calculations
        temp_df = result_df.set_index('timestamp')
        
        # Calculate 24h rolling standard deviation
        try:
            result_df[f'{col}_volatility_24h'] = temp_df[col].rolling('24H').std().reset_index(drop=True)
        except:
            # Handle case where timestamp is not a datetime index
            result_df[f'{col}_volatility_24h'] = np.nan
            
        # Reset index
        temp_df = temp_df.reset_index()
    
    # Calculate price trend features (moving averages)
    for col in price_columns:
        # Set timestamp as index for window calculations
        temp_df = result_df.set_index('timestamp')
        
        try:
            # Calculate 24h moving average
            result_df[f'{col}_ma_24h'] = temp_df[col].rolling('24H').mean().reset_index(drop=True)
            
            # Calculate 7-day moving average
            result_df[f'{col}_ma_7d'] = temp_df[col].rolling('168H').mean().reset_index(drop=True)
        except:
            # Handle case where timestamp is not a datetime index
            result_df[f'{col}_ma_24h'] = np.nan
            result_df[f'{col}_ma_7d'] = np.nan
            
        # Reset index
        temp_df = temp_df.reset_index()
    
    # Calculate price spread features (differences between related products)
    # Example: Spread between Day-Ahead and Real-Time prices
    if 'price_DALMP' in result_df.columns and 'price_RTLMP' in result_df.columns:
        result_df['price_spread_DA_RT'] = result_df['price_DALMP'] - result_df['price_RTLMP']
    
    # Fill NaN values that could be created by window operations
    result_df = result_df.fillna(method='bfill').fillna(method='ffill')
    
    return result_df

def merge_feature_dataframes(feature_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merges multiple feature DataFrames on timestamp.
    
    Args:
        feature_dfs: Dictionary of DataFrames to merge
        
    Returns:
        Combined DataFrame with all features
        
    Raises:
        ValueError: If any DataFrame doesn't have a timestamp column
    """
    # Validate that all DataFrames have a timestamp column
    for name, df in feature_dfs.items():
        if 'timestamp' not in df.columns:
            raise ValueError(f"DataFrame '{name}' is missing the timestamp column")
    
    # Start with the first DataFrame
    result_df = None
    
    # Iteratively merge each DataFrame
    for name, df in feature_dfs.items():
        if result_df is None:
            result_df = df.copy()
        else:
            # Handle duplicate column names by adding source prefixes
            # Exception for 'timestamp' which is the merge key
            duplicate_columns = set(result_df.columns).intersection(set(df.columns)) - {'timestamp'}
            if duplicate_columns:
                rename_dict = {col: f"{col}_{name}" for col in duplicate_columns}
                df_renamed = df.rename(columns=rename_dict)
                result_df = pd.merge(result_df, df_renamed, on='timestamp', how='outer')
            else:
                result_df = pd.merge(result_df, df, on='timestamp', how='outer')
    
    # Sort by timestamp
    result_df = result_df.sort_values('timestamp')
    
    # Validate that the merge was successful
    if result_df is None or result_df.empty:
        raise ValueError("Merged DataFrame is empty")
    
    # Ensure we didn't lose any rows
    expected_rows = len(feature_dfs[list(feature_dfs.keys())[0]])
    if len(result_df) < expected_rows:
        logger.warning(f"Expected at least {expected_rows} rows but got {len(result_df)} after merge")
    
    return result_df

@log_execution_time
def create_base_features(
    input_data: Optional[Dict[str, pd.DataFrame]] = None,
    start_date: Optional[datetime.datetime] = None,
    end_date: Optional[datetime.datetime] = None,
    products: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Main function to create all base features from input data.
    
    Args:
        input_data: Dictionary containing input DataFrames (if None, data will be fetched)
        start_date: Start date for the data range (if input_data is None)
        end_date: End date for the data range (if input_data is None)
        products: List of price products to fetch (if input_data is None)
        
    Returns:
        DataFrame containing all base features
        
    Raises:
        FeatureCreationError: If there's an error creating features
    """
    try:
        # If input_data is not provided, fetch the data
        if input_data is None:
            # Calculate default dates if not provided
            if start_date is None or end_date is None:
                current_time = datetime.datetime.now()
                if start_date is None:
                    # Start from 7 days ago by default
                    start_date = current_time - datetime.timedelta(days=7)
                if end_date is None:
                    # End at current time + forecast horizon
                    end_date = current_time + datetime.timedelta(hours=FORECAST_HORIZON_HOURS)
            
            logger.info(f"Fetching input data from {start_date} to {end_date}")
            input_data = fetch_base_feature_data(start_date, end_date, products)
        
        # Extract individual DataFrames
        load_df = input_data.get('load_forecast')
        price_df = input_data.get('historical_prices')
        generation_df = input_data.get('generation_forecast')
        
        # Validate that we have all required data
        if load_df is None or price_df is None or generation_df is None:
            missing = []
            if load_df is None:
                missing.append('load_forecast')
            if price_df is None:
                missing.append('historical_prices')
            if generation_df is None:
                missing.append('generation_forecast')
            raise MissingFeatureError(f"Missing required input data: {missing}", missing)
        
        logger.info("Creating temporal features")
        # Add temporal features to each DataFrame
        temporal_load_df = create_temporal_features(load_df, 'timestamp')
        temporal_price_df = create_temporal_features(price_df, 'timestamp')
        temporal_generation_df = create_temporal_features(generation_df, 'timestamp')
        
        logger.info("Creating load features")
        load_features_df = create_load_features(temporal_load_df)
        
        logger.info("Creating generation features")
        generation_features_df = create_generation_features(temporal_generation_df)
        
        logger.info("Creating price features")
        price_features_df = create_price_features(temporal_price_df)
        
        logger.info("Merging feature DataFrames")
        # Merge all feature DataFrames
        feature_dfs = {
            'load': load_features_df,
            'price': price_features_df,
            'generation': generation_features_df
        }
        combined_features_df = merge_feature_dataframes(feature_dfs)
        
        # Ensure the combined DataFrame has all required features
        feature_count = len(combined_features_df.columns) - 1  # Exclude timestamp
        logger.info(f"Created {feature_count} base features")
        
        return combined_features_df
        
    except Exception as e:
        error_message = f"Failed to create base features: {str(e)}"
        logger.error(error_message)
        if isinstance(e, FeatureCreationError) or isinstance(e, MissingFeatureError):
            raise
        raise FeatureCreationError(error_message, e)


class BaseFeatureCreator:
    """
    Class responsible for creating base features from raw input data.
    """
    
    def __init__(
        self,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        products: Optional[List[str]] = None,
        data_manager: Optional[DataIngestionManager] = None
    ):
        """
        Initializes the BaseFeatureCreator with optional parameters.
        
        Args:
            start_date: Start date for the data range
            end_date: End date for the data range
            products: List of price products to fetch
            data_manager: DataIngestionManager instance to use
        """
        # Initialize data manager
        self._data_manager = data_manager if data_manager is not None else DataIngestionManager()
        
        # Calculate default dates if not provided
        current_time = datetime.datetime.now()
        self._start_date = start_date if start_date is not None else (
            current_time - datetime.timedelta(days=7)
        )
        self._end_date = end_date if end_date is not None else (
            current_time + datetime.timedelta(hours=FORECAST_HORIZON_HOURS)
        )
        
        # Set default products if not provided
        self._products = products if products is not None else FORECAST_PRODUCTS
        
        # Initialize empty DataFrame to store features
        self._feature_df = pd.DataFrame()
        
        # Initialize empty cache for data
        self._data_cache = {}
    
    def fetch_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetches all required data from external sources.
        
        Returns:
            Dictionary of data from all sources
            
        Raises:
            FeatureCreationError: If there's an error fetching or processing the data
        """
        # Check if data is already in cache
        if self._data_cache:
            return self._data_cache
        
        try:
            logger.info(f"Fetching data from {self._start_date} to {self._end_date} for products: {self._products}")
            
            # Use the data manager to fetch all required data
            data_dict = self._data_manager.get_all_data(
                self._start_date, self._end_date, self._products
            )
            
            # Validate that all required data sources are present
            missing_sources = [source for source in REQUIRED_DATA_SOURCES if source not in data_dict]
            if missing_sources:
                raise MissingFeatureError(f"Missing required data sources: {missing_sources}", missing_sources)
            
            # Store in cache
            self._data_cache = data_dict
            
            logger.info(f"Successfully fetched data. Shapes - Load: {data_dict['load_forecast'].shape}, "
                       f"Price: {data_dict['historical_prices'].shape}, "
                       f"Generation: {data_dict['generation_forecast'].shape}")
            
            return data_dict
            
        except Exception as e:
            error_message = f"Failed to fetch data: {str(e)}"
            logger.error(error_message)
            raise FeatureCreationError(error_message, e)
    
    def create_features(self, input_data: Optional[Dict[str, pd.DataFrame]] = None) -> pd.DataFrame:
        """
        Creates all base features from input data.
        
        Args:
            input_data: Dictionary containing input DataFrames (if None, data will be fetched)
            
        Returns:
            DataFrame containing all base features
            
        Raises:
            FeatureCreationError: If there's an error creating features
        """
        try:
            # If input_data is not provided, fetch the data
            if input_data is None:
                input_data = self.fetch_data()
            
            # Extract individual DataFrames
            load_df = input_data.get('load_forecast')
            price_df = input_data.get('historical_prices')
            generation_df = input_data.get('generation_forecast')
            
            # Validate that we have all required data
            if load_df is None or price_df is None or generation_df is None:
                missing = []
                if load_df is None:
                    missing.append('load_forecast')
                if price_df is None:
                    missing.append('historical_prices')
                if generation_df is None:
                    missing.append('generation_forecast')
                raise MissingFeatureError(f"Missing required input data: {missing}", missing)
            
            logger.info("Creating temporal features")
            # Add temporal features to each DataFrame
            temporal_load_df = self.create_temporal_features(load_df, 'timestamp')
            temporal_price_df = self.create_temporal_features(price_df, 'timestamp')
            temporal_generation_df = self.create_temporal_features(generation_df, 'timestamp')
            
            logger.info("Creating load features")
            load_features_df = self.create_load_features(temporal_load_df)
            
            logger.info("Creating generation features")
            generation_features_df = self.create_generation_features(temporal_generation_df)
            
            logger.info("Creating price features")
            price_features_df = self.create_price_features(temporal_price_df)
            
            logger.info("Merging feature DataFrames")
            # Merge all feature DataFrames
            feature_dfs = {
                'load': load_features_df,
                'price': price_features_df,
                'generation': generation_features_df
            }
            self._feature_df = merge_feature_dataframes(feature_dfs)
            
            # Ensure the combined DataFrame has all required features
            feature_count = len(self._feature_df.columns) - 1  # Exclude timestamp
            logger.info(f"Created {feature_count} base features")
            
            return self._feature_df
            
        except Exception as e:
            error_message = f"Failed to create features: {str(e)}"
            logger.error(error_message)
            if isinstance(e, FeatureCreationError) or isinstance(e, MissingFeatureError):
                raise
            raise FeatureCreationError(error_message, e)
    
    def get_feature_dataframe(self) -> pd.DataFrame:
        """
        Returns the current feature DataFrame.
        
        Returns:
            Current feature DataFrame
            
        Raises:
            FeatureCreationError: If features haven't been created yet
        """
        try:
            if self._feature_df.empty:
                # If feature DataFrame is empty, create features
                return self.create_features()
            return self._feature_df
        except Exception as e:
            error_message = f"Failed to get feature DataFrame: {str(e)}"
            logger.error(error_message)
            raise FeatureCreationError(error_message, e)
    
    def create_temporal_features(self, df: pd.DataFrame, timestamp_column: str) -> pd.DataFrame:
        """
        Creates temporal features for a DataFrame.
        
        Args:
            df: DataFrame containing a timestamp column
            timestamp_column: Name of the timestamp column
            
        Returns:
            DataFrame with temporal features added
        """
        return create_temporal_features(df, timestamp_column)
    
    def create_load_features(self, load_df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates load-specific features.
        
        Args:
            load_df: DataFrame containing load forecast data
            
        Returns:
            DataFrame with load features added
        """
        return create_load_features(load_df)
    
    def create_generation_features(self, generation_df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates generation-specific features.
        
        Args:
            generation_df: DataFrame containing generation forecast data
            
        Returns:
            DataFrame with generation features added
        """
        return create_generation_features(generation_df)
    
    def create_price_features(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates price-specific features.
        
        Args:
            price_df: DataFrame containing historical price data
            
        Returns:
            DataFrame with price features added
        """
        return create_price_features(price_df)
    
    def clear_cache(self) -> None:
        """
        Clears the data cache.
        """
        self._data_cache = {}
        logger.info("Data cache cleared")