"""
Implements functionality for retrieving, validating, and processing historical price data from external sources for the Electricity Market Price Forecasting System. This module is responsible for fetching historical electricity market prices, which are essential inputs for the forecasting models.
"""

# Standard library imports
import datetime
from typing import Dict, List, Any, Optional
import time

# External imports
import pandas as pd  # version: 2.0.0+
import numpy as np  # version: 1.24.0+

# Internal imports
from .api_client import APIClient
from .exceptions import (
    DataValidationError,
    APIConnectionError,
    APIResponseError,
    DataTimeRangeError
)
from .data_validator import validate_historical_prices_data
from .data_transformer import normalize_historical_prices_data
from ..models.data_models import HistoricalPrice
from ..config.settings import FORECAST_PRODUCTS, DATA_SOURCES
from ..utils.date_utils import localize_to_cst
from ..utils.logging_utils import (
    get_logger,
    log_execution_time,
    ComponentLogger
)

# Global variables
logger = ComponentLogger('historical_prices', {'component': 'data_ingestion'})
SOURCE_NAME = 'historical_prices'


@log_execution_time
def fetch_historical_prices(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    products: List[str],
    node: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetches historical price data from the external API for a specified date range.
    
    Args:
        start_date: Start date for the data range
        end_date: End date for the data range
        products: List of price products to fetch
        node: Optional pricing node identifier
        
    Returns:
        DataFrame containing historical price data
        
    Raises:
        APIConnectionError: If connection to the API fails
        APIResponseError: If the API returns an error response
        DataValidationError: If the data fails validation
    """
    logger.log_start("Fetching historical price data", {
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'products': products,
        'node': node
    })
    
    # Create API client
    api_client = APIClient(SOURCE_NAME)
    
    # Prepare additional parameters
    additional_params = {'products': ','.join(products)}
    if node:
        additional_params['node'] = node
    
    try:
        # Fetch data from API
        response = api_client.get_data(start_date, end_date, additional_params)
        
        # Convert to DataFrame
        if 'data' in response and isinstance(response['data'], list):
            data = response['data']
            df = pd.DataFrame(data)
        else:
            logger.log_failure("Fetching historical price data", time.time(), ValueError("Invalid API response format"), {
                'response': str(response)[:200]  # Truncate to avoid huge logs
            })
            raise APIResponseError(
                api_endpoint=f"{DATA_SOURCES[SOURCE_NAME]['url']}",
                status_code=200,  # Response was received but format is incorrect
                response_data={'error': 'Invalid response format, expected "data" list'}
            )
        
        # Validate data
        validation_result = validate_historical_prices_data(df)
        if not validation_result.is_valid:
            logger.log_failure("Validating historical price data", time.time(), 
                              DataValidationError(SOURCE_NAME, validation_result.errors.get("schema", [])), {
                'errors': validation_result.errors
            })
            raise DataValidationError(SOURCE_NAME, validation_result.errors.get("schema", []))
        
        # Normalize data
        df = normalize_historical_prices_data(df)
        
        # Log success
        logger.log_completion("Fetching historical price data", time.time(), {
            'row_count': len(df),
            'products': df['product'].unique().tolist(),
            'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}"
        })
        
        return df
        
    except APIConnectionError as e:
        logger.log_failure("Fetching historical price data", time.time(), e, {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat()
        })
        raise
    
    except APIResponseError as e:
        logger.log_failure("Fetching historical price data", time.time(), e, {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'status_code': e.status_code
        })
        raise
    
    except Exception as e:
        logger.log_failure("Fetching historical price data", time.time(), e, {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat()
        })
        raise


@log_execution_time
def get_historical_prices_for_model(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    products: Optional[List[str]] = None,
    node: Optional[str] = None
) -> pd.DataFrame:
    """
    Retrieves and processes historical price data for use in forecasting models.
    
    Args:
        start_date: Start date for the data range
        end_date: End date for the data range
        products: List of price products to fetch (uses all FORECAST_PRODUCTS if None)
        node: Optional pricing node identifier
        
    Returns:
        Processed historical price data ready for model input
        
    Raises:
        APIConnectionError: If connection to the API fails
        APIResponseError: If the API returns an error response
        DataValidationError: If the data fails validation
    """
    logger.log_start("Retrieving historical prices for model", {
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'products': products,
        'node': node
    })
    
    # If products not specified, use all forecast products
    if products is None:
        products = FORECAST_PRODUCTS
    
    try:
        # Fetch historical price data
        df = fetch_historical_prices(start_date, end_date, products, node)
        
        if df.empty:
            logger.log_completion("Retrieving historical prices for model", time.time(), {
                'status': 'empty_result'
            })
            return pd.DataFrame()
        
        # Process data for model input
        # 1. Pivot dataframe to have products as columns
        df_pivoted = pivot_prices_by_product(df)
        
        # 2. Ensure all required products are present
        missing_products = set(products) - set([col.replace('price_', '') for col in df_pivoted.columns if col.startswith('price_')])
        if missing_products:
            logger.adapter.warning(f"Missing products in historical price data: {missing_products}")
            # Add missing product columns with NaN
            for product in missing_products:
                df_pivoted[f'price_{product}'] = np.nan
        
        # 3. Fill missing values using forward fill then backfill
        df_pivoted = df_pivoted.ffill().bfill()
        
        logger.log_completion("Retrieving historical prices for model", time.time(), {
            'row_count': len(df_pivoted),
            'columns': df_pivoted.columns.tolist()
        })
        
        return df_pivoted
        
    except (APIConnectionError, APIResponseError, DataValidationError) as e:
        # These exceptions are already logged in fetch_historical_prices
        raise
    
    except Exception as e:
        logger.log_failure("Retrieving historical prices for model", time.time(), e, {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat()
        })
        raise


def filter_prices_by_product(df: pd.DataFrame, products: List[str]) -> pd.DataFrame:
    """
    Filters historical price data for specific products.
    
    Args:
        df: DataFrame containing historical price data
        products: List of price products to include
        
    Returns:
        Filtered DataFrame containing only specified products
    """
    # Check if DataFrame is empty
    if df.empty:
        return df.copy()
    
    # Validate that product column exists
    if 'product' not in df.columns:
        logger.adapter.error("Cannot filter by product: 'product' column not found in DataFrame")
        raise ValueError("Column 'product' not found in DataFrame")
    
    # Filter the DataFrame
    filtered_df = df[df['product'].isin(products)].copy()
    
    return filtered_df


def pivot_prices_by_product(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots historical price data to create columns for each product.
    
    Args:
        df: DataFrame containing historical price data
        
    Returns:
        Pivoted DataFrame with products as columns
    """
    # Check if DataFrame is empty
    if df.empty:
        return df.copy()
    
    # Validate that required columns exist
    required_columns = ['timestamp', 'product', 'price']
    for col in required_columns:
        if col not in df.columns:
            logger.adapter.error(f"Cannot pivot prices: Required column '{col}' not found in DataFrame")
            raise ValueError(f"Required column '{col}' not found in DataFrame")
    
    # Pivot the DataFrame
    pivoted_df = df.pivot_table(
        index='timestamp',
        columns='product',
        values='price',
        aggfunc='mean'  # Use mean in case there are duplicate timestamps for a product
    )
    
    # Handle missing values
    pivoted_df = pivoted_df.fillna(method='ffill').fillna(method='bfill')
    
    # Reset index to make timestamp a column again
    pivoted_df = pivoted_df.reset_index()
    
    # Rename columns to include 'price_' prefix
    renamed_columns = {col: f'price_{col}' for col in pivoted_df.columns if col != 'timestamp'}
    pivoted_df = pivoted_df.rename(columns=renamed_columns)
    
    return pivoted_df


def calculate_price_statistics(
    df: pd.DataFrame,
    products: List[str],
    window: Optional[str] = None
) -> pd.DataFrame:
    """
    Calculates statistical measures from historical price data.
    
    Args:
        df: DataFrame containing historical price data
        products: List of price products to include in statistics
        window: Optional rolling window (e.g., '24H' for 24 hour) for statistics
        
    Returns:
        DataFrame with price statistics
    """
    # Check if DataFrame is empty
    if df.empty:
        return pd.DataFrame()
    
    # Filter for specified products
    filtered_df = filter_prices_by_product(df, products)
    
    if filtered_df.empty:
        logger.adapter.warning(f"No data found for products {products}")
        return pd.DataFrame()
    
    # Create a DataFrame to hold statistics
    stats_df = pd.DataFrame()
    stats_df['timestamp'] = filtered_df['timestamp'].unique()
    stats_df = stats_df.sort_values('timestamp').reset_index(drop=True)
    
    # Calculate statistics for each product
    for product in products:
        product_df = filtered_df[filtered_df['product'] == product]
        
        if product_df.empty:
            continue
        
        # Set timestamp as index for time-based operations
        product_df = product_df.set_index('timestamp')
        
        if window:
            # Calculate rolling statistics
            rolling = product_df['price'].rolling(window=window)
            stats_df[f'{product}_mean'] = rolling.mean().reindex(stats_df['timestamp']).values
            stats_df[f'{product}_std'] = rolling.std().reindex(stats_df['timestamp']).values
            stats_df[f'{product}_min'] = rolling.min().reindex(stats_df['timestamp']).values
            stats_df[f'{product}_max'] = rolling.max().reindex(stats_df['timestamp']).values
        else:
            # Calculate global statistics
            stats_df[f'{product}_mean'] = product_df['price'].mean()
            stats_df[f'{product}_std'] = product_df['price'].std()
            stats_df[f'{product}_min'] = product_df['price'].min()
            stats_df[f'{product}_max'] = product_df['price'].max()
    
    return stats_df


class HistoricalPriceClient:
    """
    Client for retrieving historical price data from external sources.
    
    This class provides methods to fetch and process historical electricity market prices,
    which are essential inputs for the forecasting models.
    """
    
    def __init__(self):
        """Initializes the historical price client."""
        self._api_client = APIClient(SOURCE_NAME)
        self._logger = ComponentLogger('historical_prices', {
            'component': 'data_ingestion',
            'class': 'HistoricalPriceClient'
        })
    
    def get_historical_prices(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        products: Optional[List[str]] = None,
        node: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieves historical price data for a specified date range and products.
        
        Args:
            start_date: Start date for the data range
            end_date: End date for the data range
            products: List of price products to fetch (uses all FORECAST_PRODUCTS if None)
            node: Optional pricing node identifier
            
        Returns:
            DataFrame containing historical price data
        """
        self._logger.log_start("Retrieving historical prices", {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'products': products,
            'node': node
        })
        
        try:
            # Use the module-level function
            result_df = fetch_historical_prices(
                start_date=start_date,
                end_date=end_date,
                products=products if products is not None else FORECAST_PRODUCTS,
                node=node
            )
            
            self._logger.log_completion("Retrieving historical prices", time.time(), {
                'row_count': len(result_df)
            })
            
            return result_df
        
        except Exception as e:
            self._logger.log_failure("Retrieving historical prices", time.time(), e, {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            })
            raise
    
    def get_prices_for_model(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        products: Optional[List[str]] = None,
        node: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieves and processes historical price data for model input.
        
        Args:
            start_date: Start date for the data range
            end_date: End date for the data range
            products: List of price products to fetch (uses all FORECAST_PRODUCTS if None)
            node: Optional pricing node identifier
            
        Returns:
            Processed historical price data ready for model input
        """
        self._logger.log_start("Retrieving prices for model", {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'products': products,
            'node': node
        })
        
        try:
            # Use the module-level function
            result_df = get_historical_prices_for_model(
                start_date=start_date,
                end_date=end_date,
                products=products,
                node=node
            )
            
            self._logger.log_completion("Retrieving prices for model", time.time(), {
                'row_count': len(result_df)
            })
            
            return result_df
        
        except Exception as e:
            self._logger.log_failure("Retrieving prices for model", time.time(), e, {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            })
            raise
    
    def get_price_statistics(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        products: Optional[List[str]] = None,
        window: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Calculates statistical measures from historical price data.
        
        Args:
            start_date: Start date for the data range
            end_date: End date for the data range
            products: List of price products to include (uses all FORECAST_PRODUCTS if None)
            window: Optional rolling window (e.g., '24H' for 24 hour) for statistics
            
        Returns:
            DataFrame with price statistics
        """
        self._logger.log_start("Calculating price statistics", {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'products': products,
            'window': window
        })
        
        try:
            # Get historical prices
            price_df = self.get_historical_prices(
                start_date=start_date,
                end_date=end_date,
                products=products if products is not None else FORECAST_PRODUCTS,
                node=None  # Don't filter by node for statistics
            )
            
            if price_df.empty:
                self._logger.log_completion("Calculating price statistics", time.time(), {
                    'status': 'empty_result'
                })
                return pd.DataFrame()
            
            # Calculate statistics
            stats_df = calculate_price_statistics(
                df=price_df,
                products=products if products is not None else FORECAST_PRODUCTS,
                window=window
            )
            
            self._logger.log_completion("Calculating price statistics", time.time(), {
                'row_count': len(stats_df)
            })
            
            return stats_df
        
        except Exception as e:
            self._logger.log_failure("Calculating price statistics", time.time(), e, {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            })
            raise