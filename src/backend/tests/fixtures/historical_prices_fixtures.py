"""
Provides test fixtures for historical price data used in testing the data ingestion components
of the Electricity Market Price Forecasting System. This module contains functions to generate
mock historical price data with various characteristics (valid, invalid, incomplete) for testing
data validation, API client functionality, and error handling.
"""

import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import numpy as np  # version: 1.24.0+
import pandas as pd  # version: 2.0.0+

from ...models.data_models import HistoricalPrice
from ...config.settings import FORECAST_PRODUCTS
from ...config.schema_config import HISTORICAL_PRICE_SCHEMA
from ...utils.date_utils import localize_to_cst

# Default values for test data
DEFAULT_NODE = "HB_NORTH"
DEFAULT_PRODUCTS = ["DALMP", "RTLMP", "RegUp"]

def create_mock_historical_price_data(
    start_date: datetime,
    end_date: datetime,
    products: Optional[List[str]] = None,
    node: Optional[str] = None
) -> pd.DataFrame:
    """
    Creates a DataFrame with mock historical price data for testing.
    
    Args:
        start_date: Start date for the historical data
        end_date: End date for the historical data
        products: List of products to include (defaults to DEFAULT_PRODUCTS)
        node: Node identifier (defaults to DEFAULT_NODE)
        
    Returns:
        DataFrame with mock historical price data
    """
    # Set default values if not provided
    if products is None:
        products = DEFAULT_PRODUCTS
    if node is None:
        node = DEFAULT_NODE
    
    # Calculate number of hours
    hours = int((end_date - start_date).total_seconds() / 3600) + 1
    
    # Generate timestamps
    timestamps = [start_date + timedelta(hours=i) for i in range(hours)]
    
    # Create lists for DataFrame columns
    timestamp_list = []
    product_list = []
    price_list = []
    node_list = []
    
    # Generate data for each timestamp and product
    for ts in timestamps:
        for product in products:
            timestamp_list.append(ts)
            product_list.append(product)
            price_list.append(generate_realistic_price(ts, product))
            node_list.append(node)
    
    # Create DataFrame
    df = pd.DataFrame({
        'timestamp': timestamp_list,
        'product': product_list,
        'price': price_list,
        'node': node_list
    })
    
    # Ensure timestamps are in CST timezone
    df['timestamp'] = df['timestamp'].apply(localize_to_cst)
    
    return df

def create_mock_historical_price_models(
    start_date: datetime,
    end_date: datetime,
    products: Optional[List[str]] = None,
    node: Optional[str] = None
) -> List[HistoricalPrice]:
    """
    Creates a list of HistoricalPrice model instances for testing.
    
    Args:
        start_date: Start date for the historical data
        end_date: End date for the historical data
        products: List of products to include (defaults to DEFAULT_PRODUCTS)
        node: Node identifier (defaults to DEFAULT_NODE)
        
    Returns:
        List of HistoricalPrice model instances
    """
    # Generate mock data
    df = create_mock_historical_price_data(start_date, end_date, products, node)
    
    # Convert DataFrame rows to HistoricalPrice instances
    models = []
    for _, row in df.iterrows():
        model = HistoricalPrice(
            timestamp=row['timestamp'],
            product=row['product'],
            price=row['price'],
            node=row['node']
        )
        models.append(model)
    
    return models

def create_incomplete_historical_price_data(
    start_date: datetime,
    end_date: datetime,
    products: Optional[List[str]] = None,
    node: Optional[str] = None,
    missing_percentage: float = 0.2
) -> pd.DataFrame:
    """
    Creates historical price data with missing hours for testing validation.
    
    Args:
        start_date: Start date for the historical data
        end_date: End date for the historical data
        products: List of products to include (defaults to DEFAULT_PRODUCTS)
        node: Node identifier (defaults to DEFAULT_NODE)
        missing_percentage: Percentage of rows to remove (0.0-1.0)
        
    Returns:
        DataFrame with incomplete historical price data
    """
    # Generate complete mock data
    df = create_mock_historical_price_data(start_date, end_date, products, node)
    
    # Calculate number of rows to remove
    total_rows = len(df)
    rows_to_remove = int(total_rows * missing_percentage)
    
    # Randomly select rows to remove
    indices_to_remove = np.random.choice(df.index, size=rows_to_remove, replace=False)
    
    # Remove selected rows
    incomplete_df = df.drop(indices_to_remove).reset_index(drop=True)
    
    return incomplete_df

def create_invalid_historical_price_data(
    start_date: datetime,
    end_date: datetime,
    invalid_type: str
) -> pd.DataFrame:
    """
    Creates invalid historical price data for testing validation failures.
    
    Args:
        start_date: Start date for the historical data
        end_date: End date for the historical data
        invalid_type: Type of invalidity to create:
                     'missing_columns': Remove required columns
                     'invalid_product': Add rows with invalid product values
                     'invalid_timestamp': Add rows with invalid timestamp format
                     'invalid_price': Add rows with non-numeric price values
        
    Returns:
        DataFrame with invalid historical price data
    """
    # Generate valid mock data
    df = create_mock_historical_price_data(start_date, end_date)
    
    if invalid_type == 'missing_columns':
        # Remove a required column
        df = df.drop(columns=['product'])
    
    elif invalid_type == 'invalid_product':
        # Add rows with invalid product values
        invalid_rows = df.sample(10).copy()
        invalid_rows['product'] = 'InvalidProduct'
        df = pd.concat([df, invalid_rows], ignore_index=True)
    
    elif invalid_type == 'invalid_timestamp':
        # Add rows with invalid timestamp format
        invalid_rows = df.sample(10).copy()
        invalid_rows['timestamp'] = 'not-a-date'
        df = pd.concat([df, invalid_rows], ignore_index=True)
    
    elif invalid_type == 'invalid_price':
        # Add rows with non-numeric price values
        invalid_rows = df.sample(10).copy()
        invalid_rows['price'] = 'not-a-number'
        df = pd.concat([df, invalid_rows], ignore_index=True)
    
    return df

def create_mock_api_response(df: pd.DataFrame) -> Dict:
    """
    Creates a mock API response for testing API client.
    
    Args:
        df: DataFrame containing historical price data
        
    Returns:
        Dictionary representing API response with data
    """
    # Convert DataFrame to dictionary format similar to API response
    records = []
    for _, row in df.iterrows():
        record = {
            'timestamp': row['timestamp'].isoformat(),
            'product': row['product'],
            'price': row['price'],
            'node': row['node']
        }
        records.append(record)
    
    # Create response structure
    response = {
        'data': records,
        'metadata': {
            'count': len(records),
            'status': 'success'
        }
    }
    
    return response

def generate_realistic_price(timestamp: datetime, product: str) -> float:
    """
    Generates realistic price values for different products and times.
    
    Args:
        timestamp: Timestamp for the price value
        product: Product identifier (DALMP, RTLMP, etc.)
        
    Returns:
        Realistic price value
    """
    # Base price ranges for different products
    base_prices = {
        "DALMP": 30.0,
        "RTLMP": 35.0,
        "RegUp": 15.0,
        "RegDown": 12.0,
        "RRS": 8.0,
        "NSRS": 5.0
    }
    
    # Get base price for product (default to 25 if not in dictionary)
    base_price = base_prices.get(product, 25.0)
    
    # Apply time-of-day factors (higher during peak hours)
    hour = timestamp.hour
    if 7 <= hour <= 10:  # Morning peak
        time_factor = 1.3
    elif 17 <= hour <= 21:  # Evening peak
        time_factor = 1.5
    elif 11 <= hour <= 16:  # Midday
        time_factor = 1.2
    else:  # Night/early morning
        time_factor = 0.8
    
    # Apply day-of-week factors (higher on weekdays)
    weekday = timestamp.weekday()
    day_factor = 1.1 if weekday < 5 else 0.9  # Weekday vs weekend
    
    # Calculate price with some random variation
    price = base_price * time_factor * day_factor
    
    # Add random variation (±15%)
    variation = random.uniform(0.85, 1.15)
    price *= variation
    
    return round(price, 2)

class MockHistoricalPriceClient:
    """
    Mock client for testing historical price API interactions.
    """
    
    def __init__(self):
        """
        Initializes the mock client with empty responses.
        """
        self._responses = {}
        self._error = None
    
    def add_response(self, params: Dict, response: Dict) -> None:
        """
        Adds a mock response for specific parameters.
        
        Args:
            params: Query parameters dictionary
            response: Mock response to return
        """
        # Convert params dict to hashable key
        key = tuple(sorted((k, str(v)) for k, v in params.items()))
        self._responses[key] = response
    
    def set_error(self, error: Exception) -> None:
        """
        Sets an error to be raised on fetch_data calls.
        
        Args:
            error: Exception to raise
        """
        self._error = error
    
    def fetch_data(self, params: Dict) -> Dict:
        """
        Mock implementation of fetch_data that returns predefined responses.
        
        Args:
            params: Query parameters
            
        Returns:
            Mock API response
            
        Raises:
            Exception: If an error was set using set_error
        """
        if self._error:
            raise self._error
        
        # Convert params to hashable key
        key = tuple(sorted((k, str(v)) for k, v in params.items()))
        
        # Return matching response or empty data
        if key in self._responses:
            return self._responses[key]
        else:
            return {'data': [], 'metadata': {'count': 0, 'status': 'success'}}
    
    def get_data(self, params: Dict) -> Dict:
        """
        Mock implementation of get_data that calls fetch_data.
        
        Args:
            params: Query parameters
            
        Returns:
            Mock API response
        """
        return self.fetch_data(params)