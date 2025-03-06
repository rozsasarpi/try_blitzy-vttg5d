"""
Defines core data model classes for the Electricity Market Price Forecasting System.

These models represent the fundamental data structures used throughout the system,
including input data sources (load forecasts, historical prices, generation forecasts)
and forecast outputs. The module provides base classes, data conversion utilities,
and dataframe integration for consistent data handling.
"""

import dataclasses  # standard library
from datetime import datetime  # standard library
from typing import Dict, List, Any, Optional, Type, TypeVar, Union, cast  # standard library

import numpy as np  # version: 1.24.0
import pandas as pd  # version: 2.0.0

from ..config.settings import FORECAST_PRODUCTS, PROBABILISTIC_SAMPLE_COUNT

# Global constants
SAMPLE_COLUMN_PREFIX = "sample_"

# Type variable for BaseDataModel generic methods
T = TypeVar('T', bound='BaseDataModel')


def create_sample_columns(count: int) -> List[str]:
    """
    Creates column names for probabilistic samples.
    
    Args:
        count: Number of sample columns to create
        
    Returns:
        List of column names for samples in format 'sample_001', 'sample_002', etc.
    
    Raises:
        ValueError: If count doesn't match the configured PROBABILISTIC_SAMPLE_COUNT
    """
    if count != PROBABILISTIC_SAMPLE_COUNT:
        raise ValueError(
            f"Sample count {count} does not match configured count {PROBABILISTIC_SAMPLE_COUNT}"
        )
    
    return [f"{SAMPLE_COLUMN_PREFIX}{i:03d}" for i in range(1, count + 1)]


def create_empty_forecast_dataframe() -> pd.DataFrame:
    """
    Creates an empty forecast dataframe with the correct structure.
    
    Returns:
        Empty DataFrame with forecast structure including timestamps, product,
        point forecast, samples, and metadata columns.
    """
    # Define column names
    columns = [
        'timestamp',
        'product',
        'point_forecast'
    ]
    
    # Add sample columns
    sample_columns = create_sample_columns(PROBABILISTIC_SAMPLE_COUNT)
    columns.extend(sample_columns)
    
    # Add metadata columns
    columns.extend(['generation_timestamp', 'is_fallback'])
    
    # Create empty DataFrame
    df = pd.DataFrame(columns=columns)
    
    # Set appropriate data types
    df = df.astype({
        'timestamp': 'datetime64[ns]',
        'product': 'string',
        'point_forecast': 'float64',
        'generation_timestamp': 'datetime64[ns]',
        'is_fallback': 'bool'
    })
    
    # Set sample columns to float type
    for col in sample_columns:
        df[col] = df[col].astype('float64')
    
    return df


@dataclasses.dataclass
class BaseDataModel:
    """
    Base class for all data models in the system.
    
    Provides common functionality for serialization and deserialization.
    """
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the model to a dictionary.
        
        Returns:
            Dictionary representation of the model
        """
        result = {}
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            # Handle datetime objects
            if isinstance(value, datetime):
                value = value.isoformat()
            result[field.name] = value
        return result
    
    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """
        Creates a model instance from a dictionary.
        
        Args:
            data: Dictionary containing model data
            
        Returns:
            Instance of the model
        """
        # Convert ISO format strings to datetime objects
        for field in dataclasses.fields(cls):
            if field.name in data and field.type == datetime and isinstance(data[field.name], str):
                data[field.name] = datetime.fromisoformat(data[field.name])
        
        return cls(**data)


@dataclasses.dataclass
class LoadForecast(BaseDataModel):
    """
    Data model for load forecast information.
    
    Represents electricity demand forecasts with timestamp, load amount, and region.
    """
    timestamp: datetime
    load_mw: float
    region: str
    
    def to_dataframe_row(self) -> Dict[str, Any]:
        """
        Converts the model to a dictionary suitable for a DataFrame row.
        
        Returns:
            Dictionary for DataFrame row
        """
        return {
            'timestamp': self.timestamp,
            'load_mw': self.load_mw,
            'region': self.region
        }


@dataclasses.dataclass
class HistoricalPrice(BaseDataModel):
    """
    Data model for historical price information.
    
    Represents historical electricity market prices with timestamp, product type,
    price value, and node location.
    """
    timestamp: datetime
    product: str
    price: float
    node: str
    
    def to_dataframe_row(self) -> Dict[str, Any]:
        """
        Converts the model to a dictionary suitable for a DataFrame row.
        
        Returns:
            Dictionary for DataFrame row
        """
        return {
            'timestamp': self.timestamp,
            'product': self.product,
            'price': self.price,
            'node': self.node
        }


@dataclasses.dataclass
class GenerationForecast(BaseDataModel):
    """
    Data model for generation forecast information.
    
    Represents electricity generation forecasts with timestamp, fuel type,
    generation amount, and region.
    """
    timestamp: datetime
    fuel_type: str
    generation_mw: float
    region: str
    
    def to_dataframe_row(self) -> Dict[str, Any]:
        """
        Converts the model to a dictionary suitable for a DataFrame row.
        
        Returns:
            Dictionary for DataFrame row
        """
        return {
            'timestamp': self.timestamp,
            'fuel_type': self.fuel_type,
            'generation_mw': self.generation_mw,
            'region': self.region
        }


@dataclasses.dataclass
class PriceForecast(BaseDataModel):
    """
    Data model for price forecast information.
    
    Represents electricity price forecasts with timestamp, product type,
    point forecast, probabilistic samples, and metadata.
    """
    timestamp: datetime
    product: str
    point_forecast: float
    samples: List[float]
    generation_timestamp: datetime
    is_fallback: bool = False
    
    def __post_init__(self):
        """
        Validates the price forecast data after initialization.
        
        Raises:
            ValueError: If product is invalid or sample count is incorrect
        """
        if self.product not in FORECAST_PRODUCTS:
            raise ValueError(f"Invalid product: {self.product}. Must be one of {FORECAST_PRODUCTS}")
        
        if len(self.samples) != PROBABILISTIC_SAMPLE_COUNT:
            raise ValueError(
                f"Sample count {len(self.samples)} does not match required count {PROBABILISTIC_SAMPLE_COUNT}"
            )
    
    def to_dataframe_row(self) -> Dict[str, Any]:
        """
        Converts the model to a dictionary suitable for a DataFrame row.
        
        Returns:
            Dictionary for DataFrame row with samples flattened into individual columns
        """
        # Create base dictionary
        result = {
            'timestamp': self.timestamp,
            'product': self.product,
            'point_forecast': self.point_forecast,
            'generation_timestamp': self.generation_timestamp,
            'is_fallback': self.is_fallback
        }
        
        # Add samples with appropriate column names
        sample_columns = create_sample_columns(len(self.samples))
        for i, sample in enumerate(self.samples):
            result[sample_columns[i]] = sample
        
        return result
    
    @classmethod
    def from_dataframe_row(cls, row: pd.Series) -> 'PriceForecast':
        """
        Creates a price forecast model from a DataFrame row.
        
        Args:
            row: Pandas Series containing a row from a forecast DataFrame
            
        Returns:
            Price forecast model instance
        """
        # Extract base fields
        timestamp = row['timestamp']
        product = row['product']
        point_forecast = row['point_forecast']
        generation_timestamp = row['generation_timestamp']
        is_fallback = row['is_fallback']
        
        # Extract samples from sample columns
        sample_columns = create_sample_columns(PROBABILISTIC_SAMPLE_COUNT)
        samples = [row[col] for col in sample_columns]
        
        return cls(
            timestamp=timestamp,
            product=product,
            point_forecast=point_forecast,
            samples=samples,
            generation_timestamp=generation_timestamp,
            is_fallback=is_fallback
        )
    
    def get_percentile(self, percentile: float) -> float:
        """
        Gets the value at a specific percentile from samples.
        
        Args:
            percentile: Percentile value between 0 and 1
            
        Returns:
            Value at the specified percentile
            
        Raises:
            ValueError: If percentile is not between 0 and 1
        """
        if not 0 <= percentile <= 1:
            raise ValueError(f"Percentile must be between 0 and 1, got {percentile}")
        
        return float(np.percentile(self.samples, percentile * 100))