"""
Module responsible for processing electricity market price forecast data for visualization
in the Dash-based dashboard. Provides functions to transform, filter, aggregate, and prepare
forecast data for various visualization components including time series plots, probability
distributions, and tabular displays.
"""

# Standard library imports
import datetime
import logging
from typing import Dict, List, Optional, Union, Any, Tuple

# External library imports
import pandas as pd  # version 2.0.0+
import numpy as np  # version 1.24.0+ 

# Internal imports
from .schema import (
    prepare_dataframe_for_visualization,
    extract_samples_from_dataframe,
    validate_forecast_dataframe
)
from ..config.product_config import (
    PRODUCTS,
    get_product_unit,
    can_be_negative
)
from ..config.dashboard_config import DISTRIBUTION_CONFIG
from ..utils.formatting import (
    format_price,
    format_datetime,
    format_range
)
from ..utils.date_helpers import (
    localize_to_cst,
    get_date_hour_label
)

# Configure logger
logger = logging.getLogger(__name__)

# Default percentiles for uncertainty bands
DEFAULT_PERCENTILES = [10, 90]


def filter_forecast_by_product(df: pd.DataFrame, product: str) -> pd.DataFrame:
    """
    Filters a forecast dataframe to include only data for a specific product.
    
    Args:
        df: Forecast dataframe
        product: Product identifier
        
    Returns:
        Filtered dataframe containing only the specified product
    """
    logger.debug(f"Filtering forecast data for product: {product}")
    
    # Validate dataframe has product column
    if 'product' not in df.columns:
        raise ValueError("Dataframe must have a 'product' column")
    
    # Validate product is valid
    if product not in PRODUCTS:
        raise ValueError(f"Invalid product: {product}. Must be one of {PRODUCTS}")
    
    # Filter dataframe by product
    filtered_df = df[df['product'] == product].copy()
    
    if filtered_df.empty:
        logger.warning(f"No data found for product: {product}")
        
    return filtered_df


def filter_forecast_by_date_range(
    df: pd.DataFrame, 
    start_date: Union[str, datetime.date, datetime.datetime],
    end_date: Union[str, datetime.date, datetime.datetime]
) -> pd.DataFrame:
    """
    Filters a forecast dataframe to include only data within a specific date range.
    
    Args:
        df: Forecast dataframe
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        
    Returns:
        Filtered dataframe containing only data within the date range
    """
    logger.debug(f"Filtering forecast data for date range: {start_date} to {end_date}")
    
    # Validate dataframe has timestamp column
    if 'timestamp' not in df.columns:
        raise ValueError("Dataframe must have a 'timestamp' column")
    
    # Convert string dates to datetime if needed
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    
    # Ensure datetime objects are pandas Timestamp objects
    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)
    
    # If end_date is a date (not datetime), set it to end of the day
    if hasattr(end_date, 'time') and end_date.time() == datetime.time(0, 0, 0):
        end_date = end_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    # Filter dataframe by date range
    filtered_df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)].copy()
    
    if filtered_df.empty:
        logger.warning(f"No data found for date range: {start_date} to {end_date}")
        
    return filtered_df


def prepare_time_series_data(df: pd.DataFrame, product: str) -> pd.DataFrame:
    """
    Prepares forecast data for time series visualization.
    
    Args:
        df: Forecast dataframe
        product: Product identifier
        
    Returns:
        Processed dataframe ready for time series visualization
    """
    logger.debug(f"Preparing time series data for product: {product}")
    
    try:
        # Filter dataframe by product
        filtered_df = filter_forecast_by_product(df, product)
        
        # Ensure required columns are present
        required_columns = ['timestamp', 'point_forecast', 'lower_bound', 'upper_bound']
        missing_columns = [col for col in required_columns if col not in filtered_df.columns]
        
        if missing_columns:
            # If missing uncertainty bounds, try to prepare it from samples
            if 'lower_bound' in missing_columns or 'upper_bound' in missing_columns:
                filtered_df = prepare_dataframe_for_visualization(filtered_df)
            else:
                raise ValueError(f"Dataframe missing required columns: {missing_columns}")
        
        # Sort by timestamp
        filtered_df = filtered_df.sort_values('timestamp')
        
        # Format timestamps for x-axis labels
        filtered_df['x_label'] = filtered_df['timestamp'].apply(get_date_hour_label)
        
        # Format values for tooltips
        filtered_df['point_forecast_formatted'] = filtered_df['point_forecast'].apply(
            lambda x: format_price(x, product)
        )
        filtered_df['lower_bound_formatted'] = filtered_df['lower_bound'].apply(
            lambda x: format_price(x, product)
        )
        filtered_df['upper_bound_formatted'] = filtered_df['upper_bound'].apply(
            lambda x: format_price(x, product)
        )
        
        # Add price range column
        filtered_df['range'] = filtered_df['upper_bound'] - filtered_df['lower_bound']
        filtered_df['range_formatted'] = filtered_df.apply(
            lambda row: format_range(row['lower_bound'], row['upper_bound'], product),
            axis=1
        )
        
        return filtered_df
        
    except Exception as e:
        logger.error(f"Error preparing time series data: {str(e)}")
        raise


def prepare_distribution_data(
    df: pd.DataFrame, 
    product: str, 
    target_hour: Union[str, datetime.date, datetime.datetime, int]
) -> pd.DataFrame:
    """
    Prepares forecast data for probability distribution visualization.
    
    Args:
        df: Forecast dataframe
        product: Product identifier
        target_hour: Specific hour to visualize (datetime, index, or timestamp)
        
    Returns:
        Processed dataframe ready for distribution visualization
    """
    logger.debug(f"Preparing distribution data for product: {product}, target hour: {target_hour}")
    
    try:
        # Filter dataframe by product
        filtered_df = filter_forecast_by_product(df, product)
        
        # Find the target row based on target_hour type
        if isinstance(target_hour, (datetime.datetime, datetime.date, str)):
            # Convert to datetime if string
            if isinstance(target_hour, str):
                target_hour = pd.to_datetime(target_hour)
                
            # Find closest timestamp in the dataframe
            target_hour = pd.Timestamp(target_hour)
            idx = (filtered_df['timestamp'] - target_hour).abs().idxmin()
            target_row = filtered_df.loc[idx]
        elif isinstance(target_hour, int):
            # Use as index for the dataframe
            if target_hour >= len(filtered_df):
                raise ValueError(f"Target hour index {target_hour} out of range (0-{len(filtered_df)-1})")
            target_row = filtered_df.iloc[target_hour]
        else:
            raise ValueError(f"Invalid target_hour type: {type(target_hour)}")
        
        # Get sample columns
        sample_columns = [col for col in filtered_df.columns if col.startswith('sample_')]
        
        if not sample_columns:
            raise ValueError("No sample columns found in dataframe")
        
        # Extract samples for the target hour
        samples = target_row[sample_columns].values
        
        # Create distribution dataframe
        distribution_df = pd.DataFrame({
            'value': samples,
            'value_formatted': [format_price(x, product) for x in samples],
        })
        
        # Add target hour information
        distribution_df['target_hour'] = target_row['timestamp']
        distribution_df['target_hour_formatted'] = get_date_hour_label(target_row['timestamp'])
        
        # Add point forecast info
        distribution_df['point_forecast'] = target_row['point_forecast']
        distribution_df['point_forecast_formatted'] = format_price(target_row['point_forecast'], product)
        
        return distribution_df
        
    except Exception as e:
        logger.error(f"Error preparing distribution data: {str(e)}")
        raise


def prepare_hourly_table_data(
    df: pd.DataFrame, 
    product: str,
    percentiles: List[int] = None
) -> pd.DataFrame:
    """
    Prepares forecast data for tabular display with hourly values.
    
    Args:
        df: Forecast dataframe
        product: Product identifier
        percentiles: List of percentiles to include (default: [10, 90])
        
    Returns:
        Processed dataframe ready for tabular display
    """
    logger.debug(f"Preparing hourly table data for product: {product}")
    
    # Use default percentiles if not provided
    if percentiles is None:
        percentiles = DEFAULT_PERCENTILES
    
    try:
        # Filter dataframe by product
        filtered_df = filter_forecast_by_product(df, product)
        
        # Extract percentiles if they don't already exist
        for percentile in percentiles:
            percentile_col = f"percentile_{percentile}"
            if percentile_col not in filtered_df.columns:
                extracted_df = extract_samples_from_dataframe(filtered_df, percentiles)
                for p in percentiles:
                    filtered_df[f"percentile_{p}"] = extracted_df[f"percentile_{p}"]
        
        # Create a new dataframe for the table
        table_df = pd.DataFrame()
        
        # Format timestamps as hour labels
        table_df['timestamp'] = filtered_df['timestamp']
        table_df['hour'] = filtered_df['timestamp'].apply(get_date_hour_label)
        
        # Add point forecast
        table_df['point_forecast'] = filtered_df['point_forecast']
        table_df['point_forecast_formatted'] = filtered_df['point_forecast'].apply(
            lambda x: format_price(x, product)
        )
        
        # Add percentiles
        for percentile in percentiles:
            percentile_col = f"percentile_{percentile}"
            table_df[percentile_col] = filtered_df[percentile_col]
            table_df[f"{percentile_col}_formatted"] = filtered_df[percentile_col].apply(
                lambda x: format_price(x, product)
            )
        
        # Calculate and format price range
        if len(percentiles) >= 2:
            min_percentile = min(percentiles)
            max_percentile = max(percentiles)
            table_df['range'] = filtered_df[f"percentile_{max_percentile}"] - filtered_df[f"percentile_{min_percentile}"]
            table_df['range_formatted'] = table_df.apply(
                lambda row: format_range(
                    row[f"percentile_{min_percentile}"], 
                    row[f"percentile_{max_percentile}"], 
                    product
                ),
                axis=1
            )
        
        return table_df
        
    except Exception as e:
        logger.error(f"Error preparing hourly table data: {str(e)}")
        raise


def prepare_comparison_data(df: pd.DataFrame, products: List[str]) -> pd.DataFrame:
    """
    Prepares forecast data for product comparison visualization.
    
    Args:
        df: Forecast dataframe
        products: List of product identifiers to compare
        
    Returns:
        Processed dataframe ready for comparison visualization
    """
    logger.debug(f"Preparing comparison data for products: {products}")
    
    try:
        # Validate products
        invalid_products = [p for p in products if p not in PRODUCTS]
        if invalid_products:
            raise ValueError(f"Invalid products: {invalid_products}. Must be in {PRODUCTS}")
        
        # Filter dataframe to include only specified products
        filtered_df = df[df['product'].isin(products)].copy()
        
        if filtered_df.empty:
            logger.warning(f"No data found for products: {products}")
            return pd.DataFrame()
        
        # Ensure required columns are present
        required_columns = ['timestamp', 'product', 'point_forecast']
        missing_columns = [col for col in required_columns if col not in filtered_df.columns]
        
        if missing_columns:
            raise ValueError(f"Dataframe missing required columns: {missing_columns}")
        
        # Sort by timestamp and product
        filtered_df = filtered_df.sort_values(['timestamp', 'product'])
        
        # Format timestamps for x-axis labels
        filtered_df['x_label'] = filtered_df['timestamp'].apply(get_date_hour_label)
        
        # Format values for tooltips
        filtered_df['point_forecast_formatted'] = filtered_df.apply(
            lambda row: format_price(row['point_forecast'], row['product']),
            axis=1
        )
        
        return filtered_df
        
    except Exception as e:
        logger.error(f"Error preparing comparison data: {str(e)}")
        raise


def calculate_statistics(df: pd.DataFrame, product: str) -> Dict[str, Any]:
    """
    Calculates statistical metrics for forecast data.
    
    Args:
        df: Forecast dataframe
        product: Product identifier
        
    Returns:
        Dictionary containing statistical metrics
    """
    logger.debug(f"Calculating statistics for product: {product}")
    
    try:
        # Filter dataframe by product
        filtered_df = filter_forecast_by_product(df, product)
        
        if filtered_df.empty:
            logger.warning(f"No data found for product: {product}")
            return {}
        
        # Calculate basic statistics for point forecast
        stats = {
            'min': filtered_df['point_forecast'].min(),
            'max': filtered_df['point_forecast'].max(),
            'mean': filtered_df['point_forecast'].mean(),
            'median': filtered_df['point_forecast'].median(),
            'std_dev': filtered_df['point_forecast'].std(),
        }
        
        # Format statistics
        unit = get_product_unit(product)
        stats_formatted = {
            'min_formatted': format_price(stats['min'], product),
            'max_formatted': format_price(stats['max'], product),
            'mean_formatted': format_price(stats['mean'], product),
            'median_formatted': format_price(stats['median'], product),
            'std_dev_formatted': format_price(stats['std_dev'], product),
            'unit': unit
        }
        
        # Identify peak hours (top 3 highest prices)
        peak_hours = filtered_df.nlargest(3, 'point_forecast')
        peak_hours_formatted = []
        for _, row in peak_hours.iterrows():
            peak_hours_formatted.append({
                'timestamp': row['timestamp'],
                'hour_formatted': get_date_hour_label(row['timestamp']),
                'price': row['point_forecast'],
                'price_formatted': format_price(row['point_forecast'], product)
            })
        
        # Combine statistics
        result = {**stats, **stats_formatted, 'peak_hours': peak_hours_formatted}
        
        return result
        
    except Exception as e:
        logger.error(f"Error calculating statistics: {str(e)}")
        raise


def identify_price_spikes(
    df: pd.DataFrame, 
    product: str,
    threshold_multiplier: float = 2.0
) -> pd.DataFrame:
    """
    Identifies potential price spikes in forecast data.
    
    Args:
        df: Forecast dataframe
        product: Product identifier
        threshold_multiplier: Multiplier for standard deviation to identify spikes
        
    Returns:
        Dataframe containing identified price spikes
    """
    logger.debug(f"Identifying price spikes for product: {product}")
    
    try:
        # Filter dataframe by product
        filtered_df = filter_forecast_by_product(df, product)
        
        if filtered_df.empty:
            logger.warning(f"No data found for product: {product}")
            return pd.DataFrame()
        
        # Calculate mean and standard deviation
        mean = filtered_df['point_forecast'].mean()
        std_dev = filtered_df['point_forecast'].std()
        
        # Define threshold for spikes
        threshold = mean + (threshold_multiplier * std_dev)
        
        # Identify rows exceeding threshold
        spikes = filtered_df[filtered_df['point_forecast'] > threshold].copy()
        
        if spikes.empty:
            logger.info(f"No price spikes identified for product: {product}")
            return pd.DataFrame()
        
        # Add formatted columns
        spikes['threshold'] = threshold
        spikes['deviation'] = spikes['point_forecast'] - mean
        spikes['deviation_percent'] = (spikes['point_forecast'] / mean - 1) * 100
        
        spikes['point_forecast_formatted'] = spikes['point_forecast'].apply(
            lambda x: format_price(x, product)
        )
        spikes['threshold_formatted'] = format_price(threshold, product)
        spikes['deviation_formatted'] = spikes['deviation'].apply(
            lambda x: format_price(x, product)
        )
        spikes['deviation_percent_formatted'] = spikes['deviation_percent'].apply(
            lambda x: f"{x:.1f}%"
        )
        spikes['hour_formatted'] = spikes['timestamp'].apply(get_date_hour_label)
        
        return spikes
        
    except Exception as e:
        logger.error(f"Error identifying price spikes: {str(e)}")
        raise


def resample_forecast_data(
    df: pd.DataFrame, 
    product: str,
    frequency: str
) -> pd.DataFrame:
    """
    Resamples forecast data to a different time frequency.
    
    Args:
        df: Forecast dataframe
        product: Product identifier
        frequency: Pandas frequency string (e.g., '4H' for 4-hour intervals)
        
    Returns:
        Resampled dataframe
    """
    logger.debug(f"Resampling forecast data for product: {product}, frequency: {frequency}")
    
    try:
        # Filter dataframe by product
        filtered_df = filter_forecast_by_product(df, product)
        
        if filtered_df.empty:
            logger.warning(f"No data found for product: {product}")
            return pd.DataFrame()
        
        # Ensure we have the necessary columns
        required_columns = ['timestamp', 'point_forecast']
        missing_columns = [col for col in required_columns if col not in filtered_df.columns]
        
        if missing_columns:
            raise ValueError(f"Dataframe missing required columns: {missing_columns}")
        
        # Set timestamp as index
        temp_df = filtered_df.set_index('timestamp')
        
        # Determine which columns to resample
        numeric_columns = ['point_forecast']
        if 'lower_bound' in temp_df.columns:
            numeric_columns.append('lower_bound')
        if 'upper_bound' in temp_df.columns:
            numeric_columns.append('upper_bound')
        
        # Resample the dataframe
        resampled = temp_df[numeric_columns].resample(frequency)
        
        # Aggregate with appropriate functions
        result = pd.DataFrame()
        result['point_forecast'] = resampled['point_forecast'].mean()
        
        if 'lower_bound' in numeric_columns:
            result['lower_bound'] = resampled['lower_bound'].min()
        
        if 'upper_bound' in numeric_columns:
            result['upper_bound'] = resampled['upper_bound'].max()
        
        # Reset index to make timestamp a column again
        result = result.reset_index()
        
        # Add product column back
        result['product'] = product
        
        return result
        
    except Exception as e:
        logger.error(f"Error resampling forecast data: {str(e)}")
        raise


def get_forecast_summary(df: pd.DataFrame, product: str) -> Dict[str, Any]:
    """
    Generates a summary of forecast data for display.
    
    Args:
        df: Forecast dataframe
        product: Product identifier
        
    Returns:
        Dictionary containing forecast summary information
    """
    logger.debug(f"Generating forecast summary for product: {product}")
    
    try:
        # Filter dataframe by product
        filtered_df = filter_forecast_by_product(df, product)
        
        if filtered_df.empty:
            logger.warning(f"No data found for product: {product}")
            return {}
        
        # Extract date range
        start_timestamp = filtered_df['timestamp'].min()
        end_timestamp = filtered_df['timestamp'].max()
        
        # Calculate average price and range
        avg_price = filtered_df['point_forecast'].mean()
        
        # Check if we have uncertainty bounds
        has_bounds = 'lower_bound' in filtered_df.columns and 'upper_bound' in filtered_df.columns
        
        if has_bounds:
            avg_lower = filtered_df['lower_bound'].mean()
            avg_upper = filtered_df['upper_bound'].mean()
            avg_range = avg_upper - avg_lower
        else:
            avg_lower = None
            avg_upper = None
            avg_range = None
        
        # Check if forecast is a fallback
        is_fallback = False
        if 'is_fallback' in filtered_df.columns:
            is_fallback = filtered_df['is_fallback'].any()
        
        # Format values
        unit = get_product_unit(product)
        
        summary = {
            'product': product,
            'product_display': get_product_unit(product),
            'start_timestamp': start_timestamp,
            'end_timestamp': end_timestamp,
            'forecast_hours': len(filtered_df),
            'avg_price': avg_price,
            'avg_lower': avg_lower,
            'avg_upper': avg_upper,
            'avg_range': avg_range,
            'is_fallback': is_fallback,
            'unit': unit,
            
            # Formatted versions
            'start_formatted': format_datetime(start_timestamp),
            'end_formatted': format_datetime(end_timestamp),
            'avg_price_formatted': format_price(avg_price, product),
        }
        
        if has_bounds:
            summary['avg_lower_formatted'] = format_price(avg_lower, product)
            summary['avg_upper_formatted'] = format_price(avg_upper, product)
            summary['avg_range_formatted'] = format_price(avg_range, product)
        
        return summary
        
    except Exception as e:
        logger.error(f"Error generating forecast summary: {str(e)}")
        raise


class ForecastDataProcessor:
    """
    Class that provides methods for processing forecast data for visualization.
    """
    
    def __init__(self):
        """
        Initializes the ForecastDataProcessor.
        """
        self.logger = logging.getLogger(__name__ + '.ForecastDataProcessor')
        self.logger.info("Initializing ForecastDataProcessor")
    
    def filter_forecast_by_product(self, df: pd.DataFrame, product: str) -> pd.DataFrame:
        """
        Filters a forecast dataframe to include only data for a specific product.
        
        Args:
            df: Forecast dataframe
            product: Product identifier
            
        Returns:
            Filtered dataframe containing only the specified product
        """
        return filter_forecast_by_product(df, product)
    
    def filter_forecast_by_date_range(
        self, 
        df: pd.DataFrame, 
        start_date: Union[str, datetime.date, datetime.datetime],
        end_date: Union[str, datetime.date, datetime.datetime]
    ) -> pd.DataFrame:
        """
        Filters a forecast dataframe to include only data within a specific date range.
        
        Args:
            df: Forecast dataframe
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            Filtered dataframe containing only data within the date range
        """
        return filter_forecast_by_date_range(df, start_date, end_date)
    
    def prepare_time_series_data(self, df: pd.DataFrame, product: str) -> pd.DataFrame:
        """
        Prepares forecast data for time series visualization.
        
        Args:
            df: Forecast dataframe
            product: Product identifier
            
        Returns:
            Processed dataframe ready for time series visualization
        """
        return prepare_time_series_data(df, product)
    
    def prepare_distribution_data(
        self,
        df: pd.DataFrame, 
        product: str, 
        target_hour: Union[str, datetime.date, datetime.datetime, int]
    ) -> pd.DataFrame:
        """
        Prepares forecast data for probability distribution visualization.
        
        Args:
            df: Forecast dataframe
            product: Product identifier
            target_hour: Specific hour to visualize (datetime, index, or timestamp)
            
        Returns:
            Processed dataframe ready for distribution visualization
        """
        return prepare_distribution_data(df, product, target_hour)
    
    def prepare_hourly_table_data(
        self,
        df: pd.DataFrame, 
        product: str,
        percentiles: List[int] = None
    ) -> pd.DataFrame:
        """
        Prepares forecast data for tabular display with hourly values.
        
        Args:
            df: Forecast dataframe
            product: Product identifier
            percentiles: List of percentiles to include (default: [10, 90])
            
        Returns:
            Processed dataframe ready for tabular display
        """
        return prepare_hourly_table_data(df, product, percentiles)
    
    def prepare_comparison_data(self, df: pd.DataFrame, products: List[str]) -> pd.DataFrame:
        """
        Prepares forecast data for product comparison visualization.
        
        Args:
            df: Forecast dataframe
            products: List of product identifiers to compare
            
        Returns:
            Processed dataframe ready for comparison visualization
        """
        return prepare_comparison_data(df, products)
    
    def calculate_statistics(self, df: pd.DataFrame, product: str) -> Dict[str, Any]:
        """
        Calculates statistical metrics for forecast data.
        
        Args:
            df: Forecast dataframe
            product: Product identifier
            
        Returns:
            Dictionary containing statistical metrics
        """
        return calculate_statistics(df, product)
    
    def identify_price_spikes(
        self,
        df: pd.DataFrame, 
        product: str,
        threshold_multiplier: float = 2.0
    ) -> pd.DataFrame:
        """
        Identifies potential price spikes in forecast data.
        
        Args:
            df: Forecast dataframe
            product: Product identifier
            threshold_multiplier: Multiplier for standard deviation to identify spikes
            
        Returns:
            Dataframe containing identified price spikes
        """
        return identify_price_spikes(df, product, threshold_multiplier)
    
    def resample_forecast_data(
        self,
        df: pd.DataFrame, 
        product: str,
        frequency: str
    ) -> pd.DataFrame:
        """
        Resamples forecast data to a different time frequency.
        
        Args:
            df: Forecast dataframe
            product: Product identifier
            frequency: Pandas frequency string (e.g., '4H' for 4-hour intervals)
            
        Returns:
            Resampled dataframe
        """
        return resample_forecast_data(df, product, frequency)
    
    def get_forecast_summary(self, df: pd.DataFrame, product: str) -> Dict[str, Any]:
        """
        Generates a summary of forecast data for display.
        
        Args:
            df: Forecast dataframe
            product: Product identifier
            
        Returns:
            Dictionary containing forecast summary information
        """
        return get_forecast_summary(df, product)


# Create a singleton instance for application-wide use
data_processor = ForecastDataProcessor()