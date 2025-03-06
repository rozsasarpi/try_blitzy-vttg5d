"""
Manages caching of forecast data for the Electricity Market Price Forecasting System's web visualization interface.

This module implements an in-memory caching system with timeout capabilities to improve dashboard 
responsiveness by reducing redundant API calls and data processing operations.
"""

import pandas as pd  # version 2.0.0+
import datetime
import logging
import hashlib
import functools
from typing import Dict, Optional, Any, Union, List, Tuple

from ..config.settings import CACHE_ENABLED, CACHE_TIMEOUT
from .schema import validate_forecast_dataframe
from ..utils.caching import CacheManager

# Set up module logger
logger = logging.getLogger(__name__)

# Global cache storage for forecasts
_forecast_cache = {}
_forecast_cache_metadata = {}
_cache_hits = 0
_cache_misses = 0

def generate_forecast_cache_key(product: str, 
                                date: Optional[Union[str, datetime.date, datetime.datetime]] = None,
                                end_date: Optional[Union[str, datetime.date, datetime.datetime]] = None,
                                format_str: Optional[str] = None) -> str:
    """
    Generates a unique cache key for forecast data based on product, date range, and format.
    
    Args:
        product: The forecast product (e.g., 'DALMP', 'RTLMP')
        date: Start date for the forecast data
        end_date: End date for the forecast data
        format_str: Optional format specification
        
    Returns:
        Unique cache key
    """
    # Convert date parameters to string format if they are date objects
    date_str = None
    end_date_str = None
    
    if date is not None:
        if isinstance(date, (datetime.date, datetime.datetime)):
            date_str = date.isoformat()
        else:
            date_str = str(date)
            
    if end_date is not None:
        if isinstance(end_date, (datetime.date, datetime.datetime)):
            end_date_str = end_date.isoformat()
        else:
            end_date_str = str(end_date)
    
    # Create a string representation of all parameters
    key_components = [
        f"product={product}",
        f"date={date_str}" if date_str else "",
        f"end_date={end_date_str}" if end_date_str else "",
        f"format={format_str}" if format_str else ""
    ]
    
    key_string = ":".join([comp for comp in key_components if comp])
    
    # Generate a hash of the string
    hash_obj = hashlib.md5(key_string.encode())
    return hash_obj.hexdigest()

def cache_forecast(key: str, forecast_df: pd.DataFrame, timeout: Optional[int] = None) -> bool:
    """
    Stores forecast data in the cache with metadata.
    
    Args:
        key: Unique cache key
        forecast_df: Forecast dataframe to cache
        timeout: Optional custom timeout in seconds
        
    Returns:
        True if caching was successful, False otherwise
    """
    global _forecast_cache, _forecast_cache_metadata
    
    # Check if caching is enabled
    if not CACHE_ENABLED:
        logger.debug("Caching is disabled, not storing forecast")
        return False
    
    # Validate the forecast dataframe
    is_valid, errors = validate_forecast_dataframe(forecast_df)
    if not is_valid:
        logger.error(f"Invalid forecast dataframe: {errors}")
        return False
    
    # Store the dataframe in the cache
    _forecast_cache[key] = forecast_df
    
    # Create metadata with timestamp and other info
    _forecast_cache_metadata[key] = {
        'timestamp': datetime.datetime.now(),
        'timeout': timeout,
        'product': forecast_df['product'].iloc[0] if 'product' in forecast_df.columns and len(forecast_df) > 0 else 'unknown',
        'rows': len(forecast_df)
    }
    
    logger.info(f"Cached forecast with key {key}: {len(forecast_df)} rows")
    return True

def get_cached_forecast(key: str) -> Optional[pd.DataFrame]:
    """
    Retrieves forecast data from the cache if it exists and is valid.
    
    Args:
        key: Cache key to retrieve
        
    Returns:
        Cached forecast dataframe or None if not found or expired
    """
    global _forecast_cache, _forecast_cache_metadata, _cache_hits, _cache_misses
    
    # Check if caching is enabled
    if not CACHE_ENABLED:
        logger.debug("Caching is disabled, not retrieving from cache")
        return None
    
    # Check if the key exists in the cache
    if key not in _forecast_cache:
        _cache_misses += 1
        logger.debug(f"Cache miss for key {key}")
        return None
    
    # Check if the cache entry is still valid
    if not is_forecast_cache_valid(key):
        _cache_misses += 1
        logger.debug(f"Cache expired for key {key}")
        
        # Remove expired cache entry
        del _forecast_cache[key]
        del _forecast_cache_metadata[key]
        return None
    
    # We have a valid cache hit
    _cache_hits += 1
    logger.debug(f"Cache hit for key {key}")
    
    return _forecast_cache[key]

def clear_forecast_cache(product: Optional[str] = None) -> int:
    """
    Clears all forecast cache or specific forecast cache entries.
    
    Args:
        product: If provided, only clears cache for this product
        
    Returns:
        Number of cache entries cleared
    """
    global _forecast_cache, _forecast_cache_metadata, _cache_hits, _cache_misses
    
    count = 0
    
    if product is None:
        # Clear all cache
        count = len(_forecast_cache)
        _forecast_cache = {}
        _forecast_cache_metadata = {}
        _cache_hits = 0
        _cache_misses = 0
        logger.info(f"Cleared all forecast cache entries ({count} items)")
    else:
        # Clear only the entries for the specified product
        keys_to_remove = []
        
        # Find all keys for the specified product
        for key, metadata in _forecast_cache_metadata.items():
            if metadata.get('product') == product:
                keys_to_remove.append(key)
        
        # Remove the keys
        for key in keys_to_remove:
            if key in _forecast_cache:
                del _forecast_cache[key]
            if key in _forecast_cache_metadata:
                del _forecast_cache_metadata[key]
            count += 1
        
        logger.info(f"Cleared forecast cache for product {product} ({count} items)")
    
    return count

def get_forecast_cache_stats() -> dict:
    """
    Returns statistics about the forecast cache usage.
    
    Returns:
        Dictionary containing cache statistics
    """
    global _forecast_cache, _forecast_cache_metadata, _cache_hits, _cache_misses
    
    # Count the number of entries in the cache
    entry_count = len(_forecast_cache)
    
    # Calculate the total size of cached forecasts (approximate)
    total_rows = sum(metadata.get('rows', 0) for metadata in _forecast_cache_metadata.values())
    
    # Count the number of expired entries
    expired_count = sum(1 for key in _forecast_cache_metadata if not is_forecast_cache_valid(key))
    
    # Calculate hit rate
    total_requests = _cache_hits + _cache_misses
    hit_rate = (_cache_hits / total_requests * 100) if total_requests > 0 else 0
    
    # Count entries by product
    products = {}
    for metadata in _forecast_cache_metadata.values():
        product = metadata.get('product', 'unknown')
        products[product] = products.get(product, 0) + 1
    
    return {
        'entry_count': entry_count,
        'total_rows': total_rows,
        'expired_count': expired_count,
        'hit_count': _cache_hits,
        'miss_count': _cache_misses,
        'hit_rate': hit_rate,
        'products': products
    }

def is_forecast_cache_valid(key: str) -> bool:
    """
    Checks if a forecast cache entry is still valid based on its timestamp.
    
    Args:
        key: Cache key to check
        
    Returns:
        True if cache is valid, False otherwise
    """
    global _forecast_cache_metadata
    
    # Check if caching is enabled
    if not CACHE_ENABLED:
        return False
    
    # Check if the key exists in the metadata
    if key not in _forecast_cache_metadata:
        return False
    
    # Get the timestamp and timeout from metadata
    metadata = _forecast_cache_metadata[key]
    timestamp = metadata.get('timestamp')
    timeout = metadata.get('timeout')
    
    if timestamp is None:
        return False
    
    # Calculate the age of the cache entry
    now = datetime.datetime.now()
    age = (now - timestamp).total_seconds()
    
    # Use the specified timeout or the default
    cache_timeout = timeout if timeout is not None else CACHE_TIMEOUT
    
    # Return True if the entry is still valid (age < timeout)
    return age < cache_timeout

def cache_forecast_decorator(timeout: Optional[int] = None):
    """
    Decorator that caches forecast data returned by a function.
    
    Args:
        timeout: Optional custom timeout in seconds
        
    Returns:
        Decorated function with forecast caching behavior
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract product and date parameters
            product = kwargs.get('product')
            if product is None and args:
                product = args[0]  # Assume first positional arg is product
            
            date = kwargs.get('date')
            end_date = kwargs.get('end_date')
            
            # If we can't determine product, fall back to function call
            if not product:
                return func(*args, **kwargs)
            
            # Generate cache key
            key = generate_forecast_cache_key(product, date, end_date)
            
            # Check if result is in cache
            cached_result = get_cached_forecast(key)
            if cached_result is not None:
                return cached_result
            
            # Call the original function
            result = func(*args, **kwargs)
            
            # Cache the result if it's a DataFrame
            if isinstance(result, pd.DataFrame):
                cache_forecast(key, result, timeout)
            
            return result
        return wrapper
    return decorator

class ForecastCacheManager:
    """
    Class that provides centralized management of forecast data caching.
    
    This class leverages the base CacheManager while adding forecast-specific
    functionality for efficiently caching and retrieving forecast dataframes.
    """
    
    def __init__(self):
        """
        Initializes the ForecastCacheManager with a CacheManager instance.
        """
        self._cache_manager = CacheManager()
        self._product_keys = {}  # Track keys by product
        self._logger = logging.getLogger(__name__ + '.ForecastCacheManager')
    
    def cache_forecast(self, product: str, forecast_df: pd.DataFrame, 
                      date: Optional[Union[str, datetime.date, datetime.datetime]] = None,
                      end_date: Optional[Union[str, datetime.date, datetime.datetime]] = None,
                      timeout: Optional[int] = None) -> bool:
        """
        Stores forecast data in the cache with metadata.
        
        Args:
            product: The forecast product
            forecast_df: Forecast dataframe to cache
            date: Start date for the forecast data
            end_date: End date for the forecast data
            timeout: Optional custom timeout in seconds
            
        Returns:
            True if caching was successful, False otherwise
        """
        # Generate a unique key for this forecast
        key = generate_forecast_cache_key(product, date, end_date)
        
        # Validate the forecast dataframe
        is_valid, errors = validate_forecast_dataframe(forecast_df)
        if not is_valid:
            self._logger.error(f"Invalid forecast dataframe: {errors}")
            return False
        
        # Store in the cache
        success = self._cache_manager.set(key, forecast_df, timeout)
        
        if success:
            # Track this key for the product
            if product not in self._product_keys:
                self._product_keys[product] = []
            self._product_keys[product].append(key)
            
            self._logger.info(f"Cached forecast for {product}: {len(forecast_df)} rows")
        
        return success
    
    def get_forecast(self, product: str, 
                    date: Optional[Union[str, datetime.date, datetime.datetime]] = None,
                    end_date: Optional[Union[str, datetime.date, datetime.datetime]] = None) -> Optional[pd.DataFrame]:
        """
        Retrieves forecast data from the cache if it exists and is valid.
        
        Args:
            product: The forecast product
            date: Start date for the forecast data
            end_date: End date for the forecast data
            
        Returns:
            Cached forecast dataframe or None if not found or expired
        """
        # Generate a unique key for this forecast
        key = generate_forecast_cache_key(product, date, end_date)
        
        # Retrieve from cache
        result = self._cache_manager.get(key)
        
        if result is not None:
            self._logger.debug(f"Cache hit for {product}")
        else:
            self._logger.debug(f"Cache miss for {product}")
        
        return result
    
    def clear_cache(self, product: Optional[str] = None) -> int:
        """
        Clears all cache or specific product cache entries.
        
        Args:
            product: If provided, only clears cache for this product
            
        Returns:
            Number of cache entries cleared
        """
        count = 0
        
        if product is None:
            # Clear all cache
            self._cache_manager.clear()
            count = sum(len(keys) for keys in self._product_keys.values())
            self._product_keys = {}
            self._logger.info(f"Cleared all forecast cache entries ({count} items)")
        else:
            # Clear only the entries for the specified product
            if product in self._product_keys:
                keys = self._product_keys[product]
                for key in keys:
                    self._cache_manager.invalidate(key)
                count = len(keys)
                del self._product_keys[product]
                self._logger.info(f"Cleared forecast cache for product {product} ({count} items)")
        
        return count
    
    def get_stats(self) -> dict:
        """
        Returns statistics about the forecast cache usage.
        
        Returns:
            Dictionary containing cache statistics
        """
        # Get basic stats from the cache manager
        basic_stats = self._cache_manager.get_stats()
        
        # Add forecast-specific stats
        forecast_stats = {
            'products': {product: len(keys) for product, keys in self._product_keys.items()},
            'product_count': len(self._product_keys)
        }
        
        # Combine the stats
        stats = {**basic_stats, **forecast_stats}
        
        return stats

# Singleton instance of ForecastCacheManager
forecast_cache_manager = ForecastCacheManager()