"""
Utility module providing general-purpose caching functionality for the Electricity Market
Price Forecasting System's web visualization interface.

This module implements memory-based and disk-based caching with timeout capabilities to
improve dashboard responsiveness and reduce redundant data processing operations.
"""

import datetime
import functools
import hashlib
import logging
import os
import pickle
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar, Union, cast

from ..config.settings import (
    CACHE_DIR,
    CACHE_ENABLED,
    CACHE_TIMEOUT,
    is_development,
)

# Set up module logger
logger = logging.getLogger(__name__)

# Global cache storage
_memory_cache: Dict[str, Any] = {}
_cache_metadata: Dict[str, Dict[str, Any]] = {}
_cache_hits: int = 0
_cache_misses: int = 0

# Type variable for function return values
T = TypeVar('T')


def generate_cache_key(func_name: str, args: Tuple, kwargs: Dict) -> str:
    """
    Generates a unique cache key based on function name and arguments.
    
    Args:
        func_name: Name of the function being cached
        args: Positional arguments passed to the function
        kwargs: Keyword arguments passed to the function
        
    Returns:
        A unique string hash that can be used as a cache key
    """
    # Convert args and kwargs to a consistent string representation
    args_str = str(args) if args else ""
    kwargs_str = str(sorted(kwargs.items())) if kwargs else ""
    
    # Combine function name with arguments
    key_base = f"{func_name}:{args_str}:{kwargs_str}"
    
    # Generate a hash to use as the key
    hash_obj = hashlib.md5(key_base.encode())
    return hash_obj.hexdigest()


def cache(timeout: Optional[int] = None) -> Callable:
    """
    Decorator that caches function results in memory.
    
    Results are stored in memory with a timeout. Subsequent calls with the same
    arguments will return the cached result until the timeout expires.
    
    Args:
        timeout: Optional custom timeout in seconds. If None, uses CACHE_TIMEOUT from settings.
        
    Returns:
        Decorated function with caching behavior
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            global _cache_hits, _cache_misses
            
            # Skip caching if disabled
            if not CACHE_ENABLED:
                return func(*args, **kwargs)
            
            # Generate a unique key for this function call
            key = generate_cache_key(func.__name__, args, kwargs)
            
            # Check if result is in cache and still valid
            cached_result = get_from_cache(key)
            if cached_result is not None:
                _cache_hits += 1
                logger.debug(f"Cache hit for {func.__name__}")
                return cast(T, cached_result)
            
            # Not in cache or expired, call the function
            _cache_misses += 1
            result = func(*args, **kwargs)
            
            # Store the result in cache
            store_in_cache(key, result, timeout)
            logger.debug(f"Cache miss for {func.__name__}, result cached")
            
            return result
        return wrapper
    return decorator


def is_cache_valid(key: str) -> bool:
    """
    Checks if a cache entry is still valid based on its timestamp.
    
    Args:
        key: Cache key to check
        
    Returns:
        True if cache entry exists and is not expired, False otherwise
    """
    if not CACHE_ENABLED:
        return False
    
    if key not in _cache_metadata:
        return False
    
    # Get the timestamp and timeout from metadata
    timestamp = _cache_metadata[key].get('timestamp')
    custom_timeout = _cache_metadata[key].get('timeout')
    
    if timestamp is None:
        return False
    
    # Calculate how old the cache entry is
    now = datetime.datetime.now()
    age = (now - timestamp).total_seconds()
    
    # Use custom timeout if provided, otherwise use the global setting
    timeout_value = custom_timeout if custom_timeout is not None else CACHE_TIMEOUT
    
    # Return True if the entry is still valid (age < timeout)
    return age < timeout_value


def get_from_cache(key: str) -> Optional[Any]:
    """
    Retrieves a value from the cache if it exists and is valid.
    
    Args:
        key: Cache key to retrieve
        
    Returns:
        Cached value if found and valid, None otherwise
    """
    global _cache_hits
    
    if not CACHE_ENABLED:
        return None
    
    if key not in _memory_cache:
        return None
    
    # Check if cache entry is still valid
    if not is_cache_valid(key):
        # Entry expired, remove it
        del _memory_cache[key]
        del _cache_metadata[key]
        return None
    
    # Valid cache hit
    logger.debug(f"Cache hit for key: {key}")
    return _memory_cache[key]


def store_in_cache(key: str, value: Any, timeout: Optional[int] = None) -> bool:
    """
    Stores a value in the cache with metadata.
    
    Args:
        key: Cache key to store the value under
        value: Value to cache
        timeout: Optional custom timeout in seconds
        
    Returns:
        True if caching was successful, False otherwise
    """
    if not CACHE_ENABLED:
        return False
    
    # Store the value
    _memory_cache[key] = value
    
    # Store metadata (timestamp and optional custom timeout)
    _cache_metadata[key] = {
        'timestamp': datetime.datetime.now(),
        'timeout': timeout
    }
    
    logger.debug(f"Stored in cache with key: {key}")
    return True


def clear_cache(key: Optional[str] = None) -> None:
    """
    Clears all cache or specific cache entries.
    
    Args:
        key: Optional specific key to clear. If None, clears all cache.
    """
    global _cache_hits, _cache_misses, _memory_cache, _cache_metadata
    
    if key is None:
        # Clear all cache
        _memory_cache = {}
        _cache_metadata = {}
        _cache_hits = 0
        _cache_misses = 0
        logger.info("Cleared all cache entries")
    else:
        # Clear specific entry
        if key in _memory_cache:
            del _memory_cache[key]
        if key in _cache_metadata:
            del _cache_metadata[key]
        logger.info(f"Cleared cache entry for key: {key}")


def get_cache_stats() -> Dict[str, Union[int, float]]:
    """
    Returns statistics about the cache usage.
    
    Returns:
        Dictionary containing cache statistics including hit count, miss count,
        hit rate, and entry count.
    """
    # Calculate hit rate
    total_requests = _cache_hits + _cache_misses
    hit_rate = (_cache_hits / total_requests) * 100 if total_requests > 0 else 0
    
    # Count expired entries
    expired_count = sum(1 for key in _cache_metadata if not is_cache_valid(key))
    
    # Estimate size (this is approximate)
    try:
        import sys
        total_size = sum(sys.getsizeof(_memory_cache.get(key, 0)) for key in _memory_cache)
    except:
        total_size = -1
    
    return {
        'hit_count': _cache_hits,
        'miss_count': _cache_misses,
        'hit_rate': hit_rate,
        'entry_count': len(_memory_cache),
        'expired_count': expired_count,
        'estimated_size_bytes': total_size
    }


def disk_cache(timeout: Optional[int] = None) -> Callable:
    """
    Decorator that caches function results to disk.
    
    Similar to the memory cache decorator, but persists results to disk for use
    across application restarts.
    
    Args:
        timeout: Optional custom timeout in seconds
        
    Returns:
        Decorated function with disk caching behavior
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            # Skip caching if disabled
            if not CACHE_ENABLED:
                return func(*args, **kwargs)
            
            # Generate a unique key for this function call
            key = generate_cache_key(func.__name__, args, kwargs)
            file_path = os.path.join(CACHE_DIR, f"{key}.pkl")
            
            # Check if cache file exists and is not expired
            if os.path.exists(file_path):
                try:
                    # Get file modification time
                    mtime = os.path.getmtime(file_path)
                    file_time = datetime.datetime.fromtimestamp(mtime)
                    
                    # Calculate age of the file
                    now = datetime.datetime.now()
                    age = (now - file_time).total_seconds()
                    
                    # Use custom timeout if provided, otherwise use the global setting
                    timeout_value = timeout if timeout is not None else CACHE_TIMEOUT
                    
                    # If file is still valid, load and return the cached value
                    if age < timeout_value:
                        with open(file_path, 'rb') as f:
                            result = pickle.load(f)
                        logger.debug(f"Disk cache hit for {func.__name__}")
                        return cast(T, result)
                except (pickle.PickleError, OSError) as e:
                    logger.warning(f"Error reading disk cache for {func.__name__}: {e}")
            
            # Not in cache, expired, or error reading cache, call the function
            result = func(*args, **kwargs)
            
            # Store the result in cache
            try:
                # Create directory if it doesn't exist
                os.makedirs(CACHE_DIR, exist_ok=True)
                
                with open(file_path, 'wb') as f:
                    pickle.dump(result, f)
                logger.debug(f"Stored result in disk cache for {func.__name__}")
            except (pickle.PickleError, OSError) as e:
                logger.warning(f"Error writing to disk cache for {func.__name__}: {e}")
            
            return result
        return wrapper
    return decorator


def clear_disk_cache(pattern: Optional[str] = None) -> int:
    """
    Clears all disk cache or specific disk cache entries.
    
    Args:
        pattern: Optional string pattern to match cache files
        
    Returns:
        Number of cache files removed
    """
    if not os.path.exists(CACHE_DIR):
        return 0
    
    removed_count = 0
    
    for filename in os.listdir(CACHE_DIR):
        if filename.endswith('.pkl') and (pattern is None or pattern in filename):
            try:
                os.remove(os.path.join(CACHE_DIR, filename))
                removed_count += 1
            except OSError as e:
                logger.warning(f"Error removing cache file {filename}: {e}")
    
    logger.info(f"Cleared {removed_count} disk cache files")
    return removed_count


def invalidate_cache_entry(key: str) -> bool:
    """
    Invalidates a specific cache entry by key.
    
    Args:
        key: Cache key to invalidate
        
    Returns:
        True if entry was found and invalidated, False otherwise
    """
    found = False
    
    # Check memory cache
    if key in _memory_cache:
        del _memory_cache[key]
        found = True
    
    if key in _cache_metadata:
        del _cache_metadata[key]
        found = True
    
    # Check disk cache
    disk_path = os.path.join(CACHE_DIR, f"{key}.pkl")
    if os.path.exists(disk_path):
        try:
            os.remove(disk_path)
            found = True
        except OSError as e:
            logger.warning(f"Error removing disk cache file for key {key}: {e}")
    
    if found:
        logger.info(f"Invalidated cache entry for key: {key}")
    
    return found


class CacheManager:
    """
    Class that provides centralized cache management functionality.
    
    This class offers an object-oriented interface to manage both memory and disk caches,
    with methods for retrieving, storing, and invalidating cached data.
    """
    
    def __init__(self):
        """
        Initializes the CacheManager with empty cache.
        """
        self._cache: Dict[str, Any] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}
        self._hits: int = 0
        self._misses: int = 0
        self._logger = logging.getLogger(__name__ + '.CacheManager')
    
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieves a value from the cache if it exists and is valid.
        
        Args:
            key: Cache key to retrieve
            
        Returns:
            Cached value if found and valid, None otherwise
        """
        if not CACHE_ENABLED:
            return None
        
        if key not in self._cache:
            return None
        
        # Check if cache entry is still valid
        if not self.is_valid(key):
            # Entry expired, remove it
            del self._cache[key]
            del self._metadata[key]
            return None
        
        # Valid cache hit
        self._hits += 1
        self._logger.debug(f"Cache hit for key: {key}")
        return self._cache[key]
    
    def set(self, key: str, value: Any, timeout: Optional[int] = None) -> bool:
        """
        Stores a value in the cache with metadata.
        
        Args:
            key: Cache key to store the value under
            value: Value to cache
            timeout: Optional custom timeout in seconds
            
        Returns:
            True if caching was successful, False otherwise
        """
        if not CACHE_ENABLED:
            return False
        
        # Store the value
        self._cache[key] = value
        
        # Store metadata
        self._metadata[key] = {
            'timestamp': datetime.datetime.now(),
            'timeout': timeout
        }
        
        self._logger.debug(f"Stored in cache with key: {key}")
        return True
    
    def clear(self, key: Optional[str] = None) -> None:
        """
        Clears all cache or specific cache entries.
        
        Args:
            key: Optional specific key to clear. If None, clears all cache.
        """
        if key is None:
            # Clear all cache
            self._cache = {}
            self._metadata = {}
            self._hits = 0
            self._misses = 0
            self._logger.info("Cleared all cache entries")
        else:
            # Clear specific entry
            if key in self._cache:
                del self._cache[key]
            if key in self._metadata:
                del self._metadata[key]
            self._logger.info(f"Cleared cache entry for key: {key}")
    
    def invalidate(self, key: str) -> bool:
        """
        Invalidates a specific cache entry by key.
        
        Args:
            key: Cache key to invalidate
            
        Returns:
            True if entry was found and invalidated, False otherwise
        """
        found = False
        
        if key in self._cache:
            del self._cache[key]
            found = True
        
        if key in self._metadata:
            del self._metadata[key]
            found = True
        
        if found:
            self._logger.info(f"Invalidated cache entry for key: {key}")
        
        return found
    
    def is_valid(self, key: str) -> bool:
        """
        Checks if a cache entry is still valid.
        
        Args:
            key: Cache key to check
            
        Returns:
            True if cache entry exists and is not expired, False otherwise
        """
        if not CACHE_ENABLED:
            return False
        
        if key not in self._metadata:
            return False
        
        # Get the timestamp and timeout from metadata
        timestamp = self._metadata[key].get('timestamp')
        custom_timeout = self._metadata[key].get('timeout')
        
        if timestamp is None:
            return False
        
        # Calculate how old the cache entry is
        now = datetime.datetime.now()
        age = (now - timestamp).total_seconds()
        
        # Use custom timeout if provided, otherwise use the global setting
        timeout_value = custom_timeout if custom_timeout is not None else CACHE_TIMEOUT
        
        # Return True if the entry is still valid (age < timeout)
        return age < timeout_value
    
    def get_stats(self) -> Dict[str, Union[int, float]]:
        """
        Returns statistics about the cache usage.
        
        Returns:
            Dictionary containing cache statistics
        """
        # Calculate hit rate
        total_requests = self._hits + self._misses
        hit_rate = (self._hits / total_requests) * 100 if total_requests > 0 else 0
        
        # Count expired entries
        expired_count = sum(1 for key in self._metadata if not self.is_valid(key))
        
        # Estimate size (this is approximate)
        try:
            import sys
            total_size = sum(sys.getsizeof(self._cache.get(key, 0)) for key in self._cache)
        except:
            total_size = -1
        
        return {
            'hit_count': self._hits,
            'miss_count': self._misses,
            'hit_rate': hit_rate,
            'entry_count': len(self._cache),
            'expired_count': expired_count,
            'estimated_size_bytes': total_size
        }


# Create a singleton instance of CacheManager
cache_manager = CacheManager()