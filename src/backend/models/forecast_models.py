"""
Defines enhanced forecast model classes and utility functions for the Electricity Market Price Forecasting System.
This module extends the base PriceForecast model with probabilistic forecasting capabilities, ensemble
management, and statistical analysis features. It provides the core data structures for
representing, manipulating, and analyzing probabilistic price forecasts.
"""

import dataclasses  # standard library
from datetime import datetime  # standard library
from typing import Dict, List, Any, Optional, Tuple, Union, cast  # standard library
import logging  # standard library

import pandas as pd  # version: 2.0.0
import numpy as np  # version: 1.24.0

from .data_models import PriceForecast, create_sample_columns
from .validation_models import ValidationResult
from ..config.settings import FORECAST_PRODUCTS, PROBABILISTIC_SAMPLE_COUNT

# Standard confidence levels for forecast intervals
CONFIDENCE_LEVELS = [0.5, 0.8, 0.9, 0.95, 0.99]

# Standard statistical metrics for forecasts
FORECAST_METRICS = ["mean", "median", "std", "min", "max", "range", "skew", "kurtosis"]

# Configure logger
logger = logging.getLogger(__name__)


def calculate_forecast_statistics(samples: List[float], metrics: Optional[List[str]] = None) -> Dict[str, float]:
    """
    Calculates statistical metrics for a set of forecast samples.
    
    Args:
        samples: List of sample values from the forecast
        metrics: List of metrics to calculate, defaults to FORECAST_METRICS
    
    Returns:
        Dictionary of statistical metrics
        
    Raises:
        ValueError: If samples is empty or contains non-numeric values
    """
    # Validate input
    if not samples:
        raise ValueError("Samples list cannot be empty")
    
    # Set default metrics if none provided
    if metrics is None:
        metrics = FORECAST_METRICS
    
    # Convert to numpy array for efficient calculation
    try:
        samples_array = np.array(samples, dtype=float)
    except (ValueError, TypeError):
        raise ValueError("Samples must contain only numeric values")
    
    # Calculate requested metrics
    result = {}
    
    for metric in metrics:
        if metric == "mean":
            result[metric] = float(np.mean(samples_array))
        elif metric == "median":
            result[metric] = float(np.median(samples_array))
        elif metric == "std":
            result[metric] = float(np.std(samples_array))
        elif metric == "min":
            result[metric] = float(np.min(samples_array))
        elif metric == "max":
            result[metric] = float(np.max(samples_array))
        elif metric == "range":
            result[metric] = float(np.max(samples_array) - np.min(samples_array))
        elif metric == "skew":
            # Calculate skewness
            if len(samples_array) > 2:
                mean = np.mean(samples_array)
                std = np.std(samples_array)
                if std > 0:
                    skewness = np.mean(((samples_array - mean) / std) ** 3)
                    result[metric] = float(skewness)
                else:
                    result[metric] = 0.0
            else:
                result[metric] = 0.0
        elif metric == "kurtosis":
            # Calculate excess kurtosis
            if len(samples_array) > 3:
                mean = np.mean(samples_array)
                std = np.std(samples_array)
                if std > 0:
                    kurtosis = np.mean(((samples_array - mean) / std) ** 4) - 3
                    result[metric] = float(kurtosis)
                else:
                    result[metric] = 0.0
            else:
                result[metric] = 0.0
    
    return result


def aggregate_forecasts(forecasts: List['ProbabilisticForecast'], 
                       aggregation_method: str = 'mean') -> 'ProbabilisticForecast':
    """
    Aggregates multiple forecasts into a single forecast.
    
    Args:
        forecasts: List of forecasts to aggregate
        aggregation_method: Method to use for aggregation ('mean' or 'median')
    
    Returns:
        Aggregated forecast
        
    Raises:
        ValueError: If forecasts is empty or forecasts have different products/timestamps
    """
    # Validate input
    if not forecasts:
        raise ValueError("Forecasts list cannot be empty")
    
    # Verify all forecasts have the same product and timestamp
    first_forecast = forecasts[0]
    for forecast in forecasts[1:]:
        if forecast.product != first_forecast.product:
            raise ValueError(
                f"Cannot aggregate forecasts with different products: {forecast.product} and {first_forecast.product}"
            )
        if forecast.timestamp != first_forecast.timestamp:
            raise ValueError(
                f"Cannot aggregate forecasts with different timestamps: {forecast.timestamp} and {first_forecast.timestamp}"
            )
    
    # Aggregate based on method
    if aggregation_method == 'mean':
        # Average the point forecasts
        aggregated_point_forecast = sum(f.point_forecast for f in forecasts) / len(forecasts)
        
        # Average the samples (element-wise)
        sample_count = len(forecasts[0].samples)
        aggregated_samples = []
        for i in range(sample_count):
            aggregated_samples.append(sum(f.samples[i] for f in forecasts) / len(forecasts))
    
    elif aggregation_method == 'median':
        # Take the median of point forecasts
        aggregated_point_forecast = np.median([f.point_forecast for f in forecasts])
        
        # Take the median of samples (element-wise)
        sample_count = len(forecasts[0].samples)
        aggregated_samples = []
        for i in range(sample_count):
            aggregated_samples.append(np.median([f.samples[i] for f in forecasts]))
    
    else:
        raise ValueError(f"Unknown aggregation method: {aggregation_method}. Use 'mean' or 'median'")
    
    # Set is_fallback to True if any input forecast is a fallback
    is_fallback = any(f.is_fallback for f in forecasts)
    
    # Create a new forecast with the aggregated values
    return ProbabilisticForecast(
        timestamp=first_forecast.timestamp,
        product=first_forecast.product,
        point_forecast=aggregated_point_forecast,
        samples=aggregated_samples,
        generation_timestamp=datetime.now(),
        is_fallback=is_fallback
    )


def create_forecast_dataframe(forecasts: List['ProbabilisticForecast']) -> pd.DataFrame:
    """
    Creates a pandas DataFrame from a list of forecast models.
    
    Args:
        forecasts: List of forecast models to convert to DataFrame
    
    Returns:
        DataFrame containing all forecasts
        
    Raises:
        ValueError: If forecasts is empty
    """
    # Validate input
    if not forecasts:
        raise ValueError("Forecasts list cannot be empty")
    
    # Create list of dataframe rows
    rows = []
    for forecast in forecasts:
        rows.append(forecast.to_dataframe_row())
    
    # Create DataFrame from rows
    return pd.DataFrame(rows)


def forecasts_from_dataframe(df: pd.DataFrame) -> List['ProbabilisticForecast']:
    """
    Creates a list of forecast models from a pandas DataFrame.
    
    Args:
        df: DataFrame containing forecast data
    
    Returns:
        List of ProbabilisticForecast objects
        
    Raises:
        ValueError: If DataFrame is empty or missing required columns
    """
    # Validate input
    if df.empty:
        raise ValueError("DataFrame cannot be empty")
    
    # Check for required columns
    required_columns = ['timestamp', 'product', 'point_forecast', 'generation_timestamp', 'is_fallback']
    sample_columns = create_sample_columns(PROBABILISTIC_SAMPLE_COUNT)
    required_columns.extend(sample_columns)
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"DataFrame missing required columns: {missing_columns}")
    
    # Create forecast objects
    forecasts = []
    for _, row in df.iterrows():
        forecast = ProbabilisticForecast.from_dataframe_row(row)
        forecasts.append(forecast)
    
    return forecasts


@dataclasses.dataclass
class ProbabilisticForecast(PriceForecast):
    """
    Enhanced forecast model with probabilistic forecasting capabilities.
    
    Extends the base PriceForecast model with additional methods for working with
    probabilistic forecasts, including confidence intervals, prediction intervals,
    and statistical analysis of forecast samples.
    """
    statistics: Dict[str, float] = dataclasses.field(default_factory=dict)
    
    def __post_init__(self):
        """
        Initializes a probabilistic forecast model.
        
        Validates sample count and calculates statistics from samples.
        
        Raises:
            ValueError: If sample count does not match required count
        """
        # Call parent's __post_init__ for basic validation
        super().__post_init__()
        
        # Validate sample count
        if len(self.samples) != PROBABILISTIC_SAMPLE_COUNT:
            raise ValueError(
                f"Sample count {len(self.samples)} does not match required count {PROBABILISTIC_SAMPLE_COUNT}"
            )
        
        # Calculate statistics from samples
        self.statistics = calculate_forecast_statistics(self.samples)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the model to a dictionary.
        
        Returns:
            Dictionary representation of the model
        """
        # Get base dictionary from parent
        result = super().to_dict()
        
        # Add statistics
        result['statistics'] = self.statistics
        
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProbabilisticForecast':
        """
        Creates a model instance from a dictionary.
        
        Args:
            data: Dictionary containing model data
            
        Returns:
            Instance of the model
        """
        # Extract statistics if present
        statistics = data.pop('statistics', {})
        
        # Create instance using parent's from_dict
        instance = cls(
            timestamp=datetime.fromisoformat(data['timestamp']) if isinstance(data['timestamp'], str) else data['timestamp'],
            product=data['product'],
            point_forecast=data['point_forecast'],
            samples=data['samples'],
            generation_timestamp=datetime.fromisoformat(data['generation_timestamp']) if isinstance(data['generation_timestamp'], str) else data['generation_timestamp'],
            is_fallback=data.get('is_fallback', False)
        )
        
        # Set statistics
        instance.statistics = statistics
        
        return instance
    
    def get_confidence_interval(self, confidence_level: float) -> Tuple[float, float]:
        """
        Gets the confidence interval at a specified level.
        
        Args:
            confidence_level: Confidence level between 0 and 1
            
        Returns:
            Tuple of (lower_bound, upper_bound)
            
        Raises:
            ValueError: If confidence_level is not between 0 and 1
        """
        if not 0 < confidence_level < 1:
            raise ValueError(f"Confidence level must be between 0 and 1, got {confidence_level}")
        
        # Calculate alpha
        alpha = (1 - confidence_level) / 2
        
        # Calculate percentiles
        lower_percentile = alpha
        upper_percentile = 1 - alpha
        
        # Get bounds
        lower_bound = self.get_percentile(lower_percentile)
        upper_bound = self.get_percentile(upper_percentile)
        
        return (lower_bound, upper_bound)
    
    def get_prediction_interval(self, confidence_level: float) -> Tuple[float, float]:
        """
        Gets the prediction interval at a specified level.
        
        Args:
            confidence_level: Confidence level between 0 and 1
            
        Returns:
            Tuple of (lower_bound, upper_bound)
            
        Raises:
            ValueError: If confidence_level is not between 0 and 1
        """
        # For a simple implementation, prediction interval is the same as confidence interval
        # In a more sophisticated implementation, prediction intervals would account for 
        # additional uncertainty in future observations
        return self.get_confidence_interval(confidence_level)
    
    def get_statistics(self, metrics: Optional[List[str]] = None) -> Dict[str, float]:
        """
        Gets statistical metrics for the forecast samples.
        
        Args:
            metrics: List of metrics to get, defaults to all available
            
        Returns:
            Dictionary of statistical metrics
        """
        if metrics is None:
            return self.statistics
        
        # Filter statistics to include only requested metrics
        return {metric: self.statistics[metric] for metric in metrics if metric in self.statistics}
    
    def to_dataframe_row(self) -> Dict[str, Any]:
        """
        Converts the model to a dictionary suitable for a DataFrame row.
        
        Returns:
            Dictionary for DataFrame row
        """
        # Get base row from parent
        return super().to_dataframe_row()
    
    @classmethod
    def from_dataframe_row(cls, row: pd.Series) -> 'ProbabilisticForecast':
        """
        Creates a probabilistic forecast model from a DataFrame row.
        
        Args:
            row: Pandas Series containing a row from a forecast DataFrame
            
        Returns:
            Probabilistic forecast model instance
        """
        # Extract fields directly from row
        timestamp = row['timestamp']
        product = row['product']
        point_forecast = row['point_forecast']
        generation_timestamp = row['generation_timestamp']
        is_fallback = row['is_fallback']
        
        # Extract samples from sample columns
        sample_columns = create_sample_columns(PROBABILISTIC_SAMPLE_COUNT)
        samples = [row[col] for col in sample_columns]
        
        # Create and return instance
        return cls(
            timestamp=timestamp,
            product=product,
            point_forecast=point_forecast,
            samples=samples,
            generation_timestamp=generation_timestamp,
            is_fallback=is_fallback
        )
    
    def validate(self) -> ValidationResult:
        """
        Validates the forecast model.
        
        Returns:
            Validation result
        """
        # Create a new ValidationResult with is_valid=True
        result = ValidationResult(is_valid=True)
        
        # Validate that product is in FORECAST_PRODUCTS
        if self.product not in FORECAST_PRODUCTS:
            result.add_error('product', f"Product must be one of {FORECAST_PRODUCTS}")
        
        # Validate that samples list has length equal to PROBABILISTIC_SAMPLE_COUNT
        if len(self.samples) != PROBABILISTIC_SAMPLE_COUNT:
            result.add_error('samples', 
                            f"Sample count must be {PROBABILISTIC_SAMPLE_COUNT}, got {len(self.samples)}")
        
        # Validate that point_forecast is a valid number
        if not isinstance(self.point_forecast, (int, float)) or np.isnan(self.point_forecast):
            result.add_error('point_forecast', "Point forecast must be a valid number")
        
        # Validate that all samples are valid numbers
        for i, sample in enumerate(self.samples):
            if not isinstance(sample, (int, float)) or np.isnan(sample):
                result.add_error('samples', f"Sample {i} must be a valid number")
        
        # Validate that generation_timestamp is not None
        if self.generation_timestamp is None:
            result.add_error('generation_timestamp', "Generation timestamp cannot be None")
        
        return result


@dataclasses.dataclass
class ForecastEnsemble:
    """
    Class representing an ensemble of forecasts for the same product and time period.
    
    An ensemble contains multiple forecasts for the same product over a specified time period,
    allowing for aggregation, analysis, and comparison of forecasts.
    """
    product: str
    start_time: datetime
    end_time: datetime
    forecasts: List[ProbabilisticForecast]
    generation_timestamp: datetime = dataclasses.field(default_factory=datetime.now)
    is_fallback: bool = False
    
    def __post_init__(self):
        """
        Initializes a forecast ensemble.
        
        Validates that all forecasts have the same product and timestamps within the specified range.
        
        Raises:
            ValueError: If forecasts is empty or contains invalid forecasts
        """
        # Validate forecasts list
        if not self.forecasts:
            raise ValueError("Forecasts list cannot be empty")
        
        # Validate that all forecasts have the same product
        for forecast in self.forecasts:
            if forecast.product != self.product:
                raise ValueError(
                    f"All forecasts must have the same product. Expected {self.product}, got {forecast.product}"
                )
        
        # Validate that all forecasts have timestamps between start_time and end_time
        for forecast in self.forecasts:
            if forecast.timestamp < self.start_time or forecast.timestamp > self.end_time:
                raise ValueError(
                    f"Forecast timestamp {forecast.timestamp} is outside the ensemble time range "
                    f"({self.start_time} to {self.end_time})"
                )
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Converts the ensemble to a pandas DataFrame.
        
        Returns:
            DataFrame containing all forecasts
        """
        # Create DataFrame from forecasts
        df = create_forecast_dataframe(self.forecasts)
        
        # Add ensemble metadata
        df['ensemble_generation_timestamp'] = self.generation_timestamp
        df['ensemble_is_fallback'] = self.is_fallback
        
        return df
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> 'ForecastEnsemble':
        """
        Creates a forecast ensemble from a pandas DataFrame.
        
        Args:
            df: DataFrame containing forecast data
            
        Returns:
            Forecast ensemble instance
        """
        # Extract forecasts
        forecasts = forecasts_from_dataframe(df)
        
        if not forecasts:
            raise ValueError("No valid forecasts found in DataFrame")
        
        # Extract product from first forecast
        product = forecasts[0].product
        
        # Determine start_time and end_time from forecast timestamps
        timestamps = [f.timestamp for f in forecasts]
        start_time = min(timestamps)
        end_time = max(timestamps)
        
        # Extract ensemble metadata if available
        generation_timestamp = df['ensemble_generation_timestamp'].iloc[0] if 'ensemble_generation_timestamp' in df.columns else forecasts[0].generation_timestamp
        is_fallback = df['ensemble_is_fallback'].iloc[0] if 'ensemble_is_fallback' in df.columns else any(f.is_fallback for f in forecasts)
        
        # Create ensemble
        return cls(
            product=product,
            start_time=start_time,
            end_time=end_time,
            forecasts=forecasts,
            generation_timestamp=generation_timestamp,
            is_fallback=is_fallback
        )
    
    def get_forecast_at_time(self, timestamp: datetime) -> Optional[ProbabilisticForecast]:
        """
        Gets the forecast for a specific timestamp.
        
        Args:
            timestamp: Target timestamp
            
        Returns:
            Forecast at the specified time, or None if not found
        """
        for forecast in self.forecasts:
            if forecast.timestamp == timestamp:
                return forecast
        
        return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Gets ensemble-level statistics.
        
        Returns:
            Dictionary of ensemble statistics
        """
        # Calculate basic ensemble statistics
        point_forecasts = [f.point_forecast for f in self.forecasts]
        
        statistics = {
            'forecast_count': len(self.forecasts),
            'avg_point_forecast': np.mean(point_forecasts) if point_forecasts else None,
            'min_point_forecast': np.min(point_forecasts) if point_forecasts else None,
            'max_point_forecast': np.max(point_forecasts) if point_forecasts else None,
            'fallback_percentage': sum(1 for f in self.forecasts if f.is_fallback) / len(self.forecasts) * 100 if self.forecasts else 0
        }
        
        return statistics
    
    def validate(self) -> ValidationResult:
        """
        Validates the forecast ensemble.
        
        Returns:
            Validation result
        """
        # Create a new ValidationResult with is_valid=True
        result = ValidationResult(is_valid=True)
        
        # Validate that product is in FORECAST_PRODUCTS
        if self.product not in FORECAST_PRODUCTS:
            result.add_error('product', f"Product must be one of {FORECAST_PRODUCTS}")
        
        # Validate that forecasts list is not empty
        if not self.forecasts:
            result.add_error('forecasts', "Forecasts list cannot be empty")
            return result  # Return early if no forecasts
        
        # Validate that all forecasts have the same product
        for forecast in self.forecasts:
            if forecast.product != self.product:
                result.add_error('forecasts', 
                                f"Forecast has product {forecast.product}, expected {self.product}")
        
        # Validate that all forecasts have timestamps between start_time and end_time
        for forecast in self.forecasts:
            if forecast.timestamp < self.start_time or forecast.timestamp > self.end_time:
                result.add_error('forecasts', 
                                f"Forecast timestamp {forecast.timestamp} is outside the ensemble time range "
                                f"({self.start_time} to {self.end_time})")
        
        # Validate that generation_timestamp is not None
        if self.generation_timestamp is None:
            result.add_error('generation_timestamp', "Generation timestamp cannot be None")
        
        # Validate each individual forecast
        for i, forecast in enumerate(self.forecasts):
            forecast_result = forecast.validate()
            if not forecast_result.is_valid:
                for category, messages in forecast_result.errors.items():
                    for message in messages:
                        result.add_error(f"forecast_{i}_{category}", message)
        
        return result


@dataclasses.dataclass
class ForecastComparison:
    """
    Class for comparing multiple forecasts or forecast ensembles.
    
    This class provides methods for comparing different forecasts or ensembles,
    calculating comparison metrics, and determining the best forecast based on
    specified criteria.
    """
    ensembles: List[ForecastEnsemble]
    metrics: Dict[str, Any] = dataclasses.field(default_factory=dict)
    
    def __post_init__(self):
        """
        Initializes a forecast comparison.
        
        Validates that all ensembles have the same product and time period.
        
        Raises:
            ValueError: If ensembles is empty or contains incompatible ensembles
        """
        # Validate ensembles list
        if not self.ensembles:
            raise ValueError("Ensembles list cannot be empty")
        
        # Validate that all ensembles have the same product
        first_ensemble = self.ensembles[0]
        for ensemble in self.ensembles[1:]:
            if ensemble.product != first_ensemble.product:
                raise ValueError(
                    f"All ensembles must have the same product. "
                    f"Expected {first_ensemble.product}, got {ensemble.product}"
                )
        
        # Validate that all ensembles cover the same time period
        for ensemble in self.ensembles[1:]:
            if ensemble.start_time != first_ensemble.start_time or ensemble.end_time != first_ensemble.end_time:
                raise ValueError(
                    f"All ensembles must cover the same time period. "
                    f"Expected {first_ensemble.start_time} to {first_ensemble.end_time}, "
                    f"got {ensemble.start_time} to {ensemble.end_time}"
                )
    
    def calculate_metrics(self, metric_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Calculates comparison metrics between ensembles.
        
        Args:
            metric_names: List of metrics to calculate, defaults to ['RMSE', 'MAE', 'bias']
            
        Returns:
            Dictionary of comparison metrics
        """
        if metric_names is None:
            metric_names = ['RMSE', 'MAE', 'bias']
        
        # Initialize metrics dictionary
        metrics = {}
        
        # Compare each ensemble pair
        for i, ensemble1 in enumerate(self.ensembles):
            metrics[f"ensemble_{i}"] = {}
            
            for j, ensemble2 in enumerate(self.ensembles):
                if i == j:
                    continue  # Skip self-comparison
                
                metrics[f"ensemble_{i}"][f"vs_ensemble_{j}"] = {}
                
                # Collect matching forecasts
                paired_forecasts = []
                for forecast1 in ensemble1.forecasts:
                    forecast2 = ensemble2.get_forecast_at_time(forecast1.timestamp)
                    if forecast2 is not None:
                        paired_forecasts.append((forecast1, forecast2))
                
                if not paired_forecasts:
                    logger.warning(f"No matching forecasts found between ensemble_{i} and ensemble_{j}")
                    continue
                
                # Calculate metrics for each pair
                point_diffs = [f1.point_forecast - f2.point_forecast for f1, f2 in paired_forecasts]
                
                for metric in metric_names:
                    if metric == 'RMSE':
                        # Root Mean Squared Error
                        metrics[f"ensemble_{i}"][f"vs_ensemble_{j}"][metric] = \
                            float(np.sqrt(np.mean(np.square(point_diffs))))
                    elif metric == 'MAE':
                        # Mean Absolute Error
                        metrics[f"ensemble_{i}"][f"vs_ensemble_{j}"][metric] = \
                            float(np.mean(np.abs(point_diffs)))
                    elif metric == 'bias':
                        # Mean Error (bias)
                        metrics[f"ensemble_{i}"][f"vs_ensemble_{j}"][metric] = \
                            float(np.mean(point_diffs))
        
        # Store metrics
        self.metrics = metrics
        
        return metrics
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Converts comparison metrics to a pandas DataFrame.
        
        Returns:
            DataFrame of comparison metrics
        """
        # Check if metrics have been calculated
        if not self.metrics:
            self.calculate_metrics()
        
        # Create list of records for DataFrame
        records = []
        
        for ensemble_key, ensemble_metrics in self.metrics.items():
            for vs_key, vs_metrics in ensemble_metrics.items():
                for metric_name, metric_value in vs_metrics.items():
                    records.append({
                        'ensemble': ensemble_key,
                        'comparison': vs_key,
                        'metric': metric_name,
                        'value': metric_value
                    })
        
        # Create DataFrame
        return pd.DataFrame(records)
    
    def get_best_forecast(self, criterion: str = 'RMSE') -> ForecastEnsemble:
        """
        Gets the best forecast ensemble based on specified criterion.
        
        Args:
            criterion: Criterion to use for ranking ('RMSE', 'MAE', or 'bias')
            
        Returns:
            Best forecast ensemble
            
        Raises:
            ValueError: If criterion is invalid or metrics haven't been calculated
        """
        # Check if metrics have been calculated
        if not self.metrics:
            self.calculate_metrics()
        
        if criterion not in ['RMSE', 'MAE', 'bias']:
            raise ValueError(f"Invalid criterion: {criterion}. Must be 'RMSE', 'MAE', or 'bias'")
        
        # Determine the best ensemble based on the criterion
        ensemble_scores = {}
        
        for ensemble_key, ensemble_metrics in self.metrics.items():
            # Extract ensemble index
            ensemble_idx = int(ensemble_key.split('_')[1])
            
            # Calculate average score across all comparisons
            scores = []
            for vs_metrics in ensemble_metrics.values():
                if criterion in vs_metrics:
                    # For bias, use absolute value (closer to zero is better)
                    if criterion == 'bias':
                        scores.append(abs(vs_metrics[criterion]))
                    else:
                        scores.append(vs_metrics[criterion])
            
            if scores:
                ensemble_scores[ensemble_idx] = np.mean(scores)
        
        if not ensemble_scores:
            raise ValueError("No valid scores found for the specified criterion")
        
        # Find the ensemble with the lowest score (best performance)
        best_idx = min(ensemble_scores, key=ensemble_scores.get)
        
        return self.ensembles[best_idx]