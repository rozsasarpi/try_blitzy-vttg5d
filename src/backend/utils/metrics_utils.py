"""
Utility functions for calculating and evaluating forecast metrics in the Electricity Market Price Forecasting System.

This module provides a comprehensive set of accuracy metrics for both point forecasts and probabilistic
forecasts, enabling comparison between different forecasting methods and evaluation against actual values.
Following the functional programming approach, all functions are designed to be pure and stateless.
"""

from typing import List, Dict, Any, Optional, Union, Tuple
import math
import numpy as np  # version: 1.24.0+
import pandas as pd  # version: 2.0.0+
from sklearn.metrics import r2_score  # version: 1.2.0+

# Internal imports
from ..models.forecast_models import ProbabilisticForecast, ForecastEnsemble, CONFIDENCE_LEVELS
from .logging_utils import get_logger

# Initialize logger
logger = get_logger(__name__)

# Standard confidence levels used for evaluation
CONFIDENCE_LEVELS = [0.5, 0.8, 0.9, 0.95, 0.99]


def calculate_rmse(y_true: List, y_pred: List) -> Optional[float]:
    """
    Calculates Root Mean Square Error between actual and predicted values.
    
    Args:
        y_true: List of actual values
        y_pred: List of predicted values
        
    Returns:
        RMSE value, or None if inputs are empty
    """
    # Validate inputs
    if not y_true or not y_pred or len(y_true) != len(y_pred):
        logger.warning("Invalid inputs for RMSE calculation: empty lists or unequal lengths")
        return None
    
    # Convert to numpy arrays for efficient calculation
    y_true_array = np.array(y_true, dtype=float)
    y_pred_array = np.array(y_pred, dtype=float)
    
    # Calculate RMSE
    squared_diff = (y_true_array - y_pred_array) ** 2
    mean_squared_error = np.mean(squared_diff)
    rmse = math.sqrt(mean_squared_error)
    
    return rmse


def calculate_mae(y_true: List, y_pred: List) -> Optional[float]:
    """
    Calculates Mean Absolute Error between actual and predicted values.
    
    Args:
        y_true: List of actual values
        y_pred: List of predicted values
        
    Returns:
        MAE value, or None if inputs are empty
    """
    # Validate inputs
    if not y_true or not y_pred or len(y_true) != len(y_pred):
        logger.warning("Invalid inputs for MAE calculation: empty lists or unequal lengths")
        return None
    
    # Convert to numpy arrays for efficient calculation
    y_true_array = np.array(y_true, dtype=float)
    y_pred_array = np.array(y_pred, dtype=float)
    
    # Calculate MAE
    absolute_diff = np.abs(y_true_array - y_pred_array)
    mae = np.mean(absolute_diff)
    
    return mae


def calculate_mape(y_true: List, y_pred: List) -> Optional[float]:
    """
    Calculates Mean Absolute Percentage Error between actual and predicted values.
    
    Args:
        y_true: List of actual values
        y_pred: List of predicted values
        
    Returns:
        MAPE value, or None if inputs are empty or contain zeros
    """
    # Validate inputs
    if not y_true or not y_pred or len(y_true) != len(y_pred):
        logger.warning("Invalid inputs for MAPE calculation: empty lists or unequal lengths")
        return None
    
    # Convert to numpy arrays for efficient calculation
    y_true_array = np.array(y_true, dtype=float)
    y_pred_array = np.array(y_pred, dtype=float)
    
    # Check for zeros in actual values to avoid division by zero
    if np.any(y_true_array == 0):
        logger.warning("MAPE calculation contains zeros in actual values, which would cause division by zero")
        return None
    
    # Calculate MAPE
    absolute_percentage_errors = np.abs((y_true_array - y_pred_array) / y_true_array) * 100
    mape = np.mean(absolute_percentage_errors)
    
    return mape


def calculate_r2(y_true: List, y_pred: List) -> Optional[float]:
    """
    Calculates R-squared (coefficient of determination) between actual and predicted values.
    
    Args:
        y_true: List of actual values
        y_pred: List of predicted values
        
    Returns:
        R² value, or None if inputs are empty
    """
    # Validate inputs
    if not y_true or not y_pred or len(y_true) != len(y_pred):
        logger.warning("Invalid inputs for R² calculation: empty lists or unequal lengths")
        return None
    
    # Use sklearn's r2_score function
    try:
        r2 = r2_score(y_true, y_pred)
        return r2
    except Exception as e:
        logger.error(f"Error calculating R²: {str(e)}")
        return None


def calculate_bias(y_true: List, y_pred: List) -> Optional[float]:
    """
    Calculates bias (mean error) between actual and predicted values.
    
    Args:
        y_true: List of actual values
        y_pred: List of predicted values
        
    Returns:
        Bias value, or None if inputs are empty
    """
    # Validate inputs
    if not y_true or not y_pred or len(y_true) != len(y_pred):
        logger.warning("Invalid inputs for bias calculation: empty lists or unequal lengths")
        return None
    
    # Convert to numpy arrays for efficient calculation
    y_true_array = np.array(y_true, dtype=float)
    y_pred_array = np.array(y_pred, dtype=float)
    
    # Calculate bias (mean error)
    errors = y_pred_array - y_true_array
    bias = np.mean(errors)
    
    return bias


def calculate_pinball_loss(y_true: List, y_pred: List, quantile: float) -> Optional[float]:
    """
    Calculates pinball loss for quantile forecasts.
    
    The pinball loss (or quantile loss) penalizes under-prediction and over-prediction 
    asymmetrically based on the specified quantile.
    
    Args:
        y_true: List of actual values
        y_pred: List of predicted values
        quantile: Target quantile between 0 and 1
        
    Returns:
        Pinball loss value, or None if inputs are empty
    """
    # Validate inputs
    if not y_true or not y_pred or len(y_true) != len(y_pred):
        logger.warning("Invalid inputs for pinball loss calculation: empty lists or unequal lengths")
        return None
    
    if not 0 < quantile < 1:
        logger.error(f"Quantile must be between 0 and 1, got {quantile}")
        return None
    
    # Convert to numpy arrays for efficient calculation
    y_true_array = np.array(y_true, dtype=float)
    y_pred_array = np.array(y_pred, dtype=float)
    
    # Calculate errors
    errors = y_true_array - y_pred_array
    
    # Apply asymmetric weighting based on quantile
    weighted_errors = np.where(errors >= 0, 
                             quantile * errors, 
                             (quantile - 1) * errors)
    
    # Return mean of weighted errors
    return np.mean(weighted_errors)


def evaluate_forecast_accuracy(y_true: List, y_pred: List, metrics: Optional[List[str]] = None) -> Dict[str, float]:
    """
    Evaluates forecast accuracy using multiple metrics.
    
    Args:
        y_true: List of actual values
        y_pred: List of predicted values
        metrics: List of metric names to calculate, defaults to ['rmse', 'mae', 'mape', 'r2', 'bias']
        
    Returns:
        Dictionary of metric names and values
    """
    # Validate inputs
    if not y_true or not y_pred or len(y_true) != len(y_pred):
        logger.warning("Invalid inputs for forecast evaluation: empty lists or unequal lengths")
        return {}
    
    # Set default metrics if none provided
    if metrics is None:
        metrics = ['rmse', 'mae', 'mape', 'r2', 'bias']
    
    # Map of metric names to calculation functions
    metric_functions = {
        "rmse": calculate_rmse,
        "mae": calculate_mae,
        "mape": calculate_mape,
        "r2": calculate_r2,
        "bias": calculate_bias,
        "pinball_loss": calculate_pinball_loss
    }
    
    # Calculate requested metrics
    results = {}
    for metric in metrics:
        if metric in metric_functions:
            if metric == "pinball_loss":
                # Pinball loss needs quantile parameter, default to median (0.5)
                results[metric] = metric_functions[metric](y_true, y_pred, 0.5)
            else:
                results[metric] = metric_functions[metric](y_true, y_pred)
        else:
            logger.warning(f"Unknown metric: {metric}")
    
    return results


def evaluate_probabilistic_forecast(y_true: List, forecasts: List[ProbabilisticForecast], 
                                   metrics: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Evaluates probabilistic forecast accuracy.
    
    Args:
        y_true: List of actual values
        forecasts: List of probabilistic forecasts
        metrics: List of metric names to calculate, defaults to ['rmse', 'mae', 'mape', 'r2', 'bias']
        
    Returns:
        Dictionary of metric names and values
    """
    # Validate inputs
    if not y_true or not forecasts or len(y_true) != len(forecasts):
        logger.warning("Invalid inputs for probabilistic forecast evaluation: empty lists or unequal lengths")
        return {}
    
    # Set default metrics if none provided
    if metrics is None:
        metrics = ['rmse', 'mae', 'mape', 'r2', 'bias']
    
    # Extract point forecasts
    y_pred = [forecast.point_forecast for forecast in forecasts]
    
    # Initialize results with point forecast metrics
    results = evaluate_forecast_accuracy(y_true, y_pred, metrics)
    
    # Calculate coverage for different confidence levels
    coverage_metrics = {}
    for confidence_level in CONFIDENCE_LEVELS:
        coverage_key = f"coverage_{int(confidence_level * 100)}"
        coverage_metrics[coverage_key] = calculate_coverage(y_true, forecasts, confidence_level)
    
    # Add coverage metrics to results
    results.update(coverage_metrics)
    
    # Calculate pinball loss for standard quantiles
    quantiles = [0.1, 0.5, 0.9]
    for q in quantiles:
        # Extract q-quantile forecasts
        q_forecasts = []
        for forecast in forecasts:
            # Sort samples and take the appropriate quantile
            sorted_samples = sorted(forecast.samples)
            idx = int(q * len(sorted_samples))
            q_forecasts.append(sorted_samples[idx])
        
        pinball_key = f"pinball_loss_{int(q * 100)}"
        results[pinball_key] = calculate_pinball_loss(y_true, q_forecasts, q)
    
    return results


def calculate_coverage(y_true: List, forecasts: List[ProbabilisticForecast], 
                      confidence_level: float) -> float:
    """
    Calculates confidence interval coverage for probabilistic forecasts.
    
    Args:
        y_true: List of actual values
        forecasts: List of probabilistic forecasts
        confidence_level: Confidence level between 0 and 1
        
    Returns:
        Coverage ratio (0-1)
    """
    # Validate inputs
    if not y_true or not forecasts or len(y_true) != len(forecasts):
        logger.warning("Invalid inputs for coverage calculation: empty lists or unequal lengths")
        return 0.0
    
    if not 0 < confidence_level < 1:
        logger.error(f"Confidence level must be between 0 and 1, got {confidence_level}")
        return 0.0
    
    # Count observations covered by confidence interval
    count_covered = 0
    for actual, forecast in zip(y_true, forecasts):
        lower, upper = forecast.get_confidence_interval(confidence_level)
        if lower <= actual <= upper:
            count_covered += 1
    
    # Calculate coverage ratio
    coverage_ratio = count_covered / len(y_true)
    
    return coverage_ratio


def compare_forecasts(y_true: List, forecasts: Dict[str, List], 
                     metrics: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Compares multiple forecast methods using specified metrics.
    
    Args:
        y_true: List of actual values
        forecasts: Dictionary of forecast method names and predictions
        metrics: List of metric names to calculate, defaults to ['rmse', 'mae', 'mape', 'r2', 'bias']
        
    Returns:
        DataFrame of comparison metrics
    """
    # Validate inputs
    if not y_true:
        logger.warning("Empty actual values list for forecast comparison")
        return pd.DataFrame()
    
    if not forecasts:
        logger.warning("Empty forecasts dictionary for comparison")
        return pd.DataFrame()
    
    # Set default metrics if none provided
    if metrics is None:
        metrics = ['rmse', 'mae', 'mape', 'r2', 'bias']
    
    # Calculate metrics for each forecast method
    results = {}
    for method_name, y_pred in forecasts.items():
        if len(y_true) != len(y_pred):
            logger.warning(f"Forecast length mismatch for {method_name}, skipping")
            continue
        
        results[method_name] = evaluate_forecast_accuracy(y_true, y_pred, metrics)
    
    # Convert to DataFrame
    return pd.DataFrame(results).T


def calculate_forecast_improvement(y_true: List, forecast1: List, forecast2: List, 
                                 metric: str = 'rmse') -> float:
    """
    Calculates percentage improvement between two forecast methods.
    
    Args:
        y_true: List of actual values
        forecast1: First forecast method (baseline)
        forecast2: Second forecast method (comparison)
        metric: Metric to use for comparison
        
    Returns:
        Percentage improvement
    """
    # Validate inputs
    if not y_true or not forecast1 or not forecast2:
        logger.warning("Empty input lists for forecast improvement calculation")
        return 0.0
    
    if len(y_true) != len(forecast1) or len(y_true) != len(forecast2):
        logger.warning("Length mismatch in forecast improvement calculation")
        return 0.0
    
    # Map of metric names to calculation functions
    metric_functions = {
        "rmse": calculate_rmse,
        "mae": calculate_mae,
        "mape": calculate_mape,
        "r2": calculate_r2,
        "bias": calculate_bias
    }
    
    if metric not in metric_functions:
        logger.error(f"Unknown metric: {metric}")
        return 0.0
    
    # Calculate metrics for both forecasts
    metric_func = metric_functions[metric]
    
    if metric == "pinball_loss":
        # Special case for pinball loss, default to median (0.5)
        value1 = metric_func(y_true, forecast1, 0.5)
        value2 = metric_func(y_true, forecast2, 0.5)
    else:
        value1 = metric_func(y_true, forecast1)
        value2 = metric_func(y_true, forecast2)
    
    # Handle None values
    if value1 is None or value2 is None:
        return 0.0
    
    # For R², higher is better, so improvement is different
    if metric == 'r2':
        if value1 == 0:
            return 0.0  # Avoid division by zero
        improvement = ((value2 - value1) / abs(value1)) * 100
    else:
        # For other metrics, lower is better
        if value1 == 0:
            return 0.0  # Avoid division by zero
        improvement = ((value1 - value2) / abs(value1)) * 100
    
    return improvement


def calculate_ensemble_metrics(y_true: List, timestamps: List, ensemble: ForecastEnsemble, 
                              metrics: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Calculates metrics for a forecast ensemble.
    
    Args:
        y_true: List of actual values
        timestamps: List of timestamps corresponding to actual values
        ensemble: ForecastEnsemble object
        metrics: List of metric names to calculate, defaults to ['rmse', 'mae', 'mape', 'r2', 'bias']
        
    Returns:
        Dictionary of metric names and values
    """
    # Validate inputs
    if not y_true or not timestamps or len(y_true) != len(timestamps):
        logger.warning("Invalid inputs for ensemble metrics calculation: empty lists or unequal lengths")
        return {}
    
    # Set default metrics if none provided
    if metrics is None:
        metrics = ['rmse', 'mae', 'mape', 'r2', 'bias']
    
    # Extract forecasts for each timestamp
    forecasts = []
    for timestamp in timestamps:
        forecast = ensemble.get_forecast_at_time(timestamp)
        if forecast is None:
            logger.warning(f"No forecast found for timestamp {timestamp}")
            return {}
        forecasts.append(forecast)
    
    # Extract point forecasts
    y_pred = [forecast.point_forecast for forecast in forecasts]
    
    # Calculate point forecast metrics
    results = evaluate_forecast_accuracy(y_true, y_pred, metrics)
    
    # Calculate coverage for different confidence levels
    coverage_metrics = {}
    for confidence_level in CONFIDENCE_LEVELS:
        coverage_key = f"coverage_{int(confidence_level * 100)}"
        coverage_metrics[coverage_key] = calculate_coverage(y_true, forecasts, confidence_level)
    
    # Add coverage metrics to results
    results.update(coverage_metrics)
    
    return results


def create_metrics_dataframe(metrics: Dict[str, Any]) -> pd.DataFrame:
    """
    Creates a pandas DataFrame from metrics dictionary.
    
    Args:
        metrics: Dictionary of metrics
        
    Returns:
        DataFrame of metrics
    """
    # Validate input
    if not metrics:
        logger.warning("Empty metrics dictionary")
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(metrics, index=[0]).T.reset_index()
    df.columns = ['Metric', 'Value']
    
    # Format numeric values
    df['Value'] = pd.to_numeric(df['Value'], errors='ignore')
    df['Value'] = df['Value'].map(lambda x: f"{x:.4f}" if isinstance(x, (float, np.float64)) else x)
    
    return df


class ForecastEvaluator:
    """
    Class for evaluating and comparing forecast performance.
    
    This class provides methods for calculating accuracy metrics, identifying
    the best forecast model, and generating performance comparison reports.
    """
    
    def __init__(self, actuals: List, forecasts: Dict[str, List]):
        """
        Initializes a forecast evaluator.
        
        Args:
            actuals: List of actual values
            forecasts: Dictionary of forecast method names and predictions
        """
        # Validate inputs
        if not actuals:
            raise ValueError("Actuals list cannot be empty")
        
        if not forecasts:
            raise ValueError("Forecasts dictionary cannot be empty")
        
        # Check that all forecast lists have the same length as actuals
        for method, preds in forecasts.items():
            if len(preds) != len(actuals):
                raise ValueError(f"Length mismatch for {method}: {len(preds)} predictions, {len(actuals)} actuals")
        
        self.actuals = actuals
        self.forecasts = forecasts
        self.metrics_results = {}
    
    def calculate_all_metrics(self, metrics: Optional[List[str]] = None) -> Dict[str, Dict[str, float]]:
        """
        Calculates all metrics for all forecasts.
        
        Args:
            metrics: List of metrics to calculate, defaults to all available
            
        Returns:
            Dictionary of metrics for all forecasts
        """
        if metrics is None:
            metrics = ['rmse', 'mae', 'mape', 'r2', 'bias']
        
        results = {}
        for method, preds in self.forecasts.items():
            results[method] = evaluate_forecast_accuracy(self.actuals, preds, metrics)
        
        # Store results
        self.metrics_results = results
        
        return results
    
    def calculate_metric(self, metric_name: str) -> Dict[str, float]:
        """
        Calculates a specific metric for all forecasts.
        
        Args:
            metric_name: Name of the metric to calculate
            
        Returns:
            Dictionary of metric values for all forecasts
        """
        # Map of metric names to calculation functions
        metric_functions = {
            "rmse": calculate_rmse,
            "mae": calculate_mae,
            "mape": calculate_mape,
            "r2": calculate_r2,
            "bias": calculate_bias,
            "pinball_loss": calculate_pinball_loss
        }
        
        if metric_name not in metric_functions:
            raise ValueError(f"Unknown metric: {metric_name}")
        
        results = {}
        for method, preds in self.forecasts.items():
            if metric_name == "pinball_loss":
                # Special case for pinball loss, default to median (0.5)
                results[method] = metric_functions[metric_name](self.actuals, preds, 0.5)
            else:
                results[method] = metric_functions[metric_name](self.actuals, preds)
        
        return results
    
    def get_best_forecast(self, metric_name: str, lower_is_better: bool = True) -> str:
        """
        Gets the best forecast method based on a metric.
        
        Args:
            metric_name: Name of the metric to use
            lower_is_better: Whether lower values are better (True for error metrics)
            
        Returns:
            Name of the best forecast method
        """
        # Calculate metrics if not already done
        if not self.metrics_results:
            self.calculate_all_metrics([metric_name])
        
        best_method = None
        best_value = None
        
        for method, metrics in self.metrics_results.items():
            if metric_name not in metrics:
                continue
            
            value = metrics[metric_name]
            if value is None:
                continue
            
            if best_value is None:
                best_method = method
                best_value = value
            elif lower_is_better and value < best_value:
                best_method = method
                best_value = value
            elif not lower_is_better and value > best_value:
                best_method = method
                best_value = value
        
        return best_method
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Converts evaluation results to a pandas DataFrame.
        
        Returns:
            DataFrame of evaluation results
        """
        # Calculate metrics if not already done
        if not self.metrics_results:
            self.calculate_all_metrics()
        
        # Create multi-level DataFrame
        data = {}
        methods = list(self.metrics_results.keys())
        metrics = set()
        
        for method in methods:
            for metric in self.metrics_results[method]:
                metrics.add(metric)
        
        metrics = sorted(list(metrics))
        
        for metric in metrics:
            data[metric] = {}
            for method in methods:
                if metric in self.metrics_results[method]:
                    data[metric][method] = self.metrics_results[method][metric]
        
        df = pd.DataFrame(data)
        
        # Format numeric values
        for col in df.columns:
            df[col] = df[col].map(lambda x: f"{x:.4f}" if isinstance(x, (float, np.float64)) else x)
        
        return df


# Map metric names to calculation functions for easy access
METRIC_FUNCTIONS = {
    "rmse": calculate_rmse,
    "mae": calculate_mae,
    "mape": calculate_mape,
    "r2": calculate_r2,
    "bias": calculate_bias,
    "pinball_loss": calculate_pinball_loss
}