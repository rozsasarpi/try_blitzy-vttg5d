"""
Unit tests for the metrics utility functions that calculate and evaluate forecast accuracy metrics.
Tests cover point forecast metrics, probabilistic forecast metrics, forecast comparison, and the ForecastEvaluator class.
"""

import pytest  # pytest: 7.0.0+
import pandas as pd  # pandas: 2.0.0+
import numpy as np  # numpy: 1.24.0+
from datetime import datetime  # standard library
import math  # standard library

# Internal imports
from ...utils.metrics_utils import (  # Function to calculate Root Mean Square Error
    calculate_rmse,
    calculate_mae,  # Function to calculate Mean Absolute Error
    calculate_mape,  # Function to calculate Mean Absolute Percentage Error
    calculate_r2,  # Function to calculate R-squared coefficient
    calculate_bias,  # Function to calculate bias (mean error)
    calculate_pinball_loss,  # Function to calculate pinball loss for quantile forecasts
    evaluate_forecast_accuracy,  # Function to evaluate forecast accuracy using multiple metrics
    evaluate_probabilistic_forecast,  # Function to evaluate probabilistic forecast accuracy
    calculate_coverage,  # Function to calculate confidence interval coverage
    compare_forecasts,  # Function to compare multiple forecast methods
    calculate_forecast_improvement,  # Function to calculate percentage improvement between forecasts
    calculate_ensemble_metrics,  # Function to calculate metrics for a forecast ensemble
    create_metrics_dataframe,  # Function to create a DataFrame from metrics dictionary
    ForecastEvaluator,  # Class for evaluating forecast performance
    METRIC_FUNCTIONS,  # Dictionary mapping metric names to calculation functions
    CONFIDENCE_LEVELS  # Standard confidence levels for forecast intervals
)
from ...models.forecast_models import ProbabilisticForecast  # Class for probabilistic forecasts
from ...models.forecast_models import ForecastEnsemble  # Class for forecast ensembles
from ..fixtures.forecast_fixtures import create_mock_probabilistic_forecast  # Function to create mock probabilistic forecasts for testing
from ..fixtures.forecast_fixtures import create_mock_forecast_ensemble  # Function to create mock forecast ensembles for testing


def test_calculate_rmse():
    """Tests the calculate_rmse function with various inputs"""
    # Test with simple known values and verify result
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1.5, 2.5, 3.5, 4.5, 5.5]
    expected_rmse = math.sqrt(0.25)  # RMSE should be 0.5
    assert calculate_rmse(y_true, y_pred) == expected_rmse

    # Test with zero error case and verify result is 0
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1, 2, 3, 4, 5]
    assert calculate_rmse(y_true, y_pred) == 0

    # Test with empty lists and verify result is None
    assert calculate_rmse([], []) is None

    # Test with lists of different lengths and verify ValueError is raised
    with pytest.raises(ValueError):
        calculate_rmse([1, 2, 3], [1, 2])


def test_calculate_mae():
    """Tests the calculate_mae function with various inputs"""
    # Test with simple known values and verify result
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1.5, 2.5, 3.5, 4.5, 5.5]
    expected_mae = 0.5  # MAE should be 0.5
    assert calculate_mae(y_true, y_pred) == expected_mae

    # Test with zero error case and verify result is 0
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1, 2, 3, 4, 5]
    assert calculate_mae(y_true, y_pred) == 0

    # Test with empty lists and verify result is None
    assert calculate_mae([], []) is None

    # Test with lists of different lengths and verify ValueError is raised
    with pytest.raises(ValueError):
        calculate_mae([1, 2, 3], [1, 2])


def test_calculate_mape():
    """Tests the calculate_mape function with various inputs"""
    # Test with simple known values and verify result
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1.1, 2.2, 3.3, 4.4, 5.5]
    expected_mape = 10.0  # MAPE should be 10.0
    assert calculate_mape(y_true, y_pred) == expected_mape

    # Test with zero error case and verify result is 0
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1, 2, 3, 4, 5]
    assert calculate_mape(y_true, y_pred) == 0

    # Test with empty lists and verify result is None
    assert calculate_mape([], []) is None

    # Test with lists of different lengths and verify ValueError is raised
    with pytest.raises(ValueError):
        calculate_mape([1, 2, 3], [1, 2])

    # Test with zero actual values and verify result is None (to avoid division by zero)
    y_true = [0, 1, 2, 3, 4]
    y_pred = [0.1, 1.1, 2.2, 3.3, 4.4]
    assert calculate_mape(y_true, y_pred) is None


def test_calculate_r2():
    """Tests the calculate_r2 function with various inputs"""
    # Test with simple known values and verify result
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1.1, 1.9, 3.0, 4.1, 4.9]
    assert calculate_r2(y_true, y_pred) == pytest.approx(0.98, 0.01)

    # Test with perfect prediction and verify result is 1.0
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1, 2, 3, 4, 5]
    assert calculate_r2(y_true, y_pred) == 1.0

    # Test with mean prediction and verify result is 0.0
    y_true = [1, 2, 3, 4, 5]
    y_pred = [3, 3, 3, 3, 3]
    assert calculate_r2(y_true, y_pred) == 0.0

    # Test with worse than mean prediction and verify result is negative
    y_true = [1, 2, 3, 4, 5]
    y_pred = [5, 4, 3, 2, 1]
    assert calculate_r2(y_true, y_pred) == pytest.approx(-3.0, 0.01)

    # Test with empty lists and verify result is None
    assert calculate_r2([], []) is None

    # Test with lists of different lengths and verify ValueError is raised
    with pytest.raises(ValueError):
        calculate_r2([1, 2, 3], [1, 2])


def test_calculate_bias():
    """Tests the calculate_bias function with various inputs"""
    # Test with simple known values and verify result
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1.5, 2.5, 3.5, 4.5, 5.5]
    expected_bias = 0.5  # Bias should be 0.5
    assert calculate_bias(y_true, y_pred) == expected_bias

    # Test with zero bias case and verify result is 0
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1, 2, 3, 4, 5]
    assert calculate_bias(y_true, y_pred) == 0

    # Test with positive bias and verify result is positive
    y_true = [1, 2, 3, 4, 5]
    y_pred = [2, 3, 4, 5, 6]
    assert calculate_bias(y_true, y_pred) == 1

    # Test with negative bias and verify result is negative
    y_true = [1, 2, 3, 4, 5]
    y_pred = [0, 1, 2, 3, 4]
    assert calculate_bias(y_true, y_pred) == -1

    # Test with empty lists and verify result is None
    assert calculate_bias([], []) is None

    # Test with lists of different lengths and verify ValueError is raised
    with pytest.raises(ValueError):
        calculate_bias([1, 2, 3], [1, 2])


def test_calculate_pinball_loss():
    """Tests the calculate_pinball_loss function with various inputs"""
    # Test with simple known values at different quantiles (0.1, 0.5, 0.9)
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1.5, 2.5, 3.5, 4.5, 5.5]
    assert calculate_pinball_loss(y_true, y_pred, 0.5) == 0.25
    assert calculate_pinball_loss(y_true, y_pred, 0.1) == 0.45
    assert calculate_pinball_loss(y_true, y_pred, 0.9) == 0.05

    # Test with perfect prediction and verify result is 0
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1, 2, 3, 4, 5]
    assert calculate_pinball_loss(y_true, y_pred, 0.5) == 0

    # Test with empty lists and verify result is None
    assert calculate_pinball_loss([], [], 0.5) is None

    # Test with lists of different lengths and verify ValueError is raised
    with pytest.raises(ValueError):
        calculate_pinball_loss([1, 2, 3], [1, 2], 0.5)

    # Test with invalid quantile values and verify ValueError is raised
    with pytest.raises(ValueError):
        calculate_pinball_loss([1, 2, 3], [1, 2, 3], 1.5)
    with pytest.raises(ValueError):
        calculate_pinball_loss([1, 2, 3], [1, 2, 3], -0.5)


def test_evaluate_forecast_accuracy():
    """Tests the evaluate_forecast_accuracy function with various inputs"""
    # Test with simple known values and default metrics
    y_true = [1, 2, 3, 4, 5]
    y_pred = [1.1, 2.2, 3.3, 4.4, 5.5]
    results = evaluate_forecast_accuracy(y_true, y_pred)
    assert "rmse" in results
    assert "mae" in results
    assert "mape" in results
    assert "r2" in results
    assert "bias" in results

    # Test with specific metrics subset
    results = evaluate_forecast_accuracy(y_true, y_pred, metrics=["rmse", "mae"])
    assert "rmse" in results
    assert "mae" in results
    assert "mape" not in results
    assert "r2" not in results
    assert "bias" not in results

    # Test with empty lists and verify appropriate error handling
    assert evaluate_forecast_accuracy([], []) == {}

    # Test with lists of different lengths and verify ValueError is raised
    with pytest.raises(ValueError):
        evaluate_forecast_accuracy([1, 2, 3], [1, 2])

    # Test with invalid metric name and verify appropriate error handling
    results = evaluate_forecast_accuracy(y_true, y_pred, metrics=["invalid_metric"])
    assert "invalid_metric" not in results


def test_evaluate_probabilistic_forecast():
    """Tests the evaluate_probabilistic_forecast function with mock probabilistic forecasts"""
    # Create mock actual values
    y_true = [1, 2, 3, 4, 5]

    # Create mock probabilistic forecasts using create_mock_probabilistic_forecast
    forecasts = [create_mock_probabilistic_forecast(point_forecast=i) for i in y_true]

    # Test with default metrics and verify results contain point forecast metrics
    results = evaluate_probabilistic_forecast(y_true, forecasts)
    assert "rmse" in results
    assert "mae" in results
    assert "mape" in results
    assert "r2" in results
    assert "bias" in results

    # Verify results contain coverage metrics for each confidence level
    for confidence_level in CONFIDENCE_LEVELS:
        coverage_key = f"coverage_{int(confidence_level * 100)}"
        assert coverage_key in results

    # Verify results contain pinball loss metrics
    assert "pinball_loss_10" in results
    assert "pinball_loss_50" in results
    assert "pinball_loss_90" in results

    # Test with specific metrics subset
    results = evaluate_probabilistic_forecast(y_true, forecasts, metrics=["rmse", "mae"])
    assert "rmse" in results
    assert "mae" in results
    assert "mape" not in results
    assert "r2" not in results
    assert "bias" not in results
    for confidence_level in CONFIDENCE_LEVELS:
        coverage_key = f"coverage_{int(confidence_level * 100)}"
        assert coverage_key not in results
    assert "pinball_loss_10" not in results
    assert "pinball_loss_50" not in results
    assert "pinball_loss_90" not in results

    # Test with empty lists and verify appropriate error handling
    assert evaluate_probabilistic_forecast([], []) == {}

    # Test with lists of different lengths and verify ValueError is raised
    with pytest.raises(ValueError):
        evaluate_probabilistic_forecast([1, 2, 3], [create_mock_probabilistic_forecast(point_forecast=i) for i in [1, 2]])


def test_calculate_coverage():
    """Tests the calculate_coverage function with mock probabilistic forecasts"""
    # Create mock actual values
    y_true = [1, 2, 3, 4, 5]

    # Create mock probabilistic forecasts with known confidence intervals
    forecasts = [create_mock_probabilistic_forecast(point_forecast=i) for i in y_true]

    # Test coverage at different confidence levels (0.5, 0.8, 0.9, 0.95)
    assert calculate_coverage(y_true, forecasts, 0.5) == pytest.approx(1.0, 0.01)
    assert calculate_coverage(y_true, forecasts, 0.8) == pytest.approx(1.0, 0.01)
    assert calculate_coverage(y_true, forecasts, 0.9) == pytest.approx(1.0, 0.01)
    assert calculate_coverage(y_true, forecasts, 0.95) == pytest.approx(1.0, 0.01)

    # Verify coverage increases with wider confidence intervals
    coverage_50 = calculate_coverage(y_true, forecasts, 0.5)
    coverage_95 = calculate_coverage(y_true, forecasts, 0.95)
    assert coverage_95 >= coverage_50

    # Test with invalid confidence level and verify ValueError is raised
    with pytest.raises(ValueError):
        calculate_coverage(y_true, forecasts, 1.5)
    with pytest.raises(ValueError):
        calculate_coverage(y_true, forecasts, -0.5)

    # Test with empty lists and verify appropriate error handling
    assert calculate_coverage([], [], 0.5) == 0.0


def test_compare_forecasts():
    """Tests the compare_forecasts function with multiple forecast methods"""
    # Create mock actual values
    y_true = [1, 2, 3, 4, 5]

    # Create multiple mock forecast methods with different accuracy levels
    forecasts = {
        "method1": [1.1, 2.2, 3.3, 4.4, 5.5],
        "method2": [1.05, 2.1, 3.15, 4.2, 5.25],
        "method3": [0.9, 1.8, 2.7, 3.6, 4.5]
    }

    # Test with default metrics and verify results contain all methods
    results = compare_forecasts(y_true, forecasts)
    assert "method1" in results.index
    assert "method2" in results.index
    assert "method3" in results.index

    # Verify results contain all specified metrics
    assert "rmse" in results.columns
    assert "mae" in results.columns
    assert "mape" in results.columns
    assert "r2" in results.columns
    assert "bias" in results.columns

    # Verify DataFrame format is correct
    assert isinstance(results, pd.DataFrame)
    assert results.shape == (3, 5)

    # Test with specific metrics subset
    results = compare_forecasts(y_true, forecasts, metrics=["rmse", "mae"])
    assert "rmse" in results.columns
    assert "mae" in results.columns
    assert "mape" not in results.columns
    assert "r2" not in results.columns
    assert "bias" not in results.columns

    # Test with empty forecasts dictionary and verify ValueError is raised
    with pytest.raises(ValueError):
        compare_forecasts(y_true, {})


def test_calculate_forecast_improvement():
    """Tests the calculate_forecast_improvement function between two forecast methods"""
    # Create mock actual values
    y_true = [1, 2, 3, 4, 5]

    # Create two mock forecast methods with known accuracy difference
    forecast1 = [1.1, 2.2, 3.3, 4.4, 5.5]
    forecast2 = [1.05, 2.1, 3.15, 4.2, 5.25]

    # Test improvement calculation for metrics where lower is better (rmse, mae, mape)
    rmse_improvement = calculate_forecast_improvement(y_true, forecast1, forecast2, metric="rmse")
    mae_improvement = calculate_forecast_improvement(y_true, forecast1, forecast2, metric="mae")
    mape_improvement = calculate_forecast_improvement(y_true, forecast1, forecast2, metric="mape")

    # Test improvement calculation for metrics where higher is better (r2)
    r2_improvement = calculate_forecast_improvement(y_true, forecast1, forecast2, metric="r2")

    # Verify positive improvement when better method is second
    assert rmse_improvement > 0
    assert mae_improvement > 0
    assert mape_improvement > 0

    # Verify negative improvement when better method is first
    assert r2_improvement < 0

    # Test with invalid metric name and verify ValueError is raised
    with pytest.raises(ValueError):
        calculate_forecast_improvement(y_true, forecast1, forecast2, metric="invalid_metric")


def test_calculate_ensemble_metrics():
    """Tests the calculate_ensemble_metrics function with a mock forecast ensemble"""
    # Create mock actual values
    y_true = [1, 2, 3, 4, 5]

    # Create mock timestamps
    timestamps = [datetime(2023, 1, 1, i) for i in range(5)]

    # Create mock forecast ensemble using create_mock_forecast_ensemble
    ensemble = create_mock_forecast_ensemble()

    # Test with default metrics and verify results contain point forecast metrics
    results = calculate_ensemble_metrics(y_true, timestamps, ensemble)
    assert "rmse" in results
    assert "mae" in results
    assert "mape" in results
    assert "r2" in results
    assert "bias" in results

    # Verify results contain coverage metrics for each confidence level
    for confidence_level in CONFIDENCE_LEVELS:
        coverage_key = f"coverage_{int(confidence_level * 100)}"
        assert coverage_key in results

    # Test with specific metrics subset
    results = calculate_ensemble_metrics(y_true, timestamps, ensemble, metrics=["rmse", "mae"])
    assert "rmse" in results
    assert "mae" in results
    assert "mape" not in results
    assert "r2" not in results
    assert "bias" not in results
    for confidence_level in CONFIDENCE_LEVELS:
        coverage_key = f"coverage_{int(confidence_level * 100)}"
        assert coverage_key not in results

    # Test with empty lists and verify appropriate error handling
    assert calculate_ensemble_metrics([], [], ensemble) == {}


def test_create_metrics_dataframe():
    """Tests the create_metrics_dataframe function with a metrics dictionary"""
    # Create mock metrics dictionary
    metrics = {
        "rmse": 0.5,
        "mae": 0.4,
        "mape": 10.0,
        "r2": 0.95,
        "bias": 0.1
    }

    # Test conversion to DataFrame
    df = create_metrics_dataframe(metrics)

    # Verify DataFrame has correct structure and values
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (5, 2)
    assert list(df.columns) == ["Metric", "Value"]
    assert set(df["Metric"]) == set(metrics.keys())
    for metric, value in metrics.items():
        assert df[df["Metric"] == metric]["Value"].iloc[0] == str(value)

    # Test with empty dictionary and verify appropriate error handling
    assert create_metrics_dataframe({}).empty


def test_forecast_evaluator_init():
    """Tests the initialization of the ForecastEvaluator class"""
    # Create mock actual values
    actuals = [1, 2, 3, 4, 5]

    # Create mock forecasts dictionary
    forecasts = {
        "method1": [1.1, 2.2, 3.3, 4.4, 5.5],
        "method2": [1.05, 2.1, 3.15, 4.2, 5.25]
    }

    # Test successful initialization
    evaluator = ForecastEvaluator(actuals, forecasts)

    # Verify attributes are set correctly
    assert evaluator.actuals == actuals
    assert evaluator.forecasts == forecasts
    assert evaluator.metrics_results == {}

    # Test with empty actuals and verify ValueError is raised
    with pytest.raises(ValueError):
        ForecastEvaluator([], forecasts)

    # Test with empty forecasts and verify ValueError is raised
    with pytest.raises(ValueError):
        ForecastEvaluator(actuals, {})

    # Test with mismatched lengths and verify ValueError is raised
    with pytest.raises(ValueError):
        ForecastEvaluator(actuals, {"method1": [1, 2, 3]})


def test_forecast_evaluator_calculate_all_metrics():
    """Tests the calculate_all_metrics method of the ForecastEvaluator class"""
    # Create mock actual values
    actuals = [1, 2, 3, 4, 5]

    # Create mock forecasts dictionary with multiple methods
    forecasts = {
        "method1": [1.1, 2.2, 3.3, 4.4, 5.5],
        "method2": [1.05, 2.1, 3.15, 4.2, 5.25]
    }

    # Initialize ForecastEvaluator
    evaluator = ForecastEvaluator(actuals, forecasts)

    # Test with default metrics and verify results contain all methods
    results = evaluator.calculate_all_metrics()
    assert "method1" in results
    assert "method2" in results

    # Verify results contain all metrics
    assert "rmse" in results["method1"]
    assert "mae" in results["method1"]
    assert "mape" in results["method1"]
    assert "r2" in results["method1"]
    assert "bias" in results["method1"]

    # Test with specific metrics subset
    results = evaluator.calculate_all_metrics(metrics=["rmse", "mae"])
    assert "rmse" in results["method1"]
    assert "mae" in results["method1"]
    assert "mape" not in results["method1"]
    assert "r2" not in results["method1"]
    assert "bias" not in results["method1"]

    # Verify metrics_results attribute is updated
    assert evaluator.metrics_results == results


def test_forecast_evaluator_calculate_metric():
    """Tests the calculate_metric method of the ForecastEvaluator class"""
    # Create mock actual values
    actuals = [1, 2, 3, 4, 5]

    # Create mock forecasts dictionary with multiple methods
    forecasts = {
        "method1": [1.1, 2.2, 3.3, 4.4, 5.5],
        "method2": [1.05, 2.1, 3.15, 4.2, 5.25]
    }

    # Initialize ForecastEvaluator
    evaluator = ForecastEvaluator(actuals, forecasts)

    # Test calculation of specific metrics (rmse, mae, r2)
    rmse_results = evaluator.calculate_metric("rmse")
    mae_results = evaluator.calculate_metric("mae")
    r2_results = evaluator.calculate_metric("r2")

    # Verify results contain all methods
    assert "method1" in rmse_results
    assert "method2" in rmse_results
    assert "method1" in mae_results
    assert "method2" in mae_results
    assert "method1" in r2_results
    assert "method2" in r2_results

    # Test with invalid metric name and verify ValueError is raised
    with pytest.raises(ValueError):
        evaluator.calculate_metric("invalid_metric")


def test_forecast_evaluator_get_best_forecast():
    """Tests the get_best_forecast method of the ForecastEvaluator class"""
    # Create mock actual values
    actuals = [1, 2, 3, 4, 5]

    # Create mock forecasts dictionary with known accuracy differences
    forecasts = {
        "method1": [1.1, 2.2, 3.3, 4.4, 5.5],  # Less accurate
        "method2": [1.05, 2.1, 3.15, 4.2, 5.25]  # More accurate
    }

    # Initialize ForecastEvaluator
    evaluator = ForecastEvaluator(actuals, forecasts)

    # Test getting best forecast for metrics where lower is better (rmse, mae)
    best_rmse = evaluator.get_best_forecast("rmse")
    best_mae = evaluator.get_best_forecast("mae")

    # Test getting best forecast for metrics where higher is better (r2)
    best_r2 = evaluator.get_best_forecast("r2", lower_is_better=False)

    # Verify correct method is identified as best
    assert best_rmse == "method2"
    assert best_mae == "method2"
    assert best_r2 == "method2"

    # Test with invalid metric name and verify ValueError is raised
    with pytest.raises(ValueError):
        evaluator.get_best_forecast("invalid_metric")


def test_forecast_evaluator_to_dataframe():
    """Tests the to_dataframe method of the ForecastEvaluator class"""
    # Create mock actual values
    actuals = [1, 2, 3, 4, 5]

    # Create mock forecasts dictionary
    forecasts = {
        "method1": [1.1, 2.2, 3.3, 4.4, 5.5],
        "method2": [1.05, 2.1, 3.15, 4.2, 5.25]
    }

    # Initialize ForecastEvaluator
    evaluator = ForecastEvaluator(actuals, forecasts)

    # Calculate metrics
    evaluator.calculate_all_metrics()

    # Test conversion to DataFrame
    df = evaluator.to_dataframe()

    # Verify DataFrame has correct structure with methods as rows and metrics as columns
    assert isinstance(df, pd.DataFrame)
    assert list(df.index) == ["rmse", "mae", "mape", "r2", "bias"]
    assert list(df.columns) == ["method1", "method2"]

    # Verify values in DataFrame match calculated metrics
    rmse_method1 = calculate_rmse(actuals, forecasts["method1"])
    assert df.loc["rmse", "method1"] == str(rmse_method1)