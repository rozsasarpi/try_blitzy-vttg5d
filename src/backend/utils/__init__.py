"""
Initialization module for the backend utilities package in the Electricity Market Price Forecasting System.

Exports commonly used utility functions and classes from the various utility modules,
providing a centralized access point for utility functionality throughout the application.
Follows the functional programming approach specified in the requirements.
"""

__version__ = "1.0.0"

# Date and Time Utilities
from .date_utils import (  # version: N/A
    get_current_time_cst,
    localize_to_cst,
    convert_to_utc,
    get_next_execution_time,
    get_forecast_start_date,
    generate_forecast_datetimes,
    generate_forecast_date_range,
    format_timestamp,
    parse_timestamp,
    shift_timestamps,
    get_previous_day_date,
    calculate_date_difference,
)

# File System Utilities
from .file_utils import (  # version: N/A
    ensure_directory_exists,
    get_forecast_directory,
    get_forecast_file_path,
    save_dataframe,
    load_dataframe,
    list_forecast_files,
    get_latest_forecast_file,
    update_latest_link,
    clean_old_forecasts,
)

# Logging Utilities
from .logging_utils import (  # version: N/A
    get_logger,
    log_execution_time,
    log_method_execution_time,
    format_exception,
    format_dict_for_logging,
    configure_component_logger,
    ContextAdapter,
    ComponentLogger,
)

# Metrics Utilities
from .metrics_utils import (  # version: N/A
    calculate_rmse,
    calculate_mae,
    calculate_mape,
    calculate_r2,
    calculate_bias,
    evaluate_forecast_accuracy,
    compare_forecasts,
    ForecastEvaluator,
)

# Validation Utilities
from .validation_utils import (  # version: N/A
    validate_dataframe,
    ValidationCategory,
    ValidationOutcome,
    DataFrameValidator,
)

# Decorators
from .decorators import (  # version: N/A
    timing_decorator,
    retry,
    validate_input,
    validate_output,
    log_exceptions,
    fallback_on_exception,
    memoize,
    PerformanceMonitor
)