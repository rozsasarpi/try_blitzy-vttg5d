"""Core implementation of the forecasting pipeline for the Electricity Market Price Forecasting System.
This module orchestrates the entire forecasting process by coordinating data ingestion, feature engineering,
forecast generation, validation, and storage. It implements the fallback mechanism to ensure forecast
availability even when errors occur.
"""

import typing
from datetime import datetime
import uuid
import time

import pandas  # package_version: 2.0.0+

# Internal imports
from .exceptions import PipelineError, PipelineExecutionError, PipelineStageError, PipelineDataError
from .pipeline_logger import log_pipeline_start, log_pipeline_completion, log_pipeline_failure, log_stage_start, log_stage_completion, log_fallback_activation
from ..data_ingestion.api_client import APIClient
from ..feature_engineering.product_hour_features import ProductHourFeatureCreator
from ..forecasting_engine.probabilistic_forecaster import ProbabilisticForecaster
from ..forecast_validation.schema_validator import validate_forecast_schema
from ..storage.storage_manager import save_forecast, retrieve_fallback_forecast
from ..utils.decorators import log_execution_time, log_exceptions
from ..utils.logging_utils import get_logger
from ..config.settings import FORECAST_PRODUCTS, FORECAST_HORIZON_HOURS, DATA_SOURCES

# Global logger
logger = get_logger(__name__)

# Pipeline name
PIPELINE_NAME = "forecasting_pipeline"


@log_execution_time
@log_exceptions
def run_forecasting_pipeline(target_date: datetime, config: dict) -> dict:
    """Main entry point for the forecasting pipeline that orchestrates the entire process

    Args:
        target_date (datetime.datetime): The target date for which to generate forecasts
        config (dict): Configuration dictionary for the pipeline

    Returns:
        dict: Dictionary with pipeline execution results and metadata
    """
    # 1. Generate a unique execution ID
    execution_id = str(uuid.uuid4())

    # 2. Initialize result dictionary
    results = {"execution_id": execution_id, "status": "pending"}

    # 3. Log pipeline start
    log_pipeline_start(PIPELINE_NAME, execution_id, config)

    # 4. Record start time
    start_time = time.time()

    try:
        # 5. Create a ForecastingPipeline instance
        pipeline = ForecastingPipeline(target_date, config, execution_id)

        # 6. Execute the pipeline
        pipeline.run()

        # 7. Get pipeline results
        results.update(pipeline.get_results())
        results["status"] = "success"

        # 8. Log pipeline completion
        log_pipeline_completion(PIPELINE_NAME, execution_id, start_time, results)

    except Exception as e:
        # Handle exceptions
        results["status"] = "failure"
        log_pipeline_failure(PIPELINE_NAME, execution_id, start_time, e, results)

    # 9. Return the result dictionary
    return results


class ForecastingPipeline:
    """Main class that implements the forecasting pipeline with all stages"""

    def __init__(self, target_date: datetime, config: dict, execution_id: str):
        """Initialize the forecasting pipeline with target date and configuration

        Args:
            target_date (datetime.datetime): The target date for which to generate forecasts
            config (dict): Configuration dictionary for the pipeline
            execution_id (str): Unique identifier for this pipeline execution
        """
        # 1. Store target_date
        self.target_date = target_date

        # 2. Initialize config
        self.config = config or {}

        # 3. Store execution_id
        self.execution_id = execution_id

        # 4. Initialize results dictionary
        self.results = {}

        # 5. Set fallback_used flag
        self.fallback_used = False

        # 6. Initialize empty data_cache dictionary
        self.data_cache = {}

        # 7. Log pipeline initialization
        logger.info(f"Initialized forecasting pipeline for {target_date} with execution ID {execution_id}")

    def run(self) -> bool:
        """Execute the complete forecasting pipeline with all stages

        Returns:
            bool: True if pipeline executed successfully, False otherwise
        """
        try:
            # 1. Log start of pipeline execution
            logger.info(f"Starting forecasting pipeline for {self.target_date}")

            # 2. Execute data ingestion stage
            ingested_data = self.ingest_data()
            self.results["ingested_data"] = ingested_data

            # 3. Execute feature engineering stage
            features = self.engineer_features(ingested_data)
            self.results["features"] = features

            # 4. Execute forecast generation stage
            forecasts = self.generate_forecasts(features, ingested_data)
            self.results["forecasts"] = forecasts

            # 5. Execute forecast validation stage
            validated_forecasts = self.validate_forecasts(forecasts)
            self.results["validated_forecasts"] = validated_forecasts

            # 6. Execute forecast storage stage
            storage_results = self.store_forecasts(validated_forecasts)
            self.results["storage_results"] = storage_results

            # 7. Update results with success status and metadata
            self.results["status"] = "success"
            self.results["completed_at"] = datetime.now()

            # 8. Return True if all stages completed successfully
            logger.info(f"Successfully completed forecasting pipeline for {self.target_date}")
            return True

        except Exception as e:
            # Handle stage failures by activating fallback mechanism
            logger.error(f"Pipeline failed: {str(e)}")
            failed_stage = getattr(e, 'stage_name', 'unknown')
            self.activate_fallback(failed_stage, e)
            return False

    @log_execution_time
    @log_exceptions
    def ingest_data(self) -> dict:
        """Data ingestion stage: fetch data from external sources

        Returns:
            dict: Dictionary of ingested data by source
        """
        # 1. Log start of data ingestion stage
        log_stage_start(PIPELINE_NAME, self.execution_id, "ingest_data")
        start_time = time.time()

        try:
            # 2. Initialize data dictionary
            data = {}

            # 3. For each data source in DATA_SOURCES:
            for source_name in DATA_SOURCES:
                # 4. Create APIClient for the source
                api_client = APIClient(source_name)

                # 5. Calculate date range for data retrieval
                start_date = self.target_date - pandas.Timedelta(days=7)
                end_date = self.target_date + pandas.Timedelta(days=3)

                # 6. Fetch data using APIClient.get_data
                data[source_name] = api_client.get_data(start_date, end_date)

            # 7. Log completion of data ingestion stage
            log_stage_completion(PIPELINE_NAME, self.execution_id, "ingest_data", start_time)

            # 8. Store data in cache for potential reuse
            self.data_cache["ingested_data"] = data

            # 9. Return the ingested data dictionary
            return data

        except Exception as e:
            # Handle exceptions by raising PipelineStageError with context
            error_msg = f"Data ingestion failed: {str(e)}"
            logger.error(error_msg)
            raise PipelineStageError(error_msg, PIPELINE_NAME, "ingest_data", self.execution_id)

    @log_execution_time
    @log_exceptions
    def engineer_features(self, input_data: dict) -> dict:
        """Feature engineering stage: create features for forecasting

        Args:
            input_data (dict): Dictionary of ingested data by source

        Returns:
            dict: Dictionary of feature dataframes by product/hour
        """
        # 1. Log start of feature engineering stage
        log_stage_start(PIPELINE_NAME, self.execution_id, "engineer_features")
        start_time = time.time()

        try:
            # 2. Initialize features dictionary
            features = {}

            # 3. Create ProductHourFeatureCreator instance
            feature_creator = ProductHourFeatureCreator()

            # 4. For each product in FORECAST_PRODUCTS:
            for product in FORECAST_PRODUCTS:
                # 5. For each hour in range(24):
                for hour in range(24):
                    # 6. Create features for product/hour combination
                    features[(product, hour)] = feature_creator.create_features(product, hour)

            # 7. Log completion of feature engineering stage
            log_stage_completion(PIPELINE_NAME, self.execution_id, "engineer_features", start_time)

            # 8. Store features in cache for potential reuse
            self.data_cache["features"] = features

            # 9. Return the features dictionary
            return features

        except Exception as e:
            # Handle exceptions by raising PipelineStageError with context
            error_msg = f"Feature engineering failed: {str(e)}"
            logger.error(error_msg)
            raise PipelineStageError(error_msg, PIPELINE_NAME, "engineer_features", self.execution_id)

    @log_execution_time
    @log_exceptions
    def generate_forecasts(self, features: dict, historical_data: dict) -> dict:
        """Forecast generation stage: create probabilistic forecasts

        Args:
            features (dict): Dictionary of feature dataframes by product/hour
            historical_data (dict): Dictionary of historical data

        Returns:
            dict: Dictionary of forecast ensembles by product
        """
        # 1. Log start of forecast generation stage
        log_stage_start(PIPELINE_NAME, self.execution_id, "generate_forecasts")
        start_time = time.time()

        try:
            # 2. Initialize forecasts dictionary
            forecasts = {}

            # 3. Create ProbabilisticForecaster instance
            forecaster = ProbabilisticForecaster()

            # 4. For each product in FORECAST_PRODUCTS:
            for product in FORECAST_PRODUCTS:
                # 5. Get features for this product
                product_features = features.get(product)

                # 6. Generate forecast ensemble for the product
                forecasts[product] = forecaster.generate_ensemble(product, product_features, historical_data, self.target_date)

            # 7. Log completion of forecast generation stage
            log_stage_completion(PIPELINE_NAME, self.execution_id, "generate_forecasts", start_time)

            # 8. Return the forecasts dictionary
            return forecasts

        except Exception as e:
            # Handle exceptions by raising PipelineStageError with context
            error_msg = f"Forecast generation failed: {str(e)}"
            logger.error(error_msg)
            raise PipelineStageError(error_msg, PIPELINE_NAME, "generate_forecasts", self.execution_id)

    @log_execution_time
    @log_exceptions
    def validate_forecasts(self, forecasts: dict) -> dict:
        """Forecast validation stage: ensure forecasts meet quality standards

        Args:
            forecasts (dict): Dictionary of forecast ensembles by product

        Returns:
            dict: Dictionary of validated forecast dataframes
        """
        # 1. Log start of forecast validation stage
        log_stage_start(PIPELINE_NAME, self.execution_id, "validate_forecasts")
        start_time = time.time()

        try:
            # 2. Initialize validated_forecasts dictionary
            validated_forecasts = {}

            # 3. For each product, forecast in forecasts.items():
            for product, forecast in forecasts.items():
                # 4. Convert forecast ensemble to dataframe
                forecast_df = forecast.to_dataframe()

                # 5. Validate forecast using validate_forecast_schema
                is_valid, errors = validate_forecast_schema(forecast_df)

                # 6. If validation passes, add to validated_forecasts
                if is_valid:
                    validated_forecasts[product] = forecast_df
                else:
                    # 7. If validation fails, log error and raise exception
                    error_msg = f"Forecast validation failed for {product}: {errors}"
                    logger.error(error_msg)
                    raise PipelineStageError(error_msg, PIPELINE_NAME, "validate_forecasts", self.execution_id)

            # 8. Log completion of forecast validation stage
            log_stage_completion(PIPELINE_NAME, self.execution_id, "validate_forecasts", start_time)

            # 9. Return the validated_forecasts dictionary
            return validated_forecasts

        except Exception as e:
            # Handle exceptions by raising PipelineStageError with context
            error_msg = f"Forecast validation failed: {str(e)}"
            logger.error(error_msg)
            raise PipelineStageError(error_msg, PIPELINE_NAME, "validate_forecasts", self.execution_id)

    @log_execution_time
    @log_exceptions
    def store_forecasts(self, validated_forecasts: dict) -> dict:
        """Forecast storage stage: save forecasts to storage system

        Args:
            validated_forecasts (dict): Dictionary of validated forecast dataframes

        Returns:
            dict: Dictionary with storage results and paths
        """
        # 1. Log start of forecast storage stage
        log_stage_start(PIPELINE_NAME, self.execution_id, "store_forecasts")
        start_time = time.time()

        try:
            # 2. Initialize storage_results dictionary
            storage_results = {}

            # 3. For each product, forecast_df in validated_forecasts.items():
            for product, forecast_df in validated_forecasts.items():
                # 4. Save forecast using save_forecast function
                file_path = save_forecast(forecast_df, self.target_date, product)

                # 5. Store file path in storage_results
                storage_results[product] = str(file_path)

            # 6. Log completion of forecast storage stage
            log_stage_completion(PIPELINE_NAME, self.execution_id, "store_forecasts", start_time)

            # 7. Return the storage_results dictionary
            return storage_results

        except Exception as e:
            # Handle exceptions by raising PipelineStageError with context
            error_msg = f"Forecast storage failed: {str(e)}"
            logger.error(error_msg)
            raise PipelineStageError(error_msg, PIPELINE_NAME, "store_forecasts", self.execution_id)

    @log_execution_time
    @log_exceptions
    def activate_fallback(self, failed_stage: str, error: Exception) -> bool:
        """Activate the fallback mechanism when a pipeline stage fails

        Args:
            failed_stage (str): Name of the stage that failed
            error (Exception): The exception that caused the failure

        Returns:
            bool: True if fallback was successful, False otherwise
        """
        # 1. Log activation of fallback mechanism
        log_fallback_activation(failed_stage, str(type(error)), {"error": str(error)})

        # 2. Set fallback_used flag to True
        self.fallback_used = True

        try:
            # 3. For each product in FORECAST_PRODUCTS:
            for product in FORECAST_PRODUCTS:
                # 4. Retrieve fallback forecast using retrieve_fallback_forecast
                fallback_df = retrieve_fallback_forecast(product, self.target_date)

                # 5. Validate fallback forecast using validate_forecast_schema
                is_valid, errors = validate_forecast_schema(fallback_df)
                if not is_valid:
                    error_msg = f"Fallback forecast validation failed for {product}: {errors}"
                    logger.error(error_msg)
                    raise PipelineStageError(error_msg, PIPELINE_NAME, "validate_forecasts", self.execution_id)

                # 6. Save fallback forecast with is_fallback=True flag
                file_path = save_forecast(fallback_df, self.target_date, product, is_fallback=True)

                # 7. Store fallback information in results
                self.results[f"fallback_{product}"] = str(file_path)

            # 8. Log successful fallback activation
            logger.info("Successfully activated fallback mechanism")

            # 9. Return True if fallback was successful
            return True

        except Exception as e:
            # Handle fallback failures by logging and returning False
            error_msg = f"Fallback mechanism failed: {str(e)}"
            logger.error(error_msg)
            return False

    def get_results(self) -> dict:
        """Get the results of the pipeline execution

        Returns:
            dict: Dictionary with pipeline execution results
        """
        return self.results

    def was_fallback_used(self) -> bool:
        """Check if fallback mechanism was used during pipeline execution

        Returns:
            bool: True if fallback was used, False otherwise
        """
        return self.fallback_used