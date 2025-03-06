"""
Initialization file for the data ingestion module of the Electricity Market Price Forecasting System.
Exposes key classes and functions for collecting, validating, and transforming data from external sources.
This module serves as the entry point for all data collection operations required by the forecasting pipeline.
"""

# Version information
__version__ = "1.0.0"

# Import from standard library
import datetime
from typing import Dict, List, Optional, Any

# Import external libraries
import pandas as pd

# Import internal components
from .api_client import APIClient, fetch_data
from .load_forecast import (
    LoadForecastClient, 
    fetch_load_forecast,
    get_load_forecast_for_horizon
)
from .historical_prices import (
    HistoricalPriceClient, 
    fetch_historical_prices,
    get_historical_prices_for_model
)
from .generation_forecast import (
    GenerationForecastClient, 
    fetch_generation_forecast,
    get_generation_forecast_by_fuel_type
)
from .data_validator import (
    DataValidator,
    validate_load_forecast_data,
    validate_historical_prices_data,
    validate_generation_forecast_data
)
from .data_transformer import (
    DataTransformer,
    normalize_load_forecast_data,
    normalize_historical_prices_data,
    normalize_generation_forecast_data
)
from .exceptions import (
    DataIngestionError,
    APIConnectionError,
    APIResponseError,
    DataValidationError,
    DataTransformationError,
    MissingDataError,
    DataTimeRangeError
)


def collect_all_data(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    products: Optional[List[str]] = None,
    fuel_types: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Collects all required data from external sources for the forecasting pipeline.
    
    Args:
        start_date: Start date for data collection
        end_date: End date for data collection
        products: List of price products to fetch (default: all configured products)
        fuel_types: List of fuel types to fetch (default: all available fuel types)
        
    Returns:
        Dictionary containing all collected data with keys:
        - 'load_forecast': Load forecast DataFrame
        - 'historical_prices': Historical prices DataFrame
        - 'generation_forecast': Generation forecast DataFrame
        
    Raises:
        DataIngestionError: If there is an error fetching or processing data
    """
    try:
        # Create client instances for each data source
        load_client = LoadForecastClient()
        price_client = HistoricalPriceClient()
        generation_client = GenerationForecastClient()
        
        # Fetch load forecast data using LoadForecastClient
        load_df = load_client.get_forecast(start_date, end_date)
        
        # Fetch historical price data using HistoricalPriceClient
        price_df = price_client.get_historical_prices(start_date, end_date, products)
        
        # Fetch generation forecast data using GenerationForecastClient
        generation_df = generation_client.get_by_fuel_type(start_date, end_date, fuel_types)
        
        # Validate all collected data using DataValidator
        validator = DataValidator()
        load_valid = validator.validate_load_forecast(load_df)
        price_valid = validator.validate_historical_prices(price_df)
        gen_valid = validator.validate_generation_forecast(generation_df)
        
        # Check validation results
        if not load_valid.is_valid:
            raise DataValidationError("load_forecast", load_valid.errors.get("validation_error", []))
        if not price_valid.is_valid:
            raise DataValidationError("historical_prices", price_valid.errors.get("validation_error", []))
        if not gen_valid.is_valid:
            raise DataValidationError("generation_forecast", gen_valid.errors.get("validation_error", []))
            
        # Transform all data to standard formats using DataTransformer
        transformer = DataTransformer()
        load_df = transformer.transform_load_forecast(load_df)
        price_df = transformer.transform_historical_prices(price_df)
        generation_df = transformer.transform_generation_forecast(generation_df)
        
        # Return dictionary with all collected and processed data
        return {
            'load_forecast': load_df,
            'historical_prices': price_df,
            'generation_forecast': generation_df
        }
        
    except (APIConnectionError, APIResponseError, DataValidationError, DataTransformationError) as e:
        # These are already specific error types, so re-raise
        raise
    except Exception as e:
        # Wrap any other exceptions in a DataIngestionError
        raise DataIngestionError(f"Failed to collect all required data: {str(e)}")


class DataIngestionManager:
    """
    Manager class for coordinating data ingestion from all external sources.
    
    This class provides a unified interface for retrieving and processing data
    from multiple sources, handling validation, transformation, and error cases.
    """
    
    def __init__(self):
        """
        Initializes the data ingestion manager with all required clients.
        """
        self._load_forecast_client = LoadForecastClient()
        self._historical_price_client = HistoricalPriceClient()
        self._generation_forecast_client = GenerationForecastClient()
        self._validator = DataValidator()
        self._transformer = DataTransformer()
        
    def get_load_forecast(
        self, 
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ) -> pd.DataFrame:
        """
        Retrieves load forecast data for the specified time range.
        
        Args:
            start_date: Start date for the forecast period
            end_date: End date for the forecast period
            
        Returns:
            Validated and normalized load forecast data
            
        Raises:
            DataIngestionError: If there is an error fetching or processing data
        """
        try:
            # Call load_forecast_client's get_forecast method
            load_df = self._load_forecast_client.get_forecast(start_date, end_date)
            
            # Validate the returned data using validator
            validation_result = self._validator.validate_load_forecast(load_df)
            if not validation_result.is_valid:
                raise DataValidationError(
                    "load_forecast",
                    validation_result.errors.get("validation_error", [])
                )
            
            # Transform the data using transformer
            transformed_df = self._transformer.transform_load_forecast(load_df)
            
            # Return the processed DataFrame
            return transformed_df
            
        except (APIConnectionError, APIResponseError, DataValidationError, DataTransformationError) as e:
            # Handle exceptions with appropriate error logging
            raise
        except Exception as e:
            # Wrap any other exceptions in a DataIngestionError
            raise DataIngestionError(f"Failed to retrieve load forecast data: {str(e)}")
    
    def get_historical_prices(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        products: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Retrieves historical price data for the specified time range and products.
        
        Args:
            start_date: Start date for the historical period
            end_date: End date for the historical period
            products: List of price products to fetch (default: all configured products)
            
        Returns:
            Validated and normalized historical price data
            
        Raises:
            DataIngestionError: If there is an error fetching or processing data
        """
        try:
            # Call historical_price_client's get_historical_prices method
            price_df = self._historical_price_client.get_historical_prices(
                start_date, end_date, products
            )
            
            # Validate the returned data using validator
            validation_result = self._validator.validate_historical_prices(price_df)
            if not validation_result.is_valid:
                raise DataValidationError(
                    "historical_prices",
                    validation_result.errors.get("validation_error", [])
                )
            
            # Transform the data using transformer
            transformed_df = self._transformer.transform_historical_prices(price_df)
            
            # Return the processed DataFrame
            return transformed_df
            
        except (APIConnectionError, APIResponseError, DataValidationError, DataTransformationError) as e:
            # Handle exceptions with appropriate error logging
            raise
        except Exception as e:
            # Wrap any other exceptions in a DataIngestionError
            raise DataIngestionError(f"Failed to retrieve historical price data: {str(e)}")
    
    def get_generation_forecast(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        fuel_types: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Retrieves generation forecast data for the specified time range and fuel types.
        
        Args:
            start_date: Start date for the forecast period
            end_date: End date for the forecast period
            fuel_types: List of fuel types to fetch (default: all available fuel types)
            
        Returns:
            Validated and normalized generation forecast data
            
        Raises:
            DataIngestionError: If there is an error fetching or processing data
        """
        try:
            # Call generation_forecast_client's get_by_fuel_type method
            generation_df = self._generation_forecast_client.get_by_fuel_type(
                start_date, end_date, fuel_types
            )
            
            # Validate the returned data using validator
            validation_result = self._validator.validate_generation_forecast(generation_df)
            if not validation_result.is_valid:
                raise DataValidationError(
                    "generation_forecast",
                    validation_result.errors.get("validation_error", [])
                )
            
            # Transform the data using transformer
            transformed_df = self._transformer.transform_generation_forecast(generation_df)
            
            # Return the processed DataFrame
            return transformed_df
            
        except (APIConnectionError, APIResponseError, DataValidationError, DataTransformationError) as e:
            # Handle exceptions with appropriate error logging
            raise
        except Exception as e:
            # Wrap any other exceptions in a DataIngestionError
            raise DataIngestionError(f"Failed to retrieve generation forecast data: {str(e)}")
    
    def get_all_data(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        products: Optional[List[str]] = None,
        fuel_types: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Retrieves all required data for the forecasting pipeline.
        
        Args:
            start_date: Start date for data collection
            end_date: End date for data collection
            products: List of price products to fetch (default: all configured products)
            fuel_types: List of fuel types to fetch (default: all available fuel types)
            
        Returns:
            Dictionary containing all collected data with keys:
            - 'load_forecast': Load forecast DataFrame
            - 'historical_prices': Historical prices DataFrame
            - 'generation_forecast': Generation forecast DataFrame
            
        Raises:
            DataIngestionError: If there is an error fetching or processing data
        """
        try:
            # Call get_load_forecast method
            load_df = self.get_load_forecast(start_date, end_date)
            
            # Call get_historical_prices method
            price_df = self.get_historical_prices(start_date, end_date, products)
            
            # Call get_generation_forecast method
            generation_df = self.get_generation_forecast(start_date, end_date, fuel_types)
            
            # Combine all data into a dictionary
            data_dict = {
                'load_forecast': load_df,
                'historical_prices': price_df,
                'generation_forecast': generation_df
            }
            
            # Return the dictionary with all data
            return data_dict
            
        except (APIConnectionError, APIResponseError, DataValidationError, DataTransformationError) as e:
            # Handle exceptions with appropriate error logging
            raise
        except Exception as e:
            # Wrap any other exceptions in a DataIngestionError
            raise DataIngestionError(f"Failed to collect all required data: {str(e)}")
    
    def prepare_model_inputs(
        self,
        data_dict: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Prepares combined dataset for use in forecasting models.
        
        Args:
            data_dict: Dictionary containing individual data sources
            
        Returns:
            Combined dataset ready for feature engineering
            
        Raises:
            DataIngestionError: If there is an error processing the data
        """
        try:
            # Extract individual dataframes from the dictionary
            load_df = data_dict.get('load_forecast')
            price_df = data_dict.get('historical_prices')
            generation_df = data_dict.get('generation_forecast')
            
            # Check that all required data is present
            if load_df is None or price_df is None or generation_df is None:
                missing = []
                if load_df is None:
                    missing.append('load_forecast')
                if price_df is None:
                    missing.append('historical_prices')
                if generation_df is None:
                    missing.append('generation_forecast')
                    
                raise MissingDataError("model_input_preparation", missing)
            
            # Call transformer's prepare_combined_dataset method
            combined_df = self._transformer.prepare_combined_dataset(
                load_df, price_df, generation_df
            )
            
            # Return the combined dataset
            return combined_df
            
        except (DataValidationError, DataTransformationError, MissingDataError) as e:
            # Handle exceptions with appropriate error logging
            raise
        except Exception as e:
            # Wrap any other exceptions in a DataIngestionError
            raise DataIngestionError(f"Failed to prepare model inputs: {str(e)}")