# Standard library imports
import datetime

# External library imports
import pytest  # pytest: 7.0.0+
import pandas  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+

# Internal imports
from src.web.data.data_processor import (  # Class for processing forecast data for visualization
    ForecastDataProcessor,
    filter_forecast_by_product,  # Filter forecast data by product
    filter_forecast_by_date_range,  # Filter forecast data by date range
    prepare_time_series_data,  # Prepare data for time series visualization
    prepare_distribution_data,  # Prepare data for probability distribution visualization
    prepare_hourly_table_data,  # Prepare data for tabular display
    prepare_comparison_data,  # Prepare data for product comparison visualization
    calculate_statistics,  # Calculate statistical metrics for forecast data
    identify_price_spikes,  # Identify potential price spikes in forecast data
    resample_forecast_data,  # Resample forecast data to different time frequency
    get_forecast_summary,  # Generate summary of forecast data for display
    DEFAULT_PERCENTILES,  # Default percentiles for uncertainty bands
)
from src.web.tests.fixtures.forecast_fixtures import (  # Create sample forecast dataframe in visualization format
    create_sample_visualization_dataframe,
    create_sample_forecast_dataframe,  # Create sample forecast dataframe in backend format
    create_multi_product_forecast_dataframe,  # Create sample multi-product forecast dataframe
    create_forecast_with_extreme_values,  # Create sample forecast with extreme values
)
from src.web.config.product_config import PRODUCTS  # List of valid electricity market products
from src.web.data.schema import prepare_dataframe_for_visualization  # Transform forecast dataframe for visualization


class TestFilterFunctions:
    """Test class for data filtering functions in the data_processor module"""

    def test_filter_forecast_by_product(self):
        """Tests that filter_forecast_by_product correctly filters a dataframe to include only data for a specific product"""
        # Create a multi-product forecast dataframe with known products
        df = create_multi_product_forecast_dataframe(products=['DALMP', 'RTLMP'])
        # Call filter_forecast_by_product with a specific product
        filtered_df = filter_forecast_by_product(df, 'DALMP')
        # Assert that the resulting dataframe only contains rows for the specified product
        assert all(filtered_df['product'] == 'DALMP')
        # Assert that the number of rows matches the expected count for that product
        assert len(filtered_df) == len(df) / 2

    def test_filter_forecast_by_product_invalid_product(self):
        """Tests that filter_forecast_by_product raises an appropriate exception when an invalid product is specified"""
        # Create a sample forecast dataframe
        df = create_sample_forecast_dataframe()
        # Use pytest.raises to verify that calling filter_forecast_by_product with an invalid product raises ValueError
        with pytest.raises(ValueError) as excinfo:
            filter_forecast_by_product(df, 'INVALID')
        # Verify the error message mentions the invalid product
        assert "Invalid product: INVALID" in str(excinfo.value)

    def test_filter_forecast_by_date_range(self):
        """Tests that filter_forecast_by_date_range correctly filters a dataframe to include only data within a specific date range"""
        # Create a sample forecast dataframe with a known date range
        start_date = datetime.datetime(2023, 1, 1)
        end_date = datetime.datetime(2023, 1, 3)
        df = create_sample_forecast_dataframe(start_time=start_date, hours=72)
        # Define start and end dates within the range
        filter_start = datetime.datetime(2023, 1, 1, 12, 0, 0)
        filter_end = datetime.datetime(2023, 1, 2, 12, 0, 0)
        # Call filter_forecast_by_date_range with the start and end dates
        filtered_df = filter_forecast_by_date_range(df, filter_start, filter_end)
        # Assert that the resulting dataframe only contains rows within the specified date range
        assert all((filtered_df['timestamp'] >= filter_start) & (filtered_df['timestamp'] <= filter_end))
        # Assert that the number of rows matches the expected count for that date range
        assert len(filtered_df) == 25

    def test_filter_forecast_by_date_range_string_dates(self):
        """Tests that filter_forecast_by_date_range correctly handles string date inputs"""
        # Create a sample forecast dataframe with a known date range
        start_date = datetime.datetime(2023, 1, 1)
        end_date = datetime.datetime(2023, 1, 3)
        df = create_sample_forecast_dataframe(start_time=start_date, hours=72)
        # Define start and end dates as strings in ISO format
        filter_start = "2023-01-01T12:00:00"
        filter_end = "2023-01-02T12:00:00"
        # Call filter_forecast_by_date_range with the string dates
        filtered_df = filter_forecast_by_date_range(df, filter_start, filter_end)
        # Assert that the resulting dataframe only contains rows within the specified date range
        assert all((filtered_df['timestamp'] >= filter_start) & (filtered_df['timestamp'] <= filter_end))
        # Assert that the number of rows matches the expected count for that date range
        assert len(filtered_df) == 25


class TestVisualizationPreparation:
    """Test class for visualization data preparation functions in the data_processor module"""

    def test_prepare_time_series_data(self):
        """Tests that prepare_time_series_data correctly processes forecast data for time series visualization"""
        # Create a sample visualization dataframe for a specific product
        df = create_sample_visualization_dataframe(product='DALMP')
        # Call prepare_time_series_data with the dataframe and product
        prepared_df = prepare_time_series_data(df, 'DALMP')
        # Assert that the resulting dataframe has the required columns (timestamp, point_forecast, lower_bound, upper_bound)
        assert all(col in prepared_df.columns for col in ['timestamp', 'point_forecast', 'lower_bound', 'upper_bound'])
        # Assert that the dataframe is sorted by timestamp
        assert prepared_df['timestamp'].is_monotonic_increasing
        # Assert that formatted price values are added for tooltips
        assert all(col in prepared_df.columns for col in ['point_forecast_formatted', 'lower_bound_formatted', 'upper_bound_formatted'])

    def test_prepare_distribution_data(self):
        """Tests that prepare_distribution_data correctly processes forecast data for probability distribution visualization"""
        # Create a sample visualization dataframe for a specific product
        df = create_sample_visualization_dataframe(product='DALMP')
        # Select a target hour (timestamp) from the dataframe
        target_hour = df['timestamp'].iloc[0]
        # Call prepare_distribution_data with the dataframe, product, and target hour
        distribution_df = prepare_distribution_data(df, 'DALMP', target_hour)
        # Assert that the resulting dataframe contains sample values for the target hour
        assert 'value' in distribution_df.columns
        # Assert that the dataframe has the expected structure for distribution visualization
        assert all(col in distribution_df.columns for col in ['value_formatted', 'target_hour', 'target_hour_formatted', 'point_forecast', 'point_forecast_formatted'])

    def test_prepare_distribution_data_with_index(self):
        """Tests that prepare_distribution_data correctly handles target hour specified as an index"""
        # Create a sample visualization dataframe for a specific product
        df = create_sample_visualization_dataframe(product='DALMP')
        # Call prepare_distribution_data with the dataframe, product, and a numeric index (e.g., 0)
        distribution_df = prepare_distribution_data(df, 'DALMP', 0)
        # Assert that the resulting dataframe contains sample values for the hour at the specified index
        assert 'value' in distribution_df.columns
        # Assert that the dataframe has the expected structure for distribution visualization
        assert all(col in distribution_df.columns for col in ['value_formatted', 'target_hour', 'target_hour_formatted', 'point_forecast', 'point_forecast_formatted'])

    def test_prepare_hourly_table_data(self):
        """Tests that prepare_hourly_table_data correctly processes forecast data for tabular display"""
        # Create a sample visualization dataframe for a specific product
        df = create_sample_visualization_dataframe(product='DALMP')
        # Call prepare_hourly_table_data with the dataframe and product
        table_df = prepare_hourly_table_data(df, 'DALMP')
        # Assert that the resulting dataframe has the required columns for tabular display
        assert all(col in table_df.columns for col in ['hour', 'point_forecast', 'point_forecast_formatted', 'range_formatted'])
        # Assert that timestamps are formatted as hour labels
        assert all(':' in hour for hour in table_df['hour'])
        # Assert that price values are formatted with appropriate currency symbols
        assert all('$' in price for price in table_df['point_forecast_formatted'])
        # Assert that price ranges are calculated and formatted correctly
        assert all('$' in r and '-' in r for r in table_df['range_formatted'])

    def test_prepare_hourly_table_data_custom_percentiles(self):
        """Tests that prepare_hourly_table_data correctly handles custom percentiles"""
        # Create a sample visualization dataframe for a specific product
        df = create_sample_visualization_dataframe(product='DALMP')
        # Define custom percentiles (e.g., [5, 95])
        custom_percentiles = [5, 95]
        # Call prepare_hourly_table_data with the dataframe, product, and custom percentiles
        table_df = prepare_hourly_table_data(df, 'DALMP', custom_percentiles)
        # Assert that the resulting dataframe contains columns for the custom percentiles
        assert all(f'percentile_{p}' in table_df.columns for p in custom_percentiles)
        # Assert that the percentile values are calculated correctly
        assert all(f'percentile_{p}_formatted' in table_df.columns for p in custom_percentiles)

    def test_prepare_comparison_data(self):
        """Tests that prepare_comparison_data correctly processes forecast data for product comparison visualization"""
        # Create a multi-product forecast dataframe with known products
        df = create_multi_product_forecast_dataframe(products=['DALMP', 'RTLMP'])
        # Define a list of products to compare
        products_to_compare = ['DALMP', 'RTLMP']
        # Call prepare_comparison_data with the dataframe and product list
        comparison_df = prepare_comparison_data(df, products_to_compare)
        # Assert that the resulting dataframe only contains the specified products
        assert all(product in products_to_compare for product in comparison_df['product'].unique())
        # Assert that the dataframe is sorted by timestamp and product
        assert comparison_df.sort_values(['timestamp', 'product']).equals(comparison_df)
        # Assert that formatted price values are added for tooltips
        assert 'point_forecast_formatted' in comparison_df.columns

    def test_prepare_comparison_data_invalid_product(self):
        """Tests that prepare_comparison_data raises an appropriate exception when an invalid product is specified"""
        # Create a sample multi-product forecast dataframe
        df = create_multi_product_forecast_dataframe(products=['DALMP', 'RTLMP'])
        # Define a list of products including an invalid product
        products_to_compare = ['DALMP', 'INVALID']
        # Use pytest.raises to verify that calling prepare_comparison_data with the invalid product raises ValueError
        with pytest.raises(ValueError) as excinfo:
            prepare_comparison_data(df, products_to_compare)
        # Verify the error message mentions the invalid product
        assert "Invalid products: ['INVALID']" in str(excinfo.value)


class TestAnalysisFunctions:
    """Test class for data analysis functions in the data_processor module"""

    def test_calculate_statistics(self):
        """Tests that calculate_statistics correctly calculates statistical metrics for forecast data"""
        # Create a sample visualization dataframe for a specific product with known values
        df = create_sample_visualization_dataframe(product='DALMP')
        # Call calculate_statistics with the dataframe and product
        statistics = calculate_statistics(df, 'DALMP')
        # Assert that the resulting dictionary contains the expected keys (min, max, mean, median, volatility, peak_hours)
        assert all(key in statistics for key in ['min', 'max', 'mean', 'median', 'std_dev', 'peak_hours'])
        # Assert that the calculated values match the expected values
        assert isinstance(statistics['mean'], float)
        assert isinstance(statistics['std_dev'], float)
        assert isinstance(statistics['peak_hours'], list)

    def test_identify_price_spikes(self):
        """Tests that identify_price_spikes correctly identifies potential price spikes in forecast data"""
        # Create a forecast dataframe with extreme values at known positions
        df = create_forecast_with_extreme_values(product='DALMP')
        # Call identify_price_spikes with the dataframe, product, and threshold multiplier
        spikes_df = identify_price_spikes(df, 'DALMP')
        # Assert that the resulting dataframe identifies the expected spike hours
        assert not spikes_df.empty
        # Assert that the dataframe contains the expected information about each spike
        assert all(col in spikes_df.columns for col in ['threshold', 'deviation', 'deviation_percent', 'point_forecast_formatted', 'threshold_formatted', 'deviation_formatted', 'deviation_percent_formatted', 'hour_formatted'])

    def test_resample_forecast_data(self):
        """Tests that resample_forecast_data correctly resamples forecast data to a different time frequency"""
        # Create a sample visualization dataframe for a specific product with hourly data
        df = create_sample_visualization_dataframe(product='DALMP')
        # Call resample_forecast_data with the dataframe, product, and a frequency (e.g., '6H')
        resampled_df = resample_forecast_data(df, 'DALMP', '6H')
        # Assert that the resulting dataframe has the expected number of rows for the new frequency
        assert len(resampled_df) == 12
        # Assert that the point_forecast values are aggregated correctly (mean)
        assert 'point_forecast' in resampled_df.columns
        # Assert that the lower_bound values are aggregated correctly (min)
        assert 'lower_bound' in resampled_df.columns
        # Assert that the upper_bound values are aggregated correctly (max)
        assert 'upper_bound' in resampled_df.columns

    def test_get_forecast_summary(self):
        """Tests that get_forecast_summary correctly generates a summary of forecast data for display"""
        # Create a sample visualization dataframe for a specific product with known values
        df = create_sample_visualization_dataframe(product='DALMP')
        # Call get_forecast_summary with the dataframe and product
        summary = get_forecast_summary(df, 'DALMP')
        # Assert that the resulting dictionary contains the expected keys (start_time, end_time, avg_price, price_range, is_fallback)
        assert all(key in summary for key in ['start_timestamp', 'end_timestamp', 'avg_price', 'avg_lower', 'avg_upper', 'avg_range', 'is_fallback'])
        # Assert that the summary values match the expected values
        assert isinstance(summary['avg_price'], float)
        assert isinstance(summary['is_fallback'], bool)
        # Assert that the values are formatted appropriately
        assert isinstance(summary['start_formatted'], str)
        assert isinstance(summary['end_formatted'], str)
        assert isinstance(summary['avg_price_formatted'], str)


class TestForecastDataProcessor:
    """Test class for the ForecastDataProcessor class"""

    def test_processor_initialization(self):
        """Tests that the ForecastDataProcessor class correctly initializes"""
        # Create an instance of ForecastDataProcessor
        processor = ForecastDataProcessor()
        # Verify that the instance is not None
        assert processor is not None
        # Verify that the instance has the expected attributes
        assert hasattr(processor, 'logger')

    def test_processor_methods(self):
        """Tests that all expected methods are available on the ForecastDataProcessor instance"""
        # Create an instance of ForecastDataProcessor
        processor = ForecastDataProcessor()
        # Verify that all expected methods are available on the instance
        assert hasattr(processor, 'filter_forecast_by_product')
        assert hasattr(processor, 'filter_forecast_by_date_range')
        assert hasattr(processor, 'prepare_time_series_data')
        assert hasattr(processor, 'prepare_distribution_data')
        assert hasattr(processor, 'prepare_hourly_table_data')
        assert hasattr(processor, 'prepare_comparison_data')
        assert hasattr(processor, 'calculate_statistics')
        assert hasattr(processor, 'identify_price_spikes')
        assert hasattr(processor, 'resample_forecast_data')
        assert hasattr(processor, 'get_forecast_summary')
        # Verify that the methods are callable
        assert callable(processor.filter_forecast_by_product)
        assert callable(processor.filter_forecast_by_date_range)
        assert callable(processor.prepare_time_series_data)
        assert callable(processor.prepare_distribution_data)
        assert callable(processor.prepare_hourly_table_data)
        assert callable(processor.prepare_comparison_data)
        assert callable(processor.calculate_statistics)
        assert callable(processor.identify_price_spikes)
        assert callable(processor.resample_forecast_data)
        assert callable(processor.get_forecast_summary)

    def test_processor_filter_by_product(self):
        """Tests the filter_forecast_by_product method of the ForecastDataProcessor class"""
        # Create an instance of ForecastDataProcessor
        processor = ForecastDataProcessor()
        # Create a multi-product forecast dataframe
        df = create_multi_product_forecast_dataframe(products=['DALMP', 'RTLMP'])
        # Call processor.filter_forecast_by_product with a specific product
        filtered_df = processor.filter_forecast_by_product(df, 'DALMP')
        # Assert that the resulting dataframe only contains rows for the specified product
        assert all(filtered_df['product'] == 'DALMP')

    def test_processor_prepare_time_series_data(self):
        """Tests the prepare_time_series_data method of the ForecastDataProcessor class"""
        # Create an instance of ForecastDataProcessor
        processor = ForecastDataProcessor()
        # Create a sample visualization dataframe
        df = create_sample_visualization_dataframe()
        # Call processor.prepare_time_series_data with the dataframe and product
        prepared_df = processor.prepare_time_series_data(df, 'DALMP')
        # Assert that the resulting dataframe has the expected structure for time series visualization
        assert all(col in prepared_df.columns for col in ['timestamp', 'point_forecast', 'lower_bound', 'upper_bound'])