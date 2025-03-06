"""
Unit tests for the data_exporter module which is responsible for exporting
electricity market price forecasts in various formats (CSV, Excel, JSON)
for download by users. Tests cover all export functions, format validations,
and error handling scenarios.
"""

import pytest  # pytest: 7.0.0+
import pandas as pd  # pandas: 2.0.0+
import numpy as np  # numpy: 1.24.0+
import datetime  # standard library
import io  # standard library
import base64  # standard library
from unittest import mock  # standard library

from src.web.data.data_exporter import (
    export_forecast_by_date_range,
    export_forecast_to_csv,
    export_forecast_to_excel,
    export_forecast_to_json,
    encode_content_for_download,
    generate_export_filename,
    prepare_dataframe_for_export,
    validate_export_format,
    EXPORT_FORMATS,
    DEFAULT_EXPORT_FORMAT,
    DEFAULT_PERCENTILES,
)
from src.web.tests.fixtures.forecast_fixtures import (
    create_sample_visualization_dataframe,
    create_sample_forecast_dataframe,
    create_multi_product_forecast_dataframe,
)
from src.web.config.product_config import PRODUCTS
from src.web.data.forecast_loader import load_forecast_by_date_range


class TestExportFormatValidation:
    """Test class for export format validation functions"""

    def test_validate_export_format_valid(self):
        """Tests that validate_export_format accepts valid export formats"""
        for format in EXPORT_FORMATS:
            # Test with lowercase format strings
            assert validate_export_format(format) == format
            # Test with uppercase format strings
            assert validate_export_format(format.upper()) == format.lower()

    def test_validate_export_format_invalid(self):
        """Tests that validate_export_format returns the default format for invalid inputs"""
        # Test with None
        assert validate_export_format(None) == DEFAULT_EXPORT_FORMAT
        # Test with empty string
        assert validate_export_format("") == DEFAULT_EXPORT_FORMAT
        # Test with non-existent format
        assert validate_export_format("invalid") == DEFAULT_EXPORT_FORMAT


class TestFilenameGeneration:
    """Test class for export filename generation functions"""

    def test_generate_export_filename(self):
        """Tests that generate_export_filename creates correct filenames"""
        # Define test parameters
        product = "DALMP"
        start_date = datetime.date(2023, 1, 1)
        end_date = datetime.date(2023, 1, 2)
        format = "csv"

        # Call generate_export_filename with these parameters
        filename = generate_export_filename(product, format, start_date, end_date)

        # Assert that the filename follows the expected pattern
        assert filename == "forecast_DALMP_2023-01-01_to_2023-01-02.csv"
        # Assert that the filename has the correct extension for the format
        assert filename.endswith(".csv")

        # Test with different products and formats
        filename = generate_export_filename("RTLMP", "excel", start_date, end_date)
        assert filename == "forecast_RTLMP_2023-01-01_to_2023-01-02.xlsx"
        assert filename.endswith(".xlsx")

    def test_generate_export_filename_with_string_dates(self):
        """Tests that generate_export_filename handles string date inputs"""
        # Define test parameters
        product = "DALMP"
        start_date = "2023-01-01"
        end_date = "2023-01-02"
        format = "csv"

        # Call generate_export_filename with these parameters
        filename = generate_export_filename(product, format, start_date, end_date)

        # Assert that the filename follows the expected pattern
        assert filename == "forecast_DALMP_2023-01-01_to_2023-01-02.csv"
        # Assert that the dates are correctly parsed and formatted in the filename
        assert filename.endswith(".csv")


class TestDataframePreparation:
    """Test class for dataframe preparation functions"""

    def test_prepare_dataframe_for_export(self):
        """Tests that prepare_dataframe_for_export correctly formats dataframes for export"""
        # Create a sample visualization dataframe
        df = create_sample_visualization_dataframe()

        # Call prepare_dataframe_for_export with include_samples=True
        export_df = prepare_dataframe_for_export(df, include_samples=True)

        # Assert that the resulting dataframe has the expected columns
        expected_columns = [
            "Forecast Time",
            "Product",
            "Forecast Value",
            "Lower Bound",
            "Upper Bound",
            "Is Fallback Forecast",
        ]
        assert all(col in export_df.columns for col in expected_columns)
        # Assert that datetime columns are formatted correctly
        assert isinstance(export_df["Forecast Time"].iloc[0], str)
        # Assert that unit information is added
        assert "unit" in export_df.columns

        # Call prepare_dataframe_for_export with include_samples=False
        export_df = prepare_dataframe_for_export(df, include_samples=False)

        # Assert that sample columns are removed
        sample_columns = [col for col in export_df.columns if col.startswith("sample_")]
        assert not sample_columns

    def test_prepare_dataframe_for_export_column_renaming(self):
        """Tests that prepare_dataframe_for_export renames columns to user-friendly names"""
        # Create a sample visualization dataframe
        df = create_sample_visualization_dataframe()

        # Call prepare_dataframe_for_export
        export_df = prepare_dataframe_for_export(df)

        # Assert that technical column names are replaced with user-friendly names
        assert "timestamp" not in export_df.columns
        assert "Forecast Time" in export_df.columns
        # Check specific column renames like 'timestamp' to 'Date/Time'
        assert "Date/Time" not in export_df.columns


class TestContentEncoding:
    """Test class for content encoding functions"""

    def test_encode_content_for_download_string(self):
        """Tests that encode_content_for_download correctly encodes string content"""
        # Create a test string content
        content = "test,data,string"

        # Call encode_content_for_download with the string and format='csv'
        encoded_content = encode_content_for_download(content, format="csv")

        # Assert that the result is a base64-encoded string
        assert isinstance(encoded_content, str)
        # Decode the result and verify it matches the original content
        decoded_content = base64.b64decode(encoded_content).decode("utf-8")
        assert decoded_content == content

    def test_encode_content_for_download_bytes(self):
        """Tests that encode_content_for_download correctly encodes bytes content"""
        # Create test bytes content
        content = b"excel data"

        # Call encode_content_for_download with the bytes and format='excel'
        encoded_content = encode_content_for_download(content, format="excel")

        # Assert that the result is a base64-encoded string
        assert isinstance(encoded_content, str)
        # Decode the result and verify it matches the original content
        decoded_content = base64.b64decode(encoded_content)
        assert decoded_content == content


class TestFormatSpecificExports:
    """Test class for format-specific export functions"""

    def test_export_forecast_to_csv(self):
        """Tests that export_forecast_to_csv correctly exports dataframe to CSV"""
        # Create a sample visualization dataframe
        df = create_sample_visualization_dataframe()

        # Call export_forecast_to_csv with include_samples=True
        csv_data = export_forecast_to_csv(df, include_samples=True)

        # Assert that the result is a string in CSV format
        assert isinstance(csv_data, str)
        # Verify that the CSV contains all expected columns
        assert "timestamp,product,point_forecast,lower_bound,upper_bound,is_fallback" in csv_data

        # Call export_forecast_to_csv with include_samples=False
        csv_data = export_forecast_to_csv(df, include_samples=False)

        # Verify that sample columns are excluded from the CSV
        assert "timestamp,product,point_forecast,lower_bound,upper_bound,is_fallback" in csv_data

    def test_export_forecast_to_excel(self):
        """Tests that export_forecast_to_excel correctly exports dataframe to Excel"""
        # Create a sample visualization dataframe
        df = create_sample_visualization_dataframe()

        # Call export_forecast_to_excel with include_samples=True
        excel_data = export_forecast_to_excel(df, include_samples=True)

        # Assert that the result is bytes containing Excel data
        assert isinstance(excel_data, bytes)
        # Use pandas to read the Excel bytes and verify content
        excel_df = pd.read_excel(io.BytesIO(excel_data))
        assert "timestamp" in excel_df.columns
        assert "product" in excel_df.columns

        # Call export_forecast_to_excel with include_samples=False
        excel_data = export_forecast_to_excel(df, include_samples=False)

        # Verify that sample columns are excluded from the Excel file
        excel_df = pd.read_excel(io.BytesIO(excel_data))
        assert "timestamp" in excel_df.columns
        assert "product" in excel_df.columns

    def test_export_forecast_to_json(self):
        """Tests that export_forecast_to_json correctly exports dataframe to JSON"""
        # Create a sample visualization dataframe
        df = create_sample_visualization_dataframe()

        # Call export_forecast_to_json with include_samples=True
        json_data = export_forecast_to_json(df, include_samples=True)

        # Assert that the result is a string in JSON format
        assert isinstance(json_data, str)
        # Parse the JSON and verify it contains all expected data
        json_df = pd.read_json(json_data, orient="records")
        assert "timestamp" in json_df.columns
        assert "product" in json_df.columns

        # Call export_forecast_to_json with include_samples=False
        json_data = export_forecast_to_json(df, include_samples=False)

        # Verify that sample columns are excluded from the JSON
        json_df = pd.read_json(json_data, orient="records")
        assert "timestamp" in json_df.columns
        assert "product" in json_df.columns


class TestMainExportFunction:
    """Test class for the main export_forecast_by_date_range function"""

    @mock.patch("src.web.data.data_exporter.load_forecast_by_date_range")
    def test_export_forecast_by_date_range(self, mock_load_forecast):
        """Tests that export_forecast_by_date_range correctly exports forecast data"""
        # Mock load_forecast_by_date_range to return a sample dataframe
        mock_load_forecast.return_value = create_sample_visualization_dataframe()

        # Define test parameters
        product = "DALMP"
        start_date = "2023-01-01"
        end_date = "2023-01-02"
        format = "csv"

        # Call export_forecast_by_date_range with these parameters
        result = export_forecast_by_date_range(product, start_date, end_date, format)

        # Assert that the result is a dictionary with content, filename, and mime_type
        assert isinstance(result, dict)
        assert "content" in result
        assert "filename" in result
        assert "mime_type" in result
        # Verify that the content is correctly encoded
        assert isinstance(result["content"], str)
        # Verify that the filename follows the expected pattern
        assert result["filename"] == "forecast_DALMP_2023-01-01_to_2023-01-02.csv"
        # Verify that the mime_type matches the export format
        assert result["mime_type"] == "text/csv"

    @mock.patch("src.web.data.data_exporter.load_forecast_by_date_range")
    def test_export_forecast_by_date_range_with_percentiles(self, mock_load_forecast):
        """Tests that export_forecast_by_date_range handles custom percentiles"""
        # Mock load_forecast_by_date_range to return a sample dataframe
        mock_load_forecast.return_value = create_sample_visualization_dataframe()

        # Define test parameters
        product = "DALMP"
        start_date = "2023-01-01"
        end_date = "2023-01-02"
        format = "csv"
        percentiles = [25, 75]

        # Call export_forecast_by_date_range with these parameters
        result = export_forecast_by_date_range(product, start_date, end_date, format, percentiles)

        # Verify that the custom percentiles are used in the export
        assert isinstance(result, dict)

    def test_export_forecast_by_date_range_invalid_product(self):
        """Tests that export_forecast_by_date_range handles invalid product"""
        # Call export_forecast_by_date_range with an invalid product
        with pytest.raises(ValueError) as excinfo:
            export_forecast_by_date_range("INVALID", "2023-01-01", "2023-01-02")

        # Use pytest.raises to verify that ValueError is raised
        assert "Invalid product: INVALID" in str(excinfo.value)
        # Verify the error message mentions the invalid product
        assert "DALMP, RTLMP, RegUp, RegDown, RRS, NSRS" in str(excinfo.value)

    @mock.patch("src.web.data.data_exporter.load_forecast_by_date_range")
    def test_export_forecast_by_date_range_invalid_format(self, mock_load_forecast):
        """Tests that export_forecast_by_date_range handles invalid export format"""
        # Mock load_forecast_by_date_range to return a sample dataframe
        mock_load_forecast.return_value = create_sample_visualization_dataframe()

        # Call export_forecast_by_date_range with an invalid format
        result = export_forecast_by_date_range("DALMP", "2023-01-01", "2023-01-02", "INVALID")

        # Verify that the default format is used instead
        assert result["filename"].endswith(".csv")
        # Check that the mime_type matches the default format
        assert result["mime_type"] == "text/csv"


class TestErrorHandling:
    """Test class for error handling in export functions"""

    def test_export_with_empty_dataframe(self):
        """Tests that export functions handle empty dataframes gracefully"""
        # Create an empty pandas DataFrame
        df = pd.DataFrame()

        # Call export_forecast_to_csv with the empty dataframe
        csv_data = export_forecast_to_csv(df, include_samples=True)

        # Verify that a valid CSV with headers is returned
        assert isinstance(csv_data, str)
        assert "timestamp" in csv_data

        # Call export_forecast_to_excel with the empty dataframe
        excel_data = export_forecast_to_excel(df, include_samples=True)

        # Verify that a valid Excel file with headers is returned
        excel_df = pd.read_excel(io.BytesIO(excel_data))
        assert "timestamp" in excel_df.columns

        # Call export_forecast_to_json with the empty dataframe
        json_data = export_forecast_to_json(df, include_samples=True)

        # Verify that a valid JSON array (empty) is returned
        json_df = pd.read_json(json_data)
        assert json_df.empty

    def test_export_with_missing_data(self):
        """Tests that export functions handle dataframes with missing values"""
        # Create a sample dataframe with some NaN values
        df = create_sample_visualization_dataframe()
        df.loc[0, "point_forecast"] = np.nan
        df.loc[1, "lower_bound"] = np.nan

        # Call export functions with this dataframe
        csv_data = export_forecast_to_csv(df, include_samples=True)
        excel_data = export_forecast_to_excel(df, include_samples=True)
        json_data = export_forecast_to_json(df, include_samples=True)

        # Verify that NaN values are handled appropriately in each format
        assert "NaN" not in csv_data
        excel_df = pd.read_excel(io.BytesIO(excel_data))
        assert not excel_df.isnull().values.any()
        json_df = pd.read_json(json_data)
        assert not json_df.isnull().values.any()

    @mock.patch("src.web.data.data_exporter.load_forecast_by_date_range")
    def test_export_forecast_by_date_range_data_loading_error(self, mock_load_forecast):
        """Tests that export_forecast_by_date_range handles data loading errors"""
        # Mock load_forecast_by_date_range to raise an exception
        mock_load_forecast.side_effect = ValueError("Data loading failed")

        # Call export_forecast_by_date_range with an invalid format
        with pytest.raises(ValueError) as excinfo:
            export_forecast_by_date_range("DALMP", "2023-01-01", "2023-01-02", "csv")

        # Use pytest.raises to verify that the exception is propagated
        assert "Data loading failed" in str(excinfo.value)
        # Verify the error message contains useful information
        assert "Error exporting forecast by date range" not in str(excinfo.value)