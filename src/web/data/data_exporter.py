"""
Module responsible for exporting electricity market price forecast data in various formats 
(CSV, Excel, JSON) for download by users. Provides functions to transform, format, and encode 
forecast data for export from the Dash-based visualization interface.
"""

import pandas as pd  # version 2.0.0+
import numpy as np  # version 1.24.0+
import io  # standard library
import base64  # standard library
import datetime  # standard library
import logging  # standard library
from typing import Dict, List, Optional, Union  # standard library

from .forecast_loader import load_forecast_by_date_range
from .schema import prepare_dataframe_for_visualization, extract_samples_from_dataframe
from .data_processor import filter_forecast_by_product, filter_forecast_by_date_range
from ..config.product_config import PRODUCTS, get_product_unit
from ..utils.formatting import format_datetime

# Set up logger
logger = logging.getLogger(__name__)

# Define export formats with MIME types and file extensions
EXPORT_FORMATS = {
    "csv": {"mime": "text/csv", "extension": ".csv"},
    "excel": {"mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "extension": ".xlsx"},
    "json": {"mime": "application/json", "extension": ".json"}
}

# Default export format if none specified
DEFAULT_EXPORT_FORMAT = "csv"

# Default percentiles to include in exports
DEFAULT_PERCENTILES = [10, 90]


def export_forecast_by_date_range(
    product: str,
    start_date: Union[str, datetime.date, datetime.datetime],
    end_date: Union[str, datetime.date, datetime.datetime],
    export_format: str = DEFAULT_EXPORT_FORMAT,
    percentiles: Optional[List[int]] = None
) -> Dict[str, str]:
    """
    Exports forecast data for a specific date range and product in the requested format.
    
    Args:
        product: Product identifier
        start_date: Start date for the forecast range
        end_date: End date for the forecast range
        export_format: Format to export (csv, excel, json)
        percentiles: Optional list of percentiles to include
        
    Returns:
        Dictionary with encoded content, filename, and MIME type
    """
    try:
        # Validate product
        if product not in PRODUCTS:
            raise ValueError(f"Invalid product: {product}. Must be one of: {', '.join(PRODUCTS)}")
        
        # Validate and normalize export format
        export_format = validate_export_format(export_format)
        
        # Use default percentiles if not provided
        if percentiles is None:
            percentiles = DEFAULT_PERCENTILES
        
        # Load forecast data for the specified date range and product
        df = load_forecast_by_date_range(product, start_date, end_date, percentiles)
        
        # Check if we need additional filtering
        df = filter_forecast_by_product(df, product)
        df = filter_forecast_by_date_range(df, start_date, end_date)
        
        # Determine whether to include samples based on format
        include_samples = export_format == "excel" or export_format == "json"
        
        # Export in the requested format
        if export_format == "csv":
            content = export_forecast_to_csv(df, include_samples=False)  # CSV typically excludes samples
        elif export_format == "excel":
            content = export_forecast_to_excel(df, include_samples=include_samples)
        elif export_format == "json":
            content = export_forecast_to_json(df, include_samples=include_samples)
        else:
            # Should never get here due to validation, but just in case
            raise ValueError(f"Unsupported export format: {export_format}")
        
        # Encode content for download
        encoded_content = encode_content_for_download(content, export_format)
        
        # Generate filename
        filename = generate_export_filename(product, export_format, start_date, end_date)
        
        # Get MIME type
        mime_type = EXPORT_FORMATS[export_format]["mime"]
        
        # Log the export
        logger.info(f"Exported {product} forecast from {start_date} to {end_date} as {export_format}")
        
        # Return dictionary with download information
        return {
            "content": encoded_content,
            "filename": filename,
            "mime_type": mime_type
        }
    
    except Exception as e:
        logger.error(f"Error exporting forecast by date range: {str(e)}")
        raise


def export_forecast_to_csv(df: pd.DataFrame, include_samples: bool) -> str:
    """
    Exports forecast data to CSV format.
    
    Args:
        df: Forecast dataframe
        include_samples: Whether to include sample columns
        
    Returns:
        CSV data as string
    """
    try:
        # Create a copy to avoid modifying the original
        export_df = df.copy()
        
        # Format datetime columns for readability
        if 'timestamp' in export_df.columns:
            export_df['timestamp'] = export_df['timestamp'].apply(
                lambda x: format_datetime(x) if not pd.isna(x) else ""
            )
        
        if 'generation_timestamp' in export_df.columns:
            export_df['generation_timestamp'] = export_df['generation_timestamp'].apply(
                lambda x: format_datetime(x) if not pd.isna(x) else ""
            )
        
        # Remove sample columns if not requested
        if not include_samples:
            sample_columns = [col for col in export_df.columns if col.startswith('sample_')]
            if sample_columns:
                export_df = export_df.drop(columns=sample_columns)
        
        # Convert to CSV
        csv_data = export_df.to_csv(index=False)
        return csv_data
    
    except Exception as e:
        logger.error(f"Error exporting forecast to CSV: {str(e)}")
        raise ValueError(f"Failed to export to CSV: {str(e)}")


def export_forecast_to_excel(df: pd.DataFrame, include_samples: bool) -> bytes:
    """
    Exports forecast data to Excel format.
    
    Args:
        df: Forecast dataframe
        include_samples: Whether to include sample columns
        
    Returns:
        Excel data as bytes
    """
    try:
        # Create a copy to avoid modifying the original
        export_df = df.copy()
        
        # Format datetime columns for readability
        if 'timestamp' in export_df.columns:
            export_df['timestamp'] = export_df['timestamp'].apply(
                lambda x: format_datetime(x) if not pd.isna(x) else ""
            )
        
        if 'generation_timestamp' in export_df.columns:
            export_df['generation_timestamp'] = export_df['generation_timestamp'].apply(
                lambda x: format_datetime(x) if not pd.isna(x) else ""
            )
        
        # Remove sample columns if not requested
        if not include_samples:
            sample_columns = [col for col in export_df.columns if col.startswith('sample_')]
            if sample_columns:
                export_df = export_df.drop(columns=sample_columns)
        
        # Create a buffer to hold the Excel file
        output = io.BytesIO()
        
        # Create Excel writer
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            export_df.to_excel(writer, index=False, sheet_name='Forecast Data')
            
            # Get the worksheet to apply formatting
            worksheet = writer.sheets['Forecast Data']
            
            # Auto-size columns (approximate)
            for i, column in enumerate(export_df.columns):
                column_width = max(len(str(column)), export_df[column].astype(str).map(len).max())
                # Set column width with some extra space
                worksheet.column_dimensions[chr(65 + i)].width = column_width + 2
        
        # Get the Excel data
        output.seek(0)
        excel_data = output.getvalue()
        
        return excel_data
    
    except Exception as e:
        logger.error(f"Error exporting forecast to Excel: {str(e)}")
        raise ValueError(f"Failed to export to Excel: {str(e)}")


def export_forecast_to_json(df: pd.DataFrame, include_samples: bool) -> str:
    """
    Exports forecast data to JSON format.
    
    Args:
        df: Forecast dataframe
        include_samples: Whether to include sample columns
        
    Returns:
        JSON data as string
    """
    try:
        # Create a copy to avoid modifying the original
        export_df = df.copy()
        
        # Format datetime columns for readability
        if 'timestamp' in export_df.columns:
            export_df['timestamp'] = export_df['timestamp'].apply(
                lambda x: format_datetime(x) if not pd.isna(x) else ""
            )
        
        if 'generation_timestamp' in export_df.columns:
            export_df['generation_timestamp'] = export_df['generation_timestamp'].apply(
                lambda x: format_datetime(x) if not pd.isna(x) else ""
            )
        
        # Remove sample columns if not requested
        if not include_samples:
            sample_columns = [col for col in export_df.columns if col.startswith('sample_')]
            if sample_columns:
                export_df = export_df.drop(columns=sample_columns)
        
        # Convert to JSON
        json_data = export_df.to_json(orient='records', date_format='iso')
        return json_data
    
    except Exception as e:
        logger.error(f"Error exporting forecast to JSON: {str(e)}")
        raise ValueError(f"Failed to export to JSON: {str(e)}")


def encode_content_for_download(content: Union[str, bytes], export_format: str) -> str:
    """
    Encodes content for download based on format.
    
    Args:
        content: Content to encode (string for CSV/JSON, bytes for Excel)
        export_format: Export format
        
    Returns:
        Base64 encoded content
    """
    try:
        # If content is already bytes (e.g., Excel), use as is
        if isinstance(content, bytes):
            content_bytes = content
        else:
            # Convert string content to bytes
            content_bytes = content.encode('utf-8')
        
        # Encode as base64
        encoded = base64.b64encode(content_bytes).decode('utf-8')
        return encoded
    
    except Exception as e:
        logger.error(f"Error encoding content for download: {str(e)}")
        raise ValueError(f"Failed to encode content: {str(e)}")


def generate_export_filename(
    product: str,
    export_format: str,
    start_date: Union[str, datetime.date, datetime.datetime],
    end_date: Union[str, datetime.date, datetime.datetime]
) -> str:
    """
    Generates a filename for the exported data.
    
    Args:
        product: Product identifier
        export_format: Export format
        start_date: Start date of the forecast data
        end_date: End date of the forecast data
        
    Returns:
        Generated filename with extension
    """
    try:
        # Format dates as strings if they're not already
        if isinstance(start_date, (datetime.date, datetime.datetime)):
            start_date_str = start_date.strftime('%Y-%m-%d')
        else:
            start_date_str = str(start_date)
        
        if isinstance(end_date, (datetime.date, datetime.datetime)):
            end_date_str = end_date.strftime('%Y-%m-%d')
        else:
            end_date_str = str(end_date)
        
        # Create filename
        filename = f"forecast_{product}_{start_date_str}_to_{end_date_str}"
        
        # Add appropriate extension
        extension = EXPORT_FORMATS[export_format]["extension"]
        filename = f"{filename}{extension}"
        
        return filename
    
    except Exception as e:
        logger.error(f"Error generating export filename: {str(e)}")
        # Return a default filename as fallback
        return f"forecast_export{EXPORT_FORMATS[export_format]['extension']}"


def prepare_dataframe_for_export(df: pd.DataFrame, include_samples: bool) -> pd.DataFrame:
    """
    Prepares a dataframe for export by formatting and cleaning data.
    
    Args:
        df: Forecast dataframe
        include_samples: Whether to include sample columns
        
    Returns:
        Prepared dataframe ready for export
    """
    # Create a copy to avoid modifying the original
    export_df = df.copy()
    
    # Format datetime columns for readability
    if 'timestamp' in export_df.columns:
        export_df['timestamp'] = export_df['timestamp'].apply(
            lambda x: format_datetime(x) if not pd.isna(x) else ""
        )
    
    if 'generation_timestamp' in export_df.columns:
        export_df['generation_timestamp'] = export_df['generation_timestamp'].apply(
            lambda x: format_datetime(x) if not pd.isna(x) else ""
        )
    
    # Add unit information if product column exists
    if 'product' in export_df.columns:
        export_df['unit'] = export_df['product'].apply(get_product_unit)
    
    # Remove sample columns if not requested
    if not include_samples:
        sample_columns = [col for col in export_df.columns if col.startswith('sample_')]
        if sample_columns:
            export_df = export_df.drop(columns=sample_columns)
    
    # Rename columns to more user-friendly names
    column_renames = {
        'point_forecast': 'Forecast Value',
        'lower_bound': 'Lower Bound',
        'upper_bound': 'Upper Bound',
        'timestamp': 'Forecast Time',
        'product': 'Product',
        'is_fallback': 'Is Fallback Forecast',
        'generation_timestamp': 'Generated At'
    }
    
    # Only rename columns that exist in the dataframe
    columns_to_rename = {k: v for k, v in column_renames.items() if k in export_df.columns}
    if columns_to_rename:
        export_df = export_df.rename(columns=columns_to_rename)
    
    # Sort by timestamp and product
    sort_columns = []
    if 'Forecast Time' in export_df.columns:
        sort_columns.append('Forecast Time')
    if 'Product' in export_df.columns:
        sort_columns.append('Product')
    
    if sort_columns:
        export_df = export_df.sort_values(sort_columns)
    
    return export_df


def validate_export_format(export_format: str) -> str:
    """
    Validates that the export format is supported.
    
    Args:
        export_format: Format to validate
        
    Returns:
        Validated export format (or default if invalid)
    """
    if export_format not in EXPORT_FORMATS:
        logger.warning(f"Invalid export format: {export_format}. Using default: {DEFAULT_EXPORT_FORMAT}")
        return DEFAULT_EXPORT_FORMAT
    return export_format