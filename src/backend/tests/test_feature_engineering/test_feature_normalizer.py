"""
Unit tests for the feature normalizer component of the Electricity Market Price Forecasting System.
Tests the functionality for normalizing feature values using different methods, persistence of normalizers, and error handling.
"""

import pytest  # pytest: 7.0.0+
import pandas as pd  # pandas: 2.0.0+
import numpy  # numpy: 1.24.0+
import os  # standard library
import tempfile  # standard library
from datetime import datetime  # standard library

# Internal imports
from src.backend.feature_engineering.feature_normalizer import FeatureNormalizer  # Class for feature normalization with persistence capabilities
from src.backend.feature_engineering.feature_normalizer import normalize_features  # Function to normalize features using specified method
from src.backend.feature_engineering.feature_normalizer import get_normalizer_path  # Function to get the file path for a normalizer based on its ID
from src.backend.feature_engineering.feature_normalizer import NORMALIZATION_METHODS  # Dictionary mapping method names to scaler classes
from src.backend.feature_engineering.exceptions import FeatureNormalizationError  # Exception for feature normalization failures
from src.backend.tests.fixtures.feature_fixtures import create_mock_feature_data  # Create mock feature data for testing

NUMERIC_COLUMNS = ['load_mw', 'wind_generation', 'solar_generation', 'price']


def create_test_dataframe() -> pd.DataFrame:
    """
    Create a test DataFrame with numeric columns for normalization testing

    Returns:
        pandas.DataFrame: DataFrame with numeric columns for testing
    """
    # Create a DataFrame with numeric columns and some outliers
    df = pd.DataFrame({
        'load_mw': [30000, 35000, 40000, 45000, 50000, 60000, 70000, 100000],
        'wind_generation': [1000, 1200, 1500, 1800, 2000, 2500, 3000, 5000],
        'solar_generation': [0, 100, 300, 500, 700, 900, 1100, 1500],
        'price': [25, 30, 35, 40, 45, 50, 55, 100],
        'non_numeric': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
    })

    # Include columns for load, generation, and price data
    return df


class TestFeatureNormalizer:
    """Test class for the FeatureNormalizer component"""

    def __init__(self):
        """Initialize the test class"""
        pass

    def test_init(self):
        """Test initialization of FeatureNormalizer with different methods"""
        # Create normalizers with different methods (standard, minmax, robust)
        normalizer_standard = FeatureNormalizer(method='standard')
        normalizer_minmax = FeatureNormalizer(method='minmax')
        normalizer_robust = FeatureNormalizer(method='robust')

        # Verify method attribute is set correctly
        assert normalizer_standard.method == 'standard'
        assert normalizer_minmax.method == 'minmax'
        assert normalizer_robust.method == 'robust'

        # Verify scalers dictionary is initialized empty
        assert normalizer_standard.scalers == {}
        assert normalizer_minmax.scalers == {}
        assert normalizer_robust.scalers == {}

        # Verify feature_stats dictionary is initialized empty
        assert normalizer_standard.feature_stats == {}
        assert normalizer_minmax.feature_stats == {}
        assert normalizer_robust.feature_stats == {}

        # Test initialization with invalid method raises FeatureNormalizationError
        with pytest.raises(ValueError):
            FeatureNormalizer(method='invalid')

    def test_fit(self):
        """Test fitting normalizers on feature data"""
        # Create test DataFrame with numeric columns
        df = create_test_dataframe()

        # Create normalizer with standard method
        normalizer = FeatureNormalizer(method='standard')

        # Fit normalizer on the DataFrame
        normalizer.fit(df)

        # Verify scalers are created for each column
        for col in NUMERIC_COLUMNS:
            assert col in normalizer.scalers

        # Verify feature_stats contains statistics for each column
        for col in NUMERIC_COLUMNS:
            assert col in normalizer.feature_stats
            assert 'mean' in normalizer.feature_stats[col]
            assert 'std' in normalizer.feature_stats[col]
            assert 'min' in normalizer.feature_stats[col]
            assert 'max' in normalizer.feature_stats[col]

        # Test fit with specific columns subset
        normalizer = FeatureNormalizer(method='standard')
        normalizer.fit(df, columns=['load_mw', 'wind_generation'])
        assert 'load_mw' in normalizer.scalers
        assert 'wind_generation' in normalizer.scalers
        assert 'solar_generation' not in normalizer.scalers
        assert 'price' not in normalizer.scalers

        # Verify method chaining works (fit returns self)
        assert normalizer.fit(df) == normalizer

    def test_transform(self):
        """Test transforming data with fitted normalizers"""
        # Create test DataFrame with numeric columns
        df = create_test_dataframe()

        # Create and fit normalizer on the DataFrame
        normalizer = FeatureNormalizer(method='standard')
        normalizer.fit(df)

        # Transform the DataFrame
        transformed_df = normalizer.transform(df)

        # Verify transformed values have expected properties (mean near 0, std near 1 for standard)
        for col in NUMERIC_COLUMNS:
            assert transformed_df[col].mean() == pytest.approx(0, abs=0.1)
            assert transformed_df[col].std() == pytest.approx(1, abs=0.1)

        # Verify original DataFrame is not modified
        assert not transformed_df.equals(df)

        # Test transform with unfitted normalizer raises FeatureNormalizationError
        normalizer = FeatureNormalizer(method='standard')
        with pytest.raises(FeatureNormalizationError):
            normalizer.transform(df)

    def test_fit_transform(self):
        """Test combined fit and transform operation"""
        # Create test DataFrame with numeric columns
        df = create_test_dataframe()

        # Create normalizer and call fit_transform
        normalizer = FeatureNormalizer(method='standard')
        transformed_df = normalizer.fit_transform(df)

        # Verify scalers are created for each column
        for col in NUMERIC_COLUMNS:
            assert col in normalizer.scalers

        # Verify transformed values have expected properties
        for col in NUMERIC_COLUMNS:
            assert transformed_df[col].mean() == pytest.approx(0, abs=0.1)
            assert transformed_df[col].std() == pytest.approx(1, abs=0.1)

        # Verify original DataFrame is not modified
        assert not transformed_df.equals(df)

    def test_inverse_transform(self):
        """Test inverse transformation back to original scale"""
        # Create test DataFrame with numeric columns
        df = create_test_dataframe()

        # Create normalizer and fit_transform the data
        normalizer = FeatureNormalizer(method='standard')
        transformed_df = normalizer.fit_transform(df)

        # Apply inverse_transform to the normalized data
        original_df = normalizer.inverse_transform(transformed_df)

        # Verify the inverse-transformed data is close to original data
        for col in NUMERIC_COLUMNS:
            assert numpy.allclose(original_df[col], df[col])

        # Test with different normalization methods
        methods = ['minmax', 'robust']
        for method in methods:
            normalizer = FeatureNormalizer(method=method)
            transformed_df = normalizer.fit_transform(df)
            original_df = normalizer.inverse_transform(transformed_df)
            for col in NUMERIC_COLUMNS:
                assert numpy.allclose(original_df[col], df[col])

        # Test inverse_transform with unfitted normalizer raises FeatureNormalizationError
        normalizer = FeatureNormalizer(method='standard')
        with pytest.raises(FeatureNormalizationError):
            normalizer.inverse_transform(df)

    def test_get_feature_stats(self):
        """Test retrieval of feature statistics"""
        # Create test DataFrame with numeric columns
        df = create_test_dataframe()

        # Create normalizer and fit on the data
        normalizer = FeatureNormalizer(method='standard')
        normalizer.fit(df)

        # Call get_feature_stats method
        stats = normalizer.get_feature_stats()

        # Verify statistics include mean, std, min, max for each column
        for col in NUMERIC_COLUMNS:
            assert col in stats
            assert 'mean' in stats[col]
            assert 'std' in stats[col]
            assert 'min' in stats[col]
            assert 'max' in stats[col]

        # Verify statistics values match expected values from the data
        for col in NUMERIC_COLUMNS:
            assert stats[col]['mean'] == pytest.approx(df[col].mean())
            assert stats[col]['std'] == pytest.approx(df[col].std())
            assert stats[col]['min'] == pytest.approx(df[col].min())
            assert stats[col]['max'] == pytest.approx(df[col].max())

    def test_save_load_scalers(self):
        """Test persistence of normalizers through save and load"""
        # Create test DataFrame with numeric columns
        df = create_test_dataframe()

        # Create normalizer with ID and fit on the data
        normalizer_id = 'test_normalizer'
        normalizer = FeatureNormalizer(method='standard', normalizer_id=normalizer_id)
        normalizer.fit(df)

        # Save the normalizer to disk
        save_successful = normalizer.save_scalers()
        assert save_successful is True

        # Create a new normalizer instance
        new_normalizer = FeatureNormalizer(method='standard', normalizer_id=normalizer_id)

        # Load the saved normalizer
        load_successful = new_normalizer.load_scalers()
        assert load_successful is True

        # Verify loaded normalizer has same method, scalers, and feature_stats
        assert new_normalizer.method == normalizer.method
        assert new_normalizer.scalers.keys() == normalizer.scalers.keys()
        assert new_normalizer.feature_stats.keys() == normalizer.feature_stats.keys()

        # Transform data with both normalizers and verify results match
        transformed_df = normalizer.transform(df)
        new_transformed_df = new_normalizer.transform(df)
        for col in NUMERIC_COLUMNS:
            assert numpy.allclose(transformed_df[col], new_transformed_df[col])

        # Test save/load with no normalizer_id raises ValueError
        normalizer = FeatureNormalizer(method='standard')
        with pytest.raises(ValueError):
            normalizer.save_scalers()
        with pytest.raises(ValueError):
            normalizer.load_scalers()

        # Test load with non-existent normalizer_id returns False
        normalizer = FeatureNormalizer(method='standard', normalizer_id='non_existent')
        load_successful = normalizer.load_scalers()
        assert load_successful is False

    def test_normalize_features_function(self):
        """Test the normalize_features function"""
        # Create test DataFrame with numeric columns
        df = create_test_dataframe()

        # Call normalize_features with different methods
        transformed_df = normalize_features(df, method='standard')

        # Verify normalized data has expected properties
        for col in NUMERIC_COLUMNS:
            assert transformed_df[col].mean() == pytest.approx(0, abs=0.1)
            assert transformed_df[col].std() == pytest.approx(1, abs=0.1)

        # Test with fit=True and fit=False
        transformed_df_fit = normalize_features(df, method='standard', fit=True)
        transformed_df_no_fit = normalize_features(df, method='standard', fit=False, normalizer_id='test_normalizer')
        for col in NUMERIC_COLUMNS:
            assert transformed_df_fit[col].mean() == pytest.approx(0, abs=0.1)
            assert transformed_df_fit[col].std() == pytest.approx(1, abs=0.1)
            assert transformed_df_no_fit[col].mean() == pytest.approx(0, abs=0.1)
            assert transformed_df_no_fit[col].std() == pytest.approx(1, abs=0.1)

        # Test with specific columns subset
        transformed_df = normalize_features(df, method='standard', columns=['load_mw', 'wind_generation'])
        assert 'load_mw' in transformed_df.columns
        assert 'wind_generation' in transformed_df.columns
        assert 'solar_generation' in transformed_df.columns
        assert 'price' in transformed_df.columns

        # Test with normalizer_id for persistence
        normalizer_id = 'test_normalizer'
        transformed_df = normalize_features(df, method='standard', normalizer_id=normalizer_id)
        new_transformed_df = normalize_features(df, method='standard', normalizer_id=normalizer_id, fit=False)
        for col in NUMERIC_COLUMNS:
            assert numpy.allclose(transformed_df[col], new_transformed_df[col])

        # Test with invalid method raises FeatureNormalizationError
        with pytest.raises(ValueError):
            normalize_features(df, method='invalid')

    def test_get_normalizer_path(self):
        """Test the get_normalizer_path function"""
        # Call get_normalizer_path with different normalizer IDs
        normalizer_id = 'test_normalizer'
        path = get_normalizer_path(normalizer_id)

        # Verify path includes the normalizer ID
        assert normalizer_id in str(path)

        # Verify path is in the correct directory
        assert 'normalizers' in str(path)

        # Verify directory is created if it doesn't exist
        assert os.path.exists(path.parent)

    def test_none_normalization(self):
        """Test the 'none' normalization method which returns data unchanged"""
        # Create test DataFrame with numeric columns
        df = create_test_dataframe()

        # Create normalizer with 'none' method
        normalizer = FeatureNormalizer(method='none')

        # Fit and transform the data
        transformed_df = normalizer.fit_transform(df)

        # Verify transformed data is identical to original data
        assert transformed_df.equals(df)

        # Test normalize_features function with method='none'
        transformed_df = normalize_features(df, method='none')
        assert transformed_df.equals(df)

    def test_different_normalization_methods(self):
        """Test different normalization methods and compare results"""
        # Create test DataFrame with numeric columns and outliers
        df = create_test_dataframe()

        # Apply standard, minmax, and robust normalization
        standard_df = normalize_features(df, method='standard')
        minmax_df = normalize_features(df, method='minmax')
        robust_df = normalize_features(df, method='robust')

        # Compare results between methods
        # Verify standard is sensitive to outliers
        assert standard_df['load_mw'].max() > 2

        # Verify robust is less sensitive to outliers
        assert robust_df['load_mw'].max() < 2

        # Verify minmax scales to [0,1] range
        assert minmax_df['load_mw'].min() == 0
        assert minmax_df['load_mw'].max() == 1

    def test_error_handling(self):
        """Test error handling in the normalizer component"""
        # Test with invalid input types
        with pytest.raises(TypeError):
            FeatureNormalizer(method='standard').fit(123)

        # Test with empty DataFrame
        df = pd.DataFrame()
        with pytest.raises(FeatureNormalizationError):
            FeatureNormalizer(method='standard').fit(df)

        # Test with non-numeric columns
        df = pd.DataFrame({'a': ['x', 'y', 'z']})
        with pytest.raises(FeatureNormalizationError):
            FeatureNormalizer(method='standard').fit(df)

        # Test with NaN values
        df = pd.DataFrame({'a': [1, 2, numpy.nan]})
        normalizer = FeatureNormalizer(method='standard')
        normalizer.fit(df)
        transformed_df = normalizer.transform(df)
        assert not transformed_df.isnull().values.any()

        # Verify appropriate FeatureNormalizationError is raised with informative message
        with pytest.raises(FeatureNormalizationError) as exc_info:
            FeatureNormalizer(method='standard').transform(df)
        assert "Error transforming data" in str(exc_info.value)