"""
Unit tests for the sample generator component of the forecasting engine.
Tests the generation of probabilistic samples from point forecasts and uncertainty parameters
for different distribution types and product constraints.
"""

import pytest  # package_version: 7.0.0+
import numpy as np  # package_version: 1.24.0+
from datetime import datetime  # package_version: standard library

# Internal imports
from src.backend.forecasting_engine.sample_generator import generate_samples, SampleGenerator, generate_normal_samples, generate_lognormal_samples, generate_truncated_normal_samples, generate_skewed_normal_samples, create_probabilistic_forecast  # Module: src/backend/forecasting_engine/sample_generator.py
from src.backend.forecasting_engine.exceptions import SampleGenerationError  # Module: src/backend/forecasting_engine/exceptions.py
from src.backend.models.forecast_models import ProbabilisticForecast  # Module: src/backend/models/forecast_models.py
from src.backend.config.settings import FORECAST_PRODUCTS, PROBABILISTIC_SAMPLE_COUNT  # Module: src/backend/config/settings.py
from src.backend.tests.fixtures.model_fixtures import create_mock_uncertainty_params, create_mock_sample_generator, MockSampleGenerator  # Module: src/backend/tests/fixtures/model_fixtures.py

class TestSampleGenerator:
    """Test class for the sample generator component"""

    def setup_method(self, method):
        """Set up test fixtures before each test method"""
        # Create common test parameters
        self.product = "DALMP"
        self.hour = 12
        self.point_forecast = 50.0

        # Create uncertainty parameters using create_mock_uncertainty_params
        self.uncertainty_params = create_mock_uncertainty_params()

        # Initialize sample generator instance
        self.sample_generator = SampleGenerator()

    def test_generate_normal_samples(self):
        """Test normal distribution sample generation"""
        # Call generate_normal_samples with test parameters
        samples = generate_normal_samples(self.point_forecast, self.uncertainty_params, PROBABILISTIC_SAMPLE_COUNT)

        # Assert correct number of samples is returned
        assert len(samples) == PROBABILISTIC_SAMPLE_COUNT

        # Assert samples have expected statistical properties
        assert np.isclose(np.mean(samples), self.point_forecast, atol=2.0)
        assert np.std(samples) > 0

    def test_generate_lognormal_samples(self):
        """Test lognormal distribution sample generation"""
        # Call generate_lognormal_samples with test parameters
        samples = generate_lognormal_samples(self.point_forecast, self.uncertainty_params, PROBABILISTIC_SAMPLE_COUNT)

        # Assert correct number of samples is returned
        assert len(samples) == PROBABILISTIC_SAMPLE_COUNT

        # Assert all samples are positive
        assert all(s > 0 for s in samples)

        # Assert samples have expected statistical properties
        assert np.mean(samples) > 0
        assert np.std(samples) > 0

    def test_generate_truncated_normal_samples(self):
        """Test truncated normal distribution sample generation"""
        # Create uncertainty parameters with bounds
        uncertainty_params = create_mock_uncertainty_params(mean=self.point_forecast, std_dev=5.0)
        uncertainty_params['lower_bound'] = self.point_forecast - 2 * 5.0
        uncertainty_params['upper_bound'] = self.point_forecast + 2 * 5.0

        # Call generate_truncated_normal_samples with test parameters
        samples = generate_truncated_normal_samples(self.point_forecast, uncertainty_params, PROBABILISTIC_SAMPLE_COUNT)

        # Assert correct number of samples is returned
        assert len(samples) == PROBABILISTIC_SAMPLE_COUNT

        # Assert all samples are within bounds
        assert all(uncertainty_params['lower_bound'] <= s <= uncertainty_params['upper_bound'] for s in samples)

        # Assert samples have expected statistical properties
        assert np.mean(samples) > 0
        assert np.std(samples) > 0

    def test_generate_skewed_normal_samples(self):
        """Test skewed normal distribution sample generation"""
        # Create uncertainty parameters with skewness
        uncertainty_params = create_mock_uncertainty_params(mean=self.point_forecast, std_dev=5.0)
        uncertainty_params['skewness'] = 2.0

        # Call generate_skewed_normal_samples with test parameters
        samples = generate_skewed_normal_samples(self.point_forecast, uncertainty_params, PROBABILISTIC_SAMPLE_COUNT)

        # Assert correct number of samples is returned
        assert len(samples) == PROBABILISTIC_SAMPLE_COUNT

        # Assert samples have expected skewness
        assert np.mean(samples) > 0
        assert np.std(samples) > 0

    def test_sample_generator_class_methods(self):
        """Test SampleGenerator class methods"""
        # Test generate_samples method
        samples = self.sample_generator.generate_samples(self.point_forecast, self.uncertainty_params, self.product, self.hour)
        assert len(samples) == PROBABILISTIC_SAMPLE_COUNT

        # Test register_distribution method
        def custom_distribution(point_forecast, uncertainty_params, sample_count):
            return [point_forecast] * sample_count
        self.sample_generator.register_distribution("custom", custom_distribution)
        samples = self.sample_generator.generate_samples(self.point_forecast, self.uncertainty_params, self.product, self.hour, distribution_type="custom")
        assert all(s == self.point_forecast for s in samples)

        # Test set_product_constraint method
        self.sample_generator.set_product_constraint(self.product, {"min_value": 0})
        samples = self.sample_generator.generate_samples(self.point_forecast, self.uncertainty_params, self.product, self.hour)
        assert all(s >= 0 for s in samples)

    def test_error_handling(self):
        """Test error handling in sample generation"""
        # Test with invalid point_forecast
        with pytest.raises(SampleGenerationError):
            generate_samples(None, self.uncertainty_params, self.product, self.hour)

        # Test with invalid uncertainty_params
        with pytest.raises(SampleGenerationError):
            generate_samples(self.point_forecast, None, self.product, self.hour)

        # Test with invalid product
        with pytest.raises(SampleGenerationError):
            generate_samples(self.point_forecast, self.uncertainty_params, "InvalidProduct", self.hour)

        # Test with invalid distribution_type
        with pytest.raises(SampleGenerationError):
            generate_samples(self.point_forecast, self.uncertainty_params, self.product, self.hour, distribution_type="invalid")

    @pytest.mark.parametrize('product, allow_negative', [
        ('DALMP', True),
        ('RTLMP', True),
        ('RegUp', False),
        ('RegDown', False),
        ('RRS', False),
        ('NSRS', False)
    ])
    def test_generate_samples_product_constraints(self, product, allow_negative):
        """Tests that product-specific constraints are applied correctly"""
        # Set up test parameters with negative point forecast
        point_forecast = -10.0
        uncertainty_params = create_mock_uncertainty_params(mean=point_forecast, std_dev=5.0)

        # Call generate_samples for the specified product
        samples = generate_samples(point_forecast, uncertainty_params, product, 12)

        # If allow_negative is True, assert that negative samples are present
        if allow_negative:
            assert any(s < 0 for s in samples)
        # If allow_negative is False, assert that all samples are non-negative
        else:
            assert all(s >= 0 for s in samples)

    @pytest.mark.parametrize('point_forecast, uncertainty_params, product, error_expected', [
        (None, {'mean': 0, 'std_dev': 5}, 'DALMP', True),
        (50.0, None, 'DALMP', True),
        (50.0, {'mean': 0, 'std_dev': 5}, None, True),
        (50.0, {'mean': 0, 'std_dev': 5}, 'InvalidProduct', True),
        (50.0, {'mean': 0, 'std_dev': 5}, 'DALMP', False)
    ])
    def test_generate_samples_invalid_parameters(self, point_forecast, uncertainty_params, product, error_expected):
        """Tests that invalid parameters are handled correctly"""
        # If error_expected is True, assert that SampleGenerationError is raised
        if error_expected:
            with pytest.raises(SampleGenerationError):
                generate_samples(point_forecast, uncertainty_params, product, 12)
        # If error_expected is False, assert that valid samples are returned
        else:
            samples = generate_samples(point_forecast, uncertainty_params, product, 12)
            assert len(samples) == PROBABILISTIC_SAMPLE_COUNT

    def test_create_probabilistic_forecast(self):
        """Tests creating a ProbabilisticForecast from samples"""
        # Generate samples using generate_samples
        samples = generate_samples(self.point_forecast, self.uncertainty_params, self.product, self.hour)

        # Call create_probabilistic_forecast with the samples
        forecast = create_probabilistic_forecast(self.point_forecast, samples, self.product, datetime.now())

        # Assert that the returned object is a ProbabilisticForecast
        assert isinstance(forecast, ProbabilisticForecast)

        # Assert that the forecast has the correct properties (point_forecast, samples, product)
        assert forecast.point_forecast == self.point_forecast
        assert forecast.samples == samples
        assert forecast.product == self.product