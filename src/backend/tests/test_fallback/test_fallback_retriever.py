"""
Unit tests for the fallback_retriever module, which is responsible for retrieving
previous forecasts to use as fallbacks when current forecast generation fails.
Tests cover the retrieval process, search strategies, validation, and error handling.
"""

import pytest  # pytest: 7.0.0+
import unittest.mock  # standard library
import datetime  # standard library
import pandas as pd  # pandas: 2.0.0+

# Internal imports
from ...fallback.fallback_retriever import (
    retrieve_fallback_forecast,
    find_suitable_fallback,
    validate_fallback_parameters,
    is_forecast_suitable,
    DEFAULT_MAX_SEARCH_DAYS
)
from ...fallback.exceptions import FallbackRetrievalError, NoFallbackAvailableError
from ...fallback.timestamp_adjuster import adjust_timestamps
from ...config.settings import FORECAST_PRODUCTS
from ...utils.date_utils import get_previous_day_date, localize_to_cst
from ..fixtures.forecast_fixtures import create_mock_forecast_data


@pytest.mark.parametrize('product', ['DALMP', 'RTLMP', 'RegUp'])
def test_retrieve_fallback_forecast_success(product, unittest):
    """Tests successful retrieval of a fallback forecast"""
    # Arrange
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    mock_forecast = create_mock_forecast_data(product=product, start_time=target_date)
    source_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    metadata = {'source': 'test'}

    # Mock the find_suitable_fallback function to return a source date and metadata
    with unittest.mock.patch('src.backend.fallback.fallback_retriever.find_suitable_fallback') as mock_find_fallback:
        mock_find_fallback.return_value = source_date, metadata

        # Mock the storage_manager.get_forecast function to return the mock forecast
        with unittest.mock.patch('src.backend.fallback.fallback_retriever.get_forecast') as mock_get_forecast:
            mock_get_forecast.return_value = mock_forecast

            # Mock the adjust_timestamps function to return the mock forecast with adjusted timestamps
            with unittest.mock.patch('src.backend.fallback.fallback_retriever.adjust_timestamps') as mock_adjust_timestamps:
                mock_adjust_timestamps.return_value = mock_forecast

                # Act
                fallback_df = retrieve_fallback_forecast(product, target_date)

                # Assert
                assert fallback_df.equals(mock_forecast)
                mock_find_fallback.assert_called_once_with(product, target_date, DEFAULT_MAX_SEARCH_DAYS)
                mock_get_forecast.assert_called_once_with(source_date, product)
                mock_adjust_timestamps.assert_called_once()


def test_retrieve_fallback_forecast_no_fallback(unittest):
    """Tests behavior when no suitable fallback is available"""
    # Arrange
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))

    # Mock the find_suitable_fallback function to raise NoFallbackAvailableError
    with unittest.mock.patch('src.backend.fallback.fallback_retriever.find_suitable_fallback') as mock_find_fallback:
        mock_find_fallback.side_effect = NoFallbackAvailableError("No fallback", "DALMP", target_date, DEFAULT_MAX_SEARCH_DAYS)

        # Act & Assert
        with pytest.raises(NoFallbackAvailableError):
            retrieve_fallback_forecast("DALMP", target_date)

        mock_find_fallback.assert_called_once_with("DALMP", target_date, DEFAULT_MAX_SEARCH_DAYS)


def test_retrieve_fallback_forecast_retrieval_error(unittest):
    """Tests behavior when forecast retrieval fails"""
    # Arrange
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    source_date = localize_to_cst(datetime.datetime(2023, 1, 1))

    # Mock the find_suitable_fallback function to return a source date and metadata
    with unittest.mock.patch('src.backend.fallback.fallback_retriever.find_suitable_fallback') as mock_find_fallback:
        mock_find_fallback.return_value = source_date, {}

        # Mock the storage_manager.get_forecast function to raise an exception
        with unittest.mock.patch('src.backend.fallback.fallback_retriever.get_forecast') as mock_get_forecast:
            mock_get_forecast.side_effect = Exception("Retrieval failed")

            # Act & Assert
            with pytest.raises(FallbackRetrievalError):
                retrieve_fallback_forecast("DALMP", target_date)

            mock_find_fallback.assert_called_once_with("DALMP", target_date, DEFAULT_MAX_SEARCH_DAYS)
            mock_get_forecast.assert_called_once_with(source_date, "DALMP")


def test_retrieve_fallback_forecast_adjustment_error(unittest):
    """Tests behavior when timestamp adjustment fails"""
    # Arrange
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    source_date = localize_to_cst(datetime.datetime(2023, 1, 1))
    mock_forecast = create_mock_forecast_data(product="DALMP", start_time=target_date)

    # Mock the find_suitable_fallback function to return a source date and metadata
    with unittest.mock.patch('src.backend.fallback.fallback_retriever.find_suitable_fallback') as mock_find_fallback:
        mock_find_fallback.return_value = source_date, {}

        # Mock the storage_manager.get_forecast function to return the mock forecast
        with unittest.mock.patch('src.backend.fallback.fallback_retriever.get_forecast') as mock_get_forecast:
            mock_get_forecast.return_value = mock_forecast

            # Mock the adjust_timestamps function to raise an exception
            with unittest.mock.patch('src.backend.fallback.fallback_retriever.adjust_timestamps') as mock_adjust_timestamps:
                mock_adjust_timestamps.side_effect = Exception("Adjustment failed")

                # Act & Assert
                with pytest.raises(FallbackRetrievalError):
                    retrieve_fallback_forecast("DALMP", target_date)

                mock_find_fallback.assert_called_once_with("DALMP", target_date, DEFAULT_MAX_SEARCH_DAYS)
                mock_get_forecast.assert_called_once_with(source_date, "DALMP")
                mock_adjust_timestamps.assert_called_once()


@pytest.mark.parametrize('product', ['DALMP', 'RTLMP', 'RegUp'])
def test_find_suitable_fallback_success(product, unittest):
    """Tests successful finding of a suitable fallback forecast"""
    # Arrange
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    previous_date = localize_to_cst(datetime.datetime(2023, 1, 1))

    # Mock the storage_manager.check_forecast_availability function to return True for a specific date
    with unittest.mock.patch('src.backend.fallback.fallback_retriever.check_forecast_availability') as mock_check_availability:
        mock_check_availability.return_value = True

        # Mock the storage_manager.get_forecast_info function to return suitable metadata
        with unittest.mock.patch('src.backend.fallback.fallback_retriever.get_forecast_info') as mock_get_info:
            mock_get_info.return_value = {'timestamp': previous_date, 'product': product, 'generation_timestamp': previous_date}

            # Mock the is_forecast_suitable function to return True
            with unittest.mock.patch('src.backend.fallback.fallback_retriever.is_forecast_suitable') as mock_is_suitable:
                mock_is_suitable.return_value = True

                # Act
                source_date, metadata = find_suitable_fallback(product, target_date, DEFAULT_MAX_SEARCH_DAYS)

                # Assert
                assert source_date == previous_date
                assert metadata == {'source_date': '2023-01-01', 'target_date': '2023-01-02', 'fallback_age_days': 1, 'is_cascading_fallback': False, 'original_generation_time': '2023-01-01 00:00:00'}
                mock_check_availability.assert_called_once_with(previous_date, product)
                mock_get_info.assert_called_once_with(previous_date, product)
                mock_is_suitable.assert_called_once()


def test_find_suitable_fallback_no_forecasts(unittest):
    """Tests behavior when no forecasts are available"""
    # Arrange
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))

    # Mock the storage_manager.check_forecast_availability function to always return False
    with unittest.mock.patch('src.backend.fallback.fallback_retriever.check_forecast_availability') as mock_check_availability:
        mock_check_availability.return_value = False

        # Act & Assert
        with pytest.raises(NoFallbackAvailableError):
            find_suitable_fallback("DALMP", target_date, DEFAULT_MAX_SEARCH_DAYS)

        # Verify that check_forecast_availability was called for each day in the search range
        assert mock_check_availability.call_count == DEFAULT_MAX_SEARCH_DAYS


def test_find_suitable_fallback_unsuitable_forecasts(unittest):
    """Tests behavior when forecasts exist but are unsuitable"""
    # Arrange
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))
    previous_date = localize_to_cst(datetime.datetime(2023, 1, 1))

    # Mock the storage_manager.check_forecast_availability function to return True
    with unittest.mock.patch('src.backend.fallback.fallback_retriever.check_forecast_availability') as mock_check_availability:
        mock_check_availability.return_value = True

        # Mock the storage_manager.get_forecast_info function to return metadata
        with unittest.mock.patch('src.backend.fallback.fallback_retriever.get_forecast_info') as mock_get_info:
            mock_get_info.return_value = {'timestamp': previous_date, 'product': "DALMP", 'generation_timestamp': previous_date}

            # Mock the is_forecast_suitable function to always return False
            with unittest.mock.patch('src.backend.fallback.fallback_retriever.is_forecast_suitable') as mock_is_suitable:
                mock_is_suitable.return_value = False

                # Act & Assert
                with pytest.raises(NoFallbackAvailableError):
                    find_suitable_fallback("DALMP", target_date, DEFAULT_MAX_SEARCH_DAYS)

                # Verify that is_forecast_suitable was called for each available forecast
                assert mock_is_suitable.call_count == DEFAULT_MAX_SEARCH_DAYS


@pytest.mark.parametrize('max_days', [1, 3, 5])
def test_find_suitable_fallback_max_search_days(max_days, unittest):
    """Tests that search respects the maximum search days parameter"""
    # Arrange
    target_date = localize_to_cst(datetime.datetime(2023, 1, 2))

    # Mock the storage_manager.check_forecast_availability function to always return False
    with unittest.mock.patch('src.backend.fallback.fallback_retriever.check_forecast_availability') as mock_check_availability:
        mock_check_availability.return_value = False

        # Act & Assert
        with pytest.raises(NoFallbackAvailableError):
            find_suitable_fallback("DALMP", target_date, max_days)

        # Verify that check_forecast_availability was called exactly max_days times
        assert mock_check_availability.call_count == max_days


@pytest.mark.parametrize('product,target_date,expected', [
    ('DALMP', datetime.datetime.now(), True),
    (None, datetime.datetime.now(), False),
    ('DALMP', None, False),
    ('InvalidProduct', datetime.datetime.now(), False)
])
def test_validate_fallback_parameters(product, target_date, expected):
    """Tests validation of fallback parameters"""
    # Act
    result = validate_fallback_parameters(product, target_date)

    # Assert
    assert result == expected


valid_metadata = {'timestamp': datetime.datetime.now(), 'product': 'DALMP', 'generation_timestamp': datetime.datetime.now()}
fallback_metadata = {'timestamp': datetime.datetime.now(), 'product': 'DALMP', 'generation_timestamp': datetime.datetime.now(), 'is_fallback': True}
incomplete_metadata = {'timestamp': datetime.datetime.now(), 'product': 'DALMP'}


@pytest.mark.parametrize('metadata,allow_fallback_cascading,expected', [
    (valid_metadata, True, True),
    (valid_metadata, False, True),
    (fallback_metadata, True, True),
    (fallback_metadata, False, False),
    (incomplete_metadata, True, False)
])
def test_is_forecast_suitable(metadata, allow_fallback_cascading, expected):
    """Tests determination of forecast suitability"""
    # Act
    result = is_forecast_suitable(metadata, allow_fallback_cascading)

    # Assert
    assert result == expected