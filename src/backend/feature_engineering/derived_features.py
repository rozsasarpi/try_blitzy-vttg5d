"""
Implements derived feature creation functionality for the Electricity Market Price Forecasting System.
This module builds on base features to create more complex, calculated features that capture market dynamics 
and relationships between different data sources. It follows a functional programming approach with clear 
separation of concerns.
"""

import pandas as pd
import numpy as np
from typing import List, Optional

# Internal imports
from ..utils.logging_utils import get_logger, log_execution_time
from .exceptions import DerivedFeatureError, MissingFeatureError
from ..config.settings import FORECAST_PRODUCTS

# Logger for this module
logger = get_logger(__name__)

# List of features that must be present in the base features DataFrame
REQUIRED_BASE_FEATURES = ["load_mw", "hour", "day_of_week", "is_weekend"]

# Threshold for handling division by small values in ratios
LOAD_GENERATION_RATIO_THRESHOLD = 0.9


def validate_base_features(df: pd.DataFrame) -> bool:
    """
    Validates that all required base features are present in the DataFrame.
    
    Args:
        df: DataFrame containing base features
        
    Returns:
        True if all required features are present, raises exception otherwise
    
    Raises:
        MissingFeatureError: If any required features are missing
    """
    missing_features = [feature for feature in REQUIRED_BASE_FEATURES if feature not in df.columns]
    
    if missing_features:
        error_msg = f"Missing required base features: {missing_features}"
        logger.error(error_msg)
        raise MissingFeatureError(error_msg, missing_features)
    
    return True


def create_load_generation_ratio(df: pd.DataFrame) -> pd.Series:
    """
    Creates a ratio feature between load and total generation.
    
    Args:
        df: DataFrame containing base features including 'load_mw' and 'total_generation'
        
    Returns:
        Series containing the load/generation ratio
    
    Raises:
        DerivedFeatureError: If required columns are missing
    """
    try:
        if 'load_mw' not in df.columns or 'total_generation' not in df.columns:
            raise DerivedFeatureError(
                "Cannot create load/generation ratio: required columns missing",
                ['load_mw', 'total_generation']
            )
        
        # Calculate ratio while handling division by small values
        ratio = df['load_mw'] / df['total_generation'].where(
            df['total_generation'] > LOAD_GENERATION_RATIO_THRESHOLD, 
            LOAD_GENERATION_RATIO_THRESHOLD
        )
        
        # Cap the ratio at a reasonable maximum to avoid extreme values
        ratio = np.minimum(ratio, 10.0)
        
        logger.info(f"Created load/generation ratio feature with shape {ratio.shape}")
        return ratio
    
    except Exception as e:
        if not isinstance(e, DerivedFeatureError):
            raise DerivedFeatureError(
                f"Error creating load/generation ratio: {str(e)}",
                ['load_mw', 'total_generation'],
                original_exception=e
            )
        raise


def create_price_spread_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates price spread features between different price products.
    
    Args:
        df: DataFrame containing base features including price columns
        
    Returns:
        DataFrame with added price spread features
    
    Raises:
        DerivedFeatureError: If required price columns are missing
    """
    try:
        result_df = df.copy()
        
        # Define pairs of price products to compute spreads
        spread_pairs = [
            ('DALMP', 'RTLMP'),  # Day-ahead vs real-time spread
            ('DALMP', 'RegUp'),  # Energy vs regulation up spread
            ('DALMP', 'RegDown'),  # Energy vs regulation down spread
            ('DALMP', 'RRS'),  # Energy vs responsive reserve spread
            ('DALMP', 'NSRS'),  # Energy vs non-spinning reserve spread
            ('RegUp', 'RegDown')  # Regulation up vs down spread
        ]
        
        created_features = []
        for product1, product2 in spread_pairs:
            col1 = f"{product1.lower()}"
            col2 = f"{product2.lower()}"
            
            if col1 in df.columns and col2 in df.columns:
                spread_name = f"spread_{col1}_{col2}"
                result_df[spread_name] = df[col1] - df[col2]
                created_features.append(spread_name)
        
        if not created_features:
            logger.warning("No price spread features were created - check if price columns exist")
            
        logger.info(f"Created {len(created_features)} price spread features: {created_features}")
        return result_df
    
    except Exception as e:
        if not isinstance(e, DerivedFeatureError):
            raise DerivedFeatureError(
                f"Error creating price spread features: {str(e)}",
                [f"{p.lower()}" for p in FORECAST_PRODUCTS],
                original_exception=e
            )
        raise


def create_volatility_features(df: pd.DataFrame, windows: List[int] = None) -> pd.DataFrame:
    """
    Creates volatility features based on historical price variations.
    
    Args:
        df: DataFrame containing base features including price columns
        windows: List of window sizes in hours for volatility calculation
        
    Returns:
        DataFrame with added volatility features
    
    Raises:
        DerivedFeatureError: If volatility calculation fails
    """
    try:
        # Default windows: 24 hours (1 day), 48 hours (2 days), 168 hours (1 week)
        if windows is None:
            windows = [24, 48, 168]
        
        result_df = df.copy()
        price_columns = [col for col in df.columns if any(product.lower() in col for product in FORECAST_PRODUCTS)]
        
        if not price_columns:
            logger.warning("No price columns found for volatility calculation")
            return result_df
        
        created_features = []
        for col in price_columns:
            for window in windows:
                if len(df) < window:
                    logger.warning(f"DataFrame length {len(df)} is less than window {window}. Skipping.")
                    continue
                
                vol_name = f"volatility_{col}_{window}h"
                # Calculate rolling standard deviation for the specified window
                result_df[vol_name] = df[col].rolling(window=window, min_periods=min(window//2, 1)).std()
                
                # Forward fill NaN values at the beginning of the series
                result_df[vol_name] = result_df[vol_name].fillna(method='ffill')
                
                # If any NaNs remain, fill with the mean of the calculated values
                if result_df[vol_name].isna().any():
                    mean_val = result_df[vol_name].mean()
                    result_df[vol_name] = result_df[vol_name].fillna(mean_val)
                
                created_features.append(vol_name)
        
        logger.info(f"Created {len(created_features)} volatility features with windows {windows}")
        return result_df
    
    except Exception as e:
        if not isinstance(e, DerivedFeatureError):
            raise DerivedFeatureError(
                f"Error creating volatility features: {str(e)}",
                price_columns if 'price_columns' in locals() else [],
                original_exception=e
            )
        raise


def create_renewable_impact_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates features that capture the impact of renewable generation on prices.
    
    Args:
        df: DataFrame containing base features including renewable generation
        
    Returns:
        DataFrame with added renewable impact features
    
    Raises:
        DerivedFeatureError: If renewable generation columns are missing
    """
    try:
        result_df = df.copy()
        
        # Check if renewable generation columns exist
        renewable_cols = ['wind_generation', 'solar_generation']
        missing_cols = [col for col in renewable_cols if col not in df.columns]
        
        if missing_cols:
            logger.warning(f"Missing renewable generation columns: {missing_cols}")
            return result_df
        
        # Calculate total renewable generation
        result_df['total_renewable'] = df['wind_generation'] + df['solar_generation']
        
        # Calculate renewable ratio if total generation exists
        if 'total_generation' in df.columns:
            # Prevent division by zero
            result_df['renewable_ratio'] = result_df['total_renewable'] / df['total_generation'].where(
                df['total_generation'] > LOAD_GENERATION_RATIO_THRESHOLD, 
                LOAD_GENERATION_RATIO_THRESHOLD
            )
            result_df['renewable_ratio'] = np.minimum(result_df['renewable_ratio'], 1.0)
        
        # Calculate renewable to load ratio
        if 'load_mw' in df.columns:
            result_df['renewable_to_load'] = result_df['total_renewable'] / df['load_mw'].where(
                df['load_mw'] > LOAD_GENERATION_RATIO_THRESHOLD, 
                LOAD_GENERATION_RATIO_THRESHOLD
            )
            result_df['renewable_to_load'] = np.minimum(result_df['renewable_to_load'], 1.5)
        
        # Create interaction with hour of day to capture diurnal patterns of renewables
        if 'hour' in df.columns:
            result_df['solar_hour_impact'] = df['solar_generation'] * np.sin(np.pi * df['hour'] / 12)
            result_df['wind_hour_impact'] = df['wind_generation'] * (1 - np.cos(np.pi * df['hour'] / 12))
        
        created_features = [col for col in result_df.columns if col not in df.columns]
        logger.info(f"Created {len(created_features)} renewable impact features: {created_features}")
        
        return result_df
    
    except Exception as e:
        if not isinstance(e, DerivedFeatureError):
            raise DerivedFeatureError(
                f"Error creating renewable impact features: {str(e)}",
                ['wind_generation', 'solar_generation', 'total_generation', 'load_mw'],
                original_exception=e
            )
        raise


def create_temporal_interaction_features(
    df: pd.DataFrame, 
    features_to_interact: List[str] = None
) -> pd.DataFrame:
    """
    Creates interaction features between temporal and other features.
    
    Args:
        df: DataFrame containing base features
        features_to_interact: List of feature names to interact with temporal features
        
    Returns:
        DataFrame with added temporal interaction features
    
    Raises:
        DerivedFeatureError: If interaction creation fails
    """
    try:
        result_df = df.copy()
        
        # Default features to interact with temporal features if none provided
        if features_to_interact is None:
            features_to_interact = [
                'load_mw', 
                'total_generation'
            ]
            
            # Add price columns if they exist
            price_columns = [col for col in df.columns if any(product.lower() in col for product in FORECAST_PRODUCTS)]
            features_to_interact.extend(price_columns)
        
        # Filter to only include features that exist in the DataFrame
        features_to_interact = [f for f in features_to_interact if f in df.columns]
        
        if not features_to_interact:
            logger.warning("No features available for temporal interaction")
            return result_df
        
        created_features = []
        
        # Create hour interactions if hour exists
        if 'hour' in df.columns:
            for feature in features_to_interact:
                interaction_name = f"hour_x_{feature}"
                result_df[interaction_name] = df['hour'] * df[feature]
                created_features.append(interaction_name)
                
                # Create hour of day cyclical interaction (to capture time-of-day patterns)
                sin_interaction = f"hour_sin_x_{feature}"
                cos_interaction = f"hour_cos_x_{feature}"
                
                # Convert hour to cyclical features using sine and cosine
                hour_sin = np.sin(2 * np.pi * df['hour'] / 24)
                hour_cos = np.cos(2 * np.pi * df['hour'] / 24)
                
                result_df[sin_interaction] = hour_sin * df[feature]
                result_df[cos_interaction] = hour_cos * df[feature]
                
                created_features.extend([sin_interaction, cos_interaction])
        
        # Create weekend interactions if is_weekend exists
        if 'is_weekend' in df.columns:
            for feature in features_to_interact:
                interaction_name = f"weekend_x_{feature}"
                result_df[interaction_name] = df['is_weekend'] * df[feature]
                created_features.append(interaction_name)
        
        # Create day of week interactions if day_of_week exists
        if 'day_of_week' in df.columns:
            for feature in features_to_interact:
                interaction_name = f"day_of_week_x_{feature}"
                result_df[interaction_name] = df['day_of_week'] * df[feature]
                created_features.append(interaction_name)
        
        logger.info(f"Created {len(created_features)} temporal interaction features")
        return result_df
    
    except Exception as e:
        if not isinstance(e, DerivedFeatureError):
            raise DerivedFeatureError(
                f"Error creating temporal interaction features: {str(e)}",
                features_to_interact if features_to_interact else ['temporal features'],
                original_exception=e
            )
        raise


@log_execution_time
def create_derived_features(base_features_df: pd.DataFrame) -> pd.DataFrame:
    """
    Main function to create all derived features from base features.
    
    Args:
        base_features_df: DataFrame containing base features
        
    Returns:
        DataFrame containing all derived features
    
    Raises:
        DerivedFeatureError: If derived feature creation fails
    """
    try:
        # Validate base features
        validate_base_features(base_features_df)
        
        # Create a copy to avoid modifying the original
        result_df = base_features_df.copy()
        
        # Add load/generation ratio if possible
        if 'load_mw' in result_df.columns and 'total_generation' in result_df.columns:
            result_df['load_generation_ratio'] = create_load_generation_ratio(result_df)
        
        # Create price spread features
        result_df = create_price_spread_features(result_df)
        
        # Create volatility features
        result_df = create_volatility_features(result_df)
        
        # Create renewable impact features
        result_df = create_renewable_impact_features(result_df)
        
        # Create temporal interaction features
        result_df = create_temporal_interaction_features(result_df)
        
        # Count how many new features were created
        new_features = [col for col in result_df.columns if col not in base_features_df.columns]
        logger.info(f"Created {len(new_features)} derived features")
        
        return result_df
    
    except Exception as e:
        if not isinstance(e, DerivedFeatureError):
            error_msg = f"Error in derived feature creation: {str(e)}"
            logger.error(error_msg)
            raise DerivedFeatureError(error_msg, ['base features'], original_exception=e)
        raise


class DerivedFeatureCreator:
    """
    Class responsible for creating derived features from base features.
    """
    
    def __init__(self, base_features_df: pd.DataFrame):
        """
        Initializes the DerivedFeatureCreator with base features.
        
        Args:
            base_features_df: DataFrame containing base features
        """
        self._base_features_df = base_features_df
        self._derived_features_df = pd.DataFrame()
        self._created_features = []
    
    def create_features(self) -> pd.DataFrame:
        """
        Creates all derived features from the base features.
        
        Returns:
            DataFrame containing derived features
        
        Raises:
            DerivedFeatureError: If derived feature creation fails
        """
        try:
            # Validate base features
            validate_base_features(self._base_features_df)
            
            # Initialize derived features with index matching base features
            self._derived_features_df = pd.DataFrame(index=self._base_features_df.index)
            
            # Create load/generation ratio
            if 'load_mw' in self._base_features_df.columns and 'total_generation' in self._base_features_df.columns:
                self._derived_features_df['load_generation_ratio'] = self.create_load_generation_ratio()
                self._created_features.append('load_generation_ratio')
            
            # Create price spread features
            price_spread_df = self.create_price_spread_features()
            new_columns = [col for col in price_spread_df.columns if col not in self._derived_features_df.columns]
            self._derived_features_df[new_columns] = price_spread_df[new_columns]
            self._created_features.extend(new_columns)
            
            # Create volatility features
            volatility_df = self.create_volatility_features()
            new_columns = [col for col in volatility_df.columns if col not in self._derived_features_df.columns]
            self._derived_features_df[new_columns] = volatility_df[new_columns]
            self._created_features.extend(new_columns)
            
            # Create renewable impact features
            renewable_df = self.create_renewable_impact_features()
            new_columns = [col for col in renewable_df.columns if col not in self._derived_features_df.columns]
            self._derived_features_df[new_columns] = renewable_df[new_columns]
            self._created_features.extend(new_columns)
            
            # Create temporal interaction features
            temporal_df = self.create_temporal_interaction_features()
            new_columns = [col for col in temporal_df.columns if col not in self._derived_features_df.columns]
            self._derived_features_df[new_columns] = temporal_df[new_columns]
            self._created_features.extend(new_columns)
            
            logger.info(f"Created {len(self._created_features)} derived features")
            return self._derived_features_df
        
        except Exception as e:
            if not isinstance(e, DerivedFeatureError):
                error_msg = f"Error in derived feature creation: {str(e)}"
                logger.error(error_msg)
                raise DerivedFeatureError(error_msg, ['base features'], original_exception=e)
            raise
    
    def get_feature_dataframe(self) -> pd.DataFrame:
        """
        Returns the current derived features DataFrame.
        
        Returns:
            Current derived features DataFrame
        
        Raises:
            DerivedFeatureError: If derived feature creation has not been done
        """
        try:
            if self._derived_features_df.empty:
                logger.info("Derived features not yet created, creating now")
                return self.create_features()
            return self._derived_features_df
        
        except Exception as e:
            if not isinstance(e, DerivedFeatureError):
                error_msg = f"Error retrieving derived features: {str(e)}"
                logger.error(error_msg)
                raise DerivedFeatureError(error_msg, [], original_exception=e)
            raise
    
    def get_combined_features(self) -> pd.DataFrame:
        """
        Returns a DataFrame with both base and derived features.
        
        Returns:
            Combined features DataFrame
        
        Raises:
            DerivedFeatureError: If combining features fails
        """
        try:
            derived_features = self.get_feature_dataframe()
            
            # Combine base and derived features
            # First, get a timestamp column if it exists to use as index for joining
            if 'timestamp' in self._base_features_df.columns:
                base_with_index = self._base_features_df.set_index('timestamp')
                derived_with_index = derived_features.set_index(self._base_features_df['timestamp'])
                combined = pd.concat([base_with_index, derived_with_index], axis=1)
                # Reset index to get timestamp back as a column
                combined = combined.reset_index()
            else:
                # If no timestamp column, just concatenate
                combined = pd.concat([self._base_features_df, derived_features], axis=1)
            
            # Remove any duplicate columns
            combined = combined.loc[:, ~combined.columns.duplicated()]
            
            logger.info(f"Combined features DataFrame with shape {combined.shape}")
            return combined
        
        except Exception as e:
            if not isinstance(e, DerivedFeatureError):
                error_msg = f"Error combining base and derived features: {str(e)}"
                logger.error(error_msg)
                raise DerivedFeatureError(error_msg, [], original_exception=e)
            raise
    
    def create_load_generation_ratio(self) -> pd.Series:
        """
        Creates a ratio feature between load and total generation.
        
        Returns:
            Series containing the load/generation ratio
        """
        return create_load_generation_ratio(self._base_features_df)
    
    def create_price_spread_features(self) -> pd.DataFrame:
        """
        Creates price spread features between different price products.
        
        Returns:
            DataFrame with price spread features
        """
        return create_price_spread_features(self._base_features_df)
    
    def create_volatility_features(self, windows: List[int] = None) -> pd.DataFrame:
        """
        Creates volatility features based on historical price variations.
        
        Args:
            windows: List of window sizes in hours for volatility calculation
        
        Returns:
            DataFrame with volatility features
        """
        return create_volatility_features(self._base_features_df, windows)
    
    def create_renewable_impact_features(self) -> pd.DataFrame:
        """
        Creates features that capture the impact of renewable generation on prices.
        
        Returns:
            DataFrame with renewable impact features
        """
        return create_renewable_impact_features(self._base_features_df)
    
    def create_temporal_interaction_features(self, features_to_interact: List[str] = None) -> pd.DataFrame:
        """
        Creates interaction features between temporal and other features.
        
        Args:
            features_to_interact: List of feature names to interact with temporal features
        
        Returns:
            DataFrame with temporal interaction features
        """
        return create_temporal_interaction_features(self._base_features_df, features_to_interact)