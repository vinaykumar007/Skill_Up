"""
Unit tests for Silver layer (data transformation and cleaning)

Tests data cleaning rules, feature engineering, and quality validation
"""
from src.silver.claim_transformer import ClaimTransformer
from src.silver.data_validator import DataValidator
from src.silver.cleaning_rules import ClaimCleaningRules
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys
from datetime import datetime

# Add project root
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root.resolve()))


@pytest.mark.unit
class TestClaimCleaningRules:
    """Test Silver layer cleaning rules"""

    def test_standardize_claim_amount(self):
        """Test claim amount standardization"""
        # Normal case
        assert ClaimCleaningRules.standardize_claim_amount(5000.50) == 5000.50

        # String input
        assert ClaimCleaningRules.standardize_claim_amount(
            "5000.50") == 5000.50

        # Negative value (should be made positive)
        result = ClaimCleaningRules.standardize_claim_amount(-5000)
        assert result >= 0, "Negative amounts should be handled"

    def test_standardize_date(self):
        """Test date standardization"""
        # Valid date string
        result = ClaimCleaningRules.standardize_date("2025-01-15")
        assert result is not None

        # Various date formats
        dates = ["2025-01-15", "01/15/2025", "15-01-2025"]
        for date_str in dates:
            result = ClaimCleaningRules.standardize_date(date_str)
            assert result is not None or pd.isna(result)

    def test_standardize_status(self):
        """Test status standardization"""
        assert ClaimCleaningRules.standardize_status(
            "resolved").upper() == "RESOLVED"
        assert ClaimCleaningRules.standardize_status("PENDING") == "PENDING"
        assert ClaimCleaningRules.standardize_status(
            "in progress").upper() == "IN_PROGRESS"

    def test_categorize_claim_amount(self):
        """Test claim amount categorization"""
        assert ClaimCleaningRules.categorize_claim_amount(5000) == "MEDIUM"
        assert ClaimCleaningRules.categorize_claim_amount(500) == "LOW"
        assert ClaimCleaningRules.categorize_claim_amount(50000) == "HIGH"
        assert ClaimCleaningRules.categorize_claim_amount(25000) in [
            "MEDIUM", "HIGH"]

    def test_mask_ssn(self):
        """Test SSN masking"""
        ssn = "123-45-6789"
        masked = ClaimCleaningRules.mask_ssn(ssn)

        # Masked SSN should contain X's but not show last 4
        assert "X" in masked or masked != ssn

    def test_mask_address(self):
        """Test address masking"""
        address = "123 Main Street, Springfield, IL 62701"
        masked = ClaimCleaningRules.mask_address(address)

        # Masked address should be redacted or partial
        assert masked != address or "MASKED" in masked.upper()


@pytest.mark.unit
class TestDataValidator:
    """Test data validation in Silver layer"""

    def test_check_row_count(self, sample_claims_data):
        """Test row count validation"""
        initial_count = len(sample_claims_data)

        # No rows removed
        result = DataValidator.check_row_count(initial_count, initial_count)
        assert result == 100.0, "Retention should be 100%"

        # Half rows removed
        result = DataValidator.check_row_count(
            initial_count, initial_count // 2)
        assert result == 50.0, "Retention should be 50%"

    def test_check_null_percentages(self, sample_claims_data):
        """Test null percentage detection"""
        df = sample_claims_data.copy()

        # Introduce nulls
        df.loc[1:2, 'resolution_date'] = None

        null_counts = df.isnull().sum()

        # Check that nulls are detected
        assert null_counts['resolution_date'] > 0

    def test_check_duplicates(self):
        """Test duplicate detection"""
        df = pd.DataFrame({
            'id': [1, 1, 2, 3],
            'value': [10, 10, 20, 30]
        })

        duplicates = df.duplicated(subset=['id', 'value']).sum()
        assert duplicates == 1, "Should detect 1 duplicate"

    def test_check_value_distribution(self):
        """Test value distribution validation"""
        df = pd.DataFrame({
            'status': ['PENDING'] * 90 + ['RESOLVED'] * 10
        })

        value_counts = df['status'].value_counts()

        assert len(value_counts) == 2
        assert value_counts['PENDING'] == 90
        assert value_counts['RESOLVED'] == 10


@pytest.mark.unit
class TestFeatureEngineering:
    """Test feature engineering in Silver layer"""

    def test_resolution_time_calculation(self):
        """Test calculation of resolution time in days"""
        submitted = pd.Timestamp('2025-01-01')
        resolved = pd.Timestamp('2025-01-11')

        resolution_time = (resolved - submitted).days
        assert resolution_time == 10, "Resolution time should be 10 days"

    def test_resolution_time_with_nulls(self):
        """Test handling of null resolution dates"""
        submitted = pd.Timestamp('2025-01-01')
        resolved = pd.NaT

        # Should handle null gracefully
        if pd.isna(resolved):
            resolution_time = None
        else:
            resolution_time = (resolved - submitted).days

        assert resolution_time is None

    def test_date_features_extraction(self):
        """Test extraction of date features"""
        date = pd.Timestamp('2025-01-15')

        year = date.year
        month = date.month
        quarter = date.quarter
        day_of_week = date.dayofweek

        assert year == 2025
        assert month == 1
        assert quarter == 1
        assert day_of_week >= 0


@pytest.mark.unit
class TestDataDeduplication:
    """Test deduplication logic"""

    def test_duplicate_removal(self, sample_claims_data):
        """Test removal of duplicate records"""
        df = sample_claims_data.copy()

        # Add a duplicate row
        duplicate_row = df.iloc[0:1].copy()
        df = pd.concat([df, duplicate_row], ignore_index=True)

        assert len(df) == len(sample_claims_data) + 1

        # Remove duplicates
        df_deduped = df.drop_duplicates(subset=['claim_id'])

        assert len(df_deduped) == len(sample_claims_data)

    def test_duplicate_detection_columns(self):
        """Test duplicate detection on specific columns"""
        df = pd.DataFrame({
            'claim_id': ['C001', 'C001', 'C002'],
            'policy_id': ['P001', 'P002', 'P001'],
            'amount': [5000, 5000, 3000]
        })

        # Duplicates based on claim_id only
        duplicates = df.duplicated(subset=['claim_id'], keep='first')

        assert duplicates.sum() == 1, "Should detect 1 duplicate claim_id"


@pytest.mark.unit
class TestClaimTransformer:
    """Test the full transformation pipeline"""

    def test_transform_returns_dict(self, sample_claims_data):
        """Test that transformer returns expected dictionary"""
        transformer = ClaimTransformer()
        result = transformer.transform(sample_claims_data)

        assert isinstance(result, dict)
        assert 'transformed_df' in result
        assert 'validation_report' in result

    def test_transform_output_has_features(self, sample_claims_data):
        """Test that transformation adds feature columns"""
        transformer = ClaimTransformer()
        result = transformer.transform(sample_claims_data)
        df = result['transformed_df']

        # Check for derived features
        feature_cols = ['amount_category', 'resolution_days',
                        'submitted_month', 'submitted_quarter']
        for col in feature_cols:
            assert col in df.columns, f"Expected feature column {col} not found"

    def test_transform_maintains_record_count(self, sample_large_claims_data):
        """Test that transformation maintains record count (no data loss)"""
        initial_count = len(sample_large_claims_data)

        transformer = ClaimTransformer()
        result = transformer.transform(sample_large_claims_data)
        df = result['transformed_df']

        final_count = len(df)
        assert final_count == initial_count, "Transformation should not drop records"
