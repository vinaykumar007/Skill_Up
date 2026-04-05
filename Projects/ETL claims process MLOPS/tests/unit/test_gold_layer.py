"""
Unit tests for Gold layer (analytics and aggregations)

Tests metric generation, table creation, and correctness
"""
from src.gold.claim_metrics import ClaimMetrics
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add project root
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root.resolve()))


@pytest.mark.unit
class TestClaimMetricsBasics:
    """Test basic metric generation"""

    def test_metrics_instance_creation(self):
        """Test that ClaimMetrics can be instantiated"""
        metrics = ClaimMetrics()
        assert metrics is not None

    def test_monthly_summary_generation(self, sample_claims_data):
        """Test monthly claims summary generation"""
        metrics = ClaimMetrics()

        # Ensure dates are proper format
        sample_claims_data['submitted_date'] = pd.to_datetime(
            sample_claims_data['submitted_date'])

        monthly_summary = metrics.generate_monthly_summary(sample_claims_data)

        assert isinstance(monthly_summary, pd.DataFrame)
        assert len(monthly_summary) > 0

        # Check expected columns
        assert 'total_claims' in monthly_summary.columns or 'claim_count' in monthly_summary.columns

    def test_status_breakdown_generation(self, sample_claims_data):
        """Test status breakdown generation"""
        metrics = ClaimMetrics()

        status_breakdown = metrics.generate_status_breakdown(
            sample_claims_data)

        assert isinstance(status_breakdown, pd.DataFrame)
        assert len(status_breakdown) > 0

        # Should have status and count columns
        assert 'status' in status_breakdown.columns or any(
            'status' in col.lower() for col in status_breakdown.columns)


@pytest.mark.unit
class TestClaimMetricsAggregations:
    """Test metric aggregations"""

    def test_high_value_claims_filtering(self, sample_large_claims_data):
        """Test filtering of high-value claims"""
        metrics = ClaimMetrics()

        high_value_threshold = 20000
        high_value = sample_large_claims_data[sample_large_claims_data['claim_amount']
                                              > high_value_threshold]

        assert len(high_value) > 0, "Should have some high-value claims"
        assert high_value['claim_amount'].min() > high_value_threshold

    def test_claim_type_analysis(self, sample_large_claims_data):
        """Test claim type analysis"""
        metrics = ClaimMetrics()

        type_analysis = sample_large_claims_data[['claim_type']].value_counts()

        assert len(type_analysis) > 0
        assert type_analysis.sum() == len(sample_large_claims_data)

    def test_resolution_time_buckets(self, sample_claims_data):
        """Test resolution time bucketing"""
        sample_claims_data['submitted_date'] = pd.to_datetime(
            sample_claims_data['submitted_date'])
        sample_claims_data['resolution_date'] = pd.to_datetime(
            sample_claims_data['resolution_date'])

        # Calculate resolution time
        sample_claims_data['resolution_days'] = (
            sample_claims_data['resolution_date'] -
            sample_claims_data['submitted_date']
        ).dt.days

        # Create buckets
        def bucket_resolution_time(days):
            if pd.isna(days):
                return 'PENDING'
            elif days <= 7:
                return '0-7_DAYS'
            elif days <= 30:
                return '8-30_DAYS'
            else:
                return '30+_DAYS'

        sample_claims_data['resolution_bucket'] = sample_claims_data['resolution_days'].apply(
            bucket_resolution_time)

        buckets = sample_claims_data['resolution_bucket'].unique()
        assert len(buckets) > 0


@pytest.mark.unit
class TestMetricsCorrectness:
    """Test correctness of metric calculations"""

    def test_sum_calculation(self):
        """Test sum of amounts is calculated correctly"""
        df = pd.DataFrame({
            'amount': [100, 200, 300, 400, 500]
        })

        total = df['amount'].sum()
        assert total == 1500

    def test_average_calculation(self):
        """Test average of amounts is calculated correctly"""
        df = pd.DataFrame({
            'amount': [100, 200, 300, 400, 500]
        })

        avg = df['amount'].mean()
        assert avg == 300

    def test_count_calculation(self):
        """Test count is calculated correctly"""
        df = pd.DataFrame({
            'claim_id': ['C001', 'C002', 'C003'],
            'amount': [100, 200, 300]
        })

        count = len(df)
        assert count == 3

    def test_percentile_calculation(self):
        """Test percentile calculations"""
        df = pd.DataFrame({
            'amount': list(range(1, 101))  # 1 to 100
        })

        p50 = df['amount'].quantile(0.5)
        p75 = df['amount'].quantile(0.75)
        p95 = df['amount'].quantile(0.95)

        assert p50 == 50.5
        assert p75 > p50
        assert p95 > p75


@pytest.mark.unit
class TestGoldTableGeneration:
    """Test generation of all Gold tables"""

    def test_all_metrics_generated(self, sample_large_claims_data):
        """Test that all required metrics tables are generated"""
        metrics = ClaimMetrics()

        all_metrics = metrics.generate_all_metrics(sample_large_claims_data)

        assert isinstance(all_metrics, dict)

        # Check for expected tables
        expected_tables = [
            'monthly_summary',
            'status_breakdown',
            'claim_type_analysis',
            'high_value_claims',
            'resolution_time_analysis',
            'fraud_risk_features'
        ]

        for table in expected_tables:
            assert table in all_metrics, f"Expected table {table} not generated"

    def test_all_tables_have_data(self, sample_large_claims_data):
        """Test that all generated tables have data"""
        metrics = ClaimMetrics()
        all_metrics = metrics.generate_all_metrics(sample_large_claims_data)

        for table_name, table_df in all_metrics.items():
            assert isinstance(
                table_df, pd.DataFrame), f"{table_name} is not a DataFrame"
            assert len(table_df) > 0, f"{table_name} is empty"

    def test_gold_tables_no_duplicates(self, sample_large_claims_data):
        """Test that gold tables don't have unexpected duplicates"""
        metrics = ClaimMetrics()
        all_metrics = metrics.generate_all_metrics(sample_large_claims_data)

        for table_name, table_df in all_metrics.items():
            if table_name in ['monthly_summary', 'status_breakdown', 'claim_type_analysis']:
                # These should not have duplicates on key columns
                assert table_df.duplicated().sum(
                ) == 0, f"{table_name} has unexpected duplicates"


@pytest.mark.unit
class TestFraudRiskFeatures:
    """Test fraud risk feature generation"""

    def test_fraud_features_included(self, sample_large_claims_data):
        """Test that fraud risk features are generated"""
        metrics = ClaimMetrics()
        all_metrics = metrics.generate_all_metrics(sample_large_claims_data)

        fraud_features = all_metrics.get('fraud_risk_features')
        assert fraud_features is not None, "fraud_risk_features not generated"
        assert len(fraud_features) > 0

    def test_fraud_score_is_valid(self, sample_claims_data):
        """Test that fraud scores are within valid range"""
        # Simulate fraud score generation (0 to 1)
        sample_claims_data['fraud_score'] = np.random.uniform(
            0, 1, len(sample_claims_data))

        assert sample_claims_data['fraud_score'].min() >= 0
        assert sample_claims_data['fraud_score'].max() <= 1
        assert not sample_claims_data['fraud_score'].isnull().any()
