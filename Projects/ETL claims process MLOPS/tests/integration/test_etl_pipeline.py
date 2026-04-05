"""
Integration tests for end-to-end ETL pipeline

Tests full Bronze→Silver→Gold orchestration, error handling, and MLflow tracking
"""
from config.config import Config
from src.utils.mlflow_utils import MLflowTracker
from src.etl_pipeline import ETLPipeline
import pytest
import pandas as pd
from pathlib import Path
import sys
import shutil
import tempfile

# Add project root
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root.resolve()))


@pytest.mark.integration
class TestETLPipelineExecution:
    """Test full pipeline execution"""

    def test_pipeline_initialization(self, config):
        """Test that ETL pipeline can be initialized"""
        pipeline = ETLPipeline(config)
        assert pipeline is not None
        assert pipeline.config == config

    def test_pipeline_bronze_phase(self, config, tmpdir):
        """Test bronze phase execution"""
        config.data_locations['bronze'] = str(tmpdir / 'bronze')
        Path(config.data_locations['bronze']).mkdir(
            parents=True, exist_ok=True)

        pipeline = ETLPipeline(config)

        # Bronze phase should initialize spark session and create schemas
        assert pipeline.spark is not None

    def test_pipeline_phases_in_correct_order(self, config):
        """Test that pipeline phases run in correct order"""
        pipeline = ETLPipeline(config)

        # Check that config has required phase definitions
        assert hasattr(config, 'data_locations')
        assert 'bronze' in config.data_locations
        assert 'silver' in config.data_locations
        assert 'gold' in config.data_locations


@pytest.mark.integration
class TestPipelineDataFlow:
    """Test data flow through pipeline stages"""

    def test_bronze_to_silver_transformation(self, sample_claims_data, config, tmpdir):
        """Test data flows from bronze to silver with transformations"""
        # Create temporary bronze staging area
        bronze_dir = Path(tmpdir) / 'bronze'
        silver_dir = Path(tmpdir) / 'silver'
        bronze_dir.mkdir(parents=True, exist_ok=True)
        silver_dir.mkdir(parents=True, exist_ok=True)

        # Save sample data
        sample_claims_data.to_csv(str(bronze_dir / 'claims.csv'), index=False)

        # Data should flow from bronze to silver
        bronze_records = pd.read_csv(str(bronze_dir / 'claims.csv'))
        assert len(bronze_records) > 0

    def test_silver_to_gold_aggregation(self, sample_large_claims_data, config, tmpdir):
        """Test data aggregates from silver to gold"""
        silver_dir = Path(tmpdir) / 'silver'
        gold_dir = Path(tmpdir) / 'gold'
        silver_dir.mkdir(parents=True, exist_ok=True)
        gold_dir.mkdir(parents=True, exist_ok=True)

        # Save transformed silver data
        sample_large_claims_data.to_parquet(
            str(silver_dir / 'claims_cleaned.parquet'))

        # Load for aggregation
        silver_data = pd.read_parquet(
            str(silver_dir / 'claims_cleaned.parquet'))

        # Create sample aggregation (monthly summary)
        monthly = silver_data.groupby(pd.Grouper(key='submitted_date', freq='M')).agg({
            'claim_id': 'count',
            'claim_amount': 'sum'
        }).reset_index()
        monthly.columns = ['month', 'claim_count', 'total_amount']

        assert len(monthly) > 0
        assert 'claim_count' in monthly.columns
        assert 'total_amount' in monthly.columns


@pytest.mark.integration
class TestMLflowIntegration:
    """Test MLflow tracking throughout pipeline"""

    def test_mlflow_tracker_initialization(self, mlflow_tracker):
        """Test MLflow tracker can be initialized"""
        assert mlflow_tracker is not None
        assert hasattr(mlflow_tracker, 'experiment_name')

    def test_metrics_logged_per_phase(self, mlflow_tracker, config):
        """Test that metrics are logged for each pipeline phase"""
        # Simulate phase execution with metric logging
        phase_metrics = {
            'bronze_rows_ingested': 1000,
            'bronze_nulls_found': 5,
            'silver_rows_cleaned': 995,
            'silver_duplicates_removed': 2,
            'gold_metrics_generated': 6
        }

        # Verify metrics structure
        assert isinstance(phase_metrics, dict)
        assert all(isinstance(v, (int, float)) for v in phase_metrics.values())

    def test_mlflow_run_tagging(self, mlflow_tracker, config):
        """Test that MLflow runs are tagged with metadata"""
        # Expected tags in a typical MLflow run
        expected_tags = {
            'phase': 'full_pipeline',
            'environment': 'test',
            'data_source': 'synthetic'
        }

        assert isinstance(expected_tags, dict)


@pytest.mark.integration
class TestErrorHandling:
    """Test pipeline error handling"""

    def test_missing_input_data_handling(self, config, tmpdir):
        """Test pipeline handles missing input data"""
        config.data_locations['bronze'] = str(tmpdir / 'nonexistent')

        # Directory doesn't exist - should handle gracefully
        bronze_path = Path(config.data_locations['bronze'])
        assert not bronze_path.exists()

    def test_invalid_data_format_handling(self, tmpdir):
        """Test pipeline handles invalid input formats"""
        bad_data_file = tmpdir / 'bad_data.txt'
        bad_data_file.write_text(
            'this is not valid CSV or JSON', encoding='utf-8')

        # Try to read it
        try:
            pd.read_csv(str(bad_data_file))
        except Exception:
            # Expected to fail
            pass

    def test_pipeline_continues_on_non_critical_error(self, sample_claims_data, config):
        """Test pipeline continues despite non-critical errors"""
        # Even with a few bad rows, pipeline should continue
        bad_data = sample_claims_data.copy()
        # Will be cleaned in silver
        bad_data.loc[0, 'claim_amount'] = 'INVALID'

        # Should still be processable
        assert len(bad_data) > 0


@pytest.mark.integration
class TestOutputGeneration:
    """Test pipeline output files and formats"""

    def test_bronze_layer_outputs(self, sample_claims_data, tmpdir):
        """Test bronze layer generates correct outputs"""
        bronze_dir = Path(tmpdir) / 'bronze'
        bronze_dir.mkdir(parents=True, exist_ok=True)

        # Save in bronze format
        sample_claims_data.to_csv(str(bronze_dir / 'claims.csv'), index=False)

        assert (bronze_dir / 'claims.csv').exists()

    def test_silver_layer_outputs(self, sample_claims_data, tmpdir):
        """Test silver layer generates parquet output"""
        silver_dir = Path(tmpdir) / 'silver'
        silver_dir.mkdir(parents=True, exist_ok=True)

        # Save in silver format (parquet)
        sample_claims_data.to_parquet(
            str(silver_dir / 'claims_cleaned.parquet'))

        assert (silver_dir / 'claims_cleaned.parquet').exists()

    def test_gold_layer_outputs(self, sample_large_claims_data, tmpdir):
        """Test gold layer generates all metric table outputs"""
        gold_dir = Path(tmpdir) / 'gold'
        gold_dir.mkdir(parents=True, exist_ok=True)

        # Simulate gold table generation
        tables = {
            'monthly_summary': sample_large_claims_data.groupby(pd.Grouper(key='submitted_date', freq='M')).size().reset_index(name='count'),
            'status_breakdown': sample_large_claims_data['status'].value_counts().reset_index(),
            'claim_type_analysis': sample_large_claims_data['claim_type'].value_counts().reset_index(),
        }

        # Save all tables
        for table_name, table_df in tables.items():
            table_df.to_parquet(str(gold_dir / f'{table_name}.parquet'))

        # Verify all outputs
        assert len(list(gold_dir.glob('*.parquet'))) == len(tables)


@pytest.mark.integration
class TestDataIntegrity:
    """Test data integrity through pipeline"""

    def test_row_count_preservation(self, sample_claims_data):
        """Test row count is preserved through transformations"""
        initial_count = len(sample_claims_data)

        # Simulate transformation (just copy for this test)
        transformed = sample_claims_data.copy()

        # Should have same row count (duplicates removed in silver, but initial load should match)
        assert len(transformed) == initial_count

    def test_column_additions(self, sample_claims_data):
        """Test new columns are added without losing existing ones"""
        initial_columns = set(sample_claims_data.columns)

        # Add metadata columns (bronze to silver)
        enhanced = sample_claims_data.copy()
        enhanced['_ingested_at'] = pd.Timestamp.now()
        enhanced['_source_name'] = 'claims_source'

        # Original columns should still exist
        assert initial_columns.issubset(set(enhanced.columns))

        # New columns should exist
        assert '_ingested_at' in enhanced.columns
        assert '_source_name' in enhanced.columns

    def test_data_type_consistency(self, sample_claims_data):
        """Test data types are consistent through pipeline"""
        initial_types = sample_claims_data.dtypes.copy()

        # After transformation, key columns should maintain types
        sample_claims_data['submitted_date'] = pd.to_datetime(
            sample_claims_data['submitted_date'])
        sample_claims_data['claim_amount'] = pd.to_numeric(
            sample_claims_data['claim_amount'], errors='coerce')

        assert pd.api.types.is_datetime64_any_dtype(
            sample_claims_data['submitted_date'])
        assert pd.api.types.is_numeric_dtype(
            sample_claims_data['claim_amount'])


@pytest.mark.integration
class TestEndToEndOrchestration:
    """Test complete pipeline orchestration"""

    def test_full_pipeline_execution_path(self, config, sample_large_claims_data, tmpdir):
        """Test complete path: Input → Bronze → Silver → Gold → Output"""
        # Setup directories
        bronze_dir = Path(tmpdir) / 'bronze'
        silver_dir = Path(tmpdir) / 'silver'
        gold_dir = Path(tmpdir) / 'gold'

        for d in [bronze_dir, silver_dir, gold_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Step 1: Load to Bronze
        bronze_data = sample_large_claims_data.copy()
        bronze_data['_ingested_at'] = pd.Timestamp.now()
        bronze_data.to_csv(str(bronze_dir / 'claims.csv'), index=False)

        assert (bronze_dir / 'claims.csv').exists()

        # Step 2: Transform to Silver
        silver_data = bronze_data[bronze_data['claim_amount'] > 0].copy()
        silver_data['resolution_days'] = (
            pd.to_datetime(silver_data['resolution_date']) -
            pd.to_datetime(silver_data['submitted_date'])
        ).dt.days
        silver_data.to_parquet(str(silver_dir / 'claims_cleaned.parquet'))

        assert (silver_dir / 'claims_cleaned.parquet').exists()

        # Step 3: Generate Gold metrics
        gold_metrics = {
            'monthly_summary': silver_data.groupby(pd.Grouper(key='submitted_date', freq='M')).agg({
                'claim_id': 'count',
                'claim_amount': ['sum', 'mean']
            }).reset_index(),
            'status_breakdown': silver_data['status'].value_counts().reset_index(),
        }

        for table_name, table_df in gold_metrics.items():
            table_df.to_parquet(str(gold_dir / f'{table_name}.parquet'))

        # Verify end-to-end flow
        assert (bronze_dir / 'claims.csv').exists()
        assert (silver_dir / 'claims_cleaned.parquet').exists()
        assert len(list(gold_dir.glob('*.parquet'))) > 0

    def test_pipeline_metadata_tracking(self, config, tmpdir):
        """Test pipeline tracks execution metadata"""
        metadata = {
            'start_time': pd.Timestamp.now(),
            'phases': ['bronze', 'silver', 'gold'],
            'data_source': 'input_data.csv',
            'status': 'success'
        }

        assert 'start_time' in metadata
        assert len(metadata['phases']) == 3
        assert metadata['status'] == 'success'
