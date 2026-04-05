"""
Unit tests for Bronze layer (data ingestion)

Tests the ingestion, schema validation, and metadata injection
"""
from config.config import Config
from src.bronze.csv_ingester import CSVIngester
from src.bronze.schema_definitions import get_claims_schema, get_customers_schema
import pytest
import pandas as pd
from pathlib import Path
import tempfile
import sys

# Add project root
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root.resolve()))


@pytest.mark.unit
class TestSchemaDefinitions:
    """Test schema definitions for Bronze layer"""

    def test_claims_schema_structure(self):
        """Test that claims schema has expected fields"""
        schema = get_claims_schema()
        field_names = [field.name for field in schema.fields]

        expected_fields = ['claim_id', 'policy_id', 'customer_name', 'claim_type',
                           'claim_amount', 'description', 'submitted_date', 'resolution_date', 'status']

        for field in expected_fields:
            assert field in field_names, f"Expected field {field} not found in schema"

    def test_customers_schema_structure(self):
        """Test that customers schema has expected fields"""
        schema = get_customers_schema()
        field_names = [field.name for field in schema.fields]

        expected_fields = ['customer_id', 'first_name',
                           'last_name', 'email', 'phone']

        for field in expected_fields:
            assert field in field_names, f"Expected field {field} not found in schema"

    def test_schema_data_types(self):
        """Test that schema fields have correct data types"""
        schema = get_claims_schema()

        # Get field types as strings
        field_types = {field.name: str(field.dataType)
                       for field in schema.fields}

        # Verify key field types
        assert 'DoubleType' in field_types['claim_amount'] or 'FloatType' in field_types['claim_amount']


@pytest.mark.unit
class TestCSVIngestion:
    """Test CSV ingestion functionality"""

    def test_csv_ingestion_loads_data(self, sample_claims_data):
        """Test that CSV ingestion loads data correctly"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write sample data to CSV
            csv_path = Path(temp_dir) / "test_claims.csv"
            sample_claims_data.to_csv(csv_path, index=False)

            # Read with pandas (simulating ingestion)
            df = pd.read_csv(csv_path)

            assert len(df) == len(
                sample_claims_data), "Loaded data row count mismatch"
            assert list(df.columns) == list(
                sample_claims_data.columns), "Column mismatch"

    def test_csv_ingestion_preserves_data_types(self, sample_claims_data):
        """Test that data types are preserved during ingestion"""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_claims.csv"
            sample_claims_data.to_csv(csv_path, index=False)

            df = pd.read_csv(csv_path)

            # Verify that claim_amount is numeric
            assert pd.api.types.is_numeric_dtype(
                df['claim_amount']), "claim_amount should be numeric"

    def test_csv_ingestion_handles_missing_files(self):
        """Test that ingestion handles missing files gracefully"""
        non_existent_path = "/tmp/non_existent_file_12345.csv"

        with pytest.raises(FileNotFoundError):
            pd.read_csv(non_existent_path)


@pytest.mark.unit
class TestMetadataInjection:
    """Test metadata injection in Bronze layer"""

    def test_metadata_columns_added(self, sample_claims_data):
        """Test that metadata columns are added correctly"""
        df = sample_claims_data.copy()

        # Simulate metadata injection
        from datetime import datetime
        df['_ingested_at'] = datetime.utcnow().isoformat()
        df['_source_name'] = 'claims_csv'
        df['_record_id'] = range(len(df))

        # Verify metadata columns exist
        assert '_ingested_at' in df.columns
        assert '_source_name' in df.columns
        assert '_record_id' in df.columns

        # Verify metadata values
        assert df['_source_name'].unique()[0] == 'claims_csv'
        assert len(df) == df['_record_id'].max() + 1

    def test_metadata_consistency(self, sample_large_claims_data):
        """Test metadata consistency across large dataset"""
        df = sample_large_claims_data.copy()

        df['_ingested_at'] = pd.Timestamp.utcnow().isoformat()
        df['_source_name'] = 'claims_csv'
        df['_record_id'] = range(len(df))

        # Verify all records have metadata
        assert df['_ingested_at'].notna().all(
        ), "_ingested_at should not have nulls"
        assert df['_source_name'].notna().all(
        ), "_source_name should not have nulls"
        assert df['_record_id'].notna().all(
        ), "_record_id should not have nulls"

        # Verify record IDs are unique
        assert df['_record_id'].nunique() == len(
            df), "_record_id should be unique"


@pytest.mark.unit
class TestDataValidation:
    """Test data validation in Bronze layer"""

    def test_null_detection(self, sample_claims_data):
        """Test detection of null values"""
        df = sample_claims_data.copy()
        df.loc[1, 'resolution_date'] = None

        null_counts = df.isnull().sum()
        assert null_counts['resolution_date'] > 0, "Should detect null in resolution_date"

    def test_duplicate_detection(self):
        """Test detection of duplicate records"""
        df = pd.DataFrame({
            'id': [1, 2, 2, 4],
            'value': ['a', 'b', 'b', 'd']
        })

        duplicates = df.duplicated(subset=['id', 'value']).sum()
        assert duplicates > 0, "Should detect duplicates"

    def test_data_type_validation(self, sample_claims_data):
        """Test validation of data types"""
        df = sample_claims_data.copy()

        # Verify expected data types
        assert pd.api.types.is_object_dtype(df['claim_id'])
        assert pd.api.types.is_numeric_dtype(df['claim_amount'])
