"""
Shared pytest configuration and fixtures for all tests
"""
import pytest
import pandas as pd
import sys
from pathlib import Path
import tempfile
import shutil

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root.resolve()))


@pytest.fixture(scope="session")
def test_data_dir():
    """Create and return a temporary directory for test data"""
    temp_dir = tempfile.mkdtemp(prefix="etl_test_")
    yield temp_dir
    # Cleanup after all tests
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_claims_data():
    """Fixture providing sample claims data for testing"""
    return pd.DataFrame({
        'claim_id': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'policy_id': ['P001', 'P002', 'P001', 'P003', 'P002'],
        'customer_name': ['Alice Smith', 'Bob Johnson', 'Alice Smith', 'Charlie Brown', 'Bob Johnson'],
        'claim_type': ['Auto', 'Home', 'Auto', 'Health', 'Home'],
        'claim_amount': [5000.00, 15000.00, 3500.00, 25000.00, 8000.00],
        'description': [
            'Car accident - rear end collision',
            'Water damage from burst pipe',
            'Windshield damage',
            'Emergency surgery and hospital',
            'Roof damage from storm'
        ],
        'submitted_date': ['2025-01-01', '2025-01-05', '2025-01-10', '2025-01-15', '2025-01-20'],
        'resolution_date': ['2025-01-15', None, '2025-01-12', '2025-02-01', '2025-02-05'],
        'status': ['RESOLVED', 'PENDING', 'RESOLVED', 'RESOLVED', 'RESOLVED']
    })


@pytest.fixture
def sample_large_claims_data():
    """Fixture providing larger sample data (1000 records)"""
    import random
    from datetime import datetime, timedelta

    random.seed(42)
    claim_types = ['Auto', 'Home', 'Health', 'Travel', 'Life']
    statuses = ['PENDING', 'IN_PROGRESS', 'RESOLVED', 'REJECTED']

    data = {
        'claim_id': [f'C{i:05d}' for i in range(1, 1001)],
        'policy_id': [f'P{random.randint(1, 100):03d}' for _ in range(1000)],
        'customer_name': [f'Customer {i}' for i in range(1, 1001)],
        'claim_type': [random.choice(claim_types) for _ in range(1000)],
        'claim_amount': [round(random.uniform(1000, 50000), 2) for _ in range(1000)],
        'description': [f'Description of claim {i}' for i in range(1, 1001)],
        'submitted_date': [
            (datetime(2025, 1, 1) +
             timedelta(days=random.randint(0, 60))).strftime('%Y-%m-%d')
            for _ in range(1000)
        ],
        'resolution_date': [
            (datetime(2025, 1, 1) +
             timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d')
            if random.random() > 0.2 else None
            for _ in range(1000)
        ],
        'status': [random.choice(statuses) for _ in range(1000)]
    }

    return pd.DataFrame(data)


@pytest.fixture
def config():
    """Fixture providing a test configuration"""
    from config.config import Config
    return Config()


@pytest.fixture
def mlflow_tracker(config):
    """Fixture providing MLflow tracker for testing"""
    from src.utils.mlflow_utils import MLflowTracker

    try:
        tracker = MLflowTracker(config=config)
        return tracker
    except Exception as e:
        # Return None if MLflow setup fails, tests should handle this
        return None


def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
