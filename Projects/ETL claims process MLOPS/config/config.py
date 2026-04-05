"""
Centralized configuration for Insurance ETL Pipeline
"""
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


class Config:
    """Base configuration class"""

    # Project root
    PROJECT_ROOT = Path(__file__).parent.parent

    # Data paths
    DATA_ROOT = PROJECT_ROOT / "data"
    RAW_DATA_PATH = DATA_ROOT / "raw"
    BRONZE_PATH = DATA_ROOT / "bronze"
    SILVER_PATH = DATA_ROOT / "silver"
    GOLD_PATH = DATA_ROOT / "gold"

    # MLflow configuration
    MLFLOW_ROOT = PROJECT_ROOT / "mlflow"
    # Use file-based tracking with proper file:// URI scheme
    MLFLOW_TRACKING_URI = f"file:///{PROJECT_ROOT / 'mlruns'}"
    MLFLOW_EXPERIMENT_NAME = "insurance-etl-pipeline"

    # Spark configuration
    SPARK_APP_NAME = "InsuranceETL"
    SPARK_DRIVER_MEMORY = "4g"
    SPARK_SHUFFLE_PARTITIONS = 4

    # Data generation
    NUM_TEST_RECORDS = int(os.getenv("NUM_TEST_RECORDS", 1000))
    RANDOM_SEED = 42

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    def __init__(self):
        """Initialize configuration and create necessary directories"""
        self.DATA_ROOT.mkdir(parents=True, exist_ok=True)
        self.BRONZE_PATH.mkdir(parents=True, exist_ok=True)
        self.SILVER_PATH.mkdir(parents=True, exist_ok=True)
        self.GOLD_PATH.mkdir(parents=True, exist_ok=True)
        self.MLFLOW_ROOT.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict:
        """Convert configuration to dictionary"""
        return {
            "project_root": str(self.PROJECT_ROOT),
            "bronze_path": str(self.BRONZE_PATH),
            "silver_path": str(self.SILVER_PATH),
            "gold_path": str(self.GOLD_PATH),
            "mlflow_tracking_uri": self.MLFLOW_TRACKING_URI,
            "spark_driver_memory": self.SPARK_DRIVER_MEMORY,
            "num_test_records": self.NUM_TEST_RECORDS,
        }


class DevelopmentConfig(Config):
    """Development environment configuration"""
    DEBUG = True
    LOG_LEVEL = "DEBUG"


class ProductionConfig(Config):
    """Production environment configuration"""
    DEBUG = False
    LOG_LEVEL = "INFO"
    SPARK_DRIVER_MEMORY = "16g"
    SPARK_SHUFFLE_PARTITIONS = 200


def get_config() -> Config:
    """Get configuration based on environment"""
    env = os.getenv("ENV", "development")
    if env == "production":
        return ProductionConfig()
    return DevelopmentConfig()
