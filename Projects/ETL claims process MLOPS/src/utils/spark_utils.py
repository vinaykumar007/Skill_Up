"""
Spark Session Manager for Insurance ETL Pipeline

Singleton pattern for centralized Spark configuration with Delta Lake support.
"""
from pyspark.sql import SparkSession
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """
    Singleton manager for Spark session lifecycle.

    Responsibilities:
    1. Initialize Spark with Delta Lake support
    2. Configure memory and partition settings
    3. Provide single access point to Spark session
    4. Ensure clean shutdown
    """

    _instance: Optional["SparkSessionManager"] = None
    _spark: Optional[SparkSession] = None

    def __new__(cls) -> "SparkSessionManager":
        """
        Implement singleton pattern.

        Ensures only one SparkSessionManager instance exists.
        First call initializes Spark, subsequent calls return existing instance.
        """
        if cls._instance is None:
            cls._instance = super(SparkSessionManager, cls).__new__(cls)
            cls._instance._initialize_spark()
        return cls._instance

    def _initialize_spark(self) -> None:
        """
        Initialize Spark session with Delta Lake configuration.

        Key configurations:

        1. Delta Lake Extensions:
           - Enables Delta SQL syntax (CREATE TABLE, MERGE, DELETE, etc.)
           - Provides Delta catalog (file-based table management)

        2. Memory Configuration:
           - driver.memory: 4GB (local testing, increase to 16GB+ in production)
           - executor.memory: 4GB

        3. Partition Settings:
           - sql.shuffle.partitions: 4 (local testing, default 200 in production)
           - Determines parallelism for aggregations/joins

        4. Performance Tuning:
           - adaptiveExecution: Enables Spark 3.0+ adaptive query execution
           - broadcastTimeout: Increase for large broadcast variables
        """
        try:
            logger.info(
                "Initializing Spark session with Delta Lake support...")

            builder = (
                SparkSession.builder
                .appName("InsuranceETL")

                # Java/Windows compatibility for PySpark 3.5.1
                .config("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")

                # Delta Lake SQL extensions
                .config(
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension"
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                )

                # Memory configuration (local development)
                .config("spark.driver.memory", "2g")
                .config("spark.driver.cores", "2")

                # Shuffle partitions (4 for local, 200+ for cluster)
                .config("spark.sql.shuffle.partitions", "4")

                # Performance optimizations
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                # 30-minute broadcast timeout
                .config("spark.sql.broadcastTimeout", "1800")

                # Delta Lake optimizations
                .config("spark.delta.logStore.class",
                        "org.apache.spark.sql.delta.storage.LogFileMetadataCache")

                # Enable column mapping for schema evolution (Delta 2.0+)
                .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            )

            self._spark = builder.getOrCreate()

            # Set log level
            self._spark.sparkContext.setLogLevel("WARN")

            logger.info("✅ Spark session initialized successfully")
            logger.info(f"   Spark Version: {self._spark.version}")
            logger.info(f"   App Name: {self._spark.sparkContext.appName}")
            logger.info(f"   Master: {self._spark.sparkContext.master}")

        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {str(e)}")
            raise

    @classmethod
    def get_spark(cls) -> SparkSession:
        """
        Get the Spark session.

        Returns:
            Active SparkSession

        Usage:
            spark = SparkSessionManager.get_spark()
            df = spark.read.csv("path/to/file.csv")
        """
        instance = cls()  # Calls __new__, which initializes if needed
        return instance._spark

    @classmethod
    def stop(cls) -> None:
        """
        Gracefully stop the Spark session.

        Should be called at end of application or in cleanup.
        """
        if cls._spark is not None:
            logger.info("Stopping Spark session...")
            cls._spark.stop()
            cls._spark = None
            cls._instance = None
            logger.info("✅ Spark session stopped")

    @classmethod
    def reset(cls) -> None:
        """
        Reset singleton - useful for testing.

        Creates fresh Spark session on next get_spark() call.
        """
        cls.stop()
        cls._instance = None
        logger.info("Spark session reset")

    @staticmethod
    def enable_delta_time_travel():
        """
        Enable Delta Lake time travel features (read historical versions).
        """
        spark = SparkSessionManager.get_spark()
        spark.sql("SET spark.databricks.delta.timeTravel.enabled = true")
        logger.info("Delta time travel enabled")


# Convenience function for quick access
def get_spark_session() -> SparkSession:
    """
    Shorthand to get Spark session.

    Usage:
        from src.utils.spark_utils import get_spark_session
        spark = get_spark_session()
    """
    return SparkSessionManager.get_spark()
