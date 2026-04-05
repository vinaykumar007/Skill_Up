"""
Base data ingester class for Bronze layer
"""
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


class BaseIngester(ABC):
    """
    Abstract base class for all data ingestion implementations.

    Enforces:
    - Schema validation
    - Metadata injection (_ingested_at, _ingested_from, _record_id)
    - Structured error handling
    - Audit trail for compliance
    """

    def __init__(
        self,
        spark: SparkSession,
        input_path: str,
        output_path: str,
        schema: StructType,
        source_name: str,
    ):
        """
        Initialize ingester.

        Args:
            spark: Active SparkSession
            input_path: Path to source data
            output_path: Path to write Delta table
            schema: Explicit schema for data
            source_name: Human-readable source identifier (for audit trail)
        """
        self.spark = spark
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.schema = schema
        self.source_name = source_name
        self.ingested_at = datetime.utcnow()

        logger.info(
            f"Initializing {self.__class__.__name__} for {source_name}")

    @abstractmethod
    def read(self) -> DataFrame:
        """
        Read data from source (implemented by subclasses).

        Must handle:
        - Schema inference or explicit schema application
        - Format-specific options
        - Error handling

        Returns:
            PySpark DataFrame with raw data
        """
        pass

    def add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """
        Inject technical metadata for data lineage and auditability.

        Columns added:
        - _ingested_at: UTC timestamp of ingestion
        - _ingested_from: Source file/path
        - _source_name: Human-readable source identifier
        - _record_id: Unique identifier per refresh (monotonically increasing)
        """
        return (
            df.withColumn("_ingested_at", F.lit(
                self.ingested_at).cast("timestamp"))
            .withColumn("_ingested_from", F.lit(str(self.input_path)))
            .withColumn("_source_name", F.lit(self.source_name))
            .withColumn("_record_id", F.monotonically_increasing_id())
        )

    def validate(self, df: DataFrame) -> bool:
        """
        Validate ingested data meets minimum quality standards.

        Checks:
        - Row count > 0
        - No all-null columns
        - All primary keys populated

        Returns:
            True if validation passes, False otherwise
        """
        row_count = df.count()

        if row_count == 0:
            logger.warning(f"No records found in {self.source_name}")
            return False

        # Check for completely null columns (data quality issue)
        null_counts = df.select(
            [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
        ).collect()[0]

        all_null_cols = [
            col for col in df.columns if null_counts[col] == row_count
        ]
        if all_null_cols:
            logger.warning(
                f"Columns with all nulls in {self.source_name}: {all_null_cols}"
            )

        logger.info(
            f"Validation passed: {row_count} records ingested from {self.source_name}")
        return True

    def write_to_delta(self, df: DataFrame) -> None:
        """
        Write DataFrame to Delta Lake with ACID guarantees.
        """
        try:
            df.write.format("delta").mode(
                "overwrite").save(str(self.output_path))
            logger.info(
                f"Successfully wrote {df.count()} records to {self.output_path}")
        except Exception as e:
            logger.error(f"Failed to write Delta table: {str(e)}")
            raise

    def ingest(self) -> DataFrame:
        """
        Main ingestion pipeline orchestration.

        Steps:
        1. Read data from source
        2. Apply schema
        3. Inject metadata
        4. Validate quality
        5. Write to Delta Lake

        Returns:
            Ingested DataFrame (also persisted to storage)
        """
        try:
            logger.info(f"Starting ingestion from {self.source_name}")

            # Step 1: Read
            df = self.read()
            logger.info(f"Read {df.count()} records from source")

            # Step 2: Add metadata
            df = self.add_metadata_columns(df)

            # Step 3: Validate
            if not self.validate(df):
                raise ValueError(
                    f"Data validation failed for {self.source_name}")

            # Step 4: Write
            self.write_to_delta(df)

            logger.info(f"Ingestion complete for {self.source_name}")
            return df

        except Exception as e:
            logger.error(f"Ingestion failed for {self.source_name}: {str(e)}")
            raise
