"""
CSV-specific ingester implementation for Bronze layer
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from src.bronze.ingester import BaseIngester
import logging

logger = logging.getLogger(__name__)


class CSVIngester(BaseIngester):
    """
    CSV file ingester with explicit schema enforcement.
    """

    def __init__(
        self,
        spark: SparkSession,
        input_path: str,
        output_path: str,
        schema: StructType,
        source_name: str = "CSV",
        delimiter: str = ",",
        header: bool = True,
        skip_rows: int = 0,
    ):
        """
        Initialize CSV ingester.

        Args:
            spark: Active SparkSession
            input_path: Path to CSV file
            output_path: Path to write Delta table
            schema: Explicit StructType schema
            source_name: Human-readable source name
            delimiter: CSV field delimiter
            header: Whether CSV has header row
            skip_rows: Number of rows to skip (e.g., for metadata)
        """
        super().__init__(spark, input_path, output_path, schema, source_name)
        self.delimiter = delimiter
        self.header = header
        self.skip_rows = skip_rows

    def read(self) -> DataFrame:
        """
        Read CSV file with explicit schema and options.

        Key options:
        - schema: Explicit StructType (prevents incorrect type inference)
        - header: Whether to treat first row as column names
        - inferSchema: false (we use explicit schema, faster and more reliable)

        Returns:
            PySpark DataFrame
        """
        logger.info(f"Reading CSV from {self.input_path}")

        return (
            self.spark.read
            .schema(self.schema)
            .option("header", "true" if self.header else "false")
            .option("delimiter", self.delimiter)
            .option("inferSchema", "false")  # Use explicit schema
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .csv(str(self.input_path))
        )
