"""
JSON-specific ingester implementation for Bronze layer
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from src.bronze.ingester import BaseIngester
import logging

logger = logging.getLogger(__name__)


class JSONIngester(BaseIngester):
    """
    JSON file ingester with schema enforcement.
    """

    def __init__(
        self,
        spark: SparkSession,
        input_path: str,
        output_path: str,
        schema: StructType,
        source_name: str = "JSON",
        mode: str = "PERMISSIVE",
    ):
        """
        Initialize JSON ingester.

        Args:
            spark: Active SparkSession
            input_path: Path to JSON file(s)
            output_path: Path to write Delta table
            schema: Explicit StructType schema
            source_name: Human-readable source name
            mode: Parse mode (PERMISSIVE, DROPMALFORMED, FAILFAST)
        """
        super().__init__(spark, input_path, output_path, schema, source_name)
        self.mode = mode

    def read(self) -> DataFrame:
        """
        Read JSON file(s) with explicit schema.

        Supports:
        - Single JSON file
        - Newline-delimited JSON (NDJSON)
        - JSON array flattening

        Key options:
        - schema: Explicit StructType
        - mode: Handling of malformed records
        - multiLine: For pretty-printed JSON

        Returns:
            PySpark DataFrame
        """
        logger.info(f"Reading JSON from {self.input_path}")

        return (
            self.spark.read
            .schema(self.schema)
            .option("mode", self.mode)
            .option("multiLine", "true")  # Handle pretty-printed JSON
            .json(str(self.input_path))
        )
