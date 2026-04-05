"""
Schema definitions for bronze layer ingestion
Defines input schemas for all data sources (CSV, JSON)
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
)


def get_claims_schema() -> StructType:
    """
    Schema for insurance claims CSV data
    """
    return StructType(
        [
            StructField("claim_id", StringType(), False),
            StructField("policy_id", StringType(), False),
            StructField("customer_name", StringType(), True),
            StructField("claim_type", StringType(), True),
            StructField("claim_amount", DecimalType(10, 2), True),
            StructField("description", StringType(), True),
            StructField("submitted_date", DateType(), False),
            StructField("resolution_date", DateType(), True),
            StructField("status", StringType(), True),
        ]
    )


def get_customers_schema() -> StructType:
    """
    Schema for customer dimension data
    """
    return StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("policy_id", StringType(), False),
            StructField("customer_name", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("customer_phone", StringType(), True),
            StructField("customer_ssn", StringType(),
                        True),  # PII - will mask later
            StructField("customer_income", DecimalType(12, 2), True),
            StructField("customer_address", StringType(),
                        True),  # PII - will mask later
            StructField("signup_date", DateType(), True),
        ]
    )


def get_claim_notes_schema() -> StructType:
    """
    Schema for semi-structured claim notes (JSON)
    Adjuster notes and observations about claims
    """
    return StructType(
        [
            StructField("claim_id", StringType(), False),
            StructField("note_id", StringType(), False),
            StructField("adjuster_name", StringType(), True),
            StructField("note_text", StringType(), True),
            StructField("note_date", DateType(), True),
            StructField("fraud_indicator", StringType(), True),  # Y/N/UNKNOWN
            StructField("severity_level", IntegerType(), True),  # 1-5 scale
        ]
    )


# Export all schemas
SCHEMAS = {
    "claims": get_claims_schema(),
    "customers": get_customers_schema(),
    "claim_notes": get_claim_notes_schema(),
}
