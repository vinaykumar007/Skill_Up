"""
Claim data transformer for Silver layer

Orchestrates standardization, cleaning, and feature engineering.
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any
import logging

from src.silver.cleaning_rules import ClaimCleaningRules, CustomerCleaningRules
from src.silver.data_validator import DataValidator

logger = logging.getLogger(__name__)


class ClaimTransformer:
    """
    Transform Bronze claim data to Silver quality standard.

    Steps:
    1. Standardize column names and types
    2. Handle missing values strategically
    3. Clean and normalize data values
    4. Engineer derived features
    5. Deduplicate records
    6. Add audit metadata
    """

    def __init__(self):
        """Initialize transformer"""
        self.rules = ClaimCleaningRules()
        self.validator = DataValidator()
        self.transformation_log = []

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names, types, and order.

        Ensures consistent schema across all runs.
        """
        logger.info("Step 1: Standardizing columns...")

        # Rename columns to standard names
        rename_map = {
            "claim_id": "claim_id",
            "policy_id": "policy_id",
            "customer_name": "customer_name",
            "claim_type": "claim_type",
            "claim_amount": "claim_amount",
            "description": "description",
            "submitted_date": "submitted_date",
            "resolution_date": "resolution_date",
            "status": "status",
        }

        # Only rename columns that exist
        actual_rename = {k: v for k, v in rename_map.items()
                         if k in df.columns}
        df = df.rename(columns=actual_rename)

        # Cast to correct types
        df["claim_amount"] = df["claim_amount"].astype("float64")

        # Ensure columns exist (even if null)
        for col in rename_map.values():
            if col not in df.columns:
                df[col] = None

        # Reorder columns
        ordered_cols = list(rename_map.values())
        df = df[ordered_cols]

        logger.info(f"  ✓ Standardized {len(ordered_cols)} columns")
        return df

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Implement missing value strategy.

        Strategy:
        - Drop rows with missing claim_id or policy_id (primary keys)
        - Fill claim_amount with 0 (no claim = no amount)
        - Fill status with "unknown"
        - Keep null dates (claim may still be pending)
        """
        logger.info("Step 2: Handling missing values...")

        before_count = len(df)

        # Drop critical nulls (claim_id, policy_id)
        df = df.dropna(subset=["claim_id", "policy_id", "submitted_date"])

        # Strategic fills
        df["claim_amount"] = df["claim_amount"].fillna(0.0)
        df["status"] = df["status"].fillna("unknown")
        df["description"] = df["description"].fillna("NO_DESCRIPTION")

        # Keep null dates (pending claims have no resolution)

        after_count = len(df)
        dropped = before_count - after_count

        logger.info(f"  ✓ Dropped {dropped} rows with missing critical fields")
        logger.info(f"  ✓ Filled strategic nulls (amount, status)")

        return df

    def normalize_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize and standardize all values using cleaning rules.

        This is where the rubber meets the road - actual data cleaning!
        """
        logger.info("Step 3: Normalizing values...")

        # Normalize claim amount
        df["claim_amount"] = df["claim_amount"].apply(
            self.rules.standardize_claim_amount
        )

        # Normalize dates
        df["submitted_date"] = df["submitted_date"].apply(
            self.rules.standardize_date
        )
        df["resolution_date"] = df["resolution_date"].apply(
            self.rules.standardize_date
        )

        # Normalize categorical values
        df["status"] = df["status"].apply(self.rules.standardize_status)
        df["claim_type"] = df["claim_type"].apply(
            self.rules.standardize_claim_type)

        logger.info("  ✓ Normalized amounts, dates, and categories")
        return df

    def add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create calculated/derived columns for analytics and ML.

        Features engineered:
        - claim_amount_category: small/medium/large
        - days_to_resolution: time from submission to resolution
        - submitted_year, submitted_month, submitted_quarter: temporal features
        """
        logger.info("Step 4: Engineering derived features...")

        # Amount categorization
        df["claim_amount_category"] = df["claim_amount"].apply(
            self.rules.categorize_claim_amount
        )

        # Time to resolution
        df["days_to_resolution"] = df.apply(
            lambda row: self.rules.calculate_days_to_resolution(
                row["submitted_date"], row["resolution_date"]
            ),
            axis=1,
        )

        # Extract date parts from submission
        date_features = df["submitted_date"].apply(
            self.rules.extract_date_parts
        )
        date_df = pd.DataFrame(date_features.tolist(), index=df.index)

        df = pd.concat([df, date_df.add_prefix("submitted_")], axis=1)

        logger.info(
            "  ✓ Added amount categorization, resolution time, date features")
        return df

    def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate claim_id records, keeping most recent version.

        In data lakes, incremental loads can create duplicates.
        """
        logger.info("Step 5: Deduplicating records...")

        before_count = len(df)

        # Keep latest version of each claim_id (assuming data has _ingested_at)
        if "_ingested_at" in df.columns:
            df = df.sort_values("_ingested_at", ascending=False)

        # Drop duplicates, keeping first (= latest if sorted)
        df = df.drop_duplicates(subset=["claim_id"], keep="first")

        after_count = len(df)
        duplicates_removed = before_count - after_count

        logger.info(f"  ✓ Removed {duplicates_removed} duplicate records")
        return df

    def add_transformation_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Inject technical metadata for audit trail.

        Columns:
        - _transformed_at: timestamp when record was transformed
        - _transformation_version: which version of Silver transform ran
        """
        df["_transformed_at"] = datetime.utcnow().isoformat()
        df["_transformation_version"] = "1.0"

        return df

    def transform(self, df_bronze: pd.DataFrame, validate: bool = True) -> Dict[str, Any]:
        """
        Main transformation orchestration pipeline.

        Args:
            df_bronze: DataFrame from Bronze layer
            validate: Run data quality checks

        Returns:
            Dict with transformed_df and validation_report

        This is the main entry point!
        """
        logger.info("\n" + "="*60)
        logger.info("SILVER LAYER TRANSFORMATION PIPELINE")
        logger.info("="*60)

        try:
            # Store original for comparison
            df_original = df_bronze.copy()

            # Execute transformation steps
            df = self.standardize_columns(df_bronze)
            df = self.handle_missing_values(df)
            df = self.normalize_values(df)
            df = self.add_derived_features(df)
            df = self.deduplicate(df)
            df = self.add_transformation_metadata(df)

            # Validate result
            validation_report = None
            if validate:
                validation_report = self.validator.run_all_checks(
                    df, df_original
                )
                self.validator.print_report(validation_report)

            logger.info("\n✅ Silver layer transformation COMPLETE")
            logger.info("="*60 + "\n")

            return {
                "transformed_df": df,
                "validation_report": validation_report,
                "status": "SUCCESS",
            }

        except Exception as e:
            logger.error(f"\n❌ Transformation FAILED: {str(e)}")
            return {
                "transformed_df": None,
                "validation_report": None,
                "status": "FAILED",
                "error": str(e),
            }


class CustomerTransformer:
    """
    Transform customer dimension with PII masking.
    """

    def __init__(self):
        self.rules = CustomerCleaningRules()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform customer data with privacy masking"""

        logger.info("Transforming customer dimension data...")

        # Mask sensitive fields
        df["customer_ssn"] = df["customer_ssn"].apply(self.rules.mask_ssn)
        df["customer_address"] = df["customer_address"].apply(
            self.rules.mask_address)

        # Standardize contact fields
        df["customer_email"] = df["customer_email"].apply(
            self.rules.standardize_email)
        df["customer_phone"] = df["customer_phone"].apply(
            self.rules.standardize_phone)

        # Add metadata
        df["_transformed_at"] = datetime.utcnow().isoformat()

        logger.info(f"  ✓ Masked PII fields (SSN, address)")
        logger.info(f"  ✓ Transformed {len(df)} customer records")

        return df
