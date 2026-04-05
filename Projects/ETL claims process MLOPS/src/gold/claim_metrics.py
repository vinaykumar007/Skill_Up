"""
Gold layer metrics generation for Insurance Claims Analytics

Creates business-ready aggregations and feature tables for:
- Dashboard/BI consumption
- ML model training datasets
- Business intelligence reports
- Executive dashboards
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)


class ClaimMetrics:
    """
    Generate business metrics and analytics tables from Silver layer data.

    Responsibilities:
    1. Aggregate claims by business dimensions (time, status, type)
    2. Calculate business KPIs (approval rates, average resolution time)
    3. Identify high-value and high-risk claims
    4. Create feature tables for ML models
    5. Generate dashboard-ready metrics
    """

    def __init__(self):
        """Initialize metrics generator"""
        self.metrics = {}

    def monthly_claims_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate claims by month and category.

        Returns:
            DataFrame with columns:
            - year, month
            - claim_amount_category (small/medium/large)
            - num_claims: count of claims
            - total_claim_amount: sum of amounts
            - avg_claim_amount: mean amount
            - approval_rate: % approved claims
            - avg_days_to_resolution: average resolution time
        """
        logger.info("Generating monthly claims summary...")

        agg_dict = {
            "claim_id": "count",  # num_claims
            "claim_amount": ["sum", "mean", "min", "max", "std"],
            "days_to_resolution": ["mean", "median", "min", "max"],
        }

        # Group by year, month, and category
        monthly = df.groupby(
            ["submitted_year", "submitted_month", "claim_amount_category"]
        ).agg(agg_dict).reset_index()

        # Flatten column names
        monthly.columns = [
            "year", "month", "claim_amount_category",
            "num_claims", "total_claim_amount", "avg_claim_amount",
            "min_claim_amount", "max_claim_amount", "std_claim_amount",
            "avg_days_to_resolution", "median_days_to_resolution",
            "min_days_to_resolution", "max_days_to_resolution",
        ]

        # Calculate approval rate
        approved_df = df[df["status"] == "approved"].groupby(
            ["submitted_year", "submitted_month", "claim_amount_category"]
        )["claim_id"].count().reset_index()
        approved_df.columns = ["year", "month",
                               "claim_amount_category", "approved_count"]

        monthly = monthly.merge(approved_df, how="left",
                                on=["year", "month", "claim_amount_category"])
        monthly["approved_count"] = monthly["approved_count"].fillna(0)
        monthly["approval_rate"] = (
            monthly["approved_count"] / monthly["num_claims"] * 100
        ).round(2)

        # Sort by year, month
        monthly = monthly.sort_values(["year", "month"])

        logger.info(f"  ✓ Generated {len(monthly)} monthly summary rows")
        return monthly

    def status_breakdown(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate claims by status with KPIs.

        Returns:
            DataFrame showing claim distribution by status with metrics.
        """
        logger.info("Generating status breakdown...")

        status_agg = df.groupby("status").agg({
            "claim_id": "count",
            "claim_amount": ["sum", "mean"],
            "days_to_resolution": "mean",
        }).reset_index()

        status_agg.columns = [
            "status", "num_claims", "total_amount", "avg_amount", "avg_resolution_days"
        ]

        # Calculate percentages
        total = status_agg["num_claims"].sum()
        status_agg["pct_of_total"] = (
            status_agg["num_claims"] / total * 100
        ).round(2)

        # Sort by count descending
        status_agg = status_agg.sort_values("num_claims", ascending=False)

        logger.info(f"  ✓ Generated {len(status_agg)} status breakdown rows")
        return status_agg

    def claim_type_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze claims by type (auto, home, health, life, disability).
        """
        logger.info("Generating claim type analysis...")

        type_agg = df.groupby("claim_type").agg({
            "claim_id": "count",
            "claim_amount": ["sum", "mean", "median"],
            "days_to_resolution": ["mean", "median"],
            "status": lambda x: (x == "approved").sum(),  # approved_count
        }).reset_index()

        type_agg.columns = [
            "claim_type", "num_claims", "total_amount", "avg_amount",
            "median_amount", "avg_resolution_days", "median_resolution_days",
            "approved_count"
        ]

        type_agg["approval_rate"] = (
            type_agg["approved_count"] / type_agg["num_claims"] * 100
        ).round(2)

        logger.info(f"  ✓ Generated {len(type_agg)} claim type analysis rows")
        return type_agg

    def high_value_claims(self, df: pd.DataFrame,
                          threshold: float = 20000.0) -> pd.DataFrame:
        """
        Identify high-value claims for special handling/audit.

        Args:
            df: Input dataframe
            threshold: Amount threshold for "high-value" classification

        Returns:
            DataFrame of high-value claims with flags.
        """
        logger.info(f"Identifying high-value claims (>${threshold:,.0f})...")

        high_value = df[df["claim_amount"] >= threshold].copy()

        # Add risk flags
        high_value["is_pending"] = high_value["status"] == "pending"
        high_value["is_large_amount"] = high_value["claim_amount"] >= threshold * 1.5

        # Sort by amount descending
        high_value = high_value.sort_values("claim_amount", ascending=False)

        # Select relevant columns
        high_value = high_value[[
            "claim_id", "policy_id", "claim_type", "claim_amount",
            "status", "submitted_date", "is_pending", "is_large_amount"
        ]]

        logger.info(f"  ✓ Identified {len(high_value)} high-value claims")
        return high_value

    def resolution_time_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze claim resolution time distribution and SLAs.
        """
        logger.info("Generating resolution time analysis...")

        # Only look at resolved claims
        resolved = df[df["days_to_resolution"].notna()].copy()

        if len(resolved) == 0:
            logger.warning("  ⚠ No resolved claims found")
            return pd.DataFrame()

        # Create time buckets
        resolved["resolution_bucket"] = pd.cut(
            resolved["days_to_resolution"],
            bins=[0, 7, 14, 30, 60, 90, float('inf')],
            labels=["0-7 days", "8-14 days", "15-30 days",
                    "31-60 days", "61-90 days", "90+ days"]
        )

        # Aggregate
        time_analysis = resolved.groupby("resolution_bucket").agg({
            "claim_id": "count",
            "claim_amount": "mean",
            "days_to_resolution": ["mean", "median", "min", "max"],
        }).reset_index()

        time_analysis.columns = [
            "resolution_bucket", "num_claims", "avg_amount",
            "avg_days", "median_days", "min_days", "max_days"
        ]

        logger.info(
            f"  ✓ Generated {len(time_analysis)} resolution time buckets")
        return time_analysis

    def fraud_risk_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create feature table for fraud detection ML models.

        Features engineered:
        - claim_amount (larger = higher fraud risk)
        - days_to_resolution (anomalies may indicate fraud)
        - claim_frequency_per_customer (repeat claims)
        - claim_type patterns
        """
        logger.info("Generating fraud risk features...")

        features = df.copy()

        # Basic features already in Silver layer
        features = features[[
            "claim_id", "policy_id", "claim_amount", "claim_type",
            "status", "days_to_resolution", "claim_amount_category",
            "submitted_year", "submitted_month"
        ]]

        # Engineer additional features for fraud detection
        features["high_amount_flag"] = (
            features["claim_amount"] >= features["claim_amount"].quantile(0.75)
        ).astype(int)

        features["long_resolution_flag"] = (
            features["days_to_resolution"] >=
            features["days_to_resolution"].quantile(0.75)
        ).astype(int)

        features["fraud_risk_score"] = (
            features["high_amount_flag"] * 0.4 +
            features["long_resolution_flag"] * 0.3 +
            (features["claim_type"] == "auto").astype(int) * 0.2 +
            (features["claim_type"] == "home").astype(int) * 0.1
        )

        # Normalize score to 0-100
        features["fraud_risk_score"] = (
            features["fraud_risk_score"] /
            features["fraud_risk_score"].max() * 100
        ).round(2)

        logger.info(f"  ✓ Generated fraud features for {len(features)} claims")
        return features

    def generate_all_metrics(self, df_silver: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Generate ALL gold layer tables - main entry point.

        Returns:
            Dictionary of table_name -> DataFrame pairs

        This orchestrates all metric generation.
        """
        logger.info("\n" + "="*60)
        logger.info("GOLD LAYER METRICS GENERATION")
        logger.info("="*60 + "\n")

        try:
            self.metrics = {
                "monthly_summary": self.monthly_claims_summary(df_silver),
                "status_breakdown": self.status_breakdown(df_silver),
                "claim_type_analysis": self.claim_type_analysis(df_silver),
                "high_value_claims": self.high_value_claims(df_silver),
                "resolution_time_analysis": self.resolution_time_analysis(df_silver),
                "fraud_risk_features": self.fraud_risk_features(df_silver),
            }

            logger.info("\n✅ All Gold layer metrics generated successfully")
            logger.info("="*60 + "\n")

            return self.metrics

        except Exception as e:
            logger.error(f"Metric generation failed: {str(e)}")
            raise

    def get_metrics_summary(self) -> Dict:
        """Return summary statistics of all generated metrics"""
        summary = {}
        for table_name, table_df in self.metrics.items():
            summary[table_name] = {
                "row_count": len(table_df),
                "columns": list(table_df.columns),
            }
        return summary
