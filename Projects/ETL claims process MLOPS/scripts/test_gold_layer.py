"""
Test script for Gold layer metric generation

Demonstrates end-to-end Silver -> Gold pipeline.

Run: python scripts/test_gold_layer.py
"""
import logging
from src.gold.claim_metrics import ClaimMetrics
from src.silver.claim_transformer import ClaimTransformer
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_section(title: str):
    """Print formatted section header"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70 + "\n")


def main():
    """Run full Bronze -> Silver -> Gold pipeline"""

    print_section("FULL PIPELINE TEST: BRONZE → SILVER → GOLD")

    # ===== LOAD BRONZE DATA =====
    print("📥 Step 1: Loading Bronze (raw) data...")
    df_bronze = pd.read_csv("data/bronze/claims.csv")
    print(f"   ✓ Loaded {len(df_bronze)} claim records\n")

    # ===== TRANSFORM TO SILVER =====
    print("🔄 Step 2: Transforming to Silver (cleaned) data...")
    transformer = ClaimTransformer()
    result = transformer.transform(
        df_bronze, validate=False)  # Skip verbose validation

    if result["status"] != "SUCCESS":
        print(f"❌ Transformation failed: {result.get('error')}")
        return 1

    df_silver = result["transformed_df"]
    print(f"   ✓ Silver layer ready: {len(df_silver)} records, "
          f"{len(df_silver.columns)} columns\n")

    # ===== GENERATE GOLD METRICS =====
    print("📊 Step 3: Generating Gold (aggregated) metrics...")
    metrics_gen = ClaimMetrics()
    metrics = metrics_gen.generate_all_metrics(df_silver)

    # ===== DISPLAY RESULTS =====
    print_section("GOLD LAYER TABLE 1: MONTHLY CLAIMS SUMMARY")

    monthly = metrics["monthly_summary"]
    print(monthly.head(10).to_string(index=False))
    print(f"\nTotal: {len(monthly)} monthly summaries")

    print_section("GOLD LAYER TABLE 2: STATUS BREAKDOWN")

    status = metrics["status_breakdown"]
    print(status.to_string(index=False))

    print_section("GOLD LAYER TABLE 3: CLAIM TYPE ANALYSIS")

    claim_type = metrics["claim_type_analysis"]
    print(claim_type.to_string(index=False))

    print_section("GOLD LAYER TABLE 4: HIGH-VALUE CLAIMS AUDIT")

    high_value = metrics["high_value_claims"]
    print(f"Found {len(high_value)} high-value claims (>${20000:,.0f})")
    print(f"\nTop 5 claims by amount:")
    print(high_value[["claim_id", "claim_type", "claim_amount", "status"]].head(5)
          .to_string(index=False))

    print_section("GOLD LAYER TABLE 5: RESOLUTION TIME ANALYSIS")

    resolution_time = metrics["resolution_time_analysis"]
    if len(resolution_time) > 0:
        print(resolution_time.to_string(index=False))
    else:
        print("No resolved claims yet")

    print_section("GOLD LAYER TABLE 6: FRAUD RISK FEATURES (ML READY)")

    fraud_features = metrics["fraud_risk_features"]
    print(f"Feature table created for ML with {len(fraud_features)} records")
    print(f"\nSample fraud risk scores:")
    print(fraud_features[["claim_id", "claim_amount", "claim_type",
                          "fraud_risk_score"]].head(10).to_string(index=False))

    # ===== SUMMARY =====
    print_section("PIPELINE SUMMARY")

    print(f"✅ Bronze  -> {len(df_bronze):,} raw claims")
    print(
        f"✅ Silver  -> {len(df_silver):,} cleaned claims, {len(df_silver.columns)} columns")
    print(f"✅ Gold    -> 6 analytics tables generated:\n")

    for table_name, table_df in metrics.items():
        print(f"   • {table_name}: {len(table_df)} rows")

    print("\n" + "="*70)
    print("🎉 MEDALLION ARCHITECTURE COMPLETE")
    print("="*70 + "\n")

    return 0


if __name__ == "__main__":
    exit(main())
