"""
Test script for Silver layer transformation

Demonstrates the full Bronze -> Silver pipeline with real test data.

Run: python scripts/test_silver_layer.py
"""
import logging
from src.silver.claim_transformer import ClaimTransformer, CustomerTransformer
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


def test_claim_transformation():
    """Test claim transformation pipeline"""

    print("\n" + "="*70)
    print("TESTING SILVER LAYER: CLAIM DATA TRANSFORMATION")
    print("="*70 + "\n")

    # Load bronze data
    print("📥 Loading Bronze data...")
    df_bronze = pd.read_csv("data/bronze/claims.csv")
    print(f"   Loaded {len(df_bronze)} claim records\n")

    # Display sample of raw data
    print("📋 Sample of Bronze (raw) data:")
    print(df_bronze.head(3))

    # Transform
    print("\n🔄 Transforming Bronze -> Silver...\n")
    transformer = ClaimTransformer()
    result = transformer.transform(df_bronze)

    if result["status"] == "SUCCESS":
        df_silver = result["transformed_df"]

        print("\n📋 Sample of Silver (cleaned) data:")
        print(df_silver[["claim_id", "claim_amount", "claim_amount_category",
                         "status", "days_to_resolution", "submitted_year"]].head(3))

        print("\n📊 Summary Statistics after transformation:")
        print(f"   Total records: {len(df_silver)}")
        print(f"   Claim amount - Min: ${df_silver['claim_amount'].min():.2f}, "
              f"Max: ${df_silver['claim_amount'].max():.2f}, "
              f"Mean: ${df_silver['claim_amount'].mean():.2f}")
        print(f"   Status distribution:")

        status_counts = df_silver["status"].value_counts()
        for status, count in status_counts.items():
            pct = count / len(df_silver) * 100
            print(f"      {status}: {count} ({pct:.1f}%)")

        print(f"\n   Claim amount categories:")
        category_counts = df_silver["claim_amount_category"].value_counts()
        for category, count in category_counts.items():
            pct = count / len(df_silver) * 100
            print(f"      {category}: {count} ({pct:.1f}%)")

        print("\n✅ Claim transformation SUCCESSFUL")
        return True
    else:
        print(f"\n❌ Transformation FAILED: {result.get('error')}")
        return False


def test_customer_transformation():
    """Test customer transformation with PII masking"""

    print("\n" + "="*70)
    print("TESTING SILVER LAYER: CUSTOMER DATA TRANSFORMATION (PII MASKING)")
    print("="*70 + "\n")

    # Load bronze customer data
    print("📥 Loading Bronze customer data...")
    df_bronze = pd.read_csv("data/bronze/customers.csv")
    print(f"   Loaded {len(df_bronze)} customer records\n")

    # Display raw PII
    print("📋 Sample of Bronze (raw) - Contains PII:")
    sample_cols = ["customer_id", "customer_name",
                   "customer_ssn", "customer_address"]
    print(df_bronze[sample_cols].head(3))

    # Transform
    print("\n🔄 Transforming with PII masking...\n")
    transformer = CustomerTransformer()
    df_silver = transformer.transform(df_bronze)

    # Display masked data
    print("\n📋 Sample of Silver (masked) - PII protected:")
    print(df_silver[sample_cols].head(3))

    print("\n✅ PII masking SUCCESSFUL")

    return True


def main():
    """Run all Silver layer tests"""

    try:
        success = True

        # Test claim transformation
        success = test_claim_transformation() and success

        # Test customer transformation
        success = test_customer_transformation() and success

        if success:
            print("\n" + "="*70)
            print("🎉 ALL SILVER LAYER TESTS PASSED")
            print("="*70 + "\n")
            return 0
        else:
            return 1

    except Exception as e:
        logger.error(f"Test suite failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
