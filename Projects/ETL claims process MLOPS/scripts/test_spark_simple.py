"""
Simplified Spark test for Windows compatibility

Run: python scripts/test_spark_simple.py
"""
import sys
from pathlib import Path
import os

# Set up environment for Windows
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Add project root to path
sys.path.insert(0, str(Path(__file__).parents[1]))

import logging

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Suppress warnings
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("\n" + "="*70)
print("TESTING SPARK AND DELTA LAKE SETUP")
print("="*70 + "\n")

try:
    print("📦 Importing PySpark...")
    from pyspark.sql import SparkSession
    print("   ✅ PySpark imported successfully")
    
    print("\n🔧 Creating Spark session with Delta Lake...")
    spark = (
        SparkSession.builder
        .appName("InsuranceETL")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    print("   ✅ Spark session created")
    
    print("\n📊 Creating test DataFrame...")
    data = [
        ("CLM-001", "POL-001", 1000.00, "approved"),
        ("CLM-002", "POL-002", 2000.00, "pending"),
        ("CLM-003", "POL-003", 3000.00, "denied"),
    ]
    columns = ["claim_id", "policy_id", "amount", "status"]
    df = spark.createDataFrame(data, columns)
    row_count = df.count()
    print(f"   ✅ Created DataFrame with {row_count} rows")
    
    print("\n📋 DataFrame preview:")
    df.show(truncate=False)
    
    print("\n💾 Writing to Delta Lake format...")
    test_path = "data/test_delta"
    df.write.format("delta").mode("overwrite").save(test_path)
    print(f"   ✅ Written {row_count} records to {test_path}")
    
    print("\n📖 Reading from Delta Lake format...")
    df_read = spark.read.format("delta").load(test_path)
    read_count = df_read.count()
    print(f"   ✅ Read {read_count} records from Delta Lake")
    
    print("\n🧹 Cleaning up test data...")
    import shutil
    if Path(test_path).exists():
        shutil.rmtree(test_path)
    print("   ✅ Cleanup complete")
    
    print("\n" + "="*70)
    print("🎉 SUCCESS! SPARK AND DELTA LAKE ARE WORKING CORRECTLY")
    print("="*70)
    print("\nSpark Version:", spark.version)
    print("Master:", spark.sparkContext.master)
    print("App Name:", spark.sparkContext.appName)
    print("\n" + "="*70 + "\n")
    
    spark.stop()
    sys.exit(0)
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    print("\nTraceback:")
    import traceback
    traceback.print_exc()
    sys.exit(1)
