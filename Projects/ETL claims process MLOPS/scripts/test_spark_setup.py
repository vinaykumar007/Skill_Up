"""
Test script to verify Spark and Delta Lake setup

Run: python scripts/test_spark_setup.py
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parents[1]))

from src.utils.spark_utils import SparkSessionManager
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_spark_initialization():
    """Test that Spark initializes correctly"""
    logger.info("Testing Spark initialization...")
    
    try:
        spark = SparkSessionManager.get_spark()
        print("\n" + "="*60)
        print("✅ SPARK INITIALIZED SUCCESSFULLY")
        print("="*60)
        print(f"Spark Version: {spark.version}")
        print(f"App Name: {spark.sparkContext.appName}")
        print(f"Master: {spark.sparkContext.master}")
        print("="*60 + "\n")
        
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Spark: {str(e)}")
        return False


def test_delta_registration():
    """Test that Delta Lake is registered"""
    logger.info("Testing Delta Lake registration...")
    
    try:
        spark = SparkSessionManager.get_spark()
        
        # Try to read a Delta table location (won't exist yet, but tests registration)
        extensions = spark.conf.get("spark.sql.extensions", "")
        
        if "io.delta.sql.DeltaSparkSessionExtension" in extensions:
            print("\n✅ Delta Lake extensions registered")
            return True
        else:
            logger.error("Delta Lake extensions not registered")
            return False
            
    except Exception as e:
        logger.error(f"Failed Delta registration test: {str(e)}")
        return False


def test_simple_dataframe():
    """Test that we can create and manipulate DataFrames"""
    logger.info("Testing DataFrame creation...")
    
    try:
        spark = SparkSessionManager.get_spark()
        
        # Create simple test data
        data = [
            ("CLM-001", "POL-001", 1000.00, "approved"),
            ("CLM-002", "POL-002", 2000.00, "pending"),
            ("CLM-003", "POL-003", 3000.00, "denied"),
        ]
        
        columns = ["claim_id", "policy_id", "amount", "status"]
        df = spark.createDataFrame(data, columns)
        
        # Show data
        print("\n✅ Created test DataFrame:")
        df.show(truncate=False)
        
        # Count rows
        row_count = df.count()
        print(f"Row count: {row_count}")
        
        if row_count == 3:
            print("✅ DataFrame operations working correctly\n")
            return True
        else:
            logger.error(f"Expected 3 rows, got {row_count}")
            return False
            
    except Exception as e:
        logger.error(f"DataFrame test failed: {str(e)}")
        return False


def test_delta_write_read(test_path: str = "data/test_delta"):
    """Test writing and reading Delta table"""
    logger.info(f"Testing Delta write/read at {test_path}...")
    
    try:
        spark = SparkSessionManager.get_spark()
        from pathlib import Path
        
        test_dir = Path(test_path)
        
        # Create test data
        data = [
            ("CLM-001", "POL-001", 1000.00),
            ("CLM-002", "POL-002", 2000.00),
        ]
        columns = ["claim_id", "policy_id", "amount"]
        df = spark.createDataFrame(data, columns)
        
        # Write as Delta
        df.write.format("delta").mode("overwrite").save(str(test_dir))
        logger.info(f"✅ Written {df.count()} records to Delta")
        
        # Read back
        df_read = spark.read.format("delta").load(str(test_dir))
        read_count = df_read.count()
        logger.info(f"✅ Read {read_count} records from Delta")
        
        # Cleanup
        import shutil
        if test_dir.exists():
            shutil.rmtree(test_dir)
        
        print(f"✅ Delta write/read test successful\n")
        return True
        
    except Exception as e:
        logger.error(f"Delta test failed: {str(e)}")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("SPARK SESSION SETUP VERIFICATION")
    print("="*60 + "\n")
    
    tests = [
        ("Spark Initialization", test_spark_initialization),
        ("Delta Lake Registration", test_delta_registration),
        ("DataFrame Operations", test_simple_dataframe),
        ("Delta Write/Read", test_delta_write_read),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"Running: {test_name}...")
        results.append((test_name, test_func()))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print(f"\nPassed: {passed}/{total}")
    print("="*60 + "\n")
    
    if passed == total:
        print("🎉 All tests passed! Spark is ready for ETL pipeline.\n")
        SparkSessionManager.stop()
        return 0
    else:
        print("⚠️  Some tests failed. Check configuration.\n")
        SparkSessionManager.stop()
        return 1


if __name__ == "__main__":
    exit(main())
