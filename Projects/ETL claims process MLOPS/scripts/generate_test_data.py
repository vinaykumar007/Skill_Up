"""
Script to generate test data for Bronze layer

Run: python scripts/generate_test_data.py
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parents[1]))

from src.utils.data_generator import InsuranceDataGenerator
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Generate test data in data/bronze directory"""
    
    # Use 1000 records for demo (change to 10000+ for full testing)
    num_records = 1000
    
    output_path = Path("data/bronze")
    
    logger.info(f"Generating {num_records} test records to {output_path}")
    
    generator = InsuranceDataGenerator(num_records=num_records, seed=42)
    paths = generator.generate_all(output_path)
    
    print("\n" + "="*60)
    print("✅ TEST DATA GENERATED SUCCESSFULLY")
    print("="*60)
    print(f"\nGenerated files:")
    for name, path in paths.items():
        print(f"  📄 {name}: {path}")
    
    print("\n" + "="*60)
    print("Next step: Review the generated data to understand the structure")
    print("Then we'll create the Spark Session Manager and ETL pipeline")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
