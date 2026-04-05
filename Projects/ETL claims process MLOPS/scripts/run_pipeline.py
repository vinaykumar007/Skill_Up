"""
Main pipeline runner script

Run this to execute the complete ETL pipeline:
  python run_pipeline.py

This is the primary entry point for demonstrations and scheduled runs.
"""
from src.etl_pipeline import ETLPipeline
from config.config import Config
import sys
from pathlib import Path
import os

# Add project root to path FIRST before any imports
script_dir = Path(__file__).parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root.resolve()))
os.chdir(str(project_root))


def main():
    """Execute the ETL pipeline"""
    try:
        print("\n" + "🚀 " * 20)
        print("\nStarting Insurance Claims ETL Pipeline...")
        print("\n" + "🚀 " * 20 + "\n")

        # Initialize and run
        config = Config()
        pipeline = ETLPipeline(config, enable_mlflow=True)
        success = pipeline.run()

        # Print results
        pipeline.print_summary()
        pipeline.save_execution_log()

        if success:
            print("\n" + "="*70)
            print("✅ PIPELINE COMPLETED SUCCESSFULLY")
            print("="*70)
            print("\nOutput files saved to:")
            print(f"  Silver layer: {config.SILVER_PATH}")
            print(f"  Gold layer:   {config.GOLD_PATH}")

            # MLflow tracking info
            if pipeline.mlflow_tracker:
                print("\n📊 MLflow Tracking:")
                print(f"  Run ID: {pipeline.mlflow_run_id}")
                print(f"  View in MLflow UI: mlflow ui")
                print(f"  Tracking backend: ./mlruns")

            print("\n" + "="*70 + "\n")
            return 0
        else:
            print("\n" + "="*70)
            print("❌ PIPELINE FAILED")
            print("="*70)
            print(
                f"\nCheck execution log: {config.PROJECT_ROOT}/pipeline_execution.log")
            print("\n" + "="*70 + "\n")
            return 1

    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user\n")
        return 130

    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}\n")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
