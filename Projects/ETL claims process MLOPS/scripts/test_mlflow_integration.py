"""
MLflow Integration Test

Demonstrates:
1. MLflow tracker initialization
2. Phase-level metrics logging
3. Data quality metrics
4. Gold table metrics
5. Execution summary logging

Run with: python scripts/test_mlflow_integration.py
"""

import logging
from config.config import Config
from src.etl_pipeline import ETLPipeline
from src.utils.mlflow_tracker import MLflowTracker
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parents[1]))


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_mlflow_tracker():
    """Test MLflow tracker directly"""
    logger.info("\n" + "="*70)
    logger.info("TEST 1: MLflow Tracker Initialization")
    logger.info("="*70)

    tracker = MLflowTracker(
        experiment_name="test-insurance-etl",
        tracking_uri=None  # Local SQLite backend
    )
    logger.info(f"✅ Tracker initialized")
    logger.info(f"   Experiment: test-insurance-etl")
    logger.info(f"   Tracking URI: {os.path.join(os.getcwd(), 'mlruns')}")

    # Start a test run
    run_id = tracker.start_run(
        run_name="test-run-basic",
        tags={"test": "basic", "layer": "all"},
        params={"test_param": 42}
    )
    logger.info(f"✅ Started test run: {run_id}")

    # Log some test metrics
    tracker.log_phase_metrics(
        phase_name="TEST_PHASE_1",
        duration_seconds=1.5,
        record_count=100,
        status="SUCCESS"
    )
    logger.info(f"✅ Logged phase metrics for TEST_PHASE_1")

    # Log data quality metrics
    quality_metrics = {
        "input_row_count": 100,
        "output_row_count": 98,
        "retention_percentage": 98.0,
        "duplicate_count": 2,
        "duplicate_percentage": 2.0,
    }
    tracker.log_data_quality_metrics("test", quality_metrics)
    logger.info(f"✅ Logged data quality metrics")

    # End run
    tracker.end_run(status="FINISHED")
    logger.info(f"✅ Completed test run")


def test_pipeline_with_mlflow():
    """Test full pipeline with MLflow tracking"""
    logger.info("\n" + "="*70)
    logger.info("TEST 2: Full Pipeline with MLflow Tracking")
    logger.info("="*70)

    config = Config()
    pipeline = ETLPipeline(config=config, enable_mlflow=True)

    logger.info("🚀 Starting ETL pipeline with MLflow tracking enabled...")
    success = pipeline.run()

    if success:
        logger.info("\n✅ Pipeline completed successfully!")
        logger.info(
            f"   Duration: {pipeline.execution_log['duration_seconds']:.2f}s")
        logger.info(f"   MLflow Run ID: {pipeline.mlflow_run_id}")
        logger.info(
            f"   View in MLflow UI: {pipeline.mlflow_tracker.get_run_url()}")
    else:
        logger.error("\n❌ Pipeline failed")
        return 1

    pipeline.print_summary()
    pipeline.save_execution_log()

    return 0


def main():
    """Run all tests"""
    logger.info("\n" + "🧪 "*30)
    logger.info("MLFLOW INTEGRATION TEST SUITE")
    logger.info("🧪 "*30)

    try:
        # Test 1: Direct tracker usage
        test_mlflow_tracker()

        # Test 2: Full pipeline integration
        test_pipeline_with_mlflow()

        logger.info("\n" + "="*70)
        logger.info("✅ ALL TESTS PASSED")
        logger.info("="*70)
        logger.info("\n📊 MLflow experiments stored in: ./mlruns/")
        logger.info("🔬 To view results, run: mlflow ui")
        logger.info("   Then navigate to: http://localhost:5000\n")

        return 0

    except Exception as e:
        logger.error(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
