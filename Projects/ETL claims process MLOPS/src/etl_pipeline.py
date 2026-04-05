"""
End-to-end ETL Pipeline Orchestration for Insurance Claims

Orchestrates: Bronze Ingestion → Silver Transformation → Gold Aggregation

This is the main entry point for the medallion architecture pipeline.
Can be scheduled in Airflow, Databricks Jobs, or run manually.
"""
from src.gold.claim_metrics import ClaimMetrics
from src.silver.claim_transformer import ClaimTransformer
from src.bronze.schema_definitions import get_claims_schema
from src.bronze.csv_ingester import CSVIngester
from src.utils.mlflow_utils import MLflowTracker
from config.config import Config
import sys
from pathlib import Path
import time
from datetime import datetime
from typing import Dict, Any, Tuple
import logging
import json
import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parents[1]))


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Main pipeline orchestrator.

    Responsibilities:
    1. Load configuration
    2. Execute Bronze → Silver → Gold transformations
    3. Save outputs to disk
    4. Report metrics and status
    5. Handle errors gracefully
    """

    def __init__(self, config: Config = None, enable_mlflow: bool = True):
        """Initialize pipeline"""
        self.config = config or Config()
        self.enable_mlflow = enable_mlflow
        self.mlflow_tracker = None
        self.mlflow_run_id = None

        if self.enable_mlflow:
            try:
                self.mlflow_tracker = MLflowTracker(config=self.config)
                logger.info("✓ MLflow tracker initialized")
            except Exception as e:
                logger.warning(f"⚠ Could not initialize MLflow: {e}")
                self.mlflow_tracker = None

        self.execution_log = {
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "status": "INITIALIZED",
            "phases": {},
            "errors": [],
        }

    def phase_start(self, phase_name: str) -> float:
        """Record phase start time"""
        start_time = time.time()
        logger.info(f"\n{'='*70}")
        logger.info(f"PHASE: {phase_name}")
        logger.info(f"{'='*70}")
        self.execution_log["phases"][phase_name] = {
            "start_time": start_time,
            "status": "RUNNING",
        }
        return start_time

    def phase_end(self, phase_name: str, start_time: float, status: str = "SUCCESS"):
        """Record phase end time"""
        duration = time.time() - start_time
        self.execution_log["phases"][phase_name]["end_time"] = time.time()
        self.execution_log["phases"][phase_name]["duration_seconds"] = duration
        self.execution_log["phases"][phase_name]["status"] = status
        logger.info(f"✅ {phase_name} completed in {duration:.2f}s\n")

    def run_bronze_layer(self) -> Tuple[pd.DataFrame, bool]:
        """
        Bronze Layer: Ingest raw data

        Returns:
            (DataFrame, success_flag)
        """
        phase_name = "BRONZE_INGESTION"
        start = self.phase_start(phase_name)

        try:
            logger.info("Loading raw claims data from CSV...")

            # Ingest claims
            claims_ingester = CSVIngester(
                spark=None,  # We're using Pandas locally
                input_path=str(self.config.DATA_ROOT /
                               "bronze" / "claims.csv"),
                output_path=str(self.config.BRONZE_PATH / "claims"),
                schema=get_claims_schema(),
                source_name="claims_csv",
            )

            # Load with Pandas since Spark has Java issues on Windows
            df_bronze = pd.read_csv(
                str(self.config.DATA_ROOT / "bronze" / "claims.csv")
            )

            logger.info(f"✓ Loaded {len(df_bronze):,} claim records")
            logger.info(f"✓ Columns: {', '.join(df_bronze.columns)}")

            # Add metadata (simulating Bronze layer behavior)
            df_bronze["_ingested_at"] = datetime.utcnow().isoformat()
            df_bronze["_source_name"] = "claims_csv"
            df_bronze["_record_id"] = range(len(df_bronze))

            # Log metrics to MLflow
            if self.mlflow_tracker:
                duration = time.time() - start
                metrics = {
                    "row_count": len(df_bronze),
                    "column_count": len(df_bronze.columns),
                    "duration_seconds": duration
                }
                self.mlflow_tracker.log_phase_metrics(phase_name, metrics)

            self.phase_end(phase_name, start, "SUCCESS")
            return df_bronze, True

        except Exception as e:
            logger.error(f"❌ Bronze layer failed: {str(e)}")
            self.execution_log["errors"].append({
                "phase": phase_name,
                "error": str(e),
            })

            # Log failure to MLflow
            if self.mlflow_tracker:
                duration = time.time() - start
                metrics = {
                    "row_count": 0,
                    "duration_seconds": duration,
                    "status": "FAILED"
                }
                self.mlflow_tracker.log_phase_metrics(phase_name, metrics)

            self.phase_end(phase_name, start, "FAILED")
            return None, False

    def run_silver_layer(self, df_bronze: pd.DataFrame) -> Tuple[pd.DataFrame, bool]:
        """
        Silver Layer: Clean and transform data

        Returns:
            (DataFrame, success_flag)
        """
        phase_name = "SILVER_TRANSFORMATION"
        start = self.phase_start(phase_name)

        try:
            logger.info("Transforming Bronze → Silver...")

            transformer = ClaimTransformer()
            result = transformer.transform(df_bronze, validate=False)

            if result["status"] != "SUCCESS":
                raise Exception(
                    f"Transformation failed: {result.get('error')}")

            df_silver = result["transformed_df"]
            logger.info(f"✓ Transformed {len(df_silver):,} records")
            logger.info(f"✓ Added {len(df_silver.columns) - len(df_bronze.columns)} "
                        f"derived features")

            # Log metrics to MLflow
            if self.mlflow_tracker:
                duration = time.time() - start
                duplicates = len(df_silver) - len(df_silver.drop_duplicates())
                metrics = {
                    "row_count_input": len(df_bronze),
                    "row_count_output": len(df_silver),
                    "retention_percentage": (len(df_silver) / len(df_bronze)) * 100 if len(df_bronze) > 0 else 0,
                    "duplicates_removed": duplicates,
                    "column_count": len(df_silver.columns),
                    "duration_seconds": duration
                }
                self.mlflow_tracker.log_phase_metrics(phase_name, metrics)

            self.phase_end(phase_name, start, "SUCCESS")
            return df_silver, True

        except Exception as e:
            logger.error(f"❌ Silver layer failed: {str(e)}")
            self.execution_log["errors"].append({
                "phase": phase_name,
                "error": str(e),
            })

            # Log failure to MLflow
            if self.mlflow_tracker:
                duration = time.time() - start
                metrics = {
                    "row_count": 0,
                    "duration_seconds": duration,
                    "status": "FAILED"
                }
                self.mlflow_tracker.log_phase_metrics(phase_name, metrics)

            self.phase_end(phase_name, start, "FAILED")
            return None, False

    def run_gold_layer(self, df_silver: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], bool]:
        """
        Gold Layer: Generate analytics tables and metrics

        Returns:
            (metrics_dict, success_flag)
        """
        phase_name = "GOLD_METRICS_GENERATION"
        start = self.phase_start(phase_name)

        try:
            logger.info("Generating Gold layer metrics...")

            metrics_gen = ClaimMetrics()
            metrics = metrics_gen.generate_all_metrics(df_silver)

            logger.info(f"✓ Generated {len(metrics)} analytics tables:")
            for table_name, table_df in metrics.items():
                logger.info(f"  • {table_name}: {len(table_df):,} rows, "
                            f"{len(table_df.columns)} columns")

            # Log metrics to MLflow
            if self.mlflow_tracker:
                duration = time.time() - start
                total_gold_rows = sum(len(df) for df in metrics.values())
                num_tables = len(metrics)

                metrics_log = {
                    "total_rows": total_gold_rows,
                    "num_tables": num_tables,
                    "duration_seconds": duration
                }
                self.mlflow_tracker.log_phase_metrics(phase_name, metrics_log)

                # Log individual table details
                for table_name, table_df in metrics.items():
                    self.mlflow_tracker.log_metric(
                        f"gold_{table_name}_rows", len(table_df))

            self.phase_end(phase_name, start, "SUCCESS")
            return metrics, True

        except Exception as e:
            logger.error(f"❌ Gold layer failed: {str(e)}")
            self.execution_log["errors"].append({
                "phase": phase_name,
                "error": str(e),
            })

            # Log failure to MLflow
            if self.mlflow_tracker:
                duration = time.time() - start
                metrics = {
                    "row_count": 0,
                    "duration_seconds": duration,
                    "status": "FAILED"
                }
                self.mlflow_tracker.log_phase_metrics(phase_name, metrics)

            self.phase_end(phase_name, start, "FAILED")
            return None, False

    def save_outputs(self, df_silver: pd.DataFrame,
                     metrics: Dict[str, pd.DataFrame]) -> bool:
        """
        Save Silver and Gold layer outputs to disk
        """
        phase_name = "SAVE_OUTPUTS"
        start = self.phase_start(phase_name)

        try:
            logger.info("Saving Silver and Gold outputs...")

            # Save Silver
            silver_path = self.config.SILVER_PATH / "claims_silver.csv"
            df_silver.to_csv(silver_path, index=False)
            logger.info(f"✓ Saved Silver layer: {silver_path}")

            # Save Gold tables
            for table_name, table_df in metrics.items():
                output_path = self.config.GOLD_PATH / f"{table_name}.csv"
                table_df.to_csv(output_path, index=False)
                logger.info(f"✓ Saved {table_name}: {output_path}")

            self.phase_end(phase_name, start, "SUCCESS")
            return True

        except Exception as e:
            logger.error(f"❌ Save outputs failed: {str(e)}")
            self.execution_log["errors"].append({
                "phase": phase_name,
                "error": str(e),
            })
            self.phase_end(phase_name, start, "FAILED")
            return False

    def run(self) -> bool:
        """
        Execute full pipeline: Bronze → Silver → Gold
        """
        self.execution_log["start_time"] = time.time()

        # Start MLflow run
        if self.mlflow_tracker:
            run_name = f"pipeline-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            tags = {
                "pipeline_type": "etl",
                "environment": "local",
                "mlflow_enabled": str(self.enable_mlflow)
            }
            self.mlflow_run_id = self.mlflow_tracker.start_run(
                run_name=run_name,
                description="Insurance Claims ETL Pipeline Execution",
                tags=tags
            )
            logger.info(
                f"\n🔬 MLflow tracking started (Run ID: {self.mlflow_run_id})")

            # Log config to MLflow
            self.mlflow_tracker.log_pipeline_config(self.config.to_dict())

        print("\n" + "="*70)
        print("INSURANCE CLAIMS ETL PIPELINE")
        print("="*70)

        # Phase 1: Bronze
        df_bronze, bronze_ok = self.run_bronze_layer()
        if not bronze_ok:
            self.execution_log["status"] = "FAILED"
            if self.mlflow_tracker:
                self.mlflow_tracker.end_run(status="FAILED")
            return False

        # Phase 2: Silver
        df_silver, silver_ok = self.run_silver_layer(df_bronze)
        if not silver_ok:
            self.execution_log["status"] = "FAILED"
            if self.mlflow_tracker:
                self.mlflow_tracker.end_run(status="FAILED")
            return False

        # Phase 3: Gold
        metrics, gold_ok = self.run_gold_layer(df_silver)
        if not gold_ok:
            self.execution_log["status"] = "FAILED"
            if self.mlflow_tracker:
                self.mlflow_tracker.end_run(status="FAILED")
            return False

        # Phase 4: Save outputs
        save_ok = self.save_outputs(df_silver, metrics)
        if not save_ok:
            self.execution_log["status"] = "FAILED"
            if self.mlflow_tracker:
                self.mlflow_tracker.end_run(status="FAILED")
            return False

        # Success!
        self.execution_log["status"] = "SUCCESS"
        self.execution_log["end_time"] = time.time()
        self.execution_log["duration_seconds"] = (
            self.execution_log["end_time"] - self.execution_log["start_time"]
        )

        # Log final summary to MLflow
        if self.mlflow_tracker:
            phase_timings = {
                phase_name: phase_info.get("duration_seconds", 0)
                for phase_name, phase_info in self.execution_log["phases"].items()
            }
            self.mlflow_tracker.log_execution_summary(
                total_duration=self.execution_log.get("duration_seconds", 0),
                phase_timings=phase_timings,
                status="SUCCESS"
            )
            self.mlflow_tracker.end_run(status="FINISHED")
            logger.info(f"\n✅ MLflow run completed: {self.mlflow_run_id}")

        return True

    def print_summary(self):
        """Print execution summary"""
        print("\n" + "="*70)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*70)

        print(f"\nStatus: {self.execution_log['status']}")
        print(f"Total Duration: {self.execution_log['duration_seconds']:.2f}s")

        print("\nPhase Details:")
        for phase_name, phase_info in self.execution_log["phases"].items():
            status = phase_info["status"]
            duration = phase_info.get("duration_seconds", 0)
            status_symbol = "✅" if status == "SUCCESS" else "❌"
            print(f"  {status_symbol} {phase_name}: {duration:.2f}s - {status}")

        if self.execution_log["errors"]:
            print("\nErrors:")
            for error in self.execution_log["errors"]:
                print(f"  ❌ {error['phase']}: {error['error']}")

        print("\n" + "="*70 + "\n")

    def save_execution_log(self):
        """Save execution log to JSON file"""
        log_path = self.config.PROJECT_ROOT / "pipeline_execution.log"
        with open(log_path, "w") as f:
            json.dump(self.execution_log, f, indent=2, default=str)
        logger.info(f"Execution log saved to {log_path}")


def main():
    """Main entry point"""
    try:
        config = Config()
        pipeline = ETLPipeline(config)

        # Run pipeline
        success = pipeline.run()

        # Print summary
        pipeline.print_summary()

        # Save execution log
        pipeline.save_execution_log()

        if success:
            print("🎉 PIPELINE EXECUTION SUCCESSFUL\n")
            return 0
        else:
            print("⚠️  PIPELINE EXECUTION FAILED\n")
            return 1

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())
