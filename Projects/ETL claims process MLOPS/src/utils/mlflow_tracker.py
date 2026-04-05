"""
MLflow experiment tracking for insurance claims pipeline.

Provides unified logging for:
- Phase execution metrics (duration, record counts)
- Data quality metrics (retention %, nulls, duplicates)
- Transformation lineage and versioning
- Model-ready feature logging
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import mlflow
import mlflow.pyfunc
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)


class MLflowTracker:
    """
    Centralized MLflow experiment tracking for the ETL pipeline.

    Manages:
    - Experiment creation and run tracking
    - Phase-level metrics logging
    - Data quality validation metrics
    - Pipeline artifact versioning
    - Model registry preparation
    """

    def __init__(
        self,
        experiment_name: str = "insurance-claims-etl",
        tracking_uri: str = None,
        artifact_path: str = None
    ):
        """
        Initialize MLflow tracker.

        Args:
            experiment_name: Name of the MLflow experiment (default: insurance-claims-etl)
            tracking_uri: MLflow tracking server URI (default: local ./mlruns)
            artifact_path: Directory for storing artifacts (default: ./mlruns)
        """
        self.experiment_name = experiment_name

        # Set tracking URI (local SQLite backend by default)
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        else:
            # Local backend: SQLite DB at ./mlruns/mlflow.db
            local_uri = os.path.join(os.getcwd(), "mlruns")
            os.makedirs(local_uri, exist_ok=True)
            mlflow.set_tracking_uri(f"sqlite:///{local_uri}/mlflow.db")

        # Create or get experiment
        self.client = MlflowClient()
        try:
            self.experiment_id = self.client.get_experiment_by_name(
                experiment_name
            ).experiment_id
            logger.info(
                f"Using existing experiment: {experiment_name} (ID: {self.experiment_id})")
        except AttributeError:
            # Experiment doesn't exist, create it
            self.experiment_id = self.client.create_experiment(experiment_name)
            logger.info(
                f"Created new experiment: {experiment_name} (ID: {self.experiment_id})")

        self.active_run = None
        self.artifact_path = artifact_path or os.path.join(
            os.getcwd(), "mlruns")
        os.makedirs(self.artifact_path, exist_ok=True)

    def start_run(
        self,
        run_name: str = None,
        tags: Dict[str, str] = None,
        params: Dict[str, Any] = None
    ) -> str:
        """
        Start a new MLflow run for the pipeline execution.

        Args:
            run_name: Optional name for the run (e.g., timestamp-based)
            tags: Optional dict of tags (e.g., {"version": "1.0", "env": "local"})
            params: Optional dict of parameters (e.g., {"batch_size": 1000})

        Returns:
            run_id: The ID of the created run
        """
        # Default run name with timestamp
        if not run_name:
            run_name = f"pipeline-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

        self.active_run = mlflow.start_run(
            experiment_id=self.experiment_id,
            run_name=run_name
        )

        # Log default tags
        default_tags = {
            "pipeline": "insurance-claims-etl",
            "timestamp": datetime.now().isoformat(),
            "version": "1.0"
        }
        if tags:
            default_tags.update(tags)

        for key, value in default_tags.items():
            mlflow.set_tag(key, value)

        # Log parameters if provided
        if params:
            for key, value in params.items():
                mlflow.log_param(key, value)

        logger.info(
            f"Started MLflow run: {run_name} (ID: {self.active_run.info.run_id})")
        return self.active_run.info.run_id

    def log_phase_metrics(
        self,
        phase_name: str,
        duration_seconds: float,
        record_count: int,
        status: str = "SUCCESS"
    ) -> None:
        """
        Log metrics for a pipeline phase.

        Args:
            phase_name: Name of the phase (e.g., 'BRONZE_INGESTION', 'SILVER_TRANSFORMATION')
            duration_seconds: Execution time in seconds
            record_count: Number of records processed
            status: Phase status ('SUCCESS', 'FAILED', 'WARNING')
        """
        prefix = f"phase_{phase_name.lower()}"

        mlflow.log_metric(f"{prefix}_duration_seconds", duration_seconds)
        mlflow.log_metric(f"{prefix}_record_count", record_count)

        # Log throughput metric
        throughput = record_count / duration_seconds if duration_seconds > 0 else 0
        mlflow.log_metric(f"{prefix}_throughput_records_per_sec", throughput)

        # Log phase status as tag
        mlflow.set_tag(f"{prefix}_status", status)

        logger.info(
            f"Logged metrics for {phase_name}: {record_count} records in {duration_seconds:.2f}s")

    def log_data_quality_metrics(
        self,
        layer: str,
        metrics: Dict[str, Any]
    ) -> None:
        """
        Log data quality validation metrics.

        Args:
            layer: Layer name (e.g., 'silver', 'gold')
            metrics: Dict of quality metrics:
                - input_row_count
                - output_row_count
                - retention_percentage
                - null_percentages (dict by column)
                - duplicate_count
                - duplicate_percentage
        """
        prefix = f"quality_{layer}"

        mlflow.log_metric(f"{prefix}_input_records",
                          metrics.get('input_row_count', 0))
        mlflow.log_metric(f"{prefix}_output_records",
                          metrics.get('output_row_count', 0))
        mlflow.log_metric(f"{prefix}_retention_percentage",
                          metrics.get('retention_percentage', 100))
        mlflow.log_metric(f"{prefix}_duplicate_count",
                          metrics.get('duplicate_count', 0))
        mlflow.log_metric(f"{prefix}_duplicate_percentage",
                          metrics.get('duplicate_percentage', 0))

        logger.info(f"Logged data quality metrics for {layer} layer")

    def log_transformation_metadata(
        self,
        transformation_name: str,
        metadata: Dict[str, Any]
    ) -> None:
        """
        Log transformation lineage and metadata.

        Args:
            transformation_name: Name of the transformation (e.g., 'claim_standardization')
            metadata: Metadata dict including:
                - version: Transformation version
                - parameters: Transformation parameters
                - input_cols: List of input columns
                - output_cols: List of output columns
        """
        # Log as JSON artifact
        artifact_filename = f"{transformation_name}_metadata.json"
        artifact_path = os.path.join(self.artifact_path, artifact_filename)

        with open(artifact_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

        mlflow.log_artifact(artifact_path, artifact_type="metadata")
        logger.info(
            f"Logged transformation metadata for {transformation_name}")

    def log_gold_tables(
        self,
        tables_info: Dict[str, Dict[str, Any]]
    ) -> None:
        """
        Log information about generated gold layer tables.

        Args:
            tables_info: Dict mapping table names to info:
                {
                    "monthly_summary": {"row_count": 35, "columns": 4},
                    "high_value_claims": {"row_count": 596, "columns": 5},
                    ...
                }
        """
        prefix = "gold_tables"

        for table_name, info in tables_info.items():
            table_prefix = f"{prefix}_{table_name}"
            mlflow.log_metric(
                f"{table_prefix}_row_count",
                info.get('row_count', 0)
            )
            mlflow.log_metric(
                f"{table_prefix}_column_count",
                info.get('column_count', 0)
            )

        logger.info(f"Logged metrics for {len(tables_info)} gold tables")

    def log_artifact_file(
        self,
        local_path: str,
        artifact_type: str = "data"
    ) -> None:
        """
        Log a file as an MLflow artifact.

        Args:
            local_path: Path to the file to log
            artifact_type: Type of artifact (e.g., 'data', 'model', 'metrics')
        """
        if os.path.exists(local_path):
            mlflow.log_artifact(local_path, artifact_type=artifact_type)
            logger.info(f"Logged artifact: {local_path}")
        else:
            logger.warning(f"Artifact file not found: {local_path}")

    def end_run(
        self,
        status: str = "FINISHED",
        final_metrics: Optional[Dict[str, float]] = None
    ) -> str:
        """
        End the active MLflow run.

        Args:
            status: Final run status ('FINISHED', 'FAILED')
            final_metrics: Optional dict of final metrics to log before ending

        Returns:
            run_id: The ID of the completed run
        """
        if not self.active_run:
            logger.warning("No active run to end")
            return None

        if final_metrics:
            for key, value in final_metrics.items():
                mlflow.log_metric(key, value)

        mlflow.set_tag("final_status", status)
        run_id = self.active_run.info.run_id
        mlflow.end_run()
        self.active_run = None

        logger.info(f"Ended MLflow run: {run_id} with status {status}")
        return run_id

    def log_execution_summary(
        self,
        summary: Dict[str, Any]
    ) -> None:
        """
        Log overall pipeline execution summary.

        Args:
            summary: Dict with keys like:
                - total_duration_seconds
                - total_records_processed
                - phases
                - status
        """
        mlflow.log_metric("total_duration_seconds",
                          summary.get('total_duration_seconds', 0))
        mlflow.log_metric("total_records_processed",
                          summary.get('total_records_processed', 0))
        mlflow.set_tag("execution_status", summary.get('status', 'UNKNOWN'))

        logger.info("Logged execution summary")

    def get_run_url(self) -> Optional[str]:
        """Get the URL to view the current run in MLflow UI."""
        if not self.active_run:
            return None

        tracking_uri = mlflow.get_tracking_uri()
        run_id = self.active_run.info.run_id
        return f"{tracking_uri}/#/experiments/{self.experiment_id}/runs/{run_id}"


def initialize_mlflow() -> MLflowTracker:
    """
    Initialize MLflow tracker with default settings.

    Returns:
        MLflowTracker instance ready for logging
    """
    return MLflowTracker(
        experiment_name="insurance-claims-etl",
        tracking_uri=None,  # Uses local SQLite backend
        artifact_path=None  # Default ./mlruns
    )
