"""
MLflow utilities for experiment tracking and metric logging
"""
import logging
import json
from typing import Dict, Any, Optional
import mlflow
from config.config import Config

logger = logging.getLogger(__name__)


class MLflowTracker:
    """Manages MLflow experiment tracking and metric logging"""

    def __init__(self, config: Config):
        """
        Initialize MLflow tracker

        Args:
            config: Configuration object with MLflow settings
        """
        self.config = config
        self._setup_mlflow()

    def _setup_mlflow(self) -> None:
        """Set up MLflow tracking URI and experiment"""
        try:
            # Set tracking URI to local SQLite database
            mlflow.set_tracking_uri(self.config.MLFLOW_TRACKING_URI)
            logger.info(
                f"✓ MLflow tracking URI set to: {self.config.MLFLOW_TRACKING_URI}")

            # Set or create experiment
            try:
                mlflow.set_experiment(self.config.MLFLOW_EXPERIMENT_NAME)
                logger.info(
                    f"✓ MLflow experiment set to: {self.config.MLFLOW_EXPERIMENT_NAME}")
            except Exception as e:
                logger.warning(f"Could not set experiment: {e}")
                mlflow.set_experiment(self.config.MLFLOW_EXPERIMENT_NAME)

        except Exception as e:
            logger.error(f"Failed to set up MLflow: {e}")
            raise

    def start_run(self, run_name: str, description: str = "", tags: Optional[Dict[str, str]] = None) -> str:
        """
        Start a new MLflow run

        Args:
            run_name: Name of the run
            description: Description of the run
            tags: Optional dictionary of tags

        Returns:
            Run ID
        """
        try:
            mlflow.start_run(run_name=run_name)
            run_id = mlflow.active_run().info.run_id
            logger.info(f"✓ Started MLflow run: {run_name} (ID: {run_id})")

            # Log description as a parameter
            if description:
                mlflow.log_param("description", description)

            # Log tags
            if tags:
                mlflow.set_tags(tags)
                logger.info(f"✓ Logged tags: {tags}")

            return run_id

        except Exception as e:
            logger.error(f"Failed to start MLflow run: {e}")
            raise

    def end_run(self, status: str = "FINISHED") -> None:
        """
        End the current MLflow run

        Args:
            status: Run status (FINISHED, FAILED)
        """
        try:
            mlflow.end_run(status=status)
            logger.info(f"✓ Ended MLflow run with status: {status}")
        except Exception as e:
            logger.error(f"Failed to end MLflow run: {e}")

    def log_phase_metrics(self, phase_name: str, metrics: Dict[str, Any]) -> None:
        """
        Log metrics for a pipeline phase

        Args:
            phase_name: Name of the pipeline phase
            metrics: Dictionary of metrics to log
        """
        try:
            for key, value in metrics.items():
                if isinstance(value, (int, float)):
                    metric_key = f"{phase_name.lower()}_{key}"
                    mlflow.log_metric(metric_key, value)
                    logger.debug(f"  Logged metric: {metric_key} = {value}")

            logger.info(
                f"✓ Logged {len(metrics)} metrics for phase: {phase_name}")

        except Exception as e:
            logger.error(f"Failed to log phase metrics: {e}")

    def log_metric(self, key: str, value: float) -> None:
        """
        Log a single metric

        Args:
            key: Metric key
            value: Metric value
        """
        try:
            mlflow.log_metric(key, value)
            logger.debug(f"Logged metric: {key} = {value}")
        except Exception as e:
            logger.error(f"Failed to log metric {key}: {e}")

    def log_data_quality_metrics(self, df_name: str, row_count: int, null_counts: Dict[str, int],
                                 duplicates: int) -> None:
        """
        Log data quality metrics

        Args:
            df_name: Name of the dataframe
            row_count: Total number of rows
            null_counts: Dictionary of null counts per column
            duplicates: Number of duplicate rows
        """
        try:
            mlflow.log_metric(f"{df_name}_row_count", row_count)
            mlflow.log_metric(f"{df_name}_duplicate_count", duplicates)

            # Log null metrics for each column
            for col, count in null_counts.items():
                null_pct = (count / row_count * 100) if row_count > 0 else 0
                mlflow.log_metric(f"{df_name}_null_{col}", null_pct)

            logger.info(f"✓ Logged data quality metrics for: {df_name}")
            logger.info(
                f"  - Rows: {row_count}, Duplicates: {duplicates}, Nulls: {len(null_counts)} columns")

        except Exception as e:
            logger.error(f"Failed to log data quality metrics: {e}")

    def log_pipeline_config(self, config_dict: Dict[str, Any]) -> None:
        """
        Log pipeline configuration as parameters

        Args:
            config_dict: Configuration dictionary
        """
        try:
            # Log important config settings as parameters
            important_keys = [
                'spark_driver_memory',
                'num_test_records',
                'bronze_path',
                'silver_path',
                'gold_path'
            ]

            for key in important_keys:
                if key in config_dict:
                    value = str(config_dict[key])
                    # Shorten paths for readability
                    if 'path' in key:
                        value = value.split(
                            '\\')[-1] if '\\' in value else value
                    mlflow.log_param(key, value)

            logger.info(f"✓ Logged pipeline configuration")

        except Exception as e:
            logger.error(f"Failed to log pipeline config: {e}")

    def log_artifact_metadata(self, artifact_type: str, path: str, record_count: int,
                              columns: list, size_mb: float) -> None:
        """
        Log metadata about output artifacts

        Args:
            artifact_type: Type of artifact (bronze, silver, gold)
            path: Path to artifact
            record_count: Number of records
            columns: List of column names
            size_mb: File size in MB
        """
        try:
            metadata = {
                "artifact_type": artifact_type,
                "path": str(path),
                "record_count": record_count,
                "num_columns": len(columns),
                "columns": columns,
                "size_mb": round(size_mb, 2)
            }

            # Log as parameters
            mlflow.log_param(f"{artifact_type}_record_count", record_count)
            mlflow.log_param(f"{artifact_type}_num_columns", len(columns))
            mlflow.log_param(f"{artifact_type}_size_mb", round(size_mb, 2))

            # Log full metadata as artifact JSON
            artifact_file = f"/tmp/{artifact_type}_metadata.json"
            with open(artifact_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            mlflow.log_artifact(artifact_file, f"{artifact_type}_metadata")

            logger.info(f"✓ Logged artifact metadata for: {artifact_type}")
            logger.info(
                f"  - Records: {record_count}, Columns: {len(columns)}, Size: {size_mb:.2f} MB")

        except Exception as e:
            logger.error(f"Failed to log artifact metadata: {e}")

    def log_execution_summary(self, total_duration: float, phase_timings: Dict[str, float],
                              status: str) -> None:
        """
        Log final execution summary

        Args:
            total_duration: Total pipeline execution time
            phase_timings: Dictionary of phase timings
            status: Final status (SUCCESS, FAILED)
        """
        try:
            mlflow.log_metric("total_duration_seconds", total_duration)
            mlflow.log_param("final_status", status)

            # Log individual phase timings
            for phase, duration in phase_timings.items():
                mlflow.log_metric(
                    f"phase_{phase.lower()}_duration_seconds", duration)

            logger.info(f"✓ Logged execution summary")
            logger.info(
                f"  - Total Duration: {total_duration:.2f}s, Status: {status}")
            logger.info(f"  - Phase Timings: {phase_timings}")

        except Exception as e:
            logger.error(f"Failed to log execution summary: {e}")

    @staticmethod
    def get_active_run_info() -> Optional[Dict[str, Any]]:
        """
        Get information about active run

        Returns:
            Dictionary with run info or None
        """
        try:
            active_run = mlflow.active_run()
            if active_run:
                return {
                    "run_id": active_run.info.run_id,
                    "experiment_id": active_run.info.experiment_id,
                    "run_name": active_run.info.run_name,
                    "status": active_run.info.status
                }
            return None

        except Exception as e:
            logger.error(f"Failed to get active run info: {e}")
            return None
