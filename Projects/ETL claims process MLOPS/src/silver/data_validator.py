"""
Data validation and quality checks for Silver layer
"""
import pandas as pd
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Centralized data quality validation.

    Validates:
    - Record counts (no data loss)
    - Null percentages (acceptable data quality)
    - Duplicate counts
    - Value ranges and distributions
    """

    @staticmethod
    def check_row_count(df_before: pd.DataFrame, df_after: pd.DataFrame,
                        tolerance: float = 0.95) -> Tuple[bool, Dict]:
        """
        Check that data transformation didn't lose too many rows.

        Args:
            df_before: DataFrame before transformation
            df_after: DataFrame after transformation
            tolerance: Acceptable ratio (0.95 = 95% of rows must remain)

        Returns:
            (is_valid, metrics_dict)
        """
        before_count = len(df_before)
        after_count = len(df_after)

        if before_count == 0:
            return False, {
                "rows_before": 0,
                "rows_after": 0,
                "retention_rate": None,
                "status": "No input data",
            }

        retention_rate = after_count / before_count
        is_valid = retention_rate >= tolerance

        metrics = {
            "rows_before": before_count,
            "rows_after": after_count,
            "rows_lost": before_count - after_count,
            "retention_rate": round(retention_rate, 4),
            "status": "PASS" if is_valid else "FAIL",
        }

        if not is_valid:
            logger.warning(
                f"Row count check FAILED: {metrics['retention_rate']*100:.1f}% retention "
                f"({after_count}/{before_count})"
            )
        else:
            logger.info(
                f"Row count check PASSED: {metrics['retention_rate']*100:.1f}% retention"
            )

        return is_valid, metrics

    @staticmethod
    def check_null_percentages(df: pd.DataFrame,
                               max_null_percent: Dict[str, float] = None) -> Tuple[bool, Dict]:
        """
        Check that null values are within acceptable thresholds.

        Args:
            df: DataFrame to validate
            max_null_percent: Dict of column -> max_null_pct (0-100)
                             Default: 20% for most columns, 10% for critical

        Returns:
            (is_valid, metrics_dict)
        """
        if max_null_percent is None:
            # Default thresholds
            critical_cols = {"claim_id", "policy_id", "submitted_date"}
            max_null_percent = {
                col: 10 if col in critical_cols else 20
                for col in df.columns
            }

        metrics = {
            "column_nulls": {},
            "violations": [],
            "status": "PASS",
        }

        total_rows = len(df)

        for col in df.columns:
            null_count = df[col].isna().sum()
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0

            metrics["column_nulls"][col] = {
                "null_count": int(null_count),
                "null_percent": round(null_pct, 2),
            }

            threshold = max_null_percent.get(col, 20)

            if null_pct > threshold:
                metrics["violations"].append({
                    "column": col,
                    "null_percent": round(null_pct, 2),
                    "threshold": threshold,
                })
                metrics["status"] = "FAIL"
                logger.warning(
                    f"Column '{col}' has {null_pct:.1f}% nulls (threshold: {threshold}%)"
                )

        is_valid = metrics["status"] == "PASS"
        return is_valid, metrics

    @staticmethod
    def check_duplicates(df: pd.DataFrame, key_column: str = "claim_id") -> Tuple[bool, Dict]:
        """
        Check for duplicate primary keys.
        """
        if key_column not in df.columns:
            return True, {
                "key_column": key_column,
                "status": "SKIP - column not found",
            }

        total_rows = len(df)
        unique_keys = df[key_column].nunique()
        duplicates = total_rows - unique_keys

        metrics = {
            "key_column": key_column,
            "total_rows": total_rows,
            "unique_keys": unique_keys,
            "duplicate_rows": duplicates,
            "duplicate_percent": round(duplicates / total_rows * 100, 2) if total_rows > 0 else 0,
            "status": "PASS" if duplicates == 0 else "FAIL",
        }

        if duplicates > 0:
            logger.warning(
                f"Found {duplicates} duplicate {key_column} values "
                f"({metrics['duplicate_percent']}% of data)"
            )
        else:
            logger.info("No duplicates found")

        is_valid = duplicates == 0
        return is_valid, metrics

    @staticmethod
    def check_value_distribution(df: pd.DataFrame, column: str,
                                 allowed_values: List = None) -> Tuple[bool, Dict]:
        """
        Check that categorical column only has expected values.

        Args:
            df: DataFrame
            column: Column to check
            allowed_values: List of valid values
        """
        if column not in df.columns:
            return True, {
                "column": column,
                "status": "SKIP - column not found",
            }

        if allowed_values is None:
            return True, {
                "column": column,
                "status": "SKIP - no allowed values specified",
            }

        allowed_set = set(allowed_values)
        actual_values = set(df[column].dropna().unique())
        invalid_values = actual_values - allowed_set

        metrics = {
            "column": column,
            "allowed_values": sorted(allowed_set),
            "actual_values": sorted(actual_values),
            "invalid_values": sorted(invalid_values),
            "invalid_count": len(df[df[column].isin(invalid_values)]),
            "status": "PASS" if len(invalid_values) == 0 else "FAIL",
        }

        if invalid_values:
            logger.warning(
                f"Column '{column}' has invalid values: {invalid_values}"
            )

        is_valid = len(invalid_values) == 0
        return is_valid, metrics

    @staticmethod
    def run_all_checks(df: pd.DataFrame, df_before: pd.DataFrame = None,
                       config: Dict = None) -> Dict:
        """
        Run all validation checks - main entry point.

        Returns:
            Validation report dict
        """
        if config is None:
            config = {}

        report = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "checks": {},
            "overall_status": "PASS",
        }

        # Row count check (if before data provided)
        if df_before is not None:
            valid, metrics = DataValidator.check_row_count(df_before, df)
            report["checks"]["row_count"] = metrics
            if not valid:
                report["overall_status"] = "FAIL"

        # Null percentages check
        valid, metrics = DataValidator.check_null_percentages(df)
        report["checks"]["null_check"] = metrics
        if not valid:
            report["overall_status"] = "FAIL"

        # Duplicates check
        valid, metrics = DataValidator.check_duplicates(df)
        report["checks"]["duplicates"] = metrics
        if not valid:
            report["overall_status"] = "FAIL"

        return report

    @staticmethod
    def print_report(report: Dict) -> None:
        """Pretty-print validation report"""
        print("\n" + "="*60)
        print("DATA VALIDATION REPORT")
        print("="*60)
        print(f"Status: {report['overall_status']}")
        print(
            f"Rows: {report['total_rows']}, Columns: {report['total_columns']}")

        for check_name, check_result in report["checks"].items():
            status = check_result.get("status", "SKIP")
            print(f"\n✓ {check_name.upper()}: {status}")

            for key, value in check_result.items():
                if key != "status" and isinstance(value, (int, float, str)):
                    print(f"    {key}: {value}")

        print("\n" + "="*60 + "\n")
