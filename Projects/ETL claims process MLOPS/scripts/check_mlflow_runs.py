import mlflow
import json
import pandas as pd

mlflow.set_tracking_uri("sqlite:///./mlflow/mlflow.db")

# Search runs
runs = mlflow.search_runs(experiment_names=["insurance-etl-pipeline"])
print(f"\n{'='*70}")
print(f"MLflow Runs Report")
print(f"{'='*70}\n")
print(f"Total runs: {len(runs)}\n")

if len(runs) > 0:
    for idx, row in runs.iterrows():
        run_id = row['run_id']
        status = row['status']
        params = {col: row[col]
                  for col in runs.columns if col.startswith('params.')}
        metrics = {col: row[col]
                   for col in runs.columns if col.startswith('metrics.')}

        print(f"Run {idx+1}:")
        print(f"  Run ID: {run_id}")
        print(f"  Status: {status}")
        print(f"  Parameters: {len(params)}")
        for key, val in list(params.items())[:5]:
            print(f"    • {key}: {val}")
        print(
            f"  Metrics: {len([m for m in metrics if pd.notna(metrics[m])])}")
        metric_vals = {k: v for k, v in metrics.items() if pd.notna(v)}
        for key in list(metric_vals.keys())[:10]:
            print(f"    • {key}: {metrics[key]}")
        print()
else:
    print("❌ No runs found in database")

print(f"{'='*70}\n")
