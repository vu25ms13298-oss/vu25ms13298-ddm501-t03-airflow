"""
Pull the registered WDBC model back from MLflow and score a few test rows.

    python scripts/fetch_and_predict.py
    python scripts/fetch_and_predict.py --version 2 --ds 2026-08-25 --rows 10

Talks only to MLflow's client API (tracking server + model registry) -- no
Airflow, no copied model file. Needs the same MLFLOW_TRACKING_URI / AWS_*
env vars as the DAG's train task; see docker-compose.yml.
"""
import argparse
import os
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd

PROJECT = Path(__file__).resolve().parents[1]
STAGING = PROJECT / "data" / "staging"
MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "wdbc-classifier")


def latest_version(client: mlflow.MlflowClient, name: str) -> int:
    versions = client.get_registered_model(name).latest_versions
    if not versions:
        raise SystemExit(f"no versions registered for {name!r} yet -- run the DAG first")
    return max(int(v.version) for v in versions)


def latest_ds() -> str:
    dates = sorted(p.name for p in STAGING.iterdir() if (p / "test.parquet").exists())
    if not dates:
        raise SystemExit(f"no run folders with test.parquet under {STAGING}")
    return dates[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", type=int, help="model version to load (default: latest)")
    parser.add_argument("--ds", help="staging date folder to score (default: newest)")
    parser.add_argument("--rows", type=int, default=5, help="how many test rows to score")
    args = parser.parse_args()

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:15020"))
    client = mlflow.MlflowClient()
    version = args.version or latest_version(client, MODEL_NAME)

    model_uri = f"models:/{MODEL_NAME}/{version}"
    model = mlflow.sklearn.load_model(model_uri)
    print(f"loaded {model_uri}")

    ds = args.ds or latest_ds()
    test = pd.read_parquet(STAGING / ds / "test.parquet").head(args.rows)
    feature_cols = [c for c in test.columns if c not in ("sample_id", "diagnosis")]

    proba = model.predict_proba(test[feature_cols])[:, 1]
    for sample_id, actual, p in zip(test["sample_id"], test["diagnosis"], proba):
        predicted = "M" if p >= 0.5 else "B"
        flag = "" if predicted == actual else "  (mismatch)"
        print(f"{sample_id}  actual={actual}  predicted={predicted}  "
              f"p(malignant)={p:.4f}{flag}")


if __name__ == "__main__":
    main()
