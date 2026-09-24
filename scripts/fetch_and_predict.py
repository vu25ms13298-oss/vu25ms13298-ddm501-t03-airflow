"""
Pull the registered WDBC model back from MLflow and score test rows.

    python scripts/fetch_and_predict.py
    python scripts/fetch_and_predict.py --version 2 --ds 2026-08-25 --rows 10
    python scripts/fetch_and_predict.py --json              # JSON output format

Talks only to MLflow's client API (tracking server + model registry) -- no
Airflow, no copied model file. Needs the same MLFLOW_TRACKING_URI as the
DAG's train task; see docker-compose.yml.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd

PROJECT = Path(__file__).resolve().parents[1]
STAGING = PROJECT / "data" / "staging"
MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "wdbc-classifier")


def latest_version(client: mlflow.MlflowClient, name: str) -> int:
    """Get the latest registered model version, with error handling."""
    try:
        versions = client.get_registered_model(name).latest_versions
        if not versions:
            raise SystemExit(f"❌ No versions registered for {name!r} yet — run the DAG first")
        return max(int(v.version) for v in versions)
    except Exception as e:
        raise SystemExit(f"❌ Failed to get model version: {e}")


def latest_ds() -> str:
    """Find the newest run folder with test data."""
    try:
        if not STAGING.exists():
            raise SystemExit(f"❌ Staging dir missing: {STAGING}")
        dates = sorted(p.name for p in STAGING.iterdir() if (p / "test.parquet").exists())
        if not dates:
            raise SystemExit(f"❌ No run folders with test.parquet under {STAGING}")
        return dates[-1]
    except Exception as e:
        raise SystemExit(f"❌ Failed to find run folder: {e}")


def validate_args(args) -> None:
    """Validate command-line arguments."""
    if args.version is not None and args.version < 1:
        raise SystemExit("❌ --version must be >= 1")
    if args.rows < 1:
        raise SystemExit("❌ --rows must be >= 1")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", type=int, help="model version to load (default: latest)")
    parser.add_argument("--ds", help="staging date folder to score (default: newest)")
    parser.add_argument("--rows", type=int, default=5, help="how many test rows to score")
    parser.add_argument("--json", action="store_true", help="output predictions as JSON")
    args = parser.parse_args()

    validate_args(args)

    try:
        # Setup MLflow
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:15030"))
        client = mlflow.MlflowClient()
        version = args.version or latest_version(client, MODEL_NAME)

        # Load model
        model_uri = f"models:/{MODEL_NAME}/{version}"
        try:
            model = mlflow.sklearn.load_model(model_uri)
        except Exception as e:
            raise SystemExit(f"❌ Failed to load model {model_uri}: {e}")

        if not args.json:
            print(f"✓ loaded {model_uri}")

        # Load test data
        ds = args.ds or latest_ds()
        test_path = STAGING / ds / "test.parquet"
        if not test_path.exists():
            raise SystemExit(f"❌ Test file missing: {test_path}")

        test = pd.read_parquet(test_path).head(args.rows)
        feature_cols = [c for c in test.columns if c not in ("sample_id", "diagnosis")]

        # Make predictions
        proba = model.predict_proba(test[feature_cols])[:, 1]
        predictions = []
        mismatches = 0

        for sample_id, actual, p in zip(test["sample_id"], test["diagnosis"], proba):
            predicted = "M" if p >= 0.5 else "B"
            is_match = predicted == actual
            if not is_match:
                mismatches += 1

            pred_dict = {
                "sample_id": str(sample_id),
                "actual": actual,
                "predicted": predicted,
                "p_malignant": round(float(p), 4),
                "match": is_match
            }
            predictions.append(pred_dict)

            if not args.json:
                flag = "" if is_match else "  (mismatch)"
                print(f"{sample_id}  actual={actual}  predicted={predicted}  "
                      f"p(malignant)={p:.4f}{flag}")

        # Output summary
        if args.json:
            # JSON output format
            output = {
                "model": model_uri,
                "run_date": ds,
                "rows_scored": len(predictions),
                "accuracy": round((len(predictions) - mismatches) / len(predictions), 4),
                "mismatches": mismatches,
                "predictions": predictions
            }
            print(json.dumps(output, indent=2))
        else:
            # Text summary
            accuracy = round((len(predictions) - mismatches) / len(predictions), 4)
            print(f"\n📊 Summary: scored {len(predictions)} rows, accuracy={accuracy} ({mismatches} mismatches)")

    except SystemExit:
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
