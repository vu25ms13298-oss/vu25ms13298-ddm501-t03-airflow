"""
Automated ML Pipeline: WDBC Breast Cancer Classification

A fully automated data pipeline that processes raw data, validates quality,
splits into train/test, applies feature scaling, trains a LogisticRegression
model, and registers it with MLflow for later inference.

Pipeline DAG:
    ingest → validate → split → scale → train → report

Key characteristics:
  - Deterministic split (hash-based, not random)
  - Idempotent (re-running same date overwrites only that date's outputs)
  - Self-contained MLflow server (no external dependencies)
  - Daily schedule with catchup disabled
  - Max 1 concurrent run to prevent race conditions
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score

log = logging.getLogger(__name__)

PROJECT = Path(__file__).resolve().parents[1]
RAW = PROJECT / "data" / "raw" / "wdbc.csv"
STAGING = PROJECT / "data" / "staging"

FEATURES_MIN = 0.0                 # every WDBC measurement is a non-negative size
LABELS = {"M", "B"}
MAX_BAD_FRACTION = 0.05            # above this the extract is not worth using
TEST_FRACTION = 0.20

# This project's own MLflow server (see docker-compose.yml's `mlflow`
# service) -- self-contained, no other tutorial's stack required.
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:15030")
MLFLOW_EXPERIMENT = "wdbc-pipeline"
MLFLOW_MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "wdbc-classifier")


def run_dir(ds: str) -> Path:
    """Create and return the staging directory for a given execution date (ds).

    Args:
        ds: Execution date in YYYY-MM-DD format

    Returns:
        Path: Directory for this run's outputs (e.g., data/staging/2026-08-25/)

    Note:
        One folder per logical date. Re-running a date overwrites its own folder
        and touches nothing else, which is what makes a re-run idempotent.
    """
    d = STAGING / ds
    d.mkdir(parents=True, exist_ok=True)
    return d


def on_failure_callback(context):
    """Callback executed when any task fails.

    Args:
        context: Airflow task context containing task_instance, exception, etc.
    """
    task_instance = context["task_instance"]
    exception = context.get("exception")
    log.error(
        "Task failed: %s in DAG %s (attempt %d/%d)",
        task_instance.task_id,
        task_instance.dag_id,
        task_instance.try_number,
        task_instance.max_tries,
    )
    if exception:
        log.error("Exception: %s", str(exception))


@dag(
    dag_id="wdbc_pipeline",
    description="WDBC: ingest, validate, split, scale, train, report model",
    schedule="@daily",
    start_date=datetime(2026, 8, 20),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,
        "on_failure_callback": on_failure_callback,
    },
    tags=["ddm501", "ml-pipeline", "wdbc"],
    owner="data-team",
)
def wdbc_pipeline():
    """WDBC Breast Cancer ML Pipeline.

    Processes raw WDBC dataset through multiple stages:
    1. Ingest: Load and snapshot raw data
    2. Validate: Check quality (nulls, negatives, duplicates, outliers)
    3. Split: Deterministic train/test split via hash
    4. Scale: Normalize features using training set statistics
    5. Train: Fit LogisticRegression and register with MLflow
    6. Report: Save summary and update history log
    """

    @task(task_id="ingest", doc="""Load raw WDBC data and snapshot it.

    Why snapshot: Reading the source again in a later task would mean two tasks
    seeing two different files if the source changes mid-run. Snapshot once to
    freeze data for this run.

    Output:
        dict with keys: rows (int), cols (int), path (str to raw.parquet)
    """)
    def ingest(ds: str = None) -> dict:
        log.info("Starting ingest for date=%s", ds)
        start_time = time.time()

        if not RAW.exists():
            raise AirflowFailException(f"source extract missing: {RAW}")

        frame = pd.read_csv(RAW)
        out = run_dir(ds) / "raw.parquet"
        frame.to_parquet(out, index=False)

        elapsed = time.time() - start_time
        log.info(
            "ingested %d rows, %d columns in %.2fs → %s",
            len(frame), frame.shape[1], elapsed, out
        )
        return {"rows": len(frame), "cols": frame.shape[1], "path": str(out)}

    @task(task_id="validate", doc="""Validate data quality and quarantine bad rows.

    Checks:
        - null: Missing values in numeric columns
        - negative: Feature values below FEATURES_MIN
        - bad_label: Diagnosis not in {M, B}
        - duplicate: Duplicate sample_ids
        - outlier: mean_area > 99th percentile * 20

    Fails if bad_fraction > MAX_BAD_FRACTION (5%), using AirflowFailException
    to skip retries (data corruption won't be fixed by retrying).

    Output:
        dict with validation counts and clean data path
    """)
    def validate(meta: dict, ds: str = None) -> dict:
        log.info("Starting validate for date=%s", ds)
        start_time = time.time()

        frame = pd.read_parquet(meta["path"])
        numeric = [c for c in frame.columns if c not in ("sample_id", "diagnosis")]

        problems = pd.DataFrame(index=frame.index)
        problems["null"] = frame[numeric].isna().any(axis=1)
        problems["negative"] = (frame[numeric] < FEATURES_MIN).any(axis=1)
        problems["bad_label"] = ~frame["diagnosis"].isin(LABELS)
        problems["duplicate"] = frame.duplicated(subset="sample_id", keep="first")
        cutoff = frame["mean_area"].quantile(0.99) * 20
        problems["outlier"] = frame["mean_area"] > cutoff

        bad = problems.any(axis=1)
        counts = {k: int(v) for k, v in problems.sum().items()}
        fraction = float(bad.mean())

        elapsed = time.time() - start_time
        log.info(
            "validation: %s (%.2f%% rejected) in %.2fs | clean=%d, rejected=%d",
            counts, fraction * 100, elapsed, (~bad).sum(), bad.sum()
        )

        clean = frame[~bad]
        rejected = frame[bad]
        rejected.to_parquet(run_dir(ds) / "rejected.parquet", index=False)
        clean_path = run_dir(ds) / "clean.parquet"
        clean.to_parquet(clean_path, index=False)
        (run_dir(ds) / "validation_report.json").write_text(
            json.dumps({
                "counts": counts,
                "bad_fraction": fraction,
                "clean_rows": len(clean),
                "rejected_rows": len(rejected),
            }, indent=2)
        )

        if fraction > MAX_BAD_FRACTION:
            raise AirflowFailException(
                f"{fraction:.1%} of rows rejected, limit is {MAX_BAD_FRACTION:.0%}. "
                f"Check {run_dir(ds) / 'validation_report.json'} for details."
            )
        return {"path": str(clean_path), "clean_rows": len(clean), **counts}

    @task(task_id="split", doc="""Deterministic train/test split.

    Uses SHA256(sample_id) → hash mod 100 to assign each row to bucket [0,100).
    Rows in buckets [0, TEST_FRACTION*100) go to test set.

    Why hash-based:
        - Reproducible: same row always in same set
        - Independent of row order: adding new data doesn't reshuffle existing rows
        - No random seed needed: works across machines and runs

    Output:
        dict with train count and test count
    """)
    def split(meta: dict, ds: str = None) -> dict:
        log.info("Starting split for date=%s", ds)
        start_time = time.time()

        frame = pd.read_parquet(meta["path"])

        def bucket(sample_id: str) -> int:
            digest = hashlib.sha256(sample_id.encode()).hexdigest()
            return int(digest[:8], 16) % 100

        is_test = frame["sample_id"].map(bucket) < TEST_FRACTION * 100
        for name, part in (("train", frame[~is_test]), ("test", frame[is_test])):
            part.to_parquet(run_dir(ds) / f"{name}_unscaled.parquet", index=False)

        elapsed = time.time() - start_time
        log.info(
            "split: %d train / %d test in %.2fs (%.1f%% test)",
            (~is_test).sum(), is_test.sum(), elapsed, 100 * is_test.mean()
        )
        return {"train": int((~is_test).sum()), "test": int(is_test.sum())}

    @task(task_id="scale", doc="""Feature normalization via z-score.

    Fits on training data only, then applies to both train and test.
    Prevents data leakage: test set statistics don't influence scaling.

    Formula: (x - mean_train) / std_train

    Output:
        dict with scaled column count and training set size
    """)
    def scale(meta: dict, ds: str = None) -> dict:
        log.info("Starting scale for date=%s", ds)
        start_time = time.time()

        train = pd.read_parquet(run_dir(ds) / "train_unscaled.parquet")
        test = pd.read_parquet(run_dir(ds) / "test_unscaled.parquet")
        numeric = [c for c in train.columns if c not in ("sample_id", "diagnosis")]

        mean, std = train[numeric].mean(), train[numeric].std().replace(0, 1)
        for name, part in (("train", train), ("test", test)):
            scaled = part.copy()
            scaled[numeric] = (part[numeric] - mean) / std
            scaled.to_parquet(run_dir(ds) / f"{name}.parquet", index=False)

        (run_dir(ds) / "scaler.json").write_text(json.dumps({
            "mean": mean.round(6).to_dict(),
            "std": std.round(6).to_dict(),
            "fitted_on_rows": len(train),
        }, indent=2))

        elapsed = time.time() - start_time
        log.info(
            "scaled %d features from %d training rows in %.2fs",
            len(numeric), len(train), elapsed
        )
        return {"scaled_columns": len(numeric), "fitted_on": len(train)}

    @task(task_id="train", doc="""Train model and register with MLflow.

    Trains LogisticRegression on scaled training data, evaluates on test set,
    and registers in MLflow model registry. Each run creates a new version.

    Parameters:
        max_iter: 1000 (sufficient for WDBC dataset)

    Metrics logged:
        - accuracy: correct predictions / total
        - roc_auc: area under ROC curve (better for imbalanced data)

    Output:
        dict with MLflow run_id, model_version, accuracy, roc_auc
    """)
    def train(scaling: dict, ds: str = None) -> dict:
        log.info("Starting train for date=%s", ds)
        start_time = time.time()

        train_df = pd.read_parquet(run_dir(ds) / "train.parquet")
        test_df = pd.read_parquet(run_dir(ds) / "test.parquet")
        feature_cols = [c for c in train_df.columns if c not in ("sample_id", "diagnosis")]

        X_train = train_df[feature_cols]
        y_train = (train_df["diagnosis"] == "M").astype(int)
        X_test = test_df[feature_cols]
        y_test = (test_df["diagnosis"] == "M").astype(int)

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT)

        with mlflow.start_run(run_name=f"wdbc-{ds}") as run:
            model = LogisticRegression(max_iter=1000)
            model.fit(X_train, y_train)
            proba = model.predict_proba(X_test)[:, 1]
            accuracy = accuracy_score(y_test, model.predict(X_test))
            auc = roc_auc_score(y_test, proba)

            mlflow.log_param("ds", ds)
            mlflow.log_param("n_features", len(feature_cols))
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("roc_auc", auc)
            mlflow.sklearn.log_model(
                model,
                artifact_path="model",
                registered_model_name=MLFLOW_MODEL_NAME,
                input_example=X_train.head(1),
            )
            run_id = run.info.run_id

        latest = mlflow.MlflowClient().get_registered_model(MLFLOW_MODEL_NAME).latest_versions
        version = max(int(v.version) for v in latest)

        elapsed = time.time() - start_time
        log.info(
            "trained on %d rows in %.2fs | accuracy=%.4f, roc_auc=%.4f → %s v%d (run %s)",
            len(X_train), elapsed, accuracy, auc, MLFLOW_MODEL_NAME, version, run_id
        )
        return {
            "mlflow_run_id": run_id,
            "model_version": version,
            "accuracy": round(accuracy, 4),
            "roc_auc": round(auc, 4),
        }

    @task(task_id="report", doc="""Generate summary and update history log.

    Combines metadata from all upstream tasks into a single summary JSON.
    Appends one line to history.jsonl (one run per line).

    Re-running same date overwrites that date's entry (idempotent).

    Output:
        str: JSON line appended to history.jsonl
    """)
    def report(validation: dict, split_info: dict, scaling: dict, training: dict, ds: str = None) -> str:
        log.info("Starting report for date=%s", ds)
        start_time = time.time()

        summary = {"ds": ds, **validation, **split_info, **scaling, **training}
        summary.pop("path", None)
        (run_dir(ds) / "summary.json").write_text(json.dumps(summary, indent=2))

        line = json.dumps(summary, sort_keys=True)
        history = STAGING / "history.jsonl"
        kept = [l for l in (history.read_text().splitlines() if history.exists() else [])
                if json.loads(l).get("ds") != ds]
        history.write_text("\n".join(kept + [line]) + "\n")

        elapsed = time.time() - start_time
        log.info("report saved in %.2fs | accuracy=%.4f, roc_auc=%.4f", elapsed, training["accuracy"], training["roc_auc"])
        return line

    # Define DAG structure
    ingested = ingest()
    validated = validate(ingested)
    split_info = split(validated)
    scaling = scale(validated)
    split_info >> scaling  # Explicit dependency: scale depends on split
    training = train(scaling)
    report(validated, split_info, scaling, training)


wdbc_pipeline()
