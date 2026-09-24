# Automated ML Pipeline with Airflow

**Student:** Nguyen Van Vu — Student ID vu25ms13298 — DDM501 HN

## Project Overview

This is an **automated data pipeline** using Apache Airflow to process and train a machine learning model on the WDBC (Wisconsin Breast Cancer) dataset. The pipeline runs automatically on schedule, processing data end-to-end without manual intervention.

**Dataset:** WDBC (Breast Cancer) — 570 records, 30 numeric features, classification task (Malignant/Benign)

## Pipeline Architecture

The pipeline consists of **6 tasks** executed automatically:

```
ingest → validate → split → scale → train → report
```

| Task | Description | Output |
|------|--------|---------|
| **ingest** | Read data from `data/raw/wdbc.csv`, snapshot to parquet | `raw.parquet` |
| **validate** | Check data quality, remove bad rows (max 5%) | `clean.parquet`, `rejected.parquet`, `validation_report.json` |
| **split** | Deterministic train/test split via sample_id hash (20% test, no randomness) | `train_unscaled.parquet`, `test_unscaled.parquet` |
| **scale** | Feature normalization using z-score | `train.parquet`, `test.parquet`, `scaler.json` |
| **train** | Train LogisticRegression, register model on MLflow | MLflow run, model version |
| **report** | Write results to history log (`history.jsonl`) | `summary.json`, `history.jsonl` |

## Self-Contained System

MLflow is **built into the project**, no external dependencies needed:
- Backend: SQLite (`./mlflow-data/mlflow.db`)
- Artifact store: HTTP proxy through MLflow server
- Config: `docker-compose.yml` sets up everything automatically

When running via `docker compose`, all env vars are pre-configured. For local Airflow, just set:

```bash
export MLFLOW_TRACKING_URI=http://127.0.0.1:15030
```

## Installation and Setup

Two ways to run the pipeline — choose based on your needs:

### Method A: Local Execution (macOS, Linux, Windows + WSL2)

**Requirements:** Python 3.11, pip, bash-compatible terminal

```bash
# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.11.txt

# Setup Airflow
export AIRFLOW_HOME=$PWD/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export MLFLOW_TRACKING_URI=http://127.0.0.1:15030

# Start
airflow standalone
```

Airflow UI: <http://127.0.0.1:8080>  
Password printed to console on first run (also in `$AIRFLOW_HOME/standalone_admin_password.txt`)

**Pros:** Fast, lightweight, easy debugging  
**Cons:** Requires exact Python 3.11, large constraint file

### Method B: Docker Compose (Recommended)

**Requirements:** Docker Desktop, WSL2 (on Windows)

**Fastest startup:**
```bash
./setup.sh
```

This script will:
1. Build image from `Dockerfile`
2. Start 2 containers: `mlflow` + `airflow`
3. Wait for both to be healthy (~1-2 minutes)
4. Print URLs and password

**Output:**
```
Airflow : http://127.0.0.1:18080  (user: admin)
Password: [random password]
MLflow  : http://127.0.0.1:15030
```

**Manual startup (without `./setup.sh`):**
```bash
# On Linux, create .env with AIRFLOW_UID
echo "AIRFLOW_UID=$(id -u)" > .env

# Build and start containers
docker compose up -d --build

# Check status (wait for STATUS = healthy)
docker compose ps

# Get admin password
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

**Subsequent runs:**
```bash
docker compose up -d
# or
./setup.sh
```

Docker reuses the image and container, much faster on restart.

**Ports:** 18080 (Airflow), 15030 (MLflow) — avoids conflicts with other projects

**Pros:** Python version-agnostic, clean environment, easy to share  
**Cons:** Requires Docker, slow first start

## Project Structure

```
.
├── dags/
│   └── wdbc_pipeline.py          # Main DAG (6 tasks)
├── scripts/
│   ├── fetch_and_predict.py      # Load model and predict
│   └── corrupt_extract.py        # Test tool: corrupt data to test validation
├── data/
│   ├── raw/
│   │   ├── wdbc.csv              # Original dataset
│   │   └── wdbc.csv.orig         # Backup (for corrupt_extract.py --repair)
│   └── staging/                  # Outputs from each run
│       ├── 2026-08-25/           # Data for a specific date
│       │   ├── raw.parquet
│       │   ├── clean.parquet
│       │   ├── rejected.parquet
│       │   ├── train.parquet
│       │   ├── test.parquet
│       │   ├── scaler.json
│       │   ├── summary.json
│       │   └── validation_report.json
│       └── history.jsonl         # History of all runs (1 line per run)
├── docs/screenshots/             # Screenshots of actual results
├── docker-compose.yml            # Config for 2 services: airflow + mlflow
├── Dockerfile                    # Base: apache/airflow:2.8.4
├── setup.sh                      # Quick startup script (Docker)
├── requirements.txt              # Python dependencies
└── .gitattributes               # Ensure setup.sh stays LF
```

## Running the Pipeline

### First Time

```bash
# Using Docker
./setup.sh

# Using local Airflow (ensure it's running in background)
# Then in another terminal:
```

### Run DAG for a Specific Date

```bash
# Using Docker
docker compose exec airflow airflow dags test wdbc_pipeline 2026-08-25

# Using local Airflow (airflow standalone already running)
airflow dags test wdbc_pipeline 2026-08-25
```

This will:
1. Execute all 6 tasks sequentially
2. Create `data/staging/2026-08-25/` with outputs
3. Write results to `data/staging/history.jsonl`
4. Register new model version on MLflow

### View Results

**Output of a single run:**
```
data/staging/2026-08-25/
├── raw.parquet              # Raw data snapshot (570 rows)
├── clean.parquet            # Valid data (564 rows)
├── rejected.parquet         # Invalid rows (6 rows)
├── validation_report.json   # Validation errors
├── train.parquet            # Training data after scaling (441 rows)
├── test.parquet             # Test data after scaling (123 rows)
├── scaler.json              # Normalization params (mean, std)
├── summary.json             # Summary: metrics, version, accuracy, roc_auc
└── validation_report.json
```

**Run history:**
```bash
# View all runs
cat data/staging/history.jsonl

# Each line is one run in JSON format
{"ds": "2026-08-25", "clean_rows": 564, "accuracy": 0.9512, ...}
```

**Access UIs:**
- **Airflow:** <http://127.0.0.1:18080> → DAGs → `wdbc_pipeline`
- **MLflow:** <http://127.0.0.1:15030> → Models → `wdbc-classifier` → view versions

## Loading and Making Predictions with Model

**Script:** `scripts/fetch_and_predict.py` — loads model from MLflow registry (Airflow not needed) and predicts on test set.

### Fastest Method (Docker)

```bash
docker compose exec airflow python scripts/fetch_and_predict.py
```

Output:
```
loaded models:/wdbc-classifier/6
WDBC-0003  actual=M  predicted=M  p(malignant)=1.0000
WDBC-0007  actual=M  predicted=M  p(malignant)=0.9999
...
```

### Running on Host Machine (No Docker)

Only needs: `mlflow`, `scikit-learn`, `pandas`, `pyarrow` (no Airflow)

```bash
python3 -m venv .venv-scripts
source .venv-scripts/bin/activate
pip install mlflow==2.19.0 scikit-learn==1.6.0 pandas pyarrow

export MLFLOW_TRACKING_URI=http://127.0.0.1:15030

python scripts/fetch_and_predict.py
```

### Options

```bash
# Use specific version (not latest)
python scripts/fetch_and_predict.py --version 3

# Use test set from specific date
python scripts/fetch_and_predict.py --ds 2026-08-22

# Predict 10 rows
python scripts/fetch_and_predict.py --rows 10

# Combine options
python scripts/fetch_and_predict.py --version 1 --ds 2026-08-22 --rows 5
```

## Exercises

These exercises help understand pipeline characteristics:

| # | Exercise | Expected Result | Notes |
|---|----------|---------|---------|
| **1** | Run same date twice:<br>`airflow dags test wdbc_pipeline 2026-08-25`<br>`airflow dags test wdbc_pipeline 2026-08-25` | 7 output files byte-identical (check `sha256sum`)<br>`history.jsonl` still has 1 line for that date<br>→ Re-run is idempotent | Shows pipeline has deterministic rules, no randomness. Split uses hash(sample_id), no random seed. |
| **2** | Corrupt data:<br>`python scripts/corrupt_extract.py`<br>Then re-run:<br>`airflow dags test wdbc_pipeline 2026-08-25`<br>Fix:<br>`python scripts/corrupt_extract.py --repair` | Task `validate` fails immediately (no 3 retries)<br>Log: `13.0% of rows rejected, limit is 5%`<br>and `Immediate failure requested`<br>→ AirflowFailException skips retries | Shows Airflow has fail-fast for logic errors. Different from timeout/errors — corrupt data won't be fixed by retrying. |
| **3** | Run backfill multi-day:<br>`airflow dags backfill wdbc_pipeline -s 2026-08-22 -e 2026-08-24` | 3 new directories:<br>`data/staging/2026-08-22/`<br>`data/staging/2026-08-23/`<br>`data/staging/2026-08-24/`<br>`history.jsonl` has 4 lines (includes 25th) | Airflow auto-processes batch by date. Each date is separate execution_date, outputs separated. |
| **4** | Make task fail (e.g., corrupt data from ex. 2), open Airflow UI:<br>Grid view → click task `validate` → Logs | See full traceback without SSH<br>Shows code line, full exception | Airflow UI enables debugging without container access. |
| **5** | Re-run date then check MLflow UI<br>`airflow dags test wdbc_pipeline 2026-08-25`<br>→ <http://127.0.0.1:15030> Models → `wdbc-classifier` | New version created<br>v1 → v2 → ... → v_n<br>Each run creates version, even re-runs of same date | MLflow auto-tracks versions, no manual management. Enables model comparison over time. |
| **6** | Load model from registry:<br>`python scripts/fetch_and_predict.py`<br>or in Docker:<br>`docker compose exec airflow python scripts/fetch_and_predict.py` | Predictions correct on test set<br>Doesn't need Airflow running<br>Only needs MLflow server + model code | Separates inference from training pipeline. Model versioned, reusable easily. |

### Useful Commands

```bash
# View DAG runs
airflow dags list-runs wdbc_pipeline

# View logs for specific task
docker compose exec airflow airflow tasks log wdbc_pipeline ingest 2026-08-25

# Clear data for one date (reset for re-run)
rm -rf data/staging/2026-08-25/

# Stop containers
docker compose down

# View model versions
docker compose exec airflow python -c "
import mlflow
mlflow.set_tracking_uri('http://127.0.0.1:15030')
model = mlflow.MlflowClient().get_registered_model('wdbc-classifier')
for v in model.latest_versions:
    print(f'Version {v.version}: {v.current_stage}')
"
```

## Troubleshooting

### Docker/WSL Issues

| Issue | Cause | Solution |
|--------|-----------|---------|
| `Docker Desktop is unable to start` | Old WSL kernel | Run `wsl --update` in PowerShell (admin), restart Docker |
| `docker: command not found` | Docker not in PATH | Add Docker to PATH or restart terminal after install |
| Container not healthy after 2 minutes | Image still downloading | Check `docker compose logs airflow`; wait or check network |
| `Permission denied` copying files (Windows) | Windows bind mount doesn't support `chmod` | Use `shutil.copyfile` instead of `shutil.copy` (already fixed) |

### Airflow Issues

| Issue | Cause | Solution |
|--------|-----------|---------|
| `No module named airflow` | Virtual environment not activated | Run `source .venv/bin/activate` (Linux/Mac) or `.venv\Scripts\activate` (Windows) |
| DAG not showing | `AIRFLOW__CORE__DAGS_FOLDER` wrong | Check: `echo $AIRFLOW__CORE__DAGS_FOLDER` should be full path |
| Task timeout | Large data or slow machine | Increase `execution_timeout` in DAG or reduce batch size |
| `MLFLOW_TRACKING_URI` not set | Environment variable deleted | Run `export MLFLOW_TRACKING_URI=http://127.0.0.1:15030` again |

### MLflow Issues

| Issue | Cause | Solution |
|--------|-----------|---------|
| MLflow server not running | Process crashed | Restart: `docker compose restart mlflow` or `./setup.sh` |
| Model won't load from registry | Version doesn't exist | Run DAG first to create version: `airflow dags test wdbc_pipeline 2026-08-25` |
| Artifact upload fails | Wrong artifact store path | Use `--artifacts-destination file:///path` (not `C:\path` on Windows) |
