# Tutorial 03 — Airflow: a pipeline that runs without you

**Học viên:** Nguyen Van Vu — MSSV vu25ms13298 — DDM501 HN

## What this tutorial is for

The pipeline: ingest, validate, split, scale, train, register. The last two
steps hand the trained model to MLflow so it can be pulled back later without
Airflow in the picture.

## MLflow: self-contained, no other tutorial needed

`docker-compose.yml` runs two services: `airflow` and `mlflow`. The `train`
task registers into this project's own MLflow server -- SQLite backend,
artifacts on a local volume (`./mlflow-data`). Nothing else needs to be
running first.

Running this DAG locally (not via `docker compose`) needs one env var
exported in the same shell, pointing at the `mlflow` service's published
port:

```bash
export MLFLOW_TRACKING_URI=http://127.0.0.1:15030
```

Running via `docker compose` needs none of this -- `docker-compose.yml`
already sets it, and `airflow` waits for `mlflow` to be healthy before it
starts.

## Two ways to run it

Both give the same DAG.

**A. Locally** (macOS, Linux, Windows + WSL2) — lighter, faster:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.11.txt

export AIRFLOW_HOME=$PWD/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone
```

The web UI comes up on <http://127.0.0.1:8080>. `standalone` prints the admin password on first start and also writes it to
`$AIRFLOW_HOME/standalone_admin_password.txt`.

**B. Docker** — two containers (`airflow` + `mlflow`), built once from the
`Dockerfile` beside this file:

```bash
./setup.sh
```

`setup.sh` builds the image, starts both services, waits for them to be
healthy, and prints the Airflow URL/password and the MLflow URL. Equivalent
by hand:

```bash
# On Linux only
echo "AIRFLOW_UID=$(id -u)" > .env

docker compose up -d --build
docker compose ps        # wait for STATUS = healthy, about a minute
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

After the first time, `docker compose up -d` (or `./setup.sh` again) is
enough — Docker reuses the image it already built. Add `--build` again only
when you change the `Dockerfile`.

<http://127.0.0.1:18080>, user `admin`. Port 18080 and not 8080, because Lab 2
owns 8080 and you will want both running one day.

## Running the pipeline

This runs every task in order, in your terminal:

```bash
airflow dags test wdbc_pipeline 2026-08-25
```

Then look at what it produced:

```
data/staging/2026-08-25/
  raw.parquet              snapshot of the extract, frozen for this run
  clean.parquet            rows that passed validation
  rejected.parquet         rows that did not, kept for inspection
  validation_report.json   what failed and how often
  train.parquet  test.parquet  scaler.json
  summary.json               now also has mlflow_run_id, model_version, accuracy, roc_auc
data/staging/history.jsonl one line per run
```

Check the registered model in the MLflow UI at <http://127.0.0.1:15030>,
experiment `wdbc-pipeline`, registered model `wdbc-classifier`.

## Pulling the model back

`scripts/fetch_and_predict.py` loads the registered model through MLflow's
client API (not Airflow, not a copied file) and scores a few test rows.

Easiest: run it inside the Airflow container, which already has the
dependencies and the env vars set:

```bash
docker compose exec airflow python scripts/fetch_and_predict.py
```

Running it on the host instead needs its own environment -- this script only
needs `mlflow`/`scikit-learn`/`pandas`/`pyarrow`, not Airflow itself, so a
plain venv is enough (no python3.11 or constraints file required):

```bash
python3 -m venv .venv-scripts
source .venv-scripts/bin/activate
pip install mlflow==2.19.0 scikit-learn==1.6.0 pandas pyarrow

export MLFLOW_TRACKING_URI=http://127.0.0.1:15030

python scripts/fetch_and_predict.py
```

Pass `--version N` to pull a specific model version instead of the latest, or
`--ds YYYY-MM-DD` to score a specific run's test set.

## The exercises

| | Do this | Look for |
|---|---|---|
| 1 | `airflow dags test wdbc_pipeline 2026-08-25` twice | The outputs are byte-identical and `history.jsonl` still has one line for that date. Re-running a date is safe. |
| 2 | `python scripts/corrupt_extract.py` then re-run | `validate` fails with `13.0% of rows rejected, limit is 5%`, and the log says **Immediate failure requested** — the three retries were skipped on purpose. Repair with `--repair`. |
| 3 | `airflow dags backfill wdbc_pipeline -s 2026-08-22 -e 2026-08-24` | Three run folders appear, one per date, three lines in `history.jsonl`. |
| 4 | Open the UI, Grid view, click a failed task, then Logs | The traceback for one task of one date, without SSH-ing anywhere. |
| 5 | `airflow dags test wdbc_pipeline 2026-08-25` again, then check the MLflow UI | A new run under experiment `wdbc-pipeline` and a new version of `wdbc-classifier` — each Airflow run registers its own model version, even on a re-run of the same date. |
| 6 | `python scripts/fetch_and_predict.py` | Predictions for a few test rows, loaded straight from the registry — no Airflow process involved. |

## Kết quả đã chạy thử (end-to-end)

Chạy ngày 2026-09-20 trên Windows 11 + Docker Desktop 4.91 (WSL 2.7.14),
lệnh chạy từ Git Bash. Toàn bộ số liệu dưới đây lấy từ chính lần chạy này.

**Dựng stack.** `./setup.sh` lần đầu (tải image `apache/airflow:2.8.4`, build,
đợi `mlflow` rồi `airflow` healthy) mất 5 phút 18 giây. Cả hai container
`ddm501-t03-mlflow` và `ddm501-t03-airflow` đều `healthy`.

**Pipeline** — `docker compose exec airflow airflow dags test wdbc_pipeline 2026-08-25`:
cả 6 task (`ingest → validate → split → scale → train → report`) SUCCESS.
Extract 570 dòng, `validate` loại 6 dòng (1,05%: null 2, negative 1, bad_label 1,
duplicate 1, outlier 1) → 564 dòng sạch, chia 441 train / 123 test. `train` đăng
ký `wdbc-classifier` lên MLflow của chính project này: `accuracy=0.9512`,
`roc_auc=0.9956`.

| Bài | Kết quả thực tế |
|---|---|
| 1. Chạy lại cùng ngày | 7 file (`raw`, `clean`, `rejected`, `train`, `test` parquet, `scaler.json`, `validation_report.json`) giống hệt nhau theo `sha256`; `history.jsonl` vẫn 1 dòng cho `2026-08-25`. |
| 2. Dữ liệu hỏng | `corrupt_extract.py` làm trống `mean_radius` ở 68/570 dòng (11,9%). `validate` fail với `13.0% of rows rejected, limit is 5%` và log ghi `Immediate failure requested` — không retry. `--repair` khôi phục `wdbc.csv` (hash trùng bản trong git). |
| 3. Backfill | `airflow dags backfill ... -s 2026-08-22 -e 2026-08-24`: 3 run, 18 task thành công, 0 lỗi; xuất hiện 3 thư mục ngày mới, `history.jsonl` có 4 dòng (kèm `2026-08-25`). |
| 5. Version mới mỗi lần chạy | Mỗi lần `train` chạy đều tạo một version mới, kể cả chạy lại cùng ngày: registry có 6 version (v1–v6) sau các lần chạy trên. |
| 6. Kéo model về | `fetch_and_predict.py` tải `models:/wdbc-classifier/6`, dự đoán đúng 5/5 dòng test; `--version 1 --ds 2026-08-22 --rows 3` cũng đúng 3/3. |

Bài 4 (đọc log của task hỏng trong Grid view) chưa được chụp lại; traceback của
`validate` xem được qua log của lệnh ở bài 2.

**Hai lỗi chỉ xuất hiện khi chạy trên Windows, đã sửa trong repo:**

- `setup.sh`: Git Bash tự đổi `/opt/airflow/...` thành `C:/Program Files/Git/opt/airflow/...`
  nên dòng `Password:` bị trống. Sửa bằng `MSYS_NO_PATHCONV=1` (không ảnh hưởng Linux/macOS).
- `scripts/corrupt_extract.py --repair`: `shutil.copy` chép nội dung xong rồi `chmod`,
  bị `PermissionError` trên ổ bind-mount của Windows. Đổi sang `shutil.copyfile`.

Trên Windows, Docker Desktop cần WSL mới: nếu Docker báo `WSL update required`, chạy
`wsl --update` trong PowerShell quyền admin rồi mở lại Docker Desktop.

**Ghi chú thiết kế.** `mlflow` dùng `--artifacts-destination` (giữ artifact root mặc
định `mlflow-artifacts:/`) thay vì một đường dẫn local làm `--default-artifact-root`.
Nếu dùng đường dẫn local, client (container `airflow`, có filesystem khác container
`mlflow`) sẽ cố ghi thẳng vào đường dẫn đó và bị `PermissionError`; với
`--artifacts-destination`, mọi đọc/ghi artifact đi qua HTTP API của MLflow server.

### Ảnh chụp màn hình

Bốn ảnh giao diện chụp trực tiếp bằng Edge headless từ Airflow (`:18080`) và MLflow
(`:15030`) đang chạy. Hai ảnh terminal được dựng lại từ output thật đã ghi log
(`setup.sh` là lần chạy thứ hai, image đã có cache; password đã được che).

| | |
|---|---|
| `./setup.sh` — build, đợi `mlflow` rồi `airflow` healthy, in URL/password | ![setup.sh output](docs/screenshots/setup-sh-output.jpg) |
| Airflow — Grid view: 4 run của `wdbc_pipeline`, cả 6 task đều SUCCESS | ![Airflow grid success](docs/screenshots/airflow-grid-success.jpg) |
| Airflow — Graph view: `ingest → validate → split → scale → train → report` | ![Airflow graph](docs/screenshots/airflow-graph.jpg) |
| MLflow — 6 version của `wdbc-classifier` đã đăng ký | ![MLflow registered versions](docs/screenshots/mlflow-registered-versions.jpg) |
| MLflow — chi tiết run `wdbc-2026-08-25`: `accuracy=0.9512`, `roc_auc=0.9956`, đăng ký `wdbc-classifier v6` | ![MLflow run metrics](docs/screenshots/mlflow-run-metrics.jpg) |
| `scripts/fetch_and_predict.py` — tải `wdbc-classifier` v6, dự đoán đúng 5/5 dòng test | ![fetch_and_predict.py output](docs/screenshots/fetch-and-predict-output.jpg) |
