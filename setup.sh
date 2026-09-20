#!/usr/bin/env bash
# One-shot bring-up for Tutorial 03: builds the image, starts mlflow +
# airflow, waits for both to be healthy, then prints what you need to log in.
#
#   ./setup.sh
set -euo pipefail
cd "$(dirname "$0")"

if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon is not running -- start Docker Desktop first." >&2
    exit 1
fi

# AIRFLOW_UID is only read on Linux; docker-compose.yml falls back to 50000
# (the image's built-in airflow user) when it's unset, which is fine on
# macOS/Windows.
if [[ "$(uname -s)" == "Linux" && ! -f .env ]]; then
    echo "AIRFLOW_UID=$(id -u)" > .env
    echo "wrote .env with AIRFLOW_UID=$(id -u)"
fi

echo "Building and starting mlflow + airflow..."
docker compose up -d --build

wait_healthy() {
    local service=$1 tries=0
    echo -n "Waiting for $service to be healthy"
    until docker compose ps "$service" --format '{{.Status}}' 2>/dev/null | grep -q healthy; do
        if (( tries++ > 60 )); then
            echo
            echo "$service never became healthy -- check: docker compose logs $service" >&2
            exit 1
        fi
        echo -n "."
        sleep 5
    done
    echo " done"
}

wait_healthy mlflow
wait_healthy airflow

echo
echo "Airflow : http://127.0.0.1:18080  (user: admin)"
echo "Password: $(docker compose exec -T airflow cat /opt/airflow/standalone_admin_password.txt)"
echo "MLflow  : http://127.0.0.1:15030"
echo
echo "Run the pipeline:"
echo "  docker compose exec airflow airflow dags test wdbc_pipeline 2026-08-25"
echo "Pull the model back:"
echo "  docker compose exec airflow python scripts/fetch_and_predict.py"
