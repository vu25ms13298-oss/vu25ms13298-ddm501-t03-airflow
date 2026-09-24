# Automated ML Pipeline: WDBC Classification with Airflow + MLflow
# Base image: Apache Airflow 2.8.4 with Python 3.11
#
# This image serves both services in docker-compose.yml:
#   - airflow: DAG scheduler and executor
#   - mlflow: Tracking server and model registry
#
# Build: docker build -t ddm501-t03-airflow:2.8.4 .
# Run as airflow: docker compose up -d
# Run as mlflow: docker compose up -d mlflow

ARG AIRFLOW_VERSION=2.8.4
ARG PYTHON_VERSION=3.11
ARG BASE_IMAGE=apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

FROM ${BASE_IMAGE}

# Metadata labels for tracking and documentation
LABEL maintainer="data-team@local" \
      version="1.0" \
      description="Airflow + MLflow for WDBC breast cancer ML pipeline" \
      airflow.version="${AIRFLOW_VERSION}" \
      python.version="${PYTHON_VERSION}"

# Set working directory
WORKDIR /opt/airflow

# Switch to airflow user (non-root for security)
# Note: base image already creates this user
USER airflow

# Build arguments for constraint file
ARG AIRFLOW_VERSION=2.8.4
ARG PYTHON_VERSION=3.11

# Install additional Python packages
# - Use Apache Airflow constraints for compatibility (prevents dependency conflicts)
# - Group by purpose: data processing, ML, experiment tracking
# - Keep --no-cache-dir to reduce image size
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      \
      # Data processing: pandas, arrow (for parquet format)
      "pandas==2.1.4" \
      "pyarrow==14.0.2" \
      \
      # ML training: scikit-learn for model training
      "scikit-learn==1.6.0" \
      \
      # Experiment tracking: MLflow for model registry and artifact store
      "mlflow==2.19.0"

# Health check for Airflow (used in docker-compose.yml)
HEALTHCHECK --interval=15s --timeout=10s --start-period=60s --retries=20 \
  CMD airflow jobs check --job-type SchedulerJob --hostname $(hostname) || exit 1

# Default command: run Airflow
CMD ["airflow", "standalone"]
