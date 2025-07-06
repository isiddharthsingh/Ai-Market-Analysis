# Dockerfile
FROM apache/airflow:2.7.0-python3.9

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy configuration
COPY config/airflow.cfg /opt/airflow/airflow.cfg