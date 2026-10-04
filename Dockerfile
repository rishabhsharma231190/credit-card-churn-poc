# Pinned POC runtime, not a claim that these are the latest production releases.
FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless python3-venv \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN python -m venv /opt/churn-venv
COPY requirements.txt /tmp/churn-requirements.txt
RUN /opt/churn-venv/bin/pip install --no-cache-dir -r /tmp/churn-requirements.txt
COPY --chown=airflow:root churn /opt/churn/churn
COPY --chown=airflow:root dags /opt/airflow/dags
ENV PYTHONPATH=/opt/churn PYSPARK_PYTHON=/opt/churn-venv/bin/python
ARG CODE_COMMIT=local
ENV CODE_COMMIT=${CODE_COMMIT}
USER airflow
