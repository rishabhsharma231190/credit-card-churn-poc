"""All heavy execution happens in the job venv, not inside the DAG parser."""
import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="credit_card_churn_poc",
    description="Snowflake -> S3 -> PySpark preparation -> features -> MLflow -> batch scoring",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    schedule=os.getenv("DAG_SCHEDULE", "0 8 * * *"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=5),
    default_args={"owner": "churn-poc", "retries": 1, "retry_delay": timedelta(minutes=2),
                  "execution_timeout": timedelta(hours=2)},
    tags=["poc", "pyspark", "churn"],
) as dag:
    previous = None
    for stage in ["ingest", "export", "prepare", "features", "train", "score"]:
        task = BashOperator(
            task_id=stage,
            # ts_nodash uniquely scopes each run; retries intentionally reuse it.
            bash_command=("/opt/churn-venv/bin/python -m churn.cli " + stage +
                          ' --mode "$PIPELINE_MODE" --rows "$ROWS" --run-id "{{ ts_nodash }}"'),
            append_env=True,
        )
        if previous:
            previous >> task
        previous = task
