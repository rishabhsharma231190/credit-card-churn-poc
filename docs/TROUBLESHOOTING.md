# Troubleshooting

| Symptom | Action |
|---|---|
| Docker daemon unavailable | Start Docker Desktop, use Linux containers, and check WSL 2/virtualization. Confirm `docker version` shows a server. |
| Local Python/PySpark versions differ | Use the Docker commands; do not install the project into your Windows global Python. Only `init_env.py` runs on the host. |
| Airflow page cannot load | Run `docker compose ps` and `docker compose logs --tail 100 airflow-webserver airflow-scheduler`. Ensure `airflow-init` succeeded. |
| No DAG visible | Run `docker compose exec airflow-scheduler airflow dags list-import-errors`. Rebuild after code changes, then recreate services. The DAG is copied into the image. |
| DAG is paused | Unpause it and trigger a manual run. New DAGs start paused. |
| DB login failure after changing `.env` | The existing PostgreSQL volume retains its original password. Restore the original setting or intentionally reset the disposable POC volumes after backing up results. |
| Permission denied for MLflow DB/artifacts/work | Run the documented `permissions` service before starting the stack. |
| Spark process dies / Py4J connection refused | Inspect earlier logs for OOM. Start with 10k rows and `local[2]`; allocate sufficient Docker RAM. Use a larger EC2 host for 2m. Lowering Java memory is not always a fix. |
| `FallbackAsyncAdaptedQueuePool` import error | Install the pinned `sqlalchemy==2.0.40` in the job venv; rebuild the Docker image. An unpinned future SQLAlchemy may break MLflow 2.22. |
| Snowflake JWT/authentication failure | Check account identifier, service username, matching public key, mounted private key and passphrase; also check clock synchronization and Snowflake network policy. |
| Snowflake integration S3 AccessDenied | Verify the exact generated IAM principal and external ID from `DESC INTEGRATION`; confirm bucket/prefix permissions and storage allowed locations. |
| EC2 S3 NoCredentialsError | Attach the instance profile, require IMDSv2 with hop limit 2, and do not set empty AWS credential variables. Test boto3 identity from the runner. |
| `_READY` absent | Upstream publication did not finish. Fix and retry the upstream task; do not create the marker manually. |
| Schema/quality check fails | Inspect raw source types, dates, duplicate keys and counts. Fix the source; do not bypass the checks. |
| MLflow `champion` not found | Run once with `TRAIN_MODEL=true`. Model reuse requires a previously registered champion. |
| MLflow model/artifact missing after moving hosts | Registry metadata and artifact volumes must be migrated together. Retraining on the destination is simplest for this POC. |
| Old settings still used | Recreate scheduler/webserver after editing `.env`; `docker compose restart` alone may not reload environment values. |
| Ports unavailable on AWS | Keep 5000/8080 closed externally. Use the documented SSH tunnel and leave it open. |
| Snowflake COPY INTO failed midway | Rerun the export task with the same run ID; it clears only that raw prefix before exporting again. |

Helpful commands:

```text
docker compose logs --tail 100 mlflow
docker compose logs --tail 100 airflow-scheduler
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags trigger credit_card_churn_poc
```

To stop services while preserving all named volumes:

```text
docker compose down
```

Do not add `-v` unless you explicitly want to erase the local Airflow/MLflow databases, models and work datasets.
