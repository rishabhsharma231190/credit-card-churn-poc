# Scheduled scoring from an existing Snowflake table

The `credit_card_churn_poc` DAG now runs:

1. `resolve_model`: select the MLflow `champion` and save its numeric version and threshold.
2. `export_existing`: unload the configured existing Snowflake scoring table to S3.
3. `prepare`: validate the scoring snapshot using PySpark.
4. `features`: compute scoring features using PySpark.
5. `score`: load the pinned model version and publish predictions and summary to S3.

Every task invokes `bash /opt/churn/scripts/run_scoring.sh <stage> <run-id> <mode>`.
The Dockerfile packages this script and Python modules in the same image. CI/CD pushes
the tested image to ECR and deploys it to EC2 using the existing workflow.

## What changes

- No scheduled Snowflake table generation or ingestion. The source receives SELECT
  queries only; COPY INTO writes Parquet to S3, not back into Snowflake.
- No scheduled training, new MLflow model versions, or champion alias updates.
- Historical training tables are not exported or prepared by this DAG.
- An existing `TRAIN_MODEL=true` setting cannot enable training in the scoring script.
- Each new run resolves the alias once. A retry reuses the completed model manifest.
  A missing champion, invalid threshold, or incompatible feature version fails visibly.
- Source `as_of_date` values are preserved. Reusing a static table does not make its
  observations current. A separate upstream process must refresh real source data.
- The export is a full snapshot each run, not an incremental export. Keep the source
  stable during extraction. The existing 2-million-row count check remains enabled.

## Configure before merging/deploying

1. Wait for any active DAG run to finish, then pause the DAG in Airflow.
2. Choose the existing scoring table in Snowflake. The earlier POC contained
   `CHURN_POC_DB.PUBLIC.POC_SCORE_B5D762AACA1A71C9` with 2 million rows;
   verify it still exists and is the snapshot you want. Do not create it again.
3. In the EC2 project `.env`, set:

   ```dotenv
   PIPELINE_MODE=cloud
   SNOWFLAKE_SCORE_TABLE=CHURN_POC_DB.PUBLIC.POC_SCORE_B5D762AACA1A71C9
   ROWS=2000000
   TRAIN_MODEL=false
   ```

   The table is an example from the earlier POC, not a hard-coded default.
   Use TABLE, SCHEMA.TABLE, or DATABASE.SCHEMA.TABLE with simple unquoted identifiers.
   Retain the existing Snowflake authentication, storage integration, S3 and MLflow settings.
   The service role needs SELECT on the chosen table and the existing unload permissions.
4. Confirm `credit_card_churn` has a `champion` alias in the persistent MLflow registry.
5. Merge the tested PR. The deployment checks that a cloud source table is configured
   before changing services. Docker Compose recreates containers with the updated `.env`.
6. Confirm the five tasks appear, then unpause the DAG and trigger one test run.
   Do not clear/retry old six-task runs using the new DAG definition.

## Manual invocation on EC2 (optional)

From `/home/ec2-user/credit-card-churn-poc`, after deploying and when no scheduled run
is active:

```bash
docker compose exec -T airflow-scheduler bash /opt/churn/scripts/run_scoring.sh all manual_score_20261012_01 cloud
```

Choose a fresh run ID for each new run. The `all` option runs the same five stages
sequentially. Airflow invokes each stage separately so it retains separate logs and retries.
The script must run inside the container, where dependencies and credentials are configured.

## Verify the result

- Five Airflow tasks succeed; no ingest/train task appears in new runs.
- Model registry version count and champion alias are unchanged after scoring.
- `model/manifest.json` records the exact model version and threshold used.
- `source/manifest.json` records source table, row count and Snowflake COPY query ID.
- Under `s3://<bucket>/churn-poc/runs/<run-id>/`, inspect `raw/score/`,
  `prepared/score/`, `features/score/`, `scores/predictions/`, and `scores/summary.json`.
- Scheduled run IDs begin with `scoring_` and include the logical timestamp and Airflow
  database run ID to separate manual and scheduled runs with the same logical date.

The legacy `churn.cli ingest/train/all` and `scripts/run_local.sh` remain explicit
bootstrap/demo tools. CI uses them only in a disposable local environment to create a
test champion, then exercises the new shell script and verifies no model version was added.
They are not called by the scheduled scoring DAG. A new source table or unavailable cloud
credentials cannot be validated by local CI; verify the first cloud run after deployment.
