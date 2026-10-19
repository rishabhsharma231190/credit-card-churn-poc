# Raw-data quality task

The scoring DAG now has six tasks:
`resolve_model -> export_existing -> data_quality -> prepare -> features -> score`.
The source is the existing Snowflake table; this change does not re-ingest data or register models.
Airflow calls `scripts/run_scoring.sh data_quality RUN_ID cloud`, which invokes
`python -m churn.cli data_quality --scoring-only ...` in the native EC2 virtual environment.

## Reports

For each run, open `churn-poc/runs/RUN_ID/data_quality/report.json` in the existing S3 bucket.
The same report exists at `/opt/airflow/work/RUN_ID/data_quality/report.json` on EC2.
It contains row counts, per-column data types, missing counts and percentages, and numeric means.
Missing means SQL NULL, numeric NaN, or empty/space-only text. Infinite numerics are counted separately.
Means exclude missing and infinite values; all-missing numeric columns have a null mean.
No raw values are imputed, dropped, or changed.

Text and boolean columns are automatically profiled as categories (excluding customer_id/as_of_date).
The current exported model features are numeric, so an empty set of categorical distributions is expected.
To treat numeric codes as categories too, optionally set `DQ_CATEGORICAL_COLUMNS="products"`
in `/etc/churn/airflow.env`, following the existing environment-file format, and restart the scheduler
when no DAG is running. Names are comma separated and case insensitive; unknown names fail the check.
This does not add columns to the Snowflake export: only columns present in raw Parquet are profiled.

The JSON shows the 20 most frequent values and an `other_count`. Complete category counts/percentages
are saved as distributed Parquet under `data_quality/DATASET/categorical/column_N/`.
`distribution_path` is relative to that dataset folder. Values are represented as strings; NULL remains NULL.
Percentages use all rows as the denominator, including missing values. No full dataset is collected into Python.
Numeric aggregates share one Spark aggregation; raw data is cached with disk spill for category scans.

## Failure policy

The task fails for missing required columns, any missing values in exported columns, infinite numerics,
numeric mean overflow, empty data, unexpected row counts, or invalid categorical configuration.
The diagnostic report is published BEFORE raising a quality failure, so downstream tasks are blocked
but the report remains available. `_READY` means the report upload completed, not that data passed:
inspect `status`. Technical read/Spark/S3 failures may prevent report creation; use task logs then.
Existing preparation checks still validate domains, dates and duplicate customer/snapshot keys.
Distribution and mean statistics are descriptive: there is no drift baseline or drift threshold in this change.

## Deploy and demonstrate

Wait for PR verification to pass; merge through the existing GitHub workflow with EC2 running and no active DAG run.
The native deployment validates the six-task DAG. Trigger one run and inspect the `data_quality` task log,
then show the report in S3. A passing run continues to preparation and champion scoring.
CI tests missing values, NaN/infinity, category counts, numeric means, empty data, and report publication on failure.
The existing 5,000-row scoring smoke test also requires a passed data-quality report.
