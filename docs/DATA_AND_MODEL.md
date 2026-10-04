# Data and modeling choices

## Business definition

Proposed label: `churn_90d = 1` if an active customer's card relationship closes, or the customer reaches 90 consecutive days with no purchase transaction, within the next 90 days after `as_of_date`. Confirm exclusions for fraud closures, deceased customers, involuntary closure and charged-off accounts with your business owner before real use. Churn is a retention outcome; it is not credit default or charge-off risk.

This POC uses a noisy synthetic Bernoulli label driven by pre-snapshot activity. It does not simulate transaction histories or derive a real event-based label. The business definition above is a proposed target for replacing synthetic data. Metrics on this data are **not evidence of bank performance, ROI or production suitability**.

Historical snapshot default: 2026-05-01. Its 90-day outcome window ends 2026-07-30. Scoring snapshot: execution date, or `SCORE_AS_OF`. Training labels must mature before scoring and before the current date. Historical and current snapshots each contain 2,000,000 customers with the same ID domain but different synthetic feature draws. This yields 4,000,000 source snapshot rows and 2,000,000 predictions.

Local Spark and Snowflake use different hash implementations. Each path is reproducible for a given date and customer ID within its own engine; the two generated datasets are distributionally similar, not byte-identical. Changing `SCORE_AS_OF` changes the current synthetic snapshot. For repeatable comparisons, set it explicitly.

## Schema

One row per customer per snapshot. Amounts use arbitrary common currency units, not an assumption about INR or USD.

| Column | Meaning / synthetic range | Model input? |
|---|---|---|
| customer_id | Unique synthetic customer, 1..N | No |
| as_of_date | Feature cutoff date | No |
| tenure_months | Months with bank, 3..119 | Yes |
| credit_limit | 1,000..20,000 | Yes |
| balance | Outstanding balance, 0..limit | Yes |
| txn_count_30d | Last 30-day purchases, 0..99 | Yes |
| spend_30d | Current 30-day spend | Yes |
| spend_prev_30d | Previous 30-day spend | Yes |
| inactive_days | Days since activity, 0..89 | Yes |
| support_calls_90d | Service contacts, 0..7 | Yes |
| products | Products held, 1..5 | Yes |
| digital_logins_30d | Logins, 0..39 | Yes |
| churn_90d | Future synthetic outcome, 0/1; history only | Label only |
| utilization | balance / credit_limit | Yes, derived |
| spend_change | spend_30d / spend_prev_30d - 1 | Yes, derived |
| avg_ticket | spend / transaction count; zero if count=0 | Yes, derived |
| low_engagement | 1 when logins < 3 | Yes, derived |

The independent synthetic fields can contain simplified combinations (for example, nonzero spend and zero purchases). They are suitable for pipeline testing, not a realistic bank data simulator. Real source validation needs business consistency rules and a documented missing-value policy.

## Preparation

PySpark normalizes names, selects the required columns and casts dates/IDs/numerics. It fails on missing, infinite, negative or invalid values, duplicate customer/snapshot keys, invalid labels, unexpected counts or unmatured labels. It does not silently discard rows or invent targets. No median or other statistic is fit globally, so there is no imputation leakage in this implementation.

## Splitting and features

Customer hash buckets allocate approximately 80% training, 10% validation and 10% test. The customer ID, date, split, label and version strings are excluded from the model input list. The four derived features use only pre-cutoff fields and are implemented once for both training and scoring. Feature contract version is `v1`.

This is a customer-held-out test within one historical date. For a real bank, use multiple fully matured historical cohorts, an out-of-time holdout, careful treatment of repeat customers, training-only learned transformations, and only features available at the prediction cutoff. Future closures, future inactivity, post-cutoff collections and future balances must never be features.

## Model and evaluation

Spark MLlib logistic regression is the baseline because it supports distributed fitting and inexpensive inference. Spark applies its internal standardization during fitting; no Python UDF or Pandas conversion is used. The model Pipeline includes its VectorAssembler, so feature ordering is preserved when loading from MLflow.

The validation set chooses an operating threshold from 0.1 to 0.7 by F1. The untouched test set is then evaluated at that threshold using ROC-AUC, PR-AUC, precision, recall, F1 and prevalence. A minimum test ROC-AUC of 0.60 is a demo sanity gate only. Class balancing and resampling are intentionally omitted from this baseline. In a real retention program, choose the threshold using contact capacity and intervention value; examine calibration, subgroup performance, data drift and randomized retention uplift before acting on customers.

Training logs parameters, metrics, a feature contract and the Spark model to MLflow. Each score has the numeric model version, feature version, threshold and pipeline run ID. This supports reproducibility and investigation. A failed gate stops the DAG before scoring rather than silently publishing a poor model.

## Output

`scores/predictions/` contains:

`customer_id, as_of_date, churn_probability, churn_prediction, threshold, model_version, feature_version, pipeline_run_id`.

A probability of 0.72 means the model assigns 72% probability under the synthetic task. It is not a calibrated forecast for your actual portfolio. There is no universal rule that every probability over 0.5 should trigger contact; this POC uses its validation-selected threshold.

## Performance and cost

- Native Spark SQL expressions avoid Python row loops and UDF overhead.
- Parquet + Snappy reduces I/O; adaptive query execution is enabled.
- Spark data stays distributed across local worker threads; only tiny aggregate summaries reach Python.
- Persistent stage outputs make tasks separately observable and retryable.
- S3 transfer uses eight bounded I/O threads. The single-node POC stages files on EBS rather than requiring Hadoop S3A JAR resolution. This uses extra disk/I/O and is not a distributed S3-native design.
- Start with 16 shuffle partitions for 4 worker threads. Measure `timings/*.json` before tuning.
- One EC2 machine and Snowflake X-Small with 60-second auto-suspend limit infrastructure complexity. Costs depend on region, runtime, storage and data transfer; no fixed price is promised.
- Scale-out next step: move Spark jobs to EMR Serverless or AWS Glue, use native S3 access, and move orchestration/tracking to managed or hardened services. Do not describe this Compose stack as production HA.

## Retry behavior and scope

Run IDs isolate S3 prefixes and Snowflake tables. Retrying an export clears only that run's raw prefix and republishes `_READY` after both snapshots succeed. Other stages also publish `_READY` only after uploads finish. Downstream readers require the marker. An interrupted upload cannot be accepted as a completed stage.

A retried training task can register an additional model version if failure happened after registration; it is at-least-once, not exactly-once registration. The successful manifest pins the selected version. Scheduled runs are serialized. Do not manually reuse a run ID for different data or run concurrent writers against the same prefix. The POC regenerates complete snapshots; it does not implement CDC, incremental extraction or a real production source ingestion connector.
