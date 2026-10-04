# Plan for 5 October 2026, 14:00 IST

Aim to have a complete working demo by 12:00 IST, leaving two hours to recover and prepare your explanation. These are work targets, not a guarantee of account provisioning or runtime.

| When | Deliverable | Stop/go check |
|---|---|---|
| First 60–90 minutes | Extract project, build Docker, start Airflow/MLflow | Both UIs open |
| Next 30–60 minutes | 10,000-customer local run | Six green tasks, registered model, score summary |
| Before overnight break | Start full 2-million local run; request any missing AWS/Snowflake access | Exact score count = 2,000,000 |
| By 09:00, 5 October | Provision EC2/S3; build image; configure Snowflake integration and key pair | Service authentication and S3 export work |
| By 10:30 | Cloud test with 10,000 customers | Data visible in Snowflake and S3; six green tasks |
| By 12:00 | Cloud run with 2 million customers | 2 million predictions in S3; MLflow version captured |
| 12:00–13:00 | Prepare demo evidence and rerun only failed checks | Save task graph, metrics and S3 screenshots |
| 13:00–14:00 | Buffer and presentation rehearsal | Be clear about synthetic data and remaining work |

If account access is blocked, keep the validated local POC and show the prepared deployment configuration. State clearly that cloud execution remains pending; do not present a local run as an AWS/Snowflake deployment. Avoid adding SageMaker endpoints, Kubernetes or a complex CI/CD stack before this deadline.

Demo sequence:

1. Explain the churn definition and two snapshots.
2. Show 2-million row counts in Snowflake or the local quality report, explicitly identifying which path ran.
3. Show the Airflow dependency graph and task logs.
4. Show S3 raw/prepared/features/scores folders for a completed cloud run.
5. Show MLflow test metrics and registered model version.
6. Show the scoring summary with 2,000,000 rows and the model version manifest.
7. Explain production gaps: real labels, out-of-time validation, security hardening, monitoring, backup and managed execution.
