# Docker-free deployment on the migrated EC2 host

The runtime migration was verified on 13 October 2026: all five native Airflow
tasks completed, 2,000,000 rows were scored, and 421,253 customers were flagged
using `credit_card_churn` champion version 7. Docker containers and the Docker
daemon were stopped. This validates the runtime migration; the new CI/CD
deployment still needs its first successful AWS execution.

## What runs where

| Component | Location on EC2 | Purpose |
| --- | --- | --- |
| Airflow Python environment | `/opt/airflow/venv` | Airflow 2.10.5 with its own dependency constraints |
| Application environment | `/opt/churn-venv` | PySpark, MLflow, Snowflake connector and S3 client |
| Application code | `/opt/churn/churn` | Export, preparation, features and scoring modules |
| Shell entry point | `/opt/churn/scripts/run_scoring.sh` | Runs one task and propagates its exit status |
| DAG | `/opt/airflow/dags/churn_pipeline.py` | Five BashOperator tasks and daily 08:00 IST schedule |
| Runtime settings | `/etc/churn/airflow.env` | Root-readable systemd EnvironmentFile; includes credentials |
| Snowflake private key | `/opt/airflow/secrets/rsa_key.p8` | Existing encrypted service-user key |
| MLflow metadata | `/mlflow/mlflow.db` | Preserved SQLite experiments and registry |
| MLflow artifacts | `/mlartifacts` | Preserved model files |
| PostgreSQL | Native `postgresql.service` | Preserved Airflow users, DAG runs and task history |
| Work files | `/opt/airflow/work` | Run-specific intermediate and summary files |

Linux services: `churn-airflow-webserver`, `churn-airflow-scheduler`,
`churn-mlflow`, and `postgresql`. Airflow and MLflow listen on loopback ports
8080 and 5000 and remain accessible through the existing SSH tunnel.

After the first native deployment, the application, application virtualenv and
DAG paths above become symlinks to one versioned release. Virtual environments
are built at their final paths, because Python virtualenv scripts use absolute
paths. Airflow's own virtualenv and databases are not replaced by a release.

## How CI/CD works

1. A pull request triggers the `verify` job in `.github/workflows/ci-cd.yml`.
2. CI installs the application versions captured in `requirements-native.lock`
   and installs Airflow separately with the official 2.10.5/Python 3.11 constraints.
3. It validates code, deployment safeguards and DAG imports. An isolated MLflow
   server and synthetic 5,000-row dataset exercise PySpark end to end. CI creates
   its own bootstrap model, then verifies that scoring preserves the champion
   alias and number of registered versions. It never uses production credentials.
4. CI packages selected source files, dependency pins and the commit SHA into
   `release.tar.gz`, computes SHA256, and uploads a GitHub artifact. Secrets,
   `.env`, databases, virtualenvs and work files are excluded.
5. A push or manual run on `main` can deploy only when the **repository variable**
   `ENABLE_DEPLOY` is exactly `true`, after `verify` succeeds. The `poc-aws`
   environment and GitHub OIDC role still apply.
6. GitHub uploads the tested archive to
   `s3://<DEPLOY_BUCKET>/churn-poc/deployments/<commit>-<run>-<attempt>/release.tar.gz`.
7. `scripts/deploy_native_ssm.py` asks AWS Systems Manager to run commands on the
   configured EC2 instance. EC2 downloads the archive using its instance role,
   verifies SHA256, validates archive paths, and invokes `scripts/deploy_native.py`.
8. EC2 builds a fresh application virtualenv from the pins while services continue
   running. It checks dependencies, parses the candidate DAG, and downloads the
   current champion's artifacts without registering a model.
9. The deployment pauses the DAG and rejects the release if a queued/running DAG
   run exists. It restores the original pause state on rejection, so the existing
   run can finish. Retry deployment after that run finishes.
10. With no active run, it stops the three application services, switches the code,
    virtualenv and DAG links, restarts services, and checks fresh scheduler health
    and unchanged registry versions/champion. It restores the original pause state.

No Docker build, container startup, ECR login or ECR push occurs in this workflow.
The `docker` Python package remains a dependency of MLflow; it does not start the
Docker daemon. Existing Docker scripts and infrastructure are retained as legacy
reference and are not invoked by native CI/CD.

## First rollout: one step at a time

1. Keep `ENABLE_DEPLOY=false`. Review this PR and wait for `verify` to pass.
2. Merge the PR. The resulting main workflow verifies code but skips deployment.
3. Confirm EC2 is running and Systems Manager shows it as online. Allow the current
   scoring run to finish. Keep the source table, model registry and credentials.
4. Set the repository variable `ENABLE_DEPLOY=true`.
5. In Actions, select **Churn CI and CD → Run workflow → main**. The workflow_dispatch
   button becomes available after this workflow is on the default branch.
6. Wait for both `verify` and `deploy`. Check Airflow and MLflow through the tunnel.
7. Run one scoring batch; verify 2 million rows, version 7 (while it remains the
   champion), unchanged registry version count, and new prediction files in S3.

Required repository variables: `AWS_DEPLOY_ROLE_ARN`, `AWS_REGION`, `DEPLOY_BUCKET`,
`EC2_INSTANCE_ID`, `ENABLE_DEPLOY`. `ECR_REPOSITORY` is no longer used and can be
removed after native deployment succeeds. Existing S3 and SSM IAM permissions use
the same deployment prefix, instance and command document. ECR permissions can
likewise be removed after validation; do not broaden permissions for this change.

## Recovery and limits

Ordinary switch/start/health failures attempt to restore the previous links,
restart services, and restore the original DAG pause state. If recovery itself
fails, the DAG stays paused and deployment fails. Inspect the SSM command output,
`journalctl -u churn-airflow-scheduler -u churn-airflow-webserver -u churn-mlflow`,
and `/opt/churn-native-backups/<release>/state.json` before any retry.

A host power loss or forced kill can interrupt automatic recovery. The state
file records each previous path before the switch; use it to restore links with
services stopped. Do not delete previous releases, migration backups or Docker
volumes during the first rollout. Each new virtualenv consumes disk space; review
old releases periodically and retain the current and previous release plus any
release referenced by rollback state. No automatic destructive cleanup is used.

This deployment targets the **already migrated AL2023/Python 3.11/Java 17 host**;
it is not a new-server provisioning script. EC2 needs Python package download
access, the AWS CLI, SSM Agent, its instance role, and enough disk for an additional
virtualenv. Pinned package versions must be available to CI and EC2; there is no
automatic fallback to untested versions. If the captured pins cannot be installed,
CI must fail and the dependency list must be reconciled before merging.

Airflow and MLflow upgrades requiring database migrations are deliberately outside
this application deployment: Airflow must remain 2.10.5 and MLflow 2.22.0. Native
libraries differ between Ubuntu CI and Amazon Linux, so EC2 also runs `pip check`,
DAG parsing, artifact retrieval and service health checks. Full Snowflake/S3 scoring
is the post-deployment acceptance check. Schedule execution requires EC2 to be on.

Official references:
- https://airflow.apache.org/docs/apache-airflow/2.10.5/installation/installing-from-pypi.html
- https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow
