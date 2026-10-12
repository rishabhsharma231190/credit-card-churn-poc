#!/usr/bin/env bash
set -euo pipefail
release_dir="$1"
image="$2"
region="$3"
app="${APP_ROOT:-/home/ec2-user/credit-card-churn-poc}"
cd "$app"
test -f .env || { echo "Perform initial host setup first"; exit 1; }
# Keep the same directory/project name: existing named volumes retain their identity.
exec 9>"${DEPLOY_LOCK_FILE:-/var/lock/churn-deploy.lock}"
flock -n 9 || { echo "Another deployment is active"; exit 1; }
registry="${image%%/*}"
aws ecr get-login-password --region "$region" | docker login --username AWS --password-stdin "$registry"
docker pull "$image"
# Validate required scoring configuration before touching the running services.
docker run --rm --env-file .env --entrypoint /opt/churn-venv/bin/python "$image" -c "import os; from churn.snowflake_io import existing_score_table; existing_score_table() if os.getenv('PIPELINE_MODE') == 'cloud' else None"
old_image=$(cat .deployed-image 2>/dev/null || printf '%s' 'credit-card-churn:0.1')
export CHURN_IMAGE="$old_image"
# Runtime upgrades need a planned DB migration/backup; do not roll them back blindly.
old_airflow=$(docker compose exec -T airflow-scheduler python -c 'import airflow; print(airflow.__version__)')
new_airflow=$(docker run --rm --entrypoint python "$image" -c 'import airflow; print(airflow.__version__)')
old_mlflow=$(docker compose exec -T mlflow /opt/churn-venv/bin/python -c 'import mlflow; print(mlflow.__version__)')
new_mlflow=$(docker run --rm --entrypoint /opt/churn-venv/bin/python "$image" -c 'import mlflow; print(mlflow.__version__)')
[[ "$old_airflow" == "$new_airflow" && "$old_mlflow" == "$new_mlflow" ]] || { echo "Runtime version change: plan database migration separately"; exit 1; }
was_paused=$(docker compose exec -T airflow-scheduler python -c "from airflow.models import DagModel; from airflow.utils.session import create_session; s=create_session(); db=s.__enter__(); d=db.query(DagModel).filter_by(dag_id='credit_card_churn_poc').one(); print(int(d.is_paused)); s.__exit__(None,None,None)")
cp compose.yaml .compose-before-deploy.yaml
changed=0
restore() {
  rc=$?
  trap - EXIT
  if (( rc != 0 && changed == 1 )); then
    echo "Deployment failed; restoring previous application image/configuration"
    cp .compose-before-deploy.yaml compose.yaml
    export CHURN_IMAGE="$old_image"
    docker compose up -d --no-build --wait postgres mlflow airflow-webserver airflow-scheduler || true
  fi
  if [[ "$was_paused" == "0" ]]; then
    docker compose exec -T airflow-scheduler airflow dags unpause credit_card_churn_poc || true
  fi
  exit "$rc"
}
trap restore EXIT
docker compose exec -T airflow-scheduler airflow dags pause credit_card_churn_poc
# Pausing can also prevent downstream tasks from starting: do not wait while paused.
# Abort and restore the prior pause state if a run is active; retry CD after it finishes.
if ! docker compose exec -T airflow-scheduler python -c "from airflow.models import DagRun; from airflow.utils.session import create_session; s=create_session(); db=s.__enter__(); n=db.query(DagRun).filter(DagRun.dag_id=='credit_card_churn_poc',DagRun.state.in_(['running','queued'])).count(); s.__exit__(None,None,None); raise SystemExit(1 if n else 0)"; then
  echo "DAG is active; previous release retained. Rerun deployment after it finishes."
  exit 1
fi
docker compose stop airflow-scheduler
changed=1
cp "$release_dir/compose.yaml" compose.yaml
export CHURN_IMAGE="$image"
docker compose up -d --no-build --wait postgres mlflow
docker compose run --rm --pull never airflow-init
docker compose run --rm --pull never --entrypoint python airflow-init -c "from airflow.models import DagBag; b=DagBag('/opt/airflow/dags'); assert not b.import_errors, b.import_errors; assert 'credit_card_churn_poc' in b.dags"
docker compose up -d --no-build --wait airflow-webserver airflow-scheduler
printf '%s\n' "$image" > .deployed-image
# Persist the image selection for later manual Compose commands/reboots.
if grep -q '^CHURN_IMAGE=' .env; then
  sed -i "s|^CHURN_IMAGE=.*|CHURN_IMAGE=$image|" .env
else
  printf '\nCHURN_IMAGE=%s\n' "$image" >> .env
fi
echo "Deployment succeeded: $image"
