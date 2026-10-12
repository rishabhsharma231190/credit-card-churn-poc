#!/usr/bin/env bash
# Called inside the deployed container by Airflow, or manually with docker compose exec.
set -euo pipefail
stage="${1:?Usage: run_scoring.sh STAGE RUN_ID [cloud|local]}"
run_id="${2:?Supply a unique run ID; reuse it only when retrying that run}"
mode="${3:-${PIPELINE_MODE:-cloud}}"
python_bin="${CHURN_PYTHON:-/opt/churn-venv/bin/python}"
case "$stage" in
  all) stages=(resolve_model export_existing prepare features score) ;;
  resolve_model|export_existing|prepare|features|score) stages=("$stage") ;;
  *) echo "Unsupported scoring stage: $stage" >&2; exit 2 ;;
esac
# Explicit CLI isolation: TRAIN_MODEL=true in an old .env cannot enable training here.
export TRAIN_MODEL=false
for current in "${stages[@]}"; do
  echo "Running scoring stage=$current run_id=$run_id mode=$mode"
  "$python_bin" -m churn.cli "$current" --scoring-only --mode "$mode" \
    --rows "${ROWS:-2000000}" --run-id "$run_id"
done
