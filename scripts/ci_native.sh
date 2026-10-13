#!/usr/bin/env bash
# Disposable CI only. Never point this at the production MLflow registry.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
scratch="$(mktemp -d)"
export WORK_ROOT="$scratch/work"
export MLFLOW_TRACKING_URI=http://127.0.0.1:15001
export PYTHONPATH="$root"
export PYSPARK_PYTHON="$CHURN_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1 SPARK_MASTER='local[2]' SPARK_DRIVER_MEMORY=1g
export MODEL_NAME=credit_card_churn TRAIN_MODEL=true
mlflow_pid=""
cleanup() {
  if [[ -n "$mlflow_pid" ]]; then
    kill "$mlflow_pid" 2>/dev/null || true
    wait "$mlflow_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT
"$CHURN_PYTHON" -m mlflow server --host 127.0.0.1 --port 15001 --workers 1 \
  --backend-store-uri "sqlite:///$scratch/mlflow.db" \
  --serve-artifacts --artifacts-destination "$scratch/artifacts" \
  >"$scratch/mlflow.log" 2>&1 &
mlflow_pid=$!
ready=false
for attempt in {1..30}; do
  if curl --fail --silent --max-time 2 "$MLFLOW_TRACKING_URI/health" >/dev/null; then
    ready=true
    break
  fi
  sleep 2
done
if [[ "$ready" != true ]]; then cat "$scratch/mlflow.log"; exit 1; fi
# Bootstrap a model only in this isolated CI registry, then prove scoring reuses it.
"$CHURN_PYTHON" -m churn.cli all --mode local --rows 5000 --run-id ci_smoke
"$CHURN_PYTHON" "$root/scripts/check_scoring_smoke.py"
