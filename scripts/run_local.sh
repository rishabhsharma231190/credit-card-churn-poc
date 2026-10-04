#!/usr/bin/env bash
set -euo pipefail
# Run from project root after initializing Compose. rows and run-id are optional.
rows="${1:-10000}"
run_id="${2:-local_$(date -u +%Y%m%dT%H%M%S)}"
for stage in ingest export prepare features train score; do
  docker compose run --rm runner -m churn.cli "$stage" --mode local --rows "$rows" --run-id "$run_id"
done
