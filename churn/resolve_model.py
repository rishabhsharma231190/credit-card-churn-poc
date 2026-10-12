"""Read the champion once per run. Never train, register, or change an alias."""
import json
import math
import os

import mlflow
from mlflow import MlflowClient
from botocore.exceptions import ClientError
from .config import FEATURE_VERSION


def validate_manifest(manifest, cfg):
    version = str(manifest["version"])
    if not version.isdigit() or manifest["model_uri"] != f"models:/{cfg.model_name}/{version}":
        raise ValueError("Invalid pinned model identity")
    if manifest["feature_version"] != FEATURE_VERSION:
        raise ValueError("Champion feature version is incompatible with scoring code")
    threshold = float(manifest["threshold"])
    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError("Champion threshold must be between zero and one")
    return manifest


def resolve_model(cfg, store):
    # In cloud mode the completion marker is authoritative, not a partial local file.
    ready = (cfg.path("model") / "manifest.json").exists()
    if cfg.mode == "cloud":
        try:
            store.s3.head_object(Bucket=cfg.bucket, Key=cfg.key("model") + "_READY")
            ready = True
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise
            ready = False
    if ready:
        manifest = validate_manifest(json.loads((store.fetch("model") / "manifest.json").read_text()), cfg)
        print(f"Reusing pinned model {manifest['model_uri']} for run {cfg.run_id}")
        return
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = MlflowClient()
    # Fail if champion is missing: no fallback to fitting or registering a model.
    version = client.get_model_version_by_alias(cfg.model_name, "champion")
    run = client.get_run(version.run_id)
    manifest = validate_manifest({
        "model_uri": f"models:/{cfg.model_name}/{version.version}",
        "version": str(version.version), "mlflow_run_id": version.run_id,
        "model_alias": "champion", "pipeline_run_id": cfg.run_id,
        "threshold": float(run.data.params["threshold"]),
        "feature_version": run.data.tags["feature_version"],
    }, cfg)
    store.write_json("model", "manifest.json", manifest)
    store.publish("model")
    print(json.dumps(manifest, indent=2))
