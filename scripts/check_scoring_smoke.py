"""CI-only check: reuse the bootstrap champion without creating another model version."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import mlflow
from mlflow import MlflowClient

mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
client = MlflowClient()
name = os.getenv("MODEL_NAME", "credit_card_churn")
before = {v.version for v in client.search_model_versions(f"name='{name}'")}
champion = client.get_model_version_by_alias(name, "champion").version
root = Path(os.environ["WORK_ROOT"])
target = root / "ci_score_only"
assert not target.exists(), "Use this script only in a disposable CI environment"
shutil.copytree(root / "ci_smoke" / "raw" / "score", target / "raw" / "score")
script = Path(__file__).resolve().parent / "run_scoring.sh"
subprocess.run(["bash", str(script), "all", "ci_score_only", "local"],
               check=True, env=dict(os.environ, ROWS="5000"))
manifest = json.loads((target / "model/manifest.json").read_text())
summary = json.loads((target / "scores/summary.json").read_text())
assert manifest["version"] == champion
assert summary["rows"] == 5000
assert not (target / "features/train").exists()
assert {v.version for v in client.search_model_versions(f"name='{name}'")} == before
assert client.get_model_version_by_alias(name, "champion").version == champion
print("Scoring-only smoke passed: 5000 rows; champion and registry versions unchanged")

quality = json.loads((target / "data_quality/report.json").read_text())
assert quality["status"] == "passed"
assert quality["datasets"]["score"]["rows"] == 5000
