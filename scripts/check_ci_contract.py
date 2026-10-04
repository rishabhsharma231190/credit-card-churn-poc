"""Fast configuration checks that require no cloud credentials."""
import json
import re
from pathlib import Path
import yaml

compose = yaml.safe_load(Path("compose.yaml").read_text())
assert "airflow-scheduler" in compose["services"]
assert "CHURN_IMAGE" in compose["services"]["runner"]["image"]
infra = json.loads(Path("aws/cloudformation.json").read_text())
assert re.fullmatch(infra["Parameters"]["AdminCidr"]["AllowedPattern"], "203.0.113.1/32")
assert infra["Resources"]["AppRepository"]["Properties"]["ImageTagMutability"] == "IMMUTABLE"
workflow = yaml.safe_load(Path(".github/workflows/ci-cd.yml").read_text())
assert workflow["jobs"]["deploy"]["needs"] == "verify"
assert workflow["jobs"]["deploy"]["environment"] == "poc-aws"
assert "pull_request_target" not in Path(".github/workflows/ci-cd.yml").read_text()
print("Configuration contracts passed")
