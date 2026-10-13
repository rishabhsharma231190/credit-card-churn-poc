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
workflow_text = Path(".github/workflows/ci-cd.yml").read_text()
assert "docker " not in workflow_text and "amazon-ecr-login" not in workflow_text
assert "requirements-native.lock" in workflow_text
assert "deploy_native_ssm.py" in workflow_text
assert "vars.ENABLE_DEPLOY == 'true'" in workflow["jobs"]["deploy"]["if"]
def pins(path):
    result = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            name, version = line.split("==")
            result[re.sub(r"[-_.]+", "-", name).lower()] = version
    return result
locked = pins("requirements-native.lock")
assert all(locked.get(name) == version for name, version in pins("requirements.txt").items()), \
    "Update requirements-native.lock alongside requirements.txt"
print("Configuration contracts passed")
