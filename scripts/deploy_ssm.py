"""Run deployment on the single authorized EC2 target and wait for its result."""
import argparse
import os
import re
import shlex
import time
import boto3

p = argparse.ArgumentParser()
p.add_argument("--release", required=True)
p.add_argument("--image", required=True)
a = p.parse_args()
if not re.fullmatch(r"[a-f0-9]{40}-[0-9]+-[0-9]+", a.release):
    raise ValueError("Invalid release id")
if not re.fullmatch(r"[0-9]+\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/[a-z0-9/_-]+@sha256:[a-f0-9]{64}", a.image):
    raise ValueError("Expected an ECR image digest")
bucket = os.environ["DEPLOY_BUCKET"]
instance = os.environ["EC2_INSTANCE_ID"]
region = os.environ["AWS_REGION"]
q = shlex.quote
release_dir = "/opt/churn-releases/" + a.release
commands = ["set -eu", "mkdir -p " + q(release_dir),
    "aws s3 cp " + q(f"s3://{bucket}/churn-poc/deployments/{a.release}/release.tar.gz") + " " + q(release_dir+"/release.tar.gz"),
    "tar -xzf " + q(release_dir+"/release.tar.gz") + " -C " + q(release_dir),
    "bash " + q(release_dir+"/scripts/deploy_ec2.sh") + " " + q(release_dir) + " " + q(a.image) + " " + q(region)]
ssm = boto3.client("ssm", region_name=region)
result = ssm.send_command(InstanceIds=[instance], DocumentName="AWS-RunShellScript",
    Parameters={"commands": commands, "executionTimeout": ["1800"]}, TimeoutSeconds=120,
    Comment="Deploy tested churn release " + a.release)
command_id = result["Command"]["CommandId"]
print("SSM command:", command_id, flush=True)
for _ in range(200):
    try:
        result = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance)
    except ssm.exceptions.InvocationDoesNotExist:
        time.sleep(10)
        continue
    if result["Status"] == "Success":
        print(result.get("StandardOutputContent", ""))
        break
    if result["Status"] in ["Cancelled", "Failed", "TimedOut", "Cancelling"]:
        print(result.get("StandardOutputContent", ""))
        raise RuntimeError(result.get("StandardErrorContent", result["Status"]))
    time.sleep(10)
else:
    raise TimeoutError("Deployment status not terminal; inspect SSM command " + command_id)
