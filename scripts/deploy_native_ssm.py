"""Deliver the checksum-verified source archive to EC2 using its IAM role."""
import argparse
import os
import re
import shlex
import time


def commands(bucket, release, digest):
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid deployment bucket")
    if not re.fullmatch(r"[a-f0-9]{40}-[0-9]+-[0-9]+", release):
        raise ValueError("Invalid release id")
    if not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise ValueError("Invalid SHA256")
    target = "/opt/churn-releases/native/" + release
    uri = f"s3://{bucket}/churn-poc/deployments/{release}/release.tar.gz"
    q = shlex.quote
    extract = '''import pathlib, sys, tarfile
root = pathlib.Path(sys.argv[1]).resolve()
with tarfile.open(root / "release.tar.gz", "r:gz") as archive:
    members = archive.getmembers()
    for member in members:
        destination = (root / member.name).resolve()
        if not destination.is_relative_to(root) or not member.isfile():
            raise ValueError("Unsafe archive member")
    archive.extractall(root, members=members, filter="data")
'''
    return [
        "set -eu", "umask 022", "test ! -e " + q(target), "mkdir -p " + q(target),
        "aws s3 cp " + q(uri) + " " + q(target + "/release.tar.gz"),
        "printf '%s  %s\\n' " + q(digest) + " " + q(target + "/release.tar.gz") + " | sha256sum -c -",
        "python3.11 -c " + q(extract) + " " + q(target),
        "python3.11 " + q(target + "/scripts/deploy_native.py") + " " + q(target),
    ]


def main():
    import boto3
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", required=True)
    parser.add_argument("--sha256", required=True)
    args = parser.parse_args()
    instance = os.environ["EC2_INSTANCE_ID"]
    if not re.fullmatch(r"i-[a-f0-9]{8,17}", instance):
        raise ValueError("Invalid instance id")
    ssm = boto3.client("ssm", region_name=os.environ["AWS_REGION"])
    response = ssm.send_command(
        InstanceIds=[instance], DocumentName="AWS-RunShellScript", TimeoutSeconds=120,
        Parameters={"commands": commands(os.environ["DEPLOY_BUCKET"], args.release, args.sha256),
                    "executionTimeout": ["2400"]},
        Comment="Deploy native churn release " + args.release,
    )
    command_id = response["Command"]["CommandId"]
    print("SSM command:", command_id, flush=True)
    for _ in range(270):
        try:
            result = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance)
        except ssm.exceptions.InvocationDoesNotExist:
            time.sleep(10)
            continue
        status = result["Status"]
        if status in {"Success", "Cancelled", "Failed", "TimedOut"}:
            print(result.get("StandardOutputContent", ""))
            if status != "Success":
                print(result.get("StandardErrorContent", ""))
                raise RuntimeError(f"SSM deployment {status}: {command_id}")
            return
        time.sleep(10)
    raise TimeoutError("Inspect the still-running SSM command before retrying: " + command_id)


if __name__ == "__main__":
    main()
