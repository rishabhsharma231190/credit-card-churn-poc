"""Exercise release control flow with fake Docker/AWS, never a real deployment."""
import os
from pathlib import Path
import subprocess
import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/deploy_ec2.sh"

@pytest.mark.parametrize("scenario", ["success", "active_run", "failed_health"])
def test_deployment_control_flow(tmp_path, scenario):
    app = tmp_path / "app"
    app.mkdir()
    (app / ".env").write_text("KEEP_SECRET=original\nCHURN_IMAGE=old-image\n")
    (app / ".deployed-image").write_text("old-image")
    (app / "compose.yaml").write_text("old compose")
    release = tmp_path / "release"
    release.mkdir()
    (release / "compose.yaml").write_text("new compose")
    binpath = tmp_path / "bin"
    binpath.mkdir()
    docker = binpath / "docker"
    docker.write_text('''#!/usr/bin/env python3
import os,sys
from pathlib import Path
args=' '.join(sys.argv[1:])
with open(os.environ['COMMAND_LOG'],'a') as f: f.write(os.environ.get('CHURN_IMAGE','')+' '+args+'\\n')
if 'import airflow' in args: print('2.10.5')
if 'import mlflow' in args: print('2.22.0')
if 'd.is_paused' in args: print('0')
if 'DagRun' in args and os.environ['SCENARIO']=='active_run': sys.exit(1)
marker=Path(os.environ['FAIL_MARKER'])
if 'up -d' in args and os.environ.get('CHURN_IMAGE')=='registry/new@sha256:abc' and os.environ['SCENARIO']=='failed_health' and not marker.exists():
 marker.touch(); sys.exit(1)
''')
    docker.chmod(0o755)
    aws = binpath / "aws"
    aws.write_text("#!/bin/sh\nprintf 'fake-login-password\\n'\n")
    aws.chmod(0o755)
    env = dict(os.environ, APP_ROOT=str(app), DEPLOY_LOCK_FILE=str(tmp_path / "lock"),
               PATH=str(binpath)+os.pathsep+os.environ["PATH"], SCENARIO=scenario,
               COMMAND_LOG=str(tmp_path / "commands"), FAIL_MARKER=str(tmp_path / "failed"))
    result = subprocess.run(["bash", str(SCRIPT), str(release), "registry/new@sha256:abc", "ap-south-1"],
                            env=env, capture_output=True, text=True)
    commands = (tmp_path / "commands").read_text()
    assert "KEEP_SECRET=original" in (app / ".env").read_text()
    assert "airflow dags unpause" in commands
    if scenario == "success":
        assert result.returncode == 0, result.stderr
        assert (app / "compose.yaml").read_text() == "new compose"
        assert "registry/new@sha256:abc" in (app / ".env").read_text()
    else:
        assert result.returncode != 0
        assert (app / "compose.yaml").read_text() == "old compose"
        assert "CHURN_IMAGE=old-image" in (app / ".env").read_text()
        if scenario == "active_run":
            assert "stop airflow-scheduler" not in commands
