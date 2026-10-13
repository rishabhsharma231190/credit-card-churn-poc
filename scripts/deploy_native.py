"""Deploy to the already-migrated EC2 host; never initialize production databases.

Run as root via SSM, with a verified release directory. Service configuration,
credentials, work data, PostgreSQL, /mlflow and /mlartifacts stay outside releases.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import time
from urllib.request import urlopen

AIRFLOW_PYTHON = "/opt/airflow/venv/bin/python"
SERVICES = ["churn-airflow-scheduler", "churn-airflow-webserver", "churn-mlflow"]
DAG_ID = "credit_card_churn_poc"


def read_environment(path):
    """Read the single-line, double-quoted EnvironmentFile created at migration.

    No shell execution or variable expansion; literal passphrases stay literal.
    """
    env = {"HOME": "/opt/airflow", "USER": "airflow", "LOGNAME": "airflow"}
    for line in Path(path).read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        fields = shlex.split(line, comments=False, posix=True)
        if len(fields) != 1 or "=" not in fields[0]:
            raise ValueError("Unsupported environment file syntax")
        key, value = fields[0].split("=", 1)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError("Invalid environment key")
        env[key] = value
    return env


def run(args, env=None, capture=False, timeout=1200):
    options = {}
    if env is not None:
        options.update(env=env, user="airflow", group="airflow", extra_groups=[],
                       cwd="/opt/airflow")
    result = subprocess.run(args, check=True, text=True, timeout=timeout,
                            stdout=subprocess.PIPE if capture else None, **options)
    return result.stdout.strip() if capture else None


def airflow_json(env, code):
    output = run([AIRFLOW_PYTHON, "-c", code], env, capture=True, timeout=60)
    # Airflow can print startup messages before our JSON result.
    return json.loads(output.splitlines()[-1])


def pause_and_inspect(env):
    return airflow_json(env, '''
import json
from airflow.models import DagModel, DagRun
from airflow.utils.session import create_session
with create_session() as session:
    dag = session.query(DagModel).filter_by(dag_id="credit_card_churn_poc").with_for_update().one()
    previous = dag.is_paused
    dag.is_paused = True
    active = session.query(DagRun).filter(DagRun.dag_id == dag.dag_id,
        DagRun.state.in_(["running", "queued"])).count()
    print(json.dumps({"was_paused": previous, "active_runs": active}))
''')


def restore_pause(env, paused):
    run(["/opt/airflow/venv/bin/airflow", "dags", "pause" if paused else "unpause", DAG_ID],
        env, timeout=60)


def registry_snapshot(env, python="/opt/churn-venv/bin/python"):
    code = '''
import json, os, tempfile
from pathlib import Path
import mlflow
from mlflow import MlflowClient
mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
client = MlflowClient()
name = os.environ.get("MODEL_NAME", "credit_card_churn")
champion = client.get_model_version_by_alias(name, "champion")
versions = client.search_model_versions("name='" + name + "'", max_results=1000)
if versions.token:
    raise RuntimeError("Registry pagination exceeds this POC deployment check")
with tempfile.TemporaryDirectory() as folder:
    path = mlflow.artifacts.download_artifacts(
        artifact_uri=f"models:/{name}/{champion.version}", dst_path=folder)
    assert (Path(path) / "MLmodel").is_file(), "Missing champion model files"
print(json.dumps({"champion": champion.version,
    "versions": sorted(v.version for v in versions)}))
'''
    output = run([python, "-c", code], env, capture=True, timeout=120)
    return json.loads(output.splitlines()[-1])


def replace_link(link, target):
    link, target = Path(link), Path(target)
    temporary = link.with_name(link.name + ".native-next")
    if temporary.is_symlink():
        temporary.unlink()
    temporary.symlink_to(target, target_is_directory=True)
    os.replace(temporary, link)


def switch_paths(pairs, backup, state):
    for link, target in pairs:
        link = Path(link)
        if link.is_symlink():
            previous = str(link.resolve(strict=True))
            moved = False
        elif link.is_dir():
            previous = str(backup / link.name)
            moved = True
        else:
            raise RuntimeError(f"Expected an existing installation at {link}")
        # Record before moving so interruption recovery knows the original path.
        state["paths"].append({"link": str(link), "previous": previous,
                               "target": str(target), "moved_directory": moved})
        (backup / "state.json").write_text(json.dumps(state, indent=2))
        if moved:
            link.rename(previous)
        replace_link(link, target)


def rollback_paths(state):
    for item in reversed(state["paths"]):
        link = Path(item["link"])
        # If a recorded rename never happened, the original directory is intact.
        if link.is_dir() and not link.is_symlink():
            continue
        replace_link(link, item["previous"])


def health(after):
    last_error = None
    for _ in range(40):
        try:
            run(["systemctl", "is-active", "--quiet", *SERVICES], timeout=10)
            with urlopen("http://127.0.0.1:8080/health", timeout=3) as response:
                status = json.load(response)
            from datetime import datetime
            heartbeat = datetime.fromisoformat(status["scheduler"]["latest_scheduler_heartbeat"])
            assert status["metadatabase"]["status"] == "healthy"
            assert status["scheduler"]["status"] == "healthy"
            assert heartbeat.timestamp() >= after
            with urlopen("http://127.0.0.1:5000/health", timeout=3) as response:
                assert response.status == 200
            return
        except Exception as error:
            last_error = error
            time.sleep(3)
    raise RuntimeError("Native service health checks failed") from last_error


def deploy(release):
    env = read_environment("/etc/churn/airflow.env")
    required = {"AIRFLOW_HOME": "/opt/airflow", "AIRFLOW_CONFIG": "/opt/airflow/airflow.cfg",
                "CHURN_PYTHON": "/opt/churn-venv/bin/python", "PYTHONPATH": "/opt/churn",
                "MLFLOW_TRACKING_URI": "http://127.0.0.1:5000", "TRAIN_MODEL": "false"}
    if any(env.get(key) != value for key, value in required.items()):
        raise RuntimeError("Host settings do not match the completed native migration")
    run(["systemctl", "is-active", "--quiet", "postgresql", *SERVICES], timeout=15)
    version = run([AIRFLOW_PYTHON, "-c",
                   "import importlib.metadata; print(importlib.metadata.version('apache-airflow'))"],
                  env, capture=True)
    if version != "2.10.5":
        raise RuntimeError("Airflow upgrades require a separate database migration")
    commit = (release / "CODE_COMMIT").read_text().strip()
    if commit != release.name.split("-")[0]:
        raise RuntimeError("Release commit mismatch")
    pins = (release / "requirements-native.lock").read_text().splitlines()
    if "mlflow==2.22.0" not in pins or "mlflow-skinny==2.22.0" not in pins:
        raise RuntimeError("MLflow server upgrades require a separate registry migration")

    # Build in a permanent release path; virtual environments cannot be relocated.
    candidate = release / "venv"
    if candidate.exists():
        raise RuntimeError("Release already prepared; rerun CI with a new attempt ID")
    run(["python3.11", "-m", "venv", str(candidate)])
    run([str(candidate / "bin/python"), "-m", "pip", "install", "--no-cache-dir",
         "-r", str(release / "requirements-native.lock")])
    run([str(candidate / "bin/python"), "-m", "pip", "check"])
    run(["chown", "-R", "root:airflow", str(release)])
    run(["chmod", "-R", "g+rX,o-rwx", str(release)])
    candidate_env = dict(env, PYTHONPATH=str(release),
                         CHURN_PYTHON=str(candidate / "bin/python"),
                         PYSPARK_PYTHON=str(candidate / "bin/python"))
    run([AIRFLOW_PYTHON, "-c", '''
import sys
from airflow.models import DagBag
bag = DagBag(sys.argv[1], include_examples=False)
assert not bag.import_errors, bag.import_errors
dag = bag.get_dag("credit_card_churn_poc")
assert set(dag.task_ids) == {"resolve_model", "export_existing", "prepare", "features", "score"}
''', str(release / "dags")], candidate_env, timeout=120)
    before = registry_snapshot(candidate_env, str(candidate / "bin/python"))

    backup = Path("/opt/churn-native-backups") / release.name
    backup.mkdir(parents=True, mode=0o750, exist_ok=False)
    run(["chown", "root:airflow", str(backup)])
    run(["chmod", "0750", str(backup)])
    activate(release, backup, env, before, [
        ("/opt/churn", release),
        ("/opt/churn-venv", candidate),
        ("/opt/airflow/dags", release / "dags"),
    ])


def activate(release, backup, env, before, pairs):
    state = {"release": str(release), "paths": [], "status": "preparing"}
    paused = None
    stopped = False
    safe_to_resume = True
    try:
        paused = pause_and_inspect(env)
        state.update(paused)
        (backup / "state.json").write_text(json.dumps(state, indent=2))
        if paused["active_runs"]:
            raise RuntimeError("Active/queued DAG run: finish it, then rerun deployment")
        stopped = True  # Restore services even if systemctl stops only some of them.
        safe_to_resume = False
        run(["systemctl", "stop", *SERVICES], timeout=180)
        switch_paths(pairs, backup, state)
        started = time.time()
        run(["systemctl", "start", "churn-mlflow", "churn-airflow-webserver",
             "churn-airflow-scheduler"], timeout=120)
        health(started)
        if registry_snapshot(env) != before:
            raise RuntimeError("Champion or registry versions changed during deployment")
        safe_to_resume = True
        state["status"] = "deployed"
        (backup / "state.json").write_text(json.dumps(state, indent=2))
        print("Native release deployed:", release.name, flush=True)
    except BaseException:
        if stopped:
            run(["systemctl", "stop", *SERVICES], timeout=180)
            rollback_paths(state)
            started = time.time()
            run(["systemctl", "start", "churn-mlflow", "churn-airflow-webserver",
                 "churn-airflow-scheduler"], timeout=120)
            health(started)
            safe_to_resume = True
        state["status"] = "rolled_back" if stopped else "rejected"
        (backup / "state.json").write_text(json.dumps(state, indent=2))
        raise
    finally:
        if paused is not None and safe_to_resume:
            restore_pause(env, paused["was_paused"])
        elif paused is not None:
            print("Recovery incomplete: DAG kept paused. Inspect native services and backup state.", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("release", type=Path)
    args = parser.parse_args()
    release = args.release.resolve()
    if os.geteuid() != 0:
        raise RuntimeError("Run through SSM as root")
    if release.parent != Path("/opt/churn-releases/native") or not re.fullmatch(
            r"[a-f0-9]{40}-[0-9]+-[0-9]+", release.name):
        raise ValueError("Invalid native release directory")
    def interrupted(signum, frame):
        raise RuntimeError("Deployment interrupted; attempting rollback")
    signal.signal(signal.SIGTERM, interrupted)
    with open("/run/lock/churn-native-deploy.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        deploy(release)


if __name__ == "__main__":
    main()
