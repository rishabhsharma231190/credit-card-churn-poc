"""Use temporary directories and mocked services; never touch an EC2 host."""
import importlib.util
import json
from pathlib import Path
import tarfile

import pytest


def load(name):
    path = Path(__file__).resolve().parents[1] / "scripts" / (name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


native = load("deploy_native")
ssm = load("deploy_native_ssm")
builder = load("build_native_release")


@pytest.mark.parametrize("previous_paused", [True, False])
@pytest.mark.parametrize("scenario", ["success", "active_run", "failed_health", "changed_registry"])
def test_cutover_and_rollback(tmp_path, monkeypatch, scenario, previous_paused):
    app = tmp_path / "app"
    app.mkdir()
    (app / "code.py").write_text("previous release")
    release = tmp_path / "release"
    release.mkdir()
    (release / "code.py").write_text("new release")
    backup = tmp_path / "backup"
    backup.mkdir()
    calls, pauses, health_checks = [], [], []
    monkeypatch.setattr(native, "run", lambda args, **kwargs: calls.append(args))
    monkeypatch.setattr(native, "pause_and_inspect", lambda env: {
        "was_paused": previous_paused, "active_runs": int(scenario == "active_run")})
    monkeypatch.setattr(native, "restore_pause", lambda env, paused: pauses.append(paused))
    monkeypatch.setattr(native, "registry_snapshot", lambda env:
                        {"champion": "8" if scenario == "changed_registry" else "7"})
    def health(after):
        health_checks.append(after)
        if scenario == "failed_health" and len(health_checks) == 1:
            raise RuntimeError("candidate unhealthy")
    monkeypatch.setattr(native, "health", health)
    if scenario == "success":
        native.activate(release, backup, {}, {"champion": "7"}, [(app, release)])
        assert app.is_symlink()
        assert (app / "code.py").read_text() == "new release"
    else:
        with pytest.raises(RuntimeError):
            native.activate(release, backup, {}, {"champion": "7"}, [(app, release)])
        assert (app / "code.py").read_text() == "previous release"
    assert pauses == [previous_paused]
    if scenario == "active_run":
        assert not calls, "Active runs must not have their services stopped"
    elif scenario != "success":
        assert len(health_checks) >= 2


def test_switch_failure_restores_partially_moved_installation(tmp_path):
    app, release, backup = (tmp_path / name for name in ("app", "release", "backup"))
    for path in (app, release, backup):
        path.mkdir()
    (app / "old").write_text("keep")
    state = {"paths": []}
    with pytest.raises(RuntimeError):
        native.switch_paths([(app, release), (tmp_path / "missing", release)], backup, state)
    native.rollback_paths(state)
    assert (app / "old").read_text() == "keep"


def test_environment_values_are_literal(tmp_path):
    value = 'a space $HOME `whoami` "quote" \\path'
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    envfile = tmp_path / "runtime.env"
    envfile.write_text('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="' + escaped + '"\n')
    assert native.read_environment(envfile)["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"] == value


def test_failed_recovery_keeps_dag_paused(tmp_path, monkeypatch):
    app, release, backup = (tmp_path / name for name in ("app", "release", "backup"))
    for path in (app, release, backup):
        path.mkdir()
    monkeypatch.setattr(native, "run", lambda *args, **kwargs: None)
    monkeypatch.setattr(native, "pause_and_inspect", lambda env:
                        {"was_paused": False, "active_runs": 0})
    resumed = []
    monkeypatch.setattr(native, "restore_pause", lambda *args: resumed.append(True))
    def failed(after):
        raise RuntimeError("services unavailable")
    monkeypatch.setattr(native, "health", failed)
    with pytest.raises(RuntimeError):
        native.activate(release, backup, {}, {}, [(app, release)])
    assert not resumed


def test_archive_omits_credentials_and_runtime_data(tmp_path):
    for folder in ("churn", "dags", "scripts", "secrets", "work"):
        (tmp_path / folder).mkdir()
    for path in ("requirements.txt", "requirements-native.lock", "churn/config.py",
                 "dags/churn_pipeline.py", "scripts/run_scoring.sh", "scripts/deploy_native.py",
                 ".env", "secrets/key.p8", "work/result.parquet", "scripts/deploy_ec2.sh"):
        (tmp_path / path).write_text("test")
    output = tmp_path / "release.tar.gz"
    builder.build(tmp_path, output, "a" * 40)
    with tarfile.open(output) as archive:
        names = set(archive.getnames())
    assert "churn/config.py" in names and "CODE_COMMIT" in names
    assert not names.intersection({".env", "secrets/key.p8", "work/result.parquet",
                                   "scripts/deploy_ec2.sh"})


def test_ssm_validates_checksum_before_extract_and_execute():
    result = ssm.commands("example-bucket", "a" * 40 + "-123-1", "b" * 64)
    assert "sha256sum -c" in result[5]
    assert "Unsafe archive member" in result[6]
    assert "deploy_native.py" in result[7]
    assert "docker" not in " ".join(result)
    with pytest.raises(ValueError):
        ssm.commands("example-bucket", "../../bad", "b" * 64)
    with pytest.raises(ValueError):
        ssm.commands("example-bucket", "a" * 40 + "-123-1", "bad; command")
