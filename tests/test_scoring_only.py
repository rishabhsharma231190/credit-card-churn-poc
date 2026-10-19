import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock, MagicMock

import pytest
from churn.config import Config
from churn.storage import Store
from churn import resolve_model as resolver
from churn import snowflake_io


def config(tmp_path, monkeypatch):
    monkeypatch.setenv("WORK_ROOT", str(tmp_path))
    return Config("reuse", "local", 100, scoring_only=True)


def test_champion_is_pinned_without_registry_writes(tmp_path, monkeypatch):
    cfg = config(tmp_path, monkeypatch)
    client = Mock(spec=["get_model_version_by_alias", "get_run"])
    client.get_model_version_by_alias.return_value = SimpleNamespace(version="4", run_id="training")
    client.get_run.return_value = SimpleNamespace(data=SimpleNamespace(params={"threshold": "0.3"}, tags={"feature_version": "v1"}))
    monkeypatch.setattr(resolver, "MlflowClient", lambda: client)
    monkeypatch.setattr(resolver.mlflow, "set_tracking_uri", lambda _: None)
    store = Store(cfg)
    resolver.resolve_model(cfg, store)
    client.get_model_version_by_alias.return_value = SimpleNamespace(version="5", run_id="other")
    resolver.resolve_model(cfg, store)
    manifest = json.loads((cfg.path("model") / "manifest.json").read_text())
    assert manifest["version"] == "4"
    assert manifest["threshold"] == 0.3
    client.get_model_version_by_alias.assert_called_once_with(cfg.model_name, "champion")


@pytest.mark.parametrize("failure", ["missing", "incompatible", "bad_threshold"])
def test_unusable_champion_fails_without_fallback(tmp_path, monkeypatch, failure):
    cfg = config(tmp_path, monkeypatch)
    client = Mock(spec=["get_model_version_by_alias", "get_run"])
    client.get_model_version_by_alias.return_value = SimpleNamespace(version="4", run_id="training")
    client.get_run.return_value = SimpleNamespace(data=SimpleNamespace(
        params={"threshold": "nan" if failure == "bad_threshold" else "0.3"},
        tags={"feature_version": "v2" if failure == "incompatible" else "v1"}))
    if failure == "missing":
        client.get_model_version_by_alias.side_effect = ValueError("No champion")
    monkeypatch.setattr(resolver, "MlflowClient", lambda: client)
    monkeypatch.setattr(resolver.mlflow, "set_tracking_uri", lambda _: None)
    with pytest.raises(ValueError):
        resolver.resolve_model(cfg, Store(cfg))
    assert not (cfg.path("model") / "manifest.json").exists()


def test_existing_export_only_reads_source(tmp_path, monkeypatch):
    cfg = config(tmp_path, monkeypatch)
    monkeypatch.setenv("SNOWFLAKE_SCORE_TABLE", "CHURN_POC_DB.PUBLIC.EXISTING_SCORE")
    monkeypatch.setenv("SNOWFLAKE_STORAGE_INTEGRATION", "CHURN_POC_S3")
    conn = MagicMock()
    cur = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = (100,)
    cur.sfqid = "copy-query"
    monkeypatch.setattr(snowflake_io, "connection", lambda: conn)
    store = Mock()
    snowflake_io.export_existing(cfg, store)
    sql = [call.args[0] for call in cur.execute.call_args_list]
    assert len(sql) == 2
    assert sql[0] == "SELECT COUNT(*) FROM CHURN_POC_DB.PUBLIC.EXISTING_SCORE"
    assert sql[1].lstrip().startswith("COPY INTO 's3://")
    assert "FROM CHURN_POC_DB.PUBLIC.EXISTING_SCORE" in sql[1]
    assert "churn_90d" not in sql[1]
    assert not any(word in " ".join(sql) for word in ("CREATE ", "INSERT ", "UPDATE ", "DELETE ", "GENERATOR"))
    store.s3.put_object.assert_called_once()
    cur.fetchone.return_value = (99,)
    store.reset_mock()
    with pytest.raises(ValueError, match="expected 100"):
        snowflake_io.export_existing(cfg, store)
    store.clear_remote.assert_not_called()


@pytest.mark.parametrize("value", ["", "A;DROP TABLE B", "A.B.C.D", "A.'B'"])
def test_unsafe_or_missing_source_rejected(monkeypatch, value):
    monkeypatch.setenv("SNOWFLAKE_SCORE_TABLE", value)
    with pytest.raises(ValueError):
        snowflake_io.existing_score_table()


def test_shell_runs_only_scoring_stages_and_propagates_failure(tmp_path):
    python = tmp_path / "python"
    python.write_text('#!/bin/bash\necho "$*" >> "$CALL_LOG"\n[[ "$*" != *" prepare "* ]]\n')
    python.chmod(0o755)
    script = Path(__file__).resolve().parents[1] / "scripts/run_scoring.sh"
    env = dict(os.environ, CHURN_PYTHON=str(python), CALL_LOG=str(tmp_path / "calls"), TRAIN_MODEL="true")
    result = subprocess.run(["bash", str(script), "all", "run_1", "cloud"], env=env, capture_output=True)
    assert result.returncode != 0
    calls = (tmp_path / "calls").read_text().splitlines()
    assert len(calls) == 4
    assert 'data_quality' in calls[2]
    assert all("--scoring-only" in call for call in calls)
    assert "resolve_model" in calls[0] and "export_existing" in calls[1]
    assert not any(" train " in call or " ingest " in call for call in calls)
    result = subprocess.run(["bash", str(script), "train", "run_1"], env=env, capture_output=True)
    assert result.returncode == 2
