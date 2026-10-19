import json
from unittest.mock import Mock
import pytest
from pyspark.sql import functions as F
from churn.data_quality import profile, data_quality
from churn.config import Config
from churn.generate import snapshot
from churn.storage import Store


@pytest.fixture(scope="module")
def spark():
    from churn.spark import session
    s = session("data-quality-tests")
    yield s
    s.stop()


def test_missing_means_and_full_category_distribution(spark, tmp_path):
    df = spark.createDataFrame([(1.0, "A"), (3.0, "A"), (None, "B"),
                                (float("nan"), " "), (float("inf"), None)],
                               "amount double, category string")
    result = profile(df, ["amount", "category"], 5, tmp_path)
    assert result["status"] == "failed"
    assert result["columns"]["amount"]["mean"] == 2.0
    assert result["columns"]["amount"]["missing_count"] == 2
    assert result["columns"]["amount"]["non_finite_count"] == 1
    assert result["columns"]["category"]["missing_percent"] == 40.0
    distribution = spark.read.parquet(str(tmp_path / result["columns"]["category"]["distribution_path"]))
    assert distribution.agg(F.sum("count")).first()[0] == 5
    assert distribution.filter("value = 'A'").first().percent == 40.0
    json.dumps(result, allow_nan=False)


def test_empty_missing_columns_and_unknown_categories(spark, tmp_path):
    df = spark.createDataFrame([], "amount double")
    result = profile(df, ["amount", "absent"], 100, tmp_path, ["unknown"])
    assert result["status"] == "failed"
    assert result["missing_columns"] == ["absent"]
    assert result["columns"]["amount"]["mean"] is None
    assert len(result["errors"]) == 3


def test_bounded_preview_and_numeric_categories(spark, tmp_path):
    df = spark.range(30).selectExpr("id AS products")
    result = profile(df, ["products"], 30, tmp_path, ["products"])
    col = result["columns"]["products"]
    assert result["status"] == "passed"
    assert col["mean"] == 14.5
    assert len(col["top_20"]) == 20
    assert col["other_count"] == 10
    assert spark.read.parquet(str(tmp_path / col["distribution_path"])).count() == 30


@pytest.mark.parametrize("bad", [False, True])
def test_report_published_before_failure_and_raw_unchanged(spark, tmp_path, monkeypatch, bad):
    monkeypatch.setenv("WORK_ROOT", str(tmp_path))
    cfg = Config("quality_test", "local", 100, scoring_only=True)
    df = snapshot(spark, 100, "2026-10-04", False)
    if bad:
        df = df.withColumn("balance", F.lit(None).cast("double"))
    df.write.parquet(str(cfg.path("raw") / "score"))
    store = Store(cfg)
    store.publish = Mock()
    if bad:
        with pytest.raises(ValueError, match="Raw data quality failed"):
            data_quality(cfg, spark, store)
    else:
        data_quality(cfg, spark, store)
    report = json.loads((cfg.path("data_quality") / "report.json").read_text())
    assert report["status"] == ("failed" if bad else "passed")
    assert report["datasets"]["score"]["rows"] == 100
    store.publish.assert_called_once_with("data_quality")
    assert spark.read.parquet(str(cfg.path("raw") / "score")).count() == 100
