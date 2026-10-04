import pytest
from pyspark.sql import functions as F
from churn.generate import snapshot
from churn.prepare import clean
from churn.features import engineer
from churn.config import FEATURES, Config

@pytest.fixture(scope="module")
def spark():
    from churn.spark import session
    s = session("tests")
    yield s
    s.stop()

def test_synthetic_snapshots_and_feature_contract(spark):
    historical = snapshot(spark, 1000, "2026-05-01", True)
    current = snapshot(spark, 1000, "2026-10-04", False)
    assert historical.count() == current.count() == 1000
    assert "churn_90d" not in current.columns
    assert not {"customer_id", "as_of_date", "churn_90d", "split"} & set(FEATURES)
    assert set(FEATURES) <= set(engineer(current).columns)
    assert historical.select("churn_90d").distinct().count() == 2

def test_feature_arithmetic_and_zero_denominator(spark):
    row = (snapshot(spark, 1, "2026-05-01", False)
           .withColumn("balance", F.lit(500.0)).withColumn("credit_limit", F.lit(1000.0))
           .withColumn("spend_30d", F.lit(50.0)).withColumn("spend_prev_30d", F.lit(100.0))
           .withColumn("txn_count_30d", F.lit(0.0)).withColumn("digital_logins_30d", F.lit(2.0)))
    result = engineer(row).first()
    assert result.utilization == 0.5
    assert result.spend_change == -0.5
    assert result.avg_ticket == 0.0
    assert result.low_engagement == 1.0

def test_invalid_values_and_duplicates_fail(spark):
    df = snapshot(spark, 100, "2026-05-01", True)
    with pytest.raises(ValueError, match="Data quality"):
        clean(df.withColumn("credit_limit", F.lit(0.0)), True)
    with pytest.raises(ValueError, match="Duplicate"):
        clean(df.union(df.limit(1)), True)
    with pytest.raises(ValueError, match="Missing"):
        clean(df.drop("churn_90d"), True)

def test_generation_reproducible(spark):
    a = snapshot(spark, 100, "2026-05-01", True)
    b = snapshot(spark, 100, "2026-05-01", True)
    assert a.exceptAll(b).count() == 0

def test_unsafe_run_id_rejected():
    with pytest.raises(ValueError):
        Config("../../outside", "local", 1000)
