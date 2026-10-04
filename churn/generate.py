"""Deterministic synthetic customer snapshots for a runnable local demo."""
import os
from datetime import date, timedelta
from pyspark.sql import functions as F

def snapshot(spark, rows, snapshot_date, labeled):
    df = spark.range(1, rows + 1).withColumnRenamed("id", "customer_id")
    def u(salt):
        return F.pmod(F.xxhash64("customer_id", F.lit(snapshot_date), F.lit(salt)), F.lit(1000000)) / 1000000.0
    df = (df.withColumn("as_of_date", F.lit(snapshot_date).cast("date"))
          .withColumn("tenure_months", F.floor(3 + u(1)*117).cast("double"))
          .withColumn("credit_limit", F.round(1000 + u(2)*19000, 2))
          .withColumn("balance", F.round(F.col("credit_limit")*u(3), 2))
          .withColumn("txn_count_30d", F.floor(u(4)*100).cast("double"))
          .withColumn("spend_prev_30d", F.round(100 + u(5)*4900, 2))
          .withColumn("spend_30d", F.round(F.col("spend_prev_30d")*(0.05 + u(6)*1.5), 2))
          .withColumn("inactive_days", F.floor(u(7)*90).cast("double"))
          .withColumn("support_calls_90d", F.floor(u(8)*8).cast("double"))
          .withColumn("products", F.floor(1+u(9)*5).cast("double"))
          .withColumn("digital_logins_30d", F.floor(u(10)*40).cast("double")))
    if labeled:
        z = (-3.5 + 0.045*F.col("inactive_days") + 0.28*F.col("support_calls_90d")
             -0.025*F.col("txn_count_30d") -0.035*F.col("digital_logins_30d")
             -1.2*(F.col("spend_30d")/F.col("spend_prev_30d")-1))
        df = df.withColumn("churn_90d", (u(11) < 1/(1+F.exp(-z))).cast("double"))
    return df

def generate(cfg, spark, store):
    historical = os.getenv("TRAIN_AS_OF", "2026-05-01")
    current = os.getenv("SCORE_AS_OF") or date.today().isoformat()
    if date.fromisoformat(historical) + timedelta(days=90) > date.fromisoformat(current):
        raise ValueError("Training labels have not matured before the scoring snapshot")
    for kind, day, labeled in [("train", historical, True), ("score", current, False)]:
        snapshot(spark, cfg.rows, day, labeled).write.mode("overwrite").parquet(str(cfg.path("raw") / kind))
    store.publish("raw")
