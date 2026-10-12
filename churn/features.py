from pyspark.sql import functions as F
from .config import FEATURE_VERSION

def engineer(df):
    return (df.withColumn("utilization", F.col("balance")/F.col("credit_limit"))
            .withColumn("spend_change", F.col("spend_30d")/F.col("spend_prev_30d")-1)
            .withColumn("avg_ticket", F.when(F.col("txn_count_30d") > 0, F.col("spend_30d")/F.col("txn_count_30d")).otherwise(0.0))
            .withColumn("low_engagement", (F.col("digital_logins_30d") < 3).cast("double"))
            .withColumn("feature_version", F.lit(FEATURE_VERSION)))

def features(cfg, spark, store):
    prepared = store.fetch("prepared")
    for kind in cfg.datasets:
        df = engineer(spark.read.parquet(str(prepared/kind)))
        if kind == "train":
            # Deterministic customer holdout; label NEVER enters this split or features.
            bucket = F.pmod(F.xxhash64("customer_id"), F.lit(10))
            df = df.withColumn("split", F.when(bucket < 8, "train").when(bucket == 8, "validation").otherwise("test"))
        df.write.mode("overwrite").parquet(str(cfg.path("features")/kind))
    store.publish("features")
