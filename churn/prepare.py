from pyspark.sql import functions as F
from .config import BASE_FEATURES

def clean(df, labeled):
    # Snowflake may preserve uppercase identifiers. Canonicalize both sources.
    df = df.toDF(*[c.lower() for c in df.columns])
    required = ["customer_id", "as_of_date"] + BASE_FEATURES + (["churn_90d"] if labeled else [])
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"Missing source columns: {sorted(missing)}")
    df = df.select(*required).withColumn("customer_id", F.col("customer_id").cast("long"))
    df = df.withColumn("as_of_date", F.col("as_of_date").cast("date"))
    for name in BASE_FEATURES + (["churn_90d"] if labeled else []):
        df = df.withColumn(name, F.col(name).cast("double"))
    invalid = F.col("customer_id").isNull() | F.col("as_of_date").isNull()
    for name in BASE_FEATURES:
        invalid = invalid | F.col(name).isNull() | F.isnan(name) | (F.abs(F.col(name)) == float("inf")) | (F.col(name) < 0)
    invalid = invalid | (F.col("credit_limit") <= 0) | (F.col("spend_prev_30d") <= 0)
    if labeled:
        invalid = invalid | F.col("churn_90d").isNull() | (~F.col("churn_90d").isin(0.0, 1.0))
    # Fail visibly instead of silently discarding customers or inventing labels.
    if df.filter(invalid).limit(1).count():
        raise ValueError("Data quality failed: invalid identifiers, dates, numerics or labels")
    if df.groupBy("customer_id", "as_of_date").count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate customer/snapshot keys; define a real source deduplication rule")
    return df

def prepare(cfg, spark, store):
    raw = store.fetch("raw")
    report = {}
    for kind in ("train", "score"):
        df = clean(spark.read.parquet(str(raw/kind)), kind == "train").cache()
        count = df.count()
        if count != cfg.rows:
            raise ValueError(f"{kind}: expected {cfg.rows} rows, got {count}")
        if kind == "train":
            if df.select("churn_90d").distinct().count() != 2:
                raise ValueError("Training data must contain both classes")
            if df.filter(F.date_add("as_of_date", 90) > F.current_date()).limit(1).count():
                raise ValueError("Training label observation window has not matured")
        df.write.mode("overwrite").parquet(str(cfg.path("prepared")/kind))
        report[kind] = {"rows": count, "columns": len(df.columns)}
        df.unpersist()
    store.write_json("prepared", "quality.json", report)
    store.publish("prepared")
