import json
import os
import mlflow
import mlflow.spark
from pyspark.sql import functions as F
from pyspark.ml.functions import vector_to_array
from .config import FEATURE_VERSION

def score(cfg, spark, store):
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    manifest = json.loads((store.fetch("model")/"manifest.json").read_text())
    if manifest["feature_version"] != FEATURE_VERSION:
        raise ValueError("Feature code and model versions do not match")
    df = spark.read.parquet(str(store.fetch("features", "score")))
    if "churn_90d" in df.columns:
        raise ValueError("Scoring snapshot must not contain future labels")
    model = mlflow.spark.load_model(manifest["model_uri"])
    predictions = (model.transform(df)
                   .withColumn("churn_probability", vector_to_array("probability")[1])
                   .withColumn("churn_prediction", (F.col("churn_probability") >= manifest["threshold"]).cast("int"))
                   .withColumn("model_version", F.lit(manifest["version"]))
                   .withColumn("threshold", F.lit(manifest["threshold"]))
                   .withColumn("pipeline_run_id", F.lit(cfg.run_id))
                   .select("customer_id", "as_of_date", "churn_probability", "churn_prediction", "threshold",
                           "model_version", "feature_version", "pipeline_run_id").cache())
    summary = predictions.agg(F.count("*").alias("rows"), F.sum("churn_prediction").alias("flagged_customers"),
                              F.avg("churn_probability").alias("average_probability")).first().asDict()
    if summary["rows"] != cfg.rows:
        raise ValueError("Scoring row count mismatch")
    predictions.write.mode("overwrite").parquet(str(cfg.path("scores")/"predictions"))
    store.write_json("scores", "summary.json", summary)
    store.publish("scores")
    predictions.unpersist()
    print(json.dumps(summary, indent=2))
