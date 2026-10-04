import json
import os
import mlflow
import mlflow.spark
from mlflow import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from .config import FEATURES, FEATURE_VERSION

def metrics(df, threshold):
    p = vector_to_array("probability")[1]
    y = F.col("churn_90d")
    r = df.agg(F.sum(((p >= threshold) & (y == 1)).cast("long")).alias("tp"),
               F.sum(((p >= threshold) & (y == 0)).cast("long")).alias("fp"),
               F.sum(((p < threshold) & (y == 1)).cast("long")).alias("fn"),
               F.avg("churn_90d").alias("prevalence")).first().asDict()
    precision = r["tp"] / max(1, r["tp"]+r["fp"])
    recall = r["tp"] / max(1, r["tp"]+r["fn"])
    return {"precision": precision, "recall": recall, "f1": 2*precision*recall/max(1e-12, precision+recall),
            "prevalence": r["prevalence"]}

def setup(cfg):
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment("credit-card-churn-poc")

def train(cfg, spark, store):
    setup(cfg)
    if os.getenv("TRAIN_MODEL", "true").lower() != "true":
        client = MlflowClient()
        version = client.get_model_version_by_alias(cfg.model_name, "champion")
        run = client.get_run(version.run_id)
        manifest = {"model_uri": f"models:/{cfg.model_name}/{version.version}", "version": version.version,
                    "threshold": float(run.data.params["threshold"]), "feature_version": run.data.tags["feature_version"]}
        store.write_json("model", "manifest.json", manifest)
        store.publish("model")
        return
    path = store.fetch("features", "train")
    df = spark.read.parquet(str(path))
    parts = {s: df.filter(F.col("split") == s).cache() for s in ["train", "validation", "test"]}
    counts = {s: d.count() for s, d in parts.items()}
    if any(d.select("churn_90d").distinct().count() != 2 for d in parts.values()):
        raise ValueError("Every split needs both classes; increase sample rows")
    pipeline = Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="features", handleInvalid="error"),
                               LogisticRegression(labelCol="churn_90d", maxIter=30, regParam=0.01)])
    with mlflow.start_run(run_name=cfg.run_id) as run:
        mlflow.set_tags({"synthetic_data": "true", "feature_version": FEATURE_VERSION, "pipeline_run_id": cfg.run_id, "code_commit": os.getenv("CODE_COMMIT", "local")})
        mlflow.log_params({"algorithm": "spark_logistic_regression", "max_iter": 30, "reg_param": 0.01,
                           "features": json.dumps(FEATURES), **{s+"_rows": n for s,n in counts.items()}})
        model = pipeline.fit(parts["train"])
        validation = model.transform(parts["validation"]).cache()
        # Tiny list of summary metrics only; no customer rows collected to driver.
        choices = [(t, metrics(validation, t)) for t in [0.1,0.2,0.3,0.4,0.5,0.6,0.7]]
        threshold, best = max(choices, key=lambda pair: pair[1]["f1"])
        validation.unpersist()
        test = model.transform(parts["test"]).cache()
        result = metrics(test, threshold)
        for metric in ["areaUnderROC", "areaUnderPR"]:
            result[metric] = BinaryClassificationEvaluator(labelCol="churn_90d", metricName=metric).evaluate(test)
        test.unpersist()
        mlflow.log_param("threshold", threshold)
        mlflow.log_metrics({"test_"+k: v for k,v in result.items()})
        mlflow.log_metrics({"validation_"+k: v for k,v in best.items()})
        mlflow.log_dict({"features": FEATURES, "feature_version": FEATURE_VERSION}, "feature_contract.json")
        if result["areaUnderROC"] < float(os.getenv("MIN_TEST_AUC", "0.60")):
            raise ValueError("Candidate failed POC quality gate; model not promoted")
        mlflow.spark.log_model(model, "spark_model", pip_requirements=["pyspark==3.5.6", "mlflow==2.22.0", "numpy==1.26.4"])
        version = mlflow.register_model(f"runs:/{run.info.run_id}/spark_model", cfg.model_name)
        MlflowClient().set_registered_model_alias(cfg.model_name, "champion", version.version)
        manifest = {"model_uri": f"models:/{cfg.model_name}/{version.version}", "version": version.version,
                    "mlflow_run_id": run.info.run_id, "threshold": threshold, "feature_version": FEATURE_VERSION,
                    "test_metrics": result, "split_counts": counts}
        store.write_json("model", "manifest.json", manifest)
        store.publish("model")
        print(json.dumps(manifest, indent=2))
    for d in parts.values():
        d.unpersist()
