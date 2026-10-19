"""Profile raw Parquet before cleaning; publish diagnostics before failing the gate."""
import json
import math
import os
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, BooleanType
from .config import BASE_FEATURES


def profile(df, required, expected_rows, category_root, categorical_columns=()):
    names = [name.lower() for name in df.columns]
    if len(names) != len(set(names)):
        return {"status": "failed", "errors": ["Duplicate case-insensitive column names"]}
    df = df.toDF(*names).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        errors = []
        absent = sorted(set(required) - set(names))
        if absent:
            errors.append(f"Missing required columns: {absent}")
        unknown = sorted(set(categorical_columns) - set(names))
        if unknown:
            errors.append(f"Configured categorical columns not found: {unknown}")
        expressions = [F.count(F.lit(1)).alias("rows")]
        columns = {}
        for i, field in enumerate(df.schema.fields):
            col = F.col('`' + field.name.replace('`', '``') + '`')
            numeric = isinstance(field.dataType, NumericType)
            missing = col.isNull()
            if isinstance(field.dataType, StringType):
                missing = missing | (F.length(F.trim(col)) == 0)
            if numeric:
                missing = missing | F.isnan(col.cast("double"))
            expressions.append(F.count(F.when(missing, 1)).alias(f"missing_{i}"))
            columns[field.name] = {"type": field.dataType.simpleString()}
            if numeric:
                finite = (~missing) & (F.abs(col.cast("double")) != float("inf"))
                expressions.extend([
                    F.count(F.when((~missing) & (~finite), 1)).alias(f"invalid_{i}"),
                    F.count(F.when(finite, 1)).alias(f"valid_{i}"),
                    F.avg(F.when(finite, col.cast("double"))).alias(f"mean_{i}")])
        totals = df.agg(*expressions).first().asDict()
        rows = totals["rows"]
        if rows == 0 or rows != expected_rows:
            errors.append(f"Expected {expected_rows} non-empty rows; found {rows}")
        for i, field in enumerate(df.schema.fields):
            info = columns[field.name]
            missing_count = totals[f"missing_{i}"]
            info.update(missing_count=missing_count,
                        missing_percent=100.0 * missing_count / rows if rows else None)
            if missing_count:
                errors.append(f"{field.name}: {missing_count} missing values")
            if isinstance(field.dataType, NumericType):
                mean = totals[f"mean_{i}"]
                info.update(mean=float(mean) if mean is not None and math.isfinite(float(mean)) else None,
                            valid_numeric_count=totals[f"valid_{i}"],
                            non_finite_count=totals[f"invalid_{i}"])
                if totals[f"invalid_{i}"]:
                    errors.append(f"{field.name}: non-finite numeric values")
                if mean is not None and not math.isfinite(float(mean)):
                    errors.append(f"{field.name}: numeric mean overflow")
            # Identifiers/dates are not meaningful categories, even when stored as text.
            categorical = (field.name in categorical_columns or
                           (isinstance(field.dataType, (StringType, BooleanType)) and
                            field.name not in ("customer_id", "as_of_date")))
            if categorical:
                col = F.col('`' + field.name.replace('`', '``') + '`')
                distribution = (df.groupBy(col.cast("string").alias("value")).count()
                                .withColumn("percent", F.col("count") * 100.0 / F.lit(rows or 1)))
                relative = f"categorical/column_{i}"
                distribution.write.mode("overwrite").parquet(str(category_root / relative))
                info["distribution_path"] = relative
                info["top_20"] = [r.asDict() for r in distribution.orderBy(
                    F.desc("count"), F.asc_nulls_first("value")).limit(20).collect()]
                info["other_count"] = rows - sum(r["count"] for r in info["top_20"])
        return {"status": "failed" if errors else "passed", "rows": rows,
                "missing_columns": absent, "errors": errors, "columns": columns}
    finally:
        df.unpersist()


def data_quality(cfg, spark, store):
    raw = store.fetch("raw")
    categories = tuple(c.strip().lower() for c in os.getenv("DQ_CATEGORICAL_COLUMNS", "").split(",") if c.strip())
    report = {"pipeline_run_id": cfg.run_id, "status": "passed", "datasets": {},
              "policy": "Fail on missing columns/values, non-finite numerics or row-count mismatch",
              "missing_definition": "SQL NULL, numeric NaN, empty/space-only strings",
              "mean_definition": "Arithmetic mean of finite, non-missing numeric values"}
    for kind in cfg.datasets:
        required = ["customer_id", "as_of_date", *BASE_FEATURES]
        if kind == "train":
            required.append("churn_90d")
        result = profile(spark.read.parquet(str(raw / kind)), required, cfg.rows,
                         cfg.path("data_quality") / kind, categories)
        report["datasets"][kind] = result
        if result["status"] == "failed":
            report["status"] = "failed"
    store.write_json("data_quality", "report.json", report)
    # _READY marks a complete report, NOT a passed quality gate. Read report.status.
    store.publish("data_quality")
    print(json.dumps({"data_quality": report["status"], "report": str(cfg.path("data_quality") / "report.json")}))
    if report["status"] == "failed":
        raise ValueError("Raw data quality failed; see data_quality/report.json before retrying")
