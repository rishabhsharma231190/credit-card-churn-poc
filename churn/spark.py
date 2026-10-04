import os
from pyspark.sql import SparkSession

def session(name):
    spark = (SparkSession.builder.appName("churn-" + name)
             .master(os.getenv("SPARK_MASTER", "local[4]"))
             .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
             .config("spark.sql.shuffle.partitions", os.getenv("SPARK_PARTITIONS", "16"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
