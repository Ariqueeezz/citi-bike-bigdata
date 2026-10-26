
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, count, sum as spark_sum,
    min, max
)

spark = (
    SparkSession.builder
    .appName("CitiBike-Date-Duration-Audit")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv("hdfs://namenode:9000/raw/2026/01/*.csv")
    )

    df = (
        raw
        .withColumn(
            "started_at_ts",
            to_timestamp("started_at", "yyyy-MM-dd HH:mm:ss.SSS")
        )
        .withColumn(
            "ended_at_ts",
            to_timestamp("ended_at", "yyyy-MM-dd HH:mm:ss.SSS")
        )
    )

    df = df.withColumn(
        "duration_minutes",
        (
            col("ended_at_ts").cast("long")
            - col("started_at_ts").cast("long")
        ) / 60.0
    )

    df.select(
        count("*").alias("total_rows"),
        spark_sum(
            (col("started_at_ts") < "2026-01-01").cast("int")
        ).alias("starts_before_january"),
        spark_sum(
            (col("started_at_ts") >= "2026-02-01").cast("int")
        ).alias("starts_after_january"),
        spark_sum(
            (col("duration_minutes") > 60).cast("int")
        ).alias("rides_over_60_min"),
        spark_sum(
            (col("duration_minutes") > 1440).cast("int")
        ).alias("rides_over_24_hours"),
        min("started_at_ts").alias("earliest_start"),
        max("started_at_ts").alias("latest_start")
    ).show(truncate=False)

finally:
    spark.stop()
