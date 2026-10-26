
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, min, max, count
)

spark = (
    SparkSession.builder
    .appName("CitiBike-Check-Raw-Dates")
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

    df = raw.withColumn(
        "started_at_ts",
        to_timestamp(
            col("started_at"),
            "yyyy-MM-dd HH:mm:ss.SSS"
        )
    ).withColumn(
        "ended_at_ts",
        to_timestamp(
            col("ended_at"),
            "yyyy-MM-dd HH:mm:ss.SSS"
        )
    )

    df.select(
        count("*").alias("total_rows"),
        min("started_at_ts").alias("earliest_start"),
        max("started_at_ts").alias("latest_start"),
        min("ended_at_ts").alias("earliest_end"),
        max("ended_at_ts").alias("latest_end")
    ).show(truncate=False)

    print("Rows starting before 2026-01-01:")
    df.filter(
        col("started_at_ts") < "2026-01-01"
    ).select(
        "ride_id", "started_at", "ended_at"
    ).show(20, truncate=False)

finally:
    spark.stop()
