
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CitiBike-Verify-Curated")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

path = "hdfs://namenode:9000/curated/2026/01/trips"

try:
    df = spark.read.parquet(path)

    print("=== SKEMA CURATED ===")
    df.printSchema()

    print("=== JUMLAH BARIS ===")
    print(df.count())

    print("=== CONTOH DATA ===")
    df.select(
        "ride_id",
        "rideable_type",
        "started_at_ts",
        "ended_at_ts",
        "duration_minutes",
        "member_casual"
    ).show(5, truncate=False)

    print("CURATED_VERIFY=SUCCESS")

finally:
    spark.stop()
