from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Check-December-Rides").getOrCreate()

df = spark.read.parquet(
    "hdfs://namenode:9000/curated/2026/01/trips"
)

df.filter(
    col("started_at_ts") < "2026-01-01"
).select(
    "ride_id",
    "started_at_ts",
    "ended_at_ts",
    "duration_minutes",
    "start_station_name",
    "end_station_name",
    "member_casual"
).orderBy(
    col("started_at_ts")
).show(30, truncate=False)

spark.stop()
