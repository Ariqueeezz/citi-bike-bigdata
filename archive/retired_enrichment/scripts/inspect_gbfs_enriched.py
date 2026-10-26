from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("InspectGBFSStatusEnriched")
    .getOrCreate()
)

path = (
    "hdfs://namenode:9000/"
    "curated/realtime/gbfs/station_status_enriched"
)

df = spark.read.parquet(path)

print("ENRICHED_SCHEMA")
df.printSchema()

print("TOTAL_ROWS")
print(df.count())

print("SAMPLE_ROWS")
df.select(
    "snapshot_timestamp",
    "station_id",
    "num_bikes_available",
    "num_docks_available",
    "info_name",
    "info_capacity",
    "metadata_match_status",
).show(10, truncate=False)

print("MATCH_STATUS_COUNTS")
df.groupBy("metadata_match_status").count().show(
    truncate=False
)

print("AVERAGE_BIKES_BY_SNAPSHOT")

(
    df.groupBy("snapshot_timestamp")
    .agg(
        F.count("*").alias("station_rows"),
        F.sum("num_bikes_available").alias("total_bikes"),
        F.avg("num_bikes_available").alias("avg_bikes_per_station"),
        F.sum(
            F.when(
                F.col("metadata_match_status") == "matched",
                1
            ).otherwise(0)
        ).alias("matched_metadata_rows"),
    )
    .orderBy("snapshot_timestamp")
    .show(20, truncate=False)
)

print("FRESHNESS_DISTRIBUTION")

(
    df.groupBy("freshness_status")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)

spark.stop()
