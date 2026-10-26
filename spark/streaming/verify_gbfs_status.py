from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    when, min as spark_min, max as spark_max
)

PATH = (
    "hdfs://namenode:9000/"
    "curated/realtime/gbfs/station_status"
)

spark = (
    SparkSession.builder
    .appName("CitiBike-GBFS-StationStatus-Verify")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    df = spark.read.parquet(PATH)

    print("=== 1. JUMLAH DAN CAKUPAN DATA ===")
    df.select(
        count("*").alias("total_rows"),
        countDistinct("snapshot_timestamp").alias("unique_snapshots"),
        countDistinct("station_id").alias("unique_station_ids"),
    ).show(truncate=False)

    print("=== 2. DISTRIBUSI FRESHNESS ===")
    df.groupBy("freshness_status").count().orderBy(
        "freshness_status"
    ).show(truncate=False)

    print("=== 3. CEK NILAI NULL ===")
    df.select(
        spark_sum(when(col("snapshot_timestamp").isNull(), 1).otherwise(0))
        .alias("null_snapshot_timestamp"),
        spark_sum(when(col("station_id").isNull(), 1).otherwise(0))
        .alias("null_station_id"),
        spark_sum(when(col("last_reported_epoch").isNull(), 1).otherwise(0))
        .alias("null_last_reported"),
        spark_sum(when(col("num_bikes_available").isNull(), 1).otherwise(0))
        .alias("null_bikes"),
        spark_sum(when(col("num_docks_available").isNull(), 1).otherwise(0))
        .alias("null_docks"),
    ).show(truncate=False)

    print("=== 4. CEK DUPLIKASI STASIUN PER SNAPSHOT ===")
    duplicate_groups = (
        df.groupBy("snapshot_timestamp", "station_id")
        .count()
        .filter(col("count") > 1)
    )
    print(f"DUPLICATE_GROUPS={duplicate_groups.count()}")
    duplicate_groups.show(10, truncate=False)

    print("=== 5. CEK FLAG DAN NILAI NEGATIF ===")
    df.select(
        spark_sum(when(col("timestamp_marker"), 1).otherwise(0))
        .alias("marker_rows"),
        spark_sum(when(col("future_timestamp"), 1).otherwise(0))
        .alias("future_flag_rows"),
        spark_sum(when(col("num_bikes_available") < 0, 1).otherwise(0))
        .alias("negative_bikes_rows"),
        spark_sum(when(col("num_docks_available") < 0, 1).otherwise(0))
        .alias("negative_docks_rows"),
        spark_sum(
            when(col("quality_issue") != "", 1).otherwise(0)
        ).alias("rows_with_quality_flags"),
    ).show(truncate=False)

    print("=== 6. RENTANG SNAPSHOT ===")
    df.select(
        spark_min("snapshot_timestamp").alias("earliest_snapshot_utc"),
        spark_max("snapshot_timestamp").alias("latest_snapshot_utc"),
    ).show(truncate=False)

    print("=== 7. CONTOH RECORD NON-MARKER ===")
    df.filter(~col("timestamp_marker")).select(
        "snapshot_timestamp",
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "last_reported_timestamp",
        "age_seconds",
        "freshness_status",
        "quality_issue",
    ).show(10, truncate=False)

    print("GBFS_STATUS_VERIFY=SUCCESS")

finally:
    spark.stop()
