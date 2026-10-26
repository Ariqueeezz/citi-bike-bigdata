from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    min as spark_min, max as spark_max, when
)

PATH = (
    "hdfs://namenode:9000/curated/realtime/gbfs/"
    "station_information"
)

spark = (
    SparkSession.builder
    .appName("CitiBike-GBFS-StationInformation-Verify")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    df = spark.read.parquet(PATH)

    print("=== 1. JUMLAH DATA ===")
    df.select(
        count("*").alias("total_rows"),
        countDistinct("snapshot_timestamp").alias("unique_snapshots"),
        countDistinct("station_id").alias("unique_station_ids"),
    ).show(truncate=False)

    print("=== 2. JUMLAH PER SNAPSHOT ===")
    df.groupBy("snapshot_timestamp").count().orderBy(
        "snapshot_timestamp"
    ).show(100, truncate=False)

    print("=== 3. NILAI NULL ===")
    df.select(
        spark_sum(when(col("snapshot_timestamp").isNull(), 1).otherwise(0))
        .alias("null_snapshot_timestamp"),
        spark_sum(when(col("station_id").isNull(), 1).otherwise(0))
        .alias("null_station_id"),
        spark_sum(when(col("name").isNull(), 1).otherwise(0))
        .alias("null_name"),
        spark_sum(when(col("capacity").isNull(), 1).otherwise(0))
        .alias("null_capacity"),
        spark_sum(when(col("lat").isNull(), 1).otherwise(0))
        .alias("null_lat"),
        spark_sum(when(col("lon").isNull(), 1).otherwise(0))
        .alias("null_lon"),
    ).show(truncate=False)

    print("=== 4. DUPLIKASI STASIUN PER SNAPSHOT ===")
    duplicate_groups = (
        df.groupBy("snapshot_timestamp", "station_id")
        .count()
        .filter(col("count") > 1)
    )
    print(f"DUPLICATE_GROUPS={duplicate_groups.count()}")
    duplicate_groups.show(10, truncate=False)

    print("=== 5. KAPASITAS DAN KOORDINAT ===")
    df.select(
        spark_sum(when(col("capacity_zero"), 1).otherwise(0))
        .alias("capacity_zero_rows"),
        spark_sum(when(col("capacity") < 0, 1).otherwise(0))
        .alias("negative_capacity_rows"),
        spark_sum(when(col("invalid_coordinates"), 1).otherwise(0))
        .alias("invalid_coordinate_rows"),
        spark_sum(
            when(col("quality_issue") != "", 1).otherwise(0)
        ).alias("rows_with_quality_flags"),
    ).show(truncate=False)

    print("=== 6. RENTANG SNAPSHOT ===")
    df.select(
        spark_min("snapshot_timestamp").alias("earliest_snapshot_utc"),
        spark_max("snapshot_timestamp").alias("latest_snapshot_utc"),
    ).show(truncate=False)

    print("=== 7. CONTOH DATA ===")
    df.select(
        "snapshot_timestamp",
        "station_id",
        "name",
        "lat",
        "lon",
        "capacity",
        "capacity_zero",
        "quality_issue",
    ).show(10, truncate=False)

    print("GBFS_STATION_INFORMATION_VERIFY=SUCCESS")

finally:
    spark.stop()
