
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, hour, count, avg, round
)

spark = (
    SparkSession.builder
    .appName("CitiBike-HDFS-Aggregation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "4")

source = "hdfs://namenode:9000/curated/2026/01/trips"
base = "hdfs://namenode:9000/analytics/2026/01"

try:
    df = spark.read.parquet(source)

    # Tambahkan dimensi tanggal dan jam
    df = (
        df.withColumn("ride_date", to_date("started_at_ts"))
          .withColumn("ride_hour", hour("started_at_ts"))
    )

    # 1. Agregasi per tanggal dan jam
    hourly = (
        df.groupBy("ride_date", "ride_hour")
          .agg(
              count("*").alias("total_rides"),
              round(avg("duration_minutes"), 2)
                  .alias("avg_duration_minutes")
          )
    )

    # 2. Agregasi berdasarkan jenis pengguna
    user_type = (
        df.groupBy("member_casual")
          .agg(
              count("*").alias("total_rides"),
              round(avg("duration_minutes"), 2)
                  .alias("avg_duration_minutes")
          )
    )

    # 3. Agregasi berdasarkan stasiun keberangkatan
    station = (
        df.filter(
            col("start_station_name").isNotNull()
            & (col("start_station_name") != "")
        )
        .groupBy("start_station_name", "start_station_id")
        .agg(count("*").alias("total_rides"))
        .orderBy(col("total_rides").desc())
    )

    # errorifexists mencegah output lama tertimpa tanpa sengaja
    hourly.write.mode("errorifexists").parquet(
        f"{base}/hourly_stats"
    )
    user_type.write.mode("errorifexists").parquet(
        f"{base}/user_type_stats"
    )
    station.write.mode("errorifexists").parquet(
        f"{base}/station_stats"
    )

    print("=== TOTAL AGREGASI ===")
    print("Hourly:", hourly.count())
    print("User type:", user_type.count())
    print("Station:", station.count())

    print("=== AGREGASI JENIS PENGGUNA ===")
    user_type.show(truncate=False)

    print("=== 10 STASIUN TERATAS ===")
    station.show(10, truncate=False)

    print("HDFS_AGGREGATION=SUCCESS")

finally:
    spark.stop()
