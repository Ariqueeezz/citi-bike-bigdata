from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, unix_micros, to_date
)

spark = (
    SparkSession.builder
    .appName("CitiBike-HDFS-Batch")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

source = "hdfs://namenode:9000/raw/2026/01/*.csv"
target = "hdfs://namenode:9000/curated/2026/01/trips"

try:
    # Baca CSV dari HDFS
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(source)
    )

    # Konversi timestamp
    df = (
        df
        .withColumn(
            "started_at_ts",
            to_timestamp(
                col("started_at"),
                "yyyy-MM-dd HH:mm:ss.SSS"
            )
        )
        .withColumn(
            "ended_at_ts",
            to_timestamp(
                col("ended_at"),
                "yyyy-MM-dd HH:mm:ss.SSS"
            )
        )
    )

    # Hitung durasi dalam menit
    df = df.withColumn(
        "duration_minutes",
        (
            unix_micros(col("ended_at_ts"))
            - unix_micros(col("started_at_ts"))
        ) / 60_000_000
    )

    print(f"Baris sumber : {df.count()}")

    # Pastikan started_at_ts dan ended_at_ts sudah diparsing
    # dan duration_minutes sudah dihitung.	
    df = df.filter(
        (col("started_at_ts") >= "2026-01-01 00:00:00") &
        (col("started_at_ts") < "2026-02-01 00:00:00") &
        col("ended_at_ts").isNotNull() &
        col("started_at_ts").isNotNull() &
        (col("duration_minutes") > 0) &
        (col("duration_minutes") <= 1440)
    )

    # Penanda untuk analisis perjalanan panjang
    df = df.withColumn(
        "is_long_ride",
        col("duration_minutes") > 60
    )

    # Filter timestamp dan durasi yang valid
    clean = df.filter(
        col("started_at_ts").isNotNull()
        & col("ended_at_ts").isNotNull()
        & (col("duration_minutes") > 0)
        & (col("duration_minutes") <= 1440)
    )

    print("=== HASIL VALIDASI ===")
    print(f"Baris valid  : {clean.count()}")

    # Simpan hasil dalam format Parquet
    output = clean.drop("started_at", "ended_at")

    (
        output.write.mode("overwrite").parquet("hdfs://namenode:9000/curated/2026/01/trips")
    )

    print(f"PARQUET_OUTPUT={target}")
    print("HDFS_BATCH_TEST=SUCCESS")

finally:
    spark.stop()
