from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, unix_micros
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

    # Filter data tidak valid dan batasi strictly pada bulan target (Januari 2026)
    # Menghindari tanggal 2025-12-31 dari trip yang mulai di bulan sebelumnya tapi selesai di Jan 2026
    clean = df.filter(
        col("started_at_ts").isNotNull()
        & col("ended_at_ts").isNotNull()
        & (col("duration_minutes") > 0)
        & (col("duration_minutes") <= 1440)
        & (col("started_at_ts") >= "2026-01-01 00:00:00")
        & (col("started_at_ts") < "2026-02-01 00:00:00")
    )

    raw_count = df.count()
    clean_count = clean.count()
    dropped_count = raw_count - clean_count

    print("=== HASIL VALIDASI KUALITAS DATA HDFS ===")
    print(f"Baris sumber : {raw_count}")
    print(f"Baris valid  : {clean_count}")
    print(f"Baris dropped: {dropped_count} (nulls, anomali durasi, atau di luar Januari 2026)")

    # Simpan hasil dalam format Parquet
    output = clean.drop("started_at", "ended_at")

    (
        output.write
        .mode("errorifexists")
        .parquet(target)
    )

    print(f"PARQUET_OUTPUT={target}")
    print("HDFS_BATCH_TEST=SUCCESS")

finally:
    spark.stop()
