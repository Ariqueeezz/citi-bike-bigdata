from pyspark.sql import SparkSession, functions as F

STATUS_PATH = (
    "hdfs://namenode:9000/curated/realtime/gbfs/station_status"
)
INFO_PATH = (
    "hdfs://namenode:9000/curated/realtime/gbfs/station_information"
)

spark = (
    SparkSession.builder
    .appName("ValidateGBFSStationJoin")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    print("=== GBFS STATION JOIN VALIDATION ===")
    print(f"STATUS_PATH={STATUS_PATH}")
    print(f"INFO_PATH={INFO_PATH}")

    # Baca output curated saja; raw dan output sumber tidak diubah.
    status = (
        spark.read.parquet(STATUS_PATH)
        .select("snapshot_timestamp", "station_id")
        .filter(
            F.col("snapshot_timestamp").isNotNull()
            & F.col("station_id").isNotNull()
        )
        .dropDuplicates(["snapshot_timestamp", "station_id"])
    )

    info = (
        spark.read.parquet(INFO_PATH)
        .select("snapshot_timestamp", "station_id")
        .filter(
            F.col("snapshot_timestamp").isNotNull()
            & F.col("station_id").isNotNull()
        )
        .dropDuplicates(["snapshot_timestamp", "station_id"])
    )

    # Snapshot status dan informasi stasiun tidak selalu sama waktunya.
    # Karena itu, validasi dilakukan dengan dua pendekatan:
    # 1. Perbandingan ID secara global.
    # 2. Perbandingan ID terhadap snapshot informasi terbaru yang tersedia.

    status_ids = status.select("station_id").distinct()
    info_ids = info.select("station_id").distinct()

    status_total = status.count()
    info_total = info.count()
    status_unique_ids = status_ids.count()
    info_unique_ids = info_ids.count()

    matched_global = status_ids.join(
        info_ids, on="station_id", how="inner"
    ).count()

    status_only_global = status_ids.join(
        info_ids, on="station_id", how="left_anti"
    ).count()

    info_only_global = info_ids.join(
        status_ids, on="station_id", how="left_anti"
    ).count()

    print("\n=== 1. RINGKASAN DATA ===")
    print(f"STATUS_ROWS_DISTINCT_SNAPSHOT_STATION={status_total}")
    print(f"INFO_ROWS_DISTINCT_SNAPSHOT_STATION={info_total}")
    print(f"STATUS_UNIQUE_STATION_IDS={status_unique_ids}")
    print(f"INFO_UNIQUE_STATION_IDS={info_unique_ids}")
    print(f"GLOBAL_MATCHED_STATION_IDS={matched_global}")
    print(f"GLOBAL_STATUS_ONLY_IDS={status_only_global}")
    print(f"GLOBAL_INFO_ONLY_IDS={info_only_global}")

    # Pilih snapshot informasi terbaru yang tersedia.
    latest_info_ts = info.agg(
        F.max("snapshot_timestamp").alias("ts")
    ).first()["ts"]

    latest_info = (
        info.filter(F.col("snapshot_timestamp") == F.lit(latest_info_ts))
        .select("station_id")
        .distinct()
    )

    print("\n=== 2. SNAPSHOT INFORMASI TERBARU ===")
    print(f"LATEST_INFO_SNAPSHOT={latest_info_ts}")

    latest_info_count = latest_info.count()
    print(f"LATEST_INFO_UNIQUE_STATION_IDS={latest_info_count}")

    # Untuk setiap snapshot status, ukur ID yang ada/tidak ada
    # pada snapshot informasi terbaru.
    per_snapshot = (
        status.groupBy("snapshot_timestamp")
        .agg(
            F.count("*").alias("status_station_ids")
        )
    )

    matched_by_snapshot = (
        status.join(latest_info, on="station_id", how="inner")
        .groupBy("snapshot_timestamp")
        .agg(F.countDistinct("station_id").alias("matched_ids"))
    )

    status_only_by_snapshot = (
        status.join(latest_info, on="station_id", how="left_anti")
        .groupBy("snapshot_timestamp")
        .agg(F.countDistinct("station_id").alias("unmatched_ids"))
    )

    comparison = (
        per_snapshot
        .join(matched_by_snapshot, "snapshot_timestamp", "left")
        .join(status_only_by_snapshot, "snapshot_timestamp", "left")
        .fillna(0, subset=["matched_ids", "unmatched_ids"])
        .withColumn(
            "match_rate_pct",
            F.round(
                F.col("matched_ids") * 100.0
                / F.col("status_station_ids"),
                2
            )
        )
        .orderBy("snapshot_timestamp")
    )

    print("\n=== 3. KECOCOKAN PER SNAPSHOT STATUS ===")
    comparison.show(100, truncate=False)

    # Cek apakah ID berulang di dalam satu snapshot info.
    # Duplikasi di sini dihitung sebelum dropDuplicates.
    raw_info = spark.read.parquet(INFO_PATH).select(
        "snapshot_timestamp", "station_id"
    )

    duplicate_groups = (
        raw_info
        .filter(
            F.col("snapshot_timestamp").isNotNull()
            & F.col("station_id").isNotNull()
        )
        .groupBy("snapshot_timestamp", "station_id")
        .count()
        .filter(F.col("count") > 1)
    )

    print("\n=== 4. DUPLIKASI ID PADA SNAPSHOT INFORMATION ===")
    print(f"INFO_DUPLICATE_GROUPS={duplicate_groups.count()}")
    duplicate_groups.show(20, truncate=False)

    print("\n=== 5. CONTOH ID STATUS YANG TIDAK ADA DI INFO TERBARU ===")
    unmatched_sample = (
        status.join(latest_info, on="station_id", how="left_anti")
        .select("snapshot_timestamp", "station_id")
        .orderBy("snapshot_timestamp", "station_id")
    )
    unmatched_sample.show(20, truncate=False)

    print("\nGBFS_STATION_JOIN_VALIDATION=SUCCESS")

finally:
    spark.stop()
