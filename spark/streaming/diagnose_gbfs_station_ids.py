from datetime import datetime
from pyspark.sql import SparkSession, functions as F

STATUS_PATH = (
    "hdfs://namenode:9000/curated/realtime/gbfs/station_status"
)
INFO_PATH = (
    "hdfs://namenode:9000/curated/realtime/gbfs/station_information"
)

spark = (
    SparkSession.builder
    .appName("DiagnoseGBFSStationIDs")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def show_ids(title, df, limit=30):
    print(f"\n=== {title} ===")
    rows = df.orderBy("station_id").limit(limit).collect()
    if not rows:
        print("(tidak ada)")
        return
    for row in rows:
        print(row.asDict())


try:
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

    status_ids = status.select("station_id").distinct()
    info_ids = info.select("station_id").distinct()

    print("=== 1. RINGKASAN ID GLOBAL ===")
    print(f"STATUS_UNIQUE_IDS={status_ids.count()}")
    print(f"INFO_UNIQUE_IDS={info_ids.count()}")

    global_status_only = status_ids.join(
        info_ids, "station_id", "left_anti"
    )
    global_info_only = info_ids.join(
        status_ids, "station_id", "left_anti"
    )

    print(f"STATUS_ONLY_GLOBAL={global_status_only.count()}")
    print(f"INFO_ONLY_GLOBAL={global_info_only.count()}")

    # Untuk setiap ID status yang tidak pernah muncul di informasi,
    # tampilkan rentang waktu dan jumlah snapshot kemunculannya.
    status_only_detail = (
        status.join(global_status_only, "station_id", "inner")
        .groupBy("station_id")
        .agg(
            F.countDistinct("snapshot_timestamp").alias("status_snapshots"),
            F.min("snapshot_timestamp").alias("first_seen"),
            F.max("snapshot_timestamp").alias("last_seen"),
        )
    )
    show_ids("2. ID STATUS YANG TIDAK PERNAH ADA DI INFO", status_only_detail)

    # Tampilkan pada snapshot informasi mana setiap ID global ditemukan.
    info_presence = (
        info.groupBy("station_id")
        .agg(
            F.countDistinct("snapshot_timestamp").alias("info_snapshots"),
            F.min("snapshot_timestamp").alias("first_info_snapshot"),
            F.max("snapshot_timestamp").alias("last_info_snapshot"),
        )
    )

    print("\n=== 3. JUMLAH SNAPSHOT INFORMASI PER ID ===")
    info_presence.groupBy("info_snapshots").count().orderBy(
        "info_snapshots"
    ).show(truncate=False)

    print("\n=== 4. PERUBAHAN ID ANTAR-SNAPSHOT INFORMATION ===")
    info_snapshots = [
        row["snapshot_timestamp"]
        for row in info.select("snapshot_timestamp").distinct()
        .orderBy("snapshot_timestamp").collect()
    ]

    previous_ids = None
    previous_ts = None

    for ts in info_snapshots:
        current_ids = set(
            row["station_id"]
            for row in info.filter(
                F.col("snapshot_timestamp") == F.lit(ts)
            ).select("station_id").collect()
        )

        print(f"\nSnapshot: {ts}")
        print(f"STATION_IDS={len(current_ids)}")

        if previous_ids is not None:
            added = current_ids - previous_ids
            removed = previous_ids - current_ids

            print(f"NEW_IDS_VS_PREVIOUS={len(added)}")
            print(f"MISSING_IDS_VS_PREVIOUS={len(removed)}")
            print(f"RETAINED_IDS={len(current_ids & previous_ids)}")

            if added:
                print("NEW_ID_SAMPLE=" + ", ".join(sorted(added)[:10]))
            if removed:
                print("MISSING_ID_SAMPLE=" + ", ".join(sorted(removed)[:10]))

        previous_ids = current_ids
        previous_ts = ts

    # Perbandingan hanya dengan snapshot informasi pada tanggal yang sama.
    # Pilih snapshot informasi terakhir yang waktunya <= snapshot status.
    # Jika tidak ada snapshot informasi pada tanggal tersebut, laporkan
    # sebagai tidak tersedia; jangan gunakan data dari tanggal lain.
    print("\n=== 5. MATCH BERDASARKAN TANGGAL DAN WAKTU ===")

    info_snapshot_rows = [
        (row["snapshot_timestamp"], row["station_id"])
        for row in info.select("snapshot_timestamp", "station_id").collect()
    ]

    info_by_timestamp = {}
    for ts, station_id in info_snapshot_rows:
        info_by_timestamp.setdefault(ts, set()).add(station_id)

    info_by_date = {}
    for ts, ids in info_by_timestamp.items():
        info_by_date.setdefault(ts.date(), []).append((ts, ids))

    status_snapshot_rows = (
        status.groupBy("snapshot_timestamp")
        .agg(F.countDistinct("station_id").alias("status_ids"))
        .orderBy("snapshot_timestamp")
        .collect()
    )

    per_snapshot_ids = {}
    for row in status.select("snapshot_timestamp", "station_id").collect():
        per_snapshot_ids.setdefault(row["snapshot_timestamp"], set()).add(
            row["station_id"]
        )

    no_same_day = 0

    for row in status_snapshot_rows:
        status_ts = row["snapshot_timestamp"]
        status_station_ids = per_snapshot_ids[status_ts]

        same_day = info_by_date.get(status_ts.date(), [])
        eligible = [(ts, ids) for ts, ids in same_day if ts <= status_ts]

        if not eligible:
            print(
                f"STATUS_SNAPSHOT={status_ts} | "
                "INFO_SNAPSHOT=NONE_AT_OR_BEFORE_SAME_DAY | "
                f"STATUS_IDS={len(status_station_ids)}"
            )
            no_same_day += 1
            continue

        chosen_ts, chosen_ids = max(eligible, key=lambda item: item[0])
        matched = status_station_ids & chosen_ids
        unmatched = status_station_ids - chosen_ids

        print(
            f"STATUS_SNAPSHOT={status_ts} | "
            f"INFO_SNAPSHOT={chosen_ts} | "
            f"STATUS_IDS={len(status_station_ids)} | "
            f"MATCHED={len(matched)} | "
            f"UNMATCHED={len(unmatched)} | "
            f"MATCH_RATE={len(matched) / len(status_station_ids) * 100:.2f}%"
        )

        if unmatched:
            print("UNMATCHED_ID_SAMPLE=" + ", ".join(sorted(unmatched)[:10]))

    print(f"\nSTATUS_SNAPSHOTS_WITHOUT_PRIOR_SAME_DAY_INFO={no_same_day}")

    print("\n=== 6. SARAN ATURAN JOIN ===")
    print(
        "Gunakan station_id untuk pencocokan identitas, tetapi gunakan "
        "snapshot information pada waktu yang sesuai jika atribut historis "
        "dibutuhkan. Snapshot informasi dari tanggal lain tidak otomatis "
        "dianggap sebagai representasi historis."
    )

    print("\nGBFS_STATION_ID_DIAGNOSIS=SUCCESS")

finally:
    spark.stop()
