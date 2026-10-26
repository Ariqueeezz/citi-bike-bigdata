
from pyspark.sql import SparkSession, functions as F

STATUS_PATH = (
    "hdfs://namenode:9000/curated/realtime/gbfs/station_status"
)
INFO_PATH = (
    "hdfs://namenode:9000/curated/realtime/gbfs/station_information"
)
TARGET = (
    "hdfs://namenode:9000/curated/realtime/gbfs/station_status_enriched"
)


def main():
    spark = (
        SparkSession.builder
        .appName("Verify-GBFS-Station-Status-Enriched")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        status = spark.read.parquet(STATUS_PATH)
        info = spark.read.parquet(INFO_PATH)
        result = spark.read.parquet(TARGET)

        source_count = status.count()
        result_count = result.count()

        print(f"SOURCE_STATUS_ROWS={source_count}")
        print(f"ENRICHED_ROWS={result_count}")

        if source_count != result_count:
            raise AssertionError("Jumlah baris tidak sama dengan sumber")

        # Pastikan tidak ada penggandaan station per snapshot.
        duplicate_groups = (
            result.groupBy("snapshot_timestamp", "station_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        print(f"DUPLICATE_SNAPSHOT_STATION_GROUPS={duplicate_groups}")

        # Pemeriksaan kolom penting yang berasal dari status.
        required = [
            "snapshot_timestamp",
            "station_id",
            "matched_info_snapshot_timestamp",
            "metadata_match_status",
            "metadata_age_seconds",
        ]
        missing = [c for c in required if c not in result.columns]
        if missing:
            raise AssertionError(f"Kolom wajib tidak ada: {missing}")

        null_counts = result.select(
            *[
                F.sum(F.col(c).isNull().cast("long")).alias(c)
                for c in ["snapshot_timestamp", "station_id"]
            ]
        )
        print("NULL_CHECKS")
        null_counts.show(truncate=False)

        print("MATCH_STATUS_COUNTS")
        result.groupBy("metadata_match_status").count().orderBy(
            "metadata_match_status"
        ).show(truncate=False)

        print("SNAPSHOT_COUNTS")
        result.groupBy("snapshot_timestamp").agg(
            F.count("*").alias("rows"),
            F.sum(
                (F.col("metadata_match_status") == "matched").cast("long")
            ).alias("matched_rows"),
        ).orderBy("snapshot_timestamp").show(100, truncate=False)

        # Status yang berhasil dipadankan wajib memiliki metadata
        # dan timestamp metadata yang tidak lebih baru dari status.

        invalid_condition = (
            F.col("matched_info_snapshot_timestamp").isNull()
            | F.col("info_name").isNull()
            | (
                F.col("matched_info_snapshot_timestamp")
                > F.col("snapshot_timestamp")
            )
            | (
                F.to_date(F.col("matched_info_snapshot_timestamp"))
                != F.to_date(F.col("snapshot_timestamp"))
            )
        )

        invalid_matches = result.filter(
            (F.col("metadata_match_status") == "matched")
            & invalid_condition
        ).count()


        print(f"INVALID_MATCH_ROWS={invalid_matches}")

        # Jumlah metadata unik per snapshot tidak boleh membuat join
        # menggandakan baris.
        info_duplicates = (
            info.groupBy("snapshot_timestamp", "station_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        print(f"SOURCE_INFO_DUPLICATE_GROUPS={info_duplicates}")

        if duplicate_groups != 0:
            raise AssertionError("Ditemukan duplikasi snapshot-station")
        if invalid_matches != 0:
            raise AssertionError("Ditemukan pasangan metadata yang tidak valid")
        if info_duplicates != 0:
            raise AssertionError("Sumber metadata memiliki duplikasi")
        if result.filter(
            F.col("snapshot_timestamp").isNull()
            | F.col("station_id").isNull()
        ).limit(1).count() > 0:
            raise AssertionError("Ada snapshot_timestamp/station_id null")

        print("GBFS_STATUS_ENRICHED_VERIFY=SUCCESS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
