
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

SOURCE_STATUS = (
    "hdfs://namenode:9000/curated/realtime/gbfs/station_status"
)
SOURCE_INFO = (
    "hdfs://namenode:9000/curated/realtime/gbfs/station_information"
)
TARGET = (
    "hdfs://namenode:9000/curated/realtime/gbfs/station_status_enriched"
)


def main():
    spark = (
        SparkSession.builder
        .appName("GBFS-Station-Status-Enrichment")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        status = spark.read.parquet(SOURCE_STATUS)
        info = spark.read.parquet(SOURCE_INFO)

        # Validasi kolom yang diperlukan.
        required_status = {"snapshot_timestamp", "station_id"}
        required_info = {
            "snapshot_timestamp",
            "station_id",
            "name",
            "capacity",
            "lat",
            "lon",
        }

        missing_status = required_status - set(status.columns)
        missing_info = required_info - set(info.columns)

        if missing_status:
            raise ValueError(
                f"Kolom status tidak ditemukan: {sorted(missing_status)}"
            )
        if missing_info:
            raise ValueError(
                f"Kolom informasi tidak ditemukan: {sorted(missing_info)}"
            )

        # Lindungi dataset sumber dan cegah penimpaan output.
        jvm = spark._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        target_path = jvm.org.apache.hadoop.fs.Path(TARGET)
        fs = target_path.getFileSystem(conf)

        if fs.exists(target_path):
            raise RuntimeError(
                f"Target sudah ada; tidak akan ditimpa: {TARGET}"
            )

        # Ambil timestamp unik dari masing-masing snapshot.
        status_snapshots = (
            status.select(
                F.col("snapshot_timestamp").alias("status_snapshot_ts")
            )
            .distinct()
        )

        info_snapshots = (
            info.select(
                F.col("snapshot_timestamp").alias("info_snapshot_ts")
            )
            .distinct()
        )

        # Kandidat metadata wajib berasal dari tanggal yang sama
        # dan tidak boleh lebih baru dari snapshot status.
        candidates = (
            status_snapshots.alias("s")
            .join(
                info_snapshots.alias("i"),
                (
                    F.to_date(F.col("s.status_snapshot_ts"))
                    == F.to_date(F.col("i.info_snapshot_ts"))
                )
                & (
                    F.col("i.info_snapshot_ts")
                    <= F.col("s.status_snapshot_ts")
                ),
                "left",
            )
        )

        window = (
            Window.partitionBy("status_snapshot_ts")
            .orderBy(F.col("info_snapshot_ts").desc_nulls_last())
        )

        selected_info = (
            candidates
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .select(
                "status_snapshot_ts",
                "info_snapshot_ts",
            )
        )

        # Pertahankan semua baris status sebagai tabel dasar.
        status_with_info_time = (
            status.alias("s")
            .join(
                selected_info.alias("m"),
                F.col("s.snapshot_timestamp")
                == F.col("m.status_snapshot_ts"),
                "left",
            )
            .select(
                "s.*",
                F.col("m.info_snapshot_ts").alias(
                    "matched_info_snapshot_timestamp"
                ),
            )
        )

        # Pilih metadata tanpa menggandakan kolom station_id.
        metadata = info.select(
            F.col("snapshot_timestamp").alias("_info_snapshot_ts"),
            "station_id",
            F.col("name").alias("info_name"),
            *(
                [
                    F.col("short_name").alias("info_short_name")
                ]
                if "short_name" in info.columns
                else []
            ),
            F.col("capacity").alias("info_capacity"),
            F.col("lat").alias("info_lat"),
            F.col("lon").alias("info_lon"),
            *(
                [
                    F.col("capacity_zero").alias("info_capacity_zero")
                ]
                if "capacity_zero" in info.columns
                else []
            ),
            *(
                [
                    F.col("quality_issue").alias("info_quality_issue")
                ]
                if "quality_issue" in info.columns
                else []
            ),
        )

        
        enriched = (
            status_with_info_time.alias("s")
            .join(
                metadata.alias("i"),
                (
                    F.col("s.station_id") == F.col("i.station_id")
                )
                & (
                    F.col("s.matched_info_snapshot_timestamp")
                    == F.col("i._info_snapshot_ts")
                ),
                "left",
            )
            # Ambil seluruh kolom status, lalu metadata tanpa station_id
            # dan timestamp internal yang dipakai untuk join.
            .select(
                "s.*",
                *[
                    F.col(f"i.{c}")
                    for c in metadata.columns
                    if c not in {"station_id", "_info_snapshot_ts"}
                ],
            )
            .withColumn(
                "metadata_match_status",
                F.when(
                    F.col("matched_info_snapshot_timestamp").isNull(),
                    F.lit("no_same_day_info_snapshot"),
                )
                .when(
                    F.col("info_name").isNull(),
                    F.lit("station_id_not_found_in_selected_info_snapshot"),
                )
                .otherwise(F.lit("matched")),
            )
            .withColumn(
                "metadata_age_seconds",
                F.when(
                    F.col("matched_info_snapshot_timestamp").isNotNull(),
                    F.col("snapshot_timestamp").cast("long")
                    - F.col("matched_info_snapshot_timestamp").cast("long"),
                ).otherwise(F.lit(None).cast("long")),
            )
        )


        # Pemeriksaan dasar sebelum menulis.
        status_count = status.count()
        output_count = enriched.count()

        if output_count != status_count:
            raise RuntimeError(
                f"Jumlah baris berubah: status={status_count}, "
                f"enriched={output_count}"
            )

        print(f"STATUS_INPUT_ROWS={status_count}")
        print(f"ENRICHED_OUTPUT_ROWS={output_count}")

        enriched.groupBy("metadata_match_status").count().orderBy(
            "metadata_match_status"
        ).show(truncate=False)

        print("METADATA_AGE_SECONDS_SUMMARY")
        enriched.filter(
            F.col("metadata_age_seconds").isNotNull()
        ).select(
            "metadata_age_seconds"
        ).summary("count", "min", "max", "mean").show(truncate=False)

        # errorifexists sebagai perlindungan tambahan.
        enriched.write.mode("errorifexists").parquet(TARGET)

        print(f"ENRICHED_OUTPUT_PATH={TARGET}")
        print("GBFS_STATUS_ENRICHMENT=SUCCESS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
