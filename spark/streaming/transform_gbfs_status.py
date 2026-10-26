import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    input_file_name,
    regexp_extract,
    to_timestamp,
    unix_timestamp,
    when,
    lit,
    concat_ws,
    array,
    filter as array_filter,
)

SOURCE = "hdfs://namenode:9000/raw/realtime/gbfs"
TARGET = "hdfs://namenode:9000/curated/realtime/gbfs/station_status"

spark = (
    SparkSession.builder
    .appName("CitiBike-GBFS-StationStatus-Transform")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def main():
    print("=== GBFS STATION STATUS TRANSFORMATION ===")
    print(f"Source: {SOURCE}")
    print(f"Target: {TARGET}")

    # Baca JSON snapshot secara rekursif.
    # station_information dikeluarkan; hanya file station_status diproses.
    raw = (
        spark.read
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .option("mode", "PERMISSIVE")
        .json(SOURCE)
        .withColumn("_source_file", input_file_name())
    )

    status_files = raw.filter(
        col("_source_file").contains("/station_status/")
        & col("_source_file").endswith(".json")
    )

    # Ubah array data.stations menjadi satu baris per stasiun per snapshot.
    stations = status_files.select(
        col("_source_file"),
        col("last_updated").cast("long").alias("feed_last_updated"),
        col("ttl").cast("long").alias("feed_ttl"),
        col("version").cast("string").alias("gbfs_version"),
        explode(col("data.stations")).alias("station"),
    )

    # Nama file snapshot menggunakan UTC: station_status_yyyyMMdd_HHmmss.json
    filename_ts = regexp_extract(
        col("_source_file"),
        r"station_status_(\d{8}_\d{6})\.json$",
        1,
    )

    df = (
        stations
        .withColumn("snapshot_filename_timestamp", filename_ts)
        .withColumn(
            "snapshot_timestamp",
            to_timestamp(
                col("snapshot_filename_timestamp"),
                "yyyyMMdd_HHmmss",
            ),
        )
        .withColumn("station_id", col("station.station_id").cast("string"))
        .withColumn(
            "num_bikes_available",
            col("station.num_bikes_available").cast("long"),
        )
        .withColumn(
            "num_docks_available",
            col("station.num_docks_available").cast("long"),
        )
        .withColumn(
            "last_reported_epoch",
            col("station.last_reported").cast("long"),
        )
        .withColumn(
            "is_installed",
            col("station.is_installed").cast("long"),
        )
        .withColumn(
            "is_renting",
            col("station.is_renting").cast("long"),
        )
        .withColumn(
            "is_returning",
            col("station.is_returning").cast("long"),
        )
        .withColumn(
            "last_reported_timestamp",
            when(
                col("last_reported_epoch").isNotNull()
                & (col("last_reported_epoch") != 86400),
                to_timestamp(
                    col("last_reported_epoch").cast("timestamp")
                ),
            ),
        )
    )

    # Umur data dihitung dari waktu snapshot (UTC), bukan waktu file HDFS.
    df = df.withColumn(
        "age_seconds",
        when(
            col("last_reported_epoch").isNotNull()
            & (col("last_reported_epoch") != 86400)
            & col("snapshot_timestamp").isNotNull(),
            unix_timestamp(col("snapshot_timestamp"))
            - col("last_reported_epoch"),
        ).cast("long"),
    )

    df = (
        df
        .withColumn(
            "timestamp_marker",
            col("last_reported_epoch") == 86400,
        )
        .withColumn(
            "future_timestamp",
            col("age_seconds") < -60,
        )
        .withColumn(
            "freshness_status",
            when(col("timestamp_marker"), lit("marker"))
            .when(col("last_reported_epoch").isNull(), lit("missing"))
            .when(col("snapshot_timestamp").isNull(), lit("invalid_snapshot_timestamp"))
            .when(col("age_seconds") < -60, lit("future"))
            .when(col("age_seconds") > 86400, lit("stale_24h"))
            .when(col("age_seconds") > 1800, lit("stale_30m"))
            .when(col("age_seconds") > 300, lit("stale_5m"))
            .otherwise(lit("fresh")),
        )
    )

    # Catat isu kualitas; jangan membuang record yang bermasalah.
    issue_candidates = array(
        when(col("station_id").isNull(), lit("missing_station_id")),
        when(col("snapshot_timestamp").isNull(), lit("invalid_snapshot_timestamp")),
        when(col("last_reported_epoch").isNull(), lit("missing_last_reported")),
        when(col("timestamp_marker"), lit("timestamp_marker_86400")),
        when(col("future_timestamp"), lit("future_timestamp_over_60s")),
        when(col("age_seconds") > 300, lit("stale_over_5m")),
        when(col("num_bikes_available").isNull(), lit("missing_bikes_value")),
        when(col("num_docks_available").isNull(), lit("missing_docks_value")),
        when(col("num_bikes_available") < 0, lit("negative_bikes_value")),
        when(col("num_docks_available") < 0, lit("negative_docks_value")),
    )

    df = df.withColumn(
        "quality_issue",
        concat_ws(
            ";",
            array_filter(issue_candidates, lambda x: x.isNotNull()),
        ),
    )

    # Hanya kolom yang relevan untuk analisis dan audit yang disimpan.
    output = df.select(
        "snapshot_timestamp",
        "snapshot_filename_timestamp",
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "last_reported_epoch",
        "last_reported_timestamp",
        "age_seconds",
        "timestamp_marker",
        "freshness_status",
        "future_timestamp",
        "is_installed",
        "is_renting",
        "is_returning",
        "feed_last_updated",
        "feed_ttl",
        "gbfs_version",
        "quality_issue",
        "_source_file",
    )

    # Materialisasi dan pemeriksaan sebelum menulis output.
    total_rows = output.count()
    print(f"TOTAL_STATUS_ROWS={total_rows}")

    print("=== SCHEMA ===")
    output.printSchema()

    print("=== FRESHNESS COUNTS ===")
    output.groupBy("freshness_status").count().orderBy(
        "freshness_status"
    ).show(truncate=False)

    print("=== QUALITY COUNTS ===")
    output.select(
        when(col("quality_issue") == "", "no_flag")
        .otherwise("has_flag")
        .alias("quality_flag")
    ).groupBy("quality_flag").count().show()

    print("=== SAMPLE ROWS ===")
    output.select(
        "snapshot_timestamp",
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "last_reported_epoch",
        "age_seconds",
        "freshness_status",
        "quality_issue",
    ).show(10, truncate=False)

    # Hindari menimpa output lama tanpa sengaja.
    # Jika target sudah ada, job berhenti dan meminta pemeriksaan manual.
    jvm = spark._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    target_path = jvm.org.apache.hadoop.fs.Path(TARGET)
    fs = target_path.getFileSystem(hadoop_conf)

    if fs.exists(target_path):
        raise RuntimeError(
            f"Target sudah ada: {TARGET}. "
            "Periksa output terlebih dahulu; tidak ada data yang ditimpa."
        )

    output.write.mode("errorifexists").parquet(TARGET)

    print(f"PARQUET_OUTPUT={TARGET}")
    print("GBFS_STATUS_TRANSFORM=SUCCESS")


try:
    main()
finally:
    spark.stop()
