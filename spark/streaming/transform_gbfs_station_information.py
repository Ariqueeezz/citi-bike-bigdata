from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, ArrayType
)
from pyspark.sql.functions import (
    col, explode, input_file_name, regexp_extract,
    to_timestamp, when, lit, array,
    concat_ws, filter as array_filter
)

SOURCE = "hdfs://namenode:9000/raw/realtime/gbfs"
TARGET = (
    "hdfs://namenode:9000/curated/realtime/gbfs/"
    "station_information"
)

# Schema eksplisit untuk GBFS station_information.
station_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("short_name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("capacity", LongType(), True),
])

feed_schema = StructType([
    StructField("last_updated", LongType(), True),
    StructField("ttl", LongType(), True),
    StructField("version", StringType(), True),
    StructField(
        "data",
        StructType([
            StructField(
                "stations",
                ArrayType(station_schema),
                True
            )
        ]),
        True
    ),
])

spark = (
    SparkSession.builder
    .appName("CitiBike-GBFS-StationInformation-Transform")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def main():
    print("=== GBFS STATION INFORMATION TRANSFORMATION ===")
    print(f"Source: {SOURCE}")
    print(f"Target: {TARGET}")

    # Membaca JSON dengan schema yang ditetapkan.
    raw = (
        spark.read
        .schema(feed_schema)
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .option("mode", "FAILFAST")
        .json(SOURCE)
        .withColumn("_source_file", input_file_name())
    )

    # Pastikan hanya file station_information yang diproses.
    info_feeds = raw.filter(
        col("_source_file").contains("/station_information/")
        & col("_source_file").endswith(".json")
    )

    filename_ts = regexp_extract(
        col("_source_file"),
        r"station_information_(\d{8}_\d{6})\.json$",
        1,
    )

    stations = info_feeds.select(
        col("_source_file"),
        col("last_updated").cast("long").alias("feed_last_updated"),
        col("ttl").cast("long").alias("feed_ttl"),
        col("version").cast("string").alias("gbfs_version"),
        explode(col("data.stations")).alias("station"),
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
        .withColumn("name", col("station.name").cast("string"))
        .withColumn("short_name", col("station.short_name").cast("string"))
        .withColumn("lat", col("station.lat").cast("double"))
        .withColumn("lon", col("station.lon").cast("double"))
        .withColumn("capacity", col("station.capacity").cast("long"))
    )

    df = (
        df
        .withColumn(
            "capacity_zero",
            col("capacity") == 0,
        )
        .withColumn(
            "invalid_coordinates",
            col("lat").isNull()
            | col("lon").isNull()
            | (col("lat") < -90)
            | (col("lat") > 90)
            | (col("lon") < -180)
            | (col("lon") > 180),
        )
    )

    issue_candidates = array(
        when(col("station_id").isNull(), lit("missing_station_id")),
        when(col("snapshot_timestamp").isNull(), lit("invalid_snapshot_timestamp")),
        when(col("name").isNull() | (col("name") == ""), lit("missing_name")),
        when(col("capacity").isNull(), lit("missing_capacity")),
        when(col("capacity") < 0, lit("negative_capacity")),
        when(col("capacity_zero"), lit("capacity_zero")),
        when(col("invalid_coordinates"), lit("invalid_coordinates")),
    )

    output = (
        df
        .withColumn(
            "quality_issue",
            concat_ws(
                ";",
                array_filter(issue_candidates, lambda x: x.isNotNull()),
            ),
        )
        .select(
            "snapshot_timestamp",
            "snapshot_filename_timestamp",
            "station_id",
            "name",
            "short_name",
            "lat",
            "lon",
            "capacity",
            "capacity_zero",
            "invalid_coordinates",
            "feed_last_updated",
            "feed_ttl",
            "gbfs_version",
            "quality_issue",
            "_source_file",
        )
    )

    total_rows = output.count()
    print(f"TOTAL_STATION_INFORMATION_ROWS={total_rows}")

    print("=== SCHEMA ===")
    output.printSchema()

    print("=== JUMLAH PER SNAPSHOT ===")
    output.groupBy("snapshot_timestamp").count().orderBy(
        "snapshot_timestamp"
    ).show(100, truncate=False)

    print("=== KAPASITAS ===")
    output.groupBy("capacity_zero").count().show(truncate=False)

    print("=== QUALITY FLAGS ===")
    output.select(
        when(col("quality_issue") == "", "no_flag")
        .otherwise("has_flag")
        .alias("quality_flag")
    ).groupBy("quality_flag").count().show()

    print("=== CONTOH DATA ===")
    output.select(
        "snapshot_timestamp",
        "station_id",
        "name",
        "short_name",
        "lat",
        "lon",
        "capacity",
        "capacity_zero",
        "quality_issue",
    ).show(10, truncate=False)

    # Jangan pernah menimpa curated output yang sudah ada.
    target_path = spark._jvm.org.apache.hadoop.fs.Path(TARGET)
    fs = target_path.getFileSystem(
        spark.sparkContext._jsc.hadoopConfiguration()
    )

    if fs.exists(target_path):
        raise RuntimeError(
            f"Target sudah ada: {TARGET}. "
            "Job dihentikan tanpa menimpa data."
        )

    output.write.mode("errorifexists").parquet(TARGET)

    print(f"PARQUET_OUTPUT={TARGET}")
    print("GBFS_STATION_INFORMATION_TRANSFORM=SUCCESS")


try:
    main()
finally:
    spark.stop()
