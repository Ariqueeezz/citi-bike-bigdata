import os
from pyspark.sql import SparkSession

SOURCE = (
    "hdfs://namenode:9000/curated/realtime/gbfs/"
    "station_status_enriched"
)
DATABASE = "citibike"
COLLECTION = "realtime_station_status_enriched"
EXPECTED_ROWS = 201537


def main():
    mongo_uri = os.environ.get("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI belum disediakan.")

    spark = (
        SparkSession.builder
        .appName("Export-GBFS-Enriched-To-MongoDB")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(SOURCE)

        required = {
            "snapshot_timestamp",
            "station_id",
            "metadata_match_status",
            "matched_info_snapshot_timestamp",
            "metadata_age_seconds",
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"Kolom wajib tidak ditemukan: {sorted(missing)}"
            )

        source_count = df.count()
        print(f"SOURCE_ROWS={source_count}")

        if source_count != EXPECTED_ROWS:
            raise RuntimeError(
                f"Jumlah sumber tidak sesuai: {source_count}; "
                "ekspor dibatalkan."
            )

        duplicate_groups = (
            df.groupBy("snapshot_timestamp", "station_id")
            .count()
            .filter("count > 1")
            .limit(1)
            .count()
        )
        if duplicate_groups:
            raise RuntimeError(
                "Ditemukan duplikat snapshot_timestamp + station_id."
            )

        print("SOURCE_VALIDATION=SUCCESS")

        (
            df.write
            .format("mongodb")
            .mode("append")
            .option("connection.uri", mongo_uri)
            .option("database", DATABASE)
            .option("collection", COLLECTION)
            .save()
        )

        print("MONGO_WRITE=COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
