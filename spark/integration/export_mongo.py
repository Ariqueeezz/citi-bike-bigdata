
import os
from urllib.parse import quote
from pyspark.sql import SparkSession

username = quote(os.environ["MONGO_USER"], safe="")
password = quote(os.environ["MONGO_PASSWORD"], safe="")

uri = (
    f"mongodb://{username}:{password}"
    "@citibike-mongodb:27017/?authSource=admin"
)

spark = (
    SparkSession.builder
    .appName("CitiBike-Experiment2-Mongo-Export")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

base = "hdfs://namenode:9000/analytics/2026/01"

datasets = {
    "hourly_stats": "exp2_hourly_stats",
    "station_stats": "exp2_station_stats",
    "user_type_stats": "exp2_user_type_stats",
}

try:
    for folder, collection in datasets.items():
        source = f"{base}/{folder}"
        print(f"=== Mengekspor {source} ===")

        df = spark.read.parquet(source)
        expected_count = df.count()

        (
            df.write
            .format("mongodb")
            .mode("overwrite")
            .option("connection.uri", uri)
            .option("database", "citibike")
            .option("collection", collection)
            .save()
        )

        print(f"Collection: {collection}")
        print(f"Baris sumber: {expected_count}")
        print(f"EXPORT_SUCCESS={collection}")

    print("MONGO_EXPORT=SUCCESS")

finally:
    spark.stop()
