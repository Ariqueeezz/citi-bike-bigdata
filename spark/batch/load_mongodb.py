from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CitiBike-Parquet-To-MongoDB")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

base_path = "/opt/spark/data/processed/2026-01"

mongo_uri = (
    "mongodb://admin:citibikemongo123"
    "@citibike-mongodb:27017/citibike"
    "?authSource=admin"
)

datasets = [
    ("hourly_stats", "historical_hourly_stats"),
    ("station_stats", "historical_station_stats"),
    ("user_type_stats", "historical_user_type_stats"),
]

for parquet_folder, collection in datasets:

    print(f"=== LOADING {parquet_folder} ===")

    df = spark.read.parquet(
        f"{base_path}/{parquet_folder}"
    )

    print(f"Rows: {df.count()}")

    (
        df.write
        .format("mongodb")
        .mode("overwrite")
        .option("spark.mongodb.write.connection.uri", mongo_uri)
        .option("database", "citibike")
        .option("collection", collection)
        .save()
    )

    print(f"=== SAVED TO {collection} ===")

print("=== ALL DATA LOADED TO MONGODB ===")

spark.stop()
