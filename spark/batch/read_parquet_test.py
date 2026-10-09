from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CitiBike-Parquet-Test")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

base_path = "/opt/spark/data/processed/2026-01"

print("=== READ HOURLY STATS ===")

hourly = spark.read.parquet(
    f"{base_path}/hourly_stats"
)

hourly.printSchema()
hourly.show(20, truncate=False)

print("=== TOTAL ROWS ===")
print(hourly.count())

print("=== READ STATION STATS ===")

station = spark.read.parquet(
    f"{base_path}/station_stats"
)

station.printSchema()
station.show(10, truncate=False)

print("=== TOTAL ROWS ===")
print(station.count())

print("=== READ USER TYPE STATS ===")

user_type = spark.read.parquet(
    f"{base_path}/user_type_stats"
)

user_type.printSchema()
user_type.show(truncate=False)

print("=== TOTAL ROWS ===")
print(user_type.count())

spark.stop()
