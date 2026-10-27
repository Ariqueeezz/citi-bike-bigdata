import os
from urllib.parse import quote
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    unix_micros,
    count,
    avg,
    round
)

# ============================================================
# Spark Session with MinIO S3A Configuration
# ============================================================

minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

spark = (
    SparkSession.builder
    .appName("CitiBike-MinIO-Batch-Processing")
    .master("spark://spark-master:7077")
    # S3A FileSystem Settings for MinIO
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 1. READ RAW CSV DATA FROM MINIO BUCKET
# ============================================================

s3_raw_path = "s3a://citibike-raw/2026/01/*.csv"
print(f"=== [MinIO] MEMBACA DATA RAW DARI: {s3_raw_path} ===")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(s3_raw_path)
)

total_raw_rows = df.count()
print(f"Total baris mentah di MinIO: {total_raw_rows:,}")
df.show(5, truncate=False)


# ============================================================
# 2. DATA QUALITY VALIDATION & CLEANING
# ============================================================

df = (
    df
    .withColumn("started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss.SSS"))
    .withColumn("ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss.SSS"))
)

# Hitung durasi perjalanan dalam menit
df = df.withColumn(
    "duration_minutes",
    (unix_micros(col("ended_at")) - unix_micros(col("started_at"))) / 60000000
)

# Filter Data Quality Gate:
# 1. Timestamp tidak null
# 2. Durasi logis: 0 < durasi <= 1440 menit (24 jam)
# 3. Filter boundary strictly Januari 2026 (menghilangkan anomali 2025-12-31)
clean_df = df.filter(
    (col("started_at").isNotNull()) &
    (col("ended_at").isNotNull()) &
    (col("duration_minutes") > 0) &
    (col("duration_minutes") <= 1440) &
    (col("started_at") >= "2026-01-01 00:00:00") &
    (col("started_at") < "2026-02-01 00:00:00")
)

clean_df = (
    clean_df
    .withColumn("ride_date", to_date(col("started_at")))
    .withColumn("ride_hour", hour(col("started_at")))
)

total_clean_rows = clean_df.count()
print(f"=== DATA BERSIH: {total_clean_rows:,} baris (Dropped {total_raw_rows - total_clean_rows:,} anomali/di luar Jan 2026) ===")


# ============================================================
# 3. SIMPAN CURATED DATA KE MINIO BUCKET
# ============================================================

s3_curated_base = "s3a://citibike-curated/2026/01"
print(f"=== [MinIO] MENYIMPAN CURATED PARQUET KE: {s3_curated_base}/trips ===")

(
    clean_df.write
    .mode("overwrite")
    .parquet(f"{s3_curated_base}/trips")
)


# ============================================================
# 4. AGREGASI ANALITIK
# ============================================================

# Hourly stats
hourly_stats = (
    clean_df
    .groupBy("ride_date", "ride_hour", "member_casual")
    .agg(
        count("*").alias("total_rides"),
        round(avg("duration_minutes"), 2).alias("avg_duration_minutes")
    )
    .orderBy("ride_date", "ride_hour", "member_casual")
)

# Station stats
station_stats = (
    clean_df
    .groupBy("start_station_id", "start_station_name")
    .agg(
        count("*").alias("total_rides"),
        round(avg("duration_minutes"), 2).alias("avg_duration_minutes")
    )
    .orderBy(col("total_rides").desc())
)

# User type stats
user_type_stats = (
    clean_df
    .groupBy("member_casual")
    .agg(
        count("*").alias("total_rides"),
        round(avg("duration_minutes"), 2).alias("avg_duration_minutes")
    )
    .orderBy(col("total_rides").desc())
)

# Simpan hasil agregasi ke MinIO
print(f"=== [MinIO] MENYIMPAN AGREGASI ANALITIK KE MINIO ===")
hourly_stats.write.mode("overwrite").parquet(f"{s3_curated_base}/hourly_stats")
station_stats.write.mode("overwrite").parquet(f"{s3_curated_base}/station_stats")
user_type_stats.write.mode("overwrite").parquet(f"{s3_curated_base}/user_type_stats")
print("✅ Seluruh data Parquet berhasil disimpan di MinIO!")


# ============================================================
# 5. EKSPOR KE MONGODB (SERVING LAYER DASHBOARD)
# ============================================================

mongo_user = quote(os.getenv("MONGO_USER", "admin"), safe="")
mongo_pass = quote(os.getenv("MONGO_PASSWORD", "citibikemongo123"), safe="")
mongo_uri = os.getenv(
    "MONGO_URI",
    f"mongodb://{mongo_user}:{mongo_pass}@citibike-mongodb:27017/citibike?authSource=admin"
)

export_targets = [
    (hourly_stats, "minio_hourly_stats"),
    (station_stats, "minio_station_stats"),
    (user_type_stats, "minio_user_type_stats"),
]

for stat_df, collection_name in export_targets:
    print(f"Mengekspor ke MongoDB collection: {collection_name} ...")
    (
        stat_df.write
        .format("mongodb")
        .mode("overwrite")
        .option("spark.mongodb.write.connection.uri", mongo_uri)
        .option("database", "citibike")
        .option("collection", collection_name)
        .save()
    )
    print(f"✅ Collection {collection_name} tersimpan di MongoDB.")

print("\n=== PIPELINE BATCH MINIO -> SPARK -> MONGODB SUKSES! ===")
spark.stop()
