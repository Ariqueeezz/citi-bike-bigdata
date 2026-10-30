import os
from pyspark.sql import SparkSession

minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

spark = (
    SparkSession.builder
    .appName("CitiBike-Verify-Curated-MinIO")
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

path = "s3a://citibike-curated/2026/01/trips"

try:
    print(f"Memverifikasi data Curated di MinIO: {path} ...")
    df = spark.read.parquet(path)

    print("=== SKEMA CURATED MINIO S3 ===")
    df.printSchema()

    print("=== JUMLAH BARIS CURATED ===")
    print(f"Total baris: {df.count():,}")

    print("=== CONTOH DATA CURATED ===")
    df.select(
        "ride_id",
        "rideable_type",
        "started_at_ts",
        "ended_at_ts",
        "duration_minutes",
        "member_casual"
    ).show(5, truncate=False)

    print("CURATED_MINIO_VERIFY=SUCCESS")

except Exception as e:
    print(f"Peringatan: Tidak dapat membaca curated path di MinIO ({e}). Pastikan batch processing telah dijalankan.")

finally:
    spark.stop()
