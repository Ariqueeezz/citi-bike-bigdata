"""
Citi Bike HDFS to MinIO Data Lake Migration Script
Memindahkan dataset dari Hadoop HDFS ke MinIO Object Storage
menggunakan Apache Spark secara terdistribusi.
"""

import os
from pyspark.sql import SparkSession

minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

spark = (
    SparkSession.builder
    .appName("CitiBike-HDFS-to-MinIO-Migration")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

migration_tasks = [
    {
        "type": "csv",
        "hdfs_source": "hdfs://namenode:9000/raw/2026/01/*.csv",
        "minio_target": "s3a://citibike-raw/2026/01",
        "desc": "Raw CSV Trip Data (Januari 2026)"
    },
    {
        "type": "parquet",
        "hdfs_source": "hdfs://namenode:9000/curated/2026/01/trips",
        "minio_target": "s3a://citibike-curated/2026/01/trips",
        "desc": "Curated Clean Trips Parquet"
    },
    {
        "type": "parquet",
        "hdfs_source": "hdfs://namenode:9000/analytics/2026/01/hourly_stats",
        "minio_target": "s3a://citibike-curated/2026/01/hourly_stats",
        "desc": "Analytics Hourly Statistics"
    },
    {
        "type": "parquet",
        "hdfs_source": "hdfs://namenode:9000/analytics/2026/01/station_stats",
        "minio_target": "s3a://citibike-curated/2026/01/station_stats",
        "desc": "Analytics Station Statistics"
    },
    {
        "type": "parquet",
        "hdfs_source": "hdfs://namenode:9000/analytics/2026/01/user_type_stats",
        "minio_target": "s3a://citibike-curated/2026/01/user_type_stats",
        "desc": "Analytics User Type Statistics"
    }
]

print("=" * 65)
print("  [MIGRASI DATA] Memulai Migrasi Hadoop HDFS -> MinIO")
print("=" * 65)

success_count = 0
for task in migration_tasks:
    print(f"\nMemproses: {task['desc']} ...")
    print(f"  • Source HDFS : {task['hdfs_source']}")
    print(f"  • Target MinIO: {task['minio_target']}")
    try:
        if task["type"] == "csv":
            df = spark.read.option("header", "true").csv(task["hdfs_source"])
            count = df.count()
            (
                df.write
                .mode("overwrite")
                .option("header", "true")
                .csv(task["minio_target"])
            )
        elif task["type"] == "parquet":
            df = spark.read.parquet(task["hdfs_source"])
            count = df.count()
            (
                df.write
                .mode("overwrite")
                .parquet(task["minio_target"])
            )

        print(f"  ✅ Sukses: {count:,} baris dipindahkan ke MinIO.")
        success_count += 1
    except Exception as exc:
        print(f"  ℹ️ Path sumber belum ada di HDFS atau dilewati ({exc})")

print("\n" + "=" * 65)
print(f"  [MIGRASI SELESAI] {success_count} dataset berhasil dipindahkan ke MinIO!")
print("=" * 65)

spark.stop()
