"""
Citi Bike Batch Processing Pipeline (MinIO S3A Data Lake)
Membaca dataset mentah perjalanan bulanan dari MinIO bucket `citibike-raw`,
melakukan pembersihan dan transformasi data, menulis Parquet terkurasi
ke `citibike-curated`, dan mengekspor agregasi analitik ke MongoDB.
"""

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
    round as spark_round,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# ============================================================
# Spark Session with MinIO S3A Configuration
# ============================================================

minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
spark_master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

spark = (
    SparkSession.builder
    .appName("CitiBike-MinIO-Batch-Processing")
    .master(spark_master)
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    # S3A FileSystem Settings for MinIO Data Lake
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
# Schema Definition
# ============================================================

trip_schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True),
])

# Daftar dataset bulanan yang akan diproses di Data Lake MinIO
datasets_config = [
    {
        "month_key": "2026-01",
        "month_label": "Januari 2026",
        "raw_path": "s3a://citibike-raw/2026/01/*.csv",
        "curated_path": "s3a://citibike-curated/2026/01",
    },
    {
        "month_key": "2026-10",
        "month_label": "Oktober 2026",
        "raw_path": "s3a://citibike-raw/2026/10/*.csv",
        "curated_path": "s3a://citibike-curated/2026/10",
    }
]

all_hourly_list = []
all_station_list = []
all_user_type_list = []

for cfg in datasets_config:
    month_key = cfg["month_key"]
    month_label = cfg["month_label"]
    raw_path = cfg["raw_path"]
    curated_path = cfg["curated_path"]

    print("\n" + "=" * 60)
    print(f"=== [MinIO] MEMPROSES DATASET: {month_label} ({raw_path}) ===")
    print("=" * 60)

    try:
        df = (
            spark.read
            .option("header", True)
            .schema(trip_schema)
            .csv(raw_path)
        )

        total_raw_rows = df.count()
        print(f"Total baris mentah ({month_label}): {total_raw_rows:,}")

        if total_raw_rows == 0:
            print(f"Dataset {month_label} kosong, melewati...")
            continue

        # Parsing Timestamp
        df = (
            df
            .withColumn("started_at_ts", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss.SSS"))
            .withColumn("ended_at_ts", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss.SSS"))
        )

        # Hitung durasi dalam menit
        df = df.withColumn(
            "duration_minutes",
            (unix_micros(col("ended_at_ts")) - unix_micros(col("started_at_ts"))) / 60000000
        )

        # Data Quality Gate:
        # 1. Timestamp valid (not null)
        # 2. Durasi logis: 0 < durasi <= 1440 menit (24 jam)
        clean_df = df.filter(
            (col("started_at_ts").isNotNull()) &
            (col("ended_at_ts").isNotNull()) &
            (col("duration_minutes") > 0) &
            (col("duration_minutes") <= 1440)
        )

        clean_df = (
            clean_df
            .withColumn("ride_date", to_date(col("started_at_ts")))
            .withColumn("ride_hour", hour(col("started_at_ts")))
            .withColumn("month_key", lit(month_key))
            .withColumn("month_label", lit(month_label))
        )

        total_clean_rows = clean_df.count()
        print(f"Data Bersih ({month_label}): {total_clean_rows:,} baris (Dropped {total_raw_rows - total_clean_rows:,} baris anomali)")

        # Simpan curated parquet ke MinIO
        print(f"Menyimpan Curated Parquet ke: {curated_path}/trips ...")
        (
            clean_df
            .select(
                "ride_id", "rideable_type", "started_at_ts", "ended_at_ts",
                "start_station_name", "start_station_id", "end_station_name", "end_station_id",
                "duration_minutes", "member_casual", "ride_date", "ride_hour", "month_key", "month_label"
            )
            .write
            .mode("overwrite")
            .parquet(f"{curated_path}/trips")
        )

        # 1. Agregasi Hourly
        hourly = (
            clean_df
            .groupBy("month_key", "month_label", "ride_date", "ride_hour", "member_casual")
            .agg(
                count("*").alias("total_rides"),
                spark_round(avg("duration_minutes"), 2).alias("avg_duration_minutes")
            )
        )
        all_hourly_list.append(hourly)

        # 2. Agregasi Top Stations (Top 50 stasiun per bulan)
        station = (
            clean_df
            .filter(col("start_station_name").isNotNull())
            .groupBy("month_key", "month_label", "start_station_id", "start_station_name")
            .agg(
                count("*").alias("total_rides"),
                spark_round(avg("duration_minutes"), 2).alias("avg_duration_minutes")
            )
            .orderBy(col("total_rides").desc())
            .limit(50)
        )
        all_station_list.append(station)

        # 3. Agregasi User Type (Member vs Casual)
        user_type = (
            clean_df
            .groupBy("month_key", "month_label", "member_casual")
            .agg(
                count("*").alias("total_rides"),
                spark_round(avg("duration_minutes"), 2).alias("avg_duration_minutes")
            )
        )
        all_user_type_list.append(user_type)

        print(f"✅ Agregasi {month_label} berhasil diproses.")

    except Exception as exc:
        print(f"❌ Error saat memproses {month_label}: {exc}")

# ============================================================
# Gabungkan Semua Bulan & Ekspor ke MongoDB
# ============================================================

if all_hourly_list and all_station_list and all_user_type_list:
    final_hourly = all_hourly_list[0]
    for h in all_hourly_list[1:]:
        final_hourly = final_hourly.unionByName(h)

    final_station = all_station_list[0]
    for s in all_station_list[1:]:
        final_station = final_station.unionByName(s)

    final_user_type = all_user_type_list[0]
    for u in all_user_type_list[1:]:
        final_user_type = final_user_type.unionByName(u)

    mongo_user = quote(os.getenv("MONGO_USER", "admin"), safe="")
    mongo_pass = quote(os.getenv("MONGO_PASSWORD", "citibikemongo123"), safe="")
    mongo_uri = os.getenv(
        "MONGO_URI",
        f"mongodb://{mongo_user}:{mongo_pass}@citibike-mongodb:27017/citibike?authSource=admin"
    )

    export_targets = [
        (final_hourly, "minio_hourly_stats"),
        (final_station, "minio_station_stats"),
        (final_user_type, "minio_user_type_stats"),
    ]

    print("\n" + "=" * 60)
    print("=== [MongoDB] MENGEKSPOR AGREGASI MULTI-BULAN KE MONGODB ===")
    print("=" * 60)

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
        print(f"✅ Collection {collection_name} berhasil disimpan.")

    print("\n🎉 SELURUH PIPELINE BATCH MINIO DATA LAKE BERHASIL SELESAI!")

spark.stop()
