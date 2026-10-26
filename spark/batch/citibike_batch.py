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

spark = (
    SparkSession.builder
    .appName("CitiBike-January-2026-Batch")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 1. READ RAW DATA
# ============================================================

input_path = "/opt/spark/data/raw/2026-01/*.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

total_raw_rows = df.count()
print(f"=== RAW DATA: {total_raw_rows} rows ===")
df.show(5, truncate=False)

print("=== SCHEMA ===")
df.printSchema()


# ============================================================
# 2. TRANSFORMATION & DATA QUALITY VALIDATION
# ============================================================

df = (
    df
    .withColumn(
        "started_at",
        to_timestamp(
            col("started_at"),
            "yyyy-MM-dd HH:mm:ss.SSS"
        )
    )
    .withColumn(
        "ended_at",
        to_timestamp(
            col("ended_at"),
            "yyyy-MM-dd HH:mm:ss.SSS"
        )
    )
)

# Gunakan microseconds agar milidetik tidak hilang
df = df.withColumn(
    "duration_minutes",
    (
        unix_micros(col("ended_at"))
        - unix_micros(col("started_at"))
    ) / 60000000
)

# VALIDASI KUALITAS DATA:
# 1. Pastikan timestamp tidak null
# 2. Durasi logis: > 0 menit dan <= 1440 menit (24 jam)
# 3. Filter boundary tanggal target (Januari 2026):
#    Mencegah trip akhir bulan sebelumnya (seperti 2025-12-31 23:xx) yang masuk karena trip berakhir di Jan 2026
df = df.filter(
    (col("started_at").isNotNull()) &
    (col("ended_at").isNotNull()) &
    (col("duration_minutes") > 0) &
    (col("duration_minutes") <= 1440) &
    (col("started_at") >= "2026-01-01 00:00:00") &
    (col("started_at") < "2026-02-01 00:00:00")
)

# Derived columns
df = (
    df
    .withColumn("ride_date", to_date(col("started_at")))
    .withColumn("ride_hour", hour(col("started_at")))
)

total_clean_rows = df.count()
print(f"=== CLEANED DATA: {total_clean_rows} rows (Dropped {total_raw_rows - total_clean_rows} invalid/out-of-bounds rows) ===")
df.show(5, truncate=False)


# ============================================================
# 3. VALIDATE DATE RANGE (SANITY CHECK)
# ============================================================

print("=== DATE RANGE (Strictly 2026-01-01 to 2026-01-31) ===")

df.select(
    "ride_date"
).groupBy(
    "ride_date"
).count().orderBy(
    "ride_date"
).show(40, truncate=False)


# ============================================================
# 4. HOURLY STATISTICS
# ============================================================

hourly_stats = (
    df
    .groupBy(
        "ride_date",
        "ride_hour",
        "member_casual"
    )
    .agg(
        count("*").alias("total_rides"),
        round(
            avg("duration_minutes"),
            2
        ).alias("avg_duration_minutes")
    )
    .orderBy(
        "ride_date",
        "ride_hour",
        "member_casual"
    )
)

print("=== HOURLY STATISTICS ===")
hourly_stats.show(30, truncate=False)


# ============================================================
# 5. TOP START STATIONS
# ============================================================

station_stats = (
    df
    .groupBy(
        "start_station_id",
        "start_station_name"
    )
    .agg(
        count("*").alias("total_rides"),
        round(
            avg("duration_minutes"),
            2
        ).alias("avg_duration_minutes")
    )
    .orderBy(
        col("total_rides").desc()
    )
)

print("=== TOP START STATIONS ===")
station_stats.show(20, truncate=False)


# ============================================================
# 6. MEMBER VS CASUAL
# ============================================================

user_type_stats = (
    df
    .groupBy("member_casual")
    .agg(
        count("*").alias("total_rides"),
        round(
            avg("duration_minutes"),
            2
        ).alias("avg_duration_minutes")
    )
    .orderBy(
        col("total_rides").desc()
    )
)

print("=== MEMBER VS CASUAL ===")
user_type_stats.show(truncate=False)


# ============================================================
# 7. SAVE PROCESSED DATA
# ============================================================

output_base = "/opt/spark/data/processed/2026-01"

print("=== SAVING PARQUET FILES ===")

hourly_stats.write.mode("overwrite").parquet(
    f"{output_base}/hourly_stats"
)

station_stats.write.mode("overwrite").parquet(
    f"{output_base}/station_stats"
)

user_type_stats.write.mode("overwrite").parquet(
    f"{output_base}/user_type_stats"
)

print("=== PARQUET OUTPUT SAVED ===")

spark.stop()
