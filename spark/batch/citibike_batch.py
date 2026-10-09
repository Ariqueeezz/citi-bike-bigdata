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

print("=== RAW DATA ===")
df.show(5, truncate=False)

print("=== SCHEMA ===")
df.printSchema()


# ============================================================
# 2. TRANSFORMATION & CLEANING
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

# Filter data tidak valid
df = df.filter(
    (col("started_at").isNotNull()) &
    (col("ended_at").isNotNull()) &
    (col("duration_minutes") > 0) &
    (col("duration_minutes") <= 1440)
)

# Derived columns
df = (
    df
    .withColumn("ride_date", to_date(col("started_at")))
    .withColumn("ride_hour", hour(col("started_at")))
)

print("=== CLEANED DATA ===")
df.show(5, truncate=False)


# ============================================================
# 3. VALIDATE DATE RANGE
# ============================================================

print("=== DATE RANGE ===")

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
