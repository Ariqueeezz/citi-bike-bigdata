import os
from urllib.parse import quote
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    current_timestamp,
    from_unixtime,
    to_timestamp,
    when,
    unix_timestamp,
    from_json
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    ArrayType,
)

# ============================================================
# Spark Session Setup
# ============================================================

spark = (
    SparkSession.builder
    .appName("CitiBike-Kafka-Realtime-Streaming")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Configuration: Kafka & MongoDB
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "citibike-kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "citibike-station-status")

mongo_user = quote(os.getenv("MONGO_USER", "admin"), safe="")
mongo_pass = quote(os.getenv("MONGO_PASSWORD", "citibikemongo123"), safe="")
mongo_uri = os.getenv(
    "MONGO_URI",
    f"mongodb://{mongo_user}:{mongo_pass}@citibike-mongodb:27017/citibike?authSource=admin"
)

MONGO_DATABASE = "citibike"
MONGO_COLLECTION = "realtime_station_status"


# ============================================================
# GBFS Schema Definition
# ============================================================

vehicle_type_schema = StructType([
    StructField("vehicle_type_id", StringType(), True),
    StructField("count", IntegerType(), True),
])

station_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_bikes_disabled", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
    StructField("num_docks_disabled", IntegerType(), True),
    StructField("is_installed", IntegerType(), True),
    StructField("is_renting", IntegerType(), True),
    StructField("is_returning", IntegerType(), True),
    StructField("last_reported", LongType(), True),
    StructField("vehicle_types_available", ArrayType(vehicle_type_schema), True),
    StructField("num_ebikes_available", IntegerType(), True),
])

root_schema = StructType([
    StructField(
        "data",
        StructType([
            StructField("stations", ArrayType(station_schema), True)
        ]),
        True
    )
])


# ============================================================
# Read Streaming Events from Apache Kafka
# ============================================================

print(f"Connecting to Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS} on Topic: {KAFKA_TOPIC} ...")

kafka_raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON string from Kafka 'value' binary column
parsed_stream = (
    kafka_raw_stream
    .select(from_json(col("value").cast("string"), root_schema).alias("json_payload"))
    .select("json_payload.*")
)


# ============================================================
# Flatten stations[] & Data Quality Validation
# ============================================================

station_stream = (
    parsed_stream
    .select(explode(col("data.stations")).alias("station"))
    .select(
        col("station.station_id").alias("station_id"),
        col("station.num_bikes_available").alias("num_bikes_available"),
        col("station.num_bikes_disabled").alias("num_bikes_disabled"),
        col("station.num_docks_available").alias("num_docks_available"),
        col("station.num_docks_disabled").alias("num_docks_disabled"),
        col("station.is_installed").alias("is_installed"),
        col("station.is_renting").alias("is_renting"),
        col("station.is_returning").alias("is_returning"),
        col("station.last_reported").alias("last_reported"),
        to_timestamp(from_unixtime(col("station.last_reported"))).alias("last_reported_time"),
        when(
            col("station.last_reported").isNotNull() & (col("station.last_reported") > 0),
            True
        ).otherwise(False).alias("is_valid_reporting"),
        when(
            col("station.last_reported").isNull() | (col("station.last_reported") <= 0),
            True
        ).when(
            (unix_timestamp(current_timestamp()) - col("station.last_reported")) > 3600,
            True
        ).otherwise(False).alias("is_stale"),
        col("station.num_ebikes_available").alias("num_ebikes_available"),
    )
    .withColumn("snapshot_time", current_timestamp())
)


# ============================================================
# Write Each Micro-Batch to MongoDB
# ============================================================

def write_to_mongodb(batch_df, batch_id):
    print(f"\n=== [Kafka -> Spark Streaming] PROCESSING BATCH {batch_id} ===")
    if batch_df.isEmpty():
        print("Batch is empty. Waiting for next Kafka message...")
        return

    total_count = batch_df.count()
    stale_count = batch_df.filter(col("is_stale") == True).count()
    print(f"Kafka message ingested: {total_count} station records (Stale/delayed: {stale_count})")

    (
        batch_df.write
        .format("mongodb")
        .mode("append")
        .option("spark.mongodb.write.connection.uri", mongo_uri)
        .option("database", MONGO_DATABASE)
        .option("collection", MONGO_COLLECTION)
        .save()
    )

    print(f"=== [MongoDB] BATCH {batch_id} COMMITTED TO '{MONGO_COLLECTION}' ===")


# ============================================================
# Start Continuous Stream Processing
# ============================================================

checkpoint_dir = "/opt/spark/data/checkpoint_kafka"

query = (
    station_stream.writeStream
    .foreachBatch(write_to_mongodb)
    .option("checkpointLocation", checkpoint_dir)
    .trigger(processingTime="15 seconds")
    .start()
)

print("\n" + "=" * 55)
print("🚀 Citi Bike Kafka-Spark Streaming Pipeline Started!")
print(f"Source   : Kafka Broker ({KAFKA_BOOTSTRAP_SERVERS})")
print(f"Topic    : {KAFKA_TOPIC}")
print(f"Sink     : MongoDB ({MONGO_COLLECTION})")
print(f"Trigger  : Every 15 seconds")
print("=" * 55 + "\n")

query.awaitTermination()
