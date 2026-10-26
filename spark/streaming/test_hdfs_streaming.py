
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, ArrayType
)

spark = (
    SparkSession.builder
    .appName("CitiBike-HDFS-Streaming-Stage1")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schema GBFS station_status
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
    StructField(
        "vehicle_types_available",
        ArrayType(vehicle_type_schema),
        True,
    ),
    StructField("num_ebikes_available", IntegerType(), True),
])

root_schema = StructType([
    StructField(
        "data",
        StructType([
            StructField(
                "stations",
                ArrayType(station_schema),
                True,
            )
        ]),
        True,
    )
])

# HDFS archive for one date only
INPUT_PATH = (
    "hdfs://namenode:9000/"
    "raw/realtime/gbfs/2026/10/23/station_status/"
)

# New checkpoint: do not reuse the existing checkpoint
CHECKPOINT_PATH = (
    "/tmp/citibike_checkpoint_stage1_hdfs"
)

raw_stream = (
    spark.readStream
    .schema(root_schema)
    .option("maxFilesPerTrigger", 1)
    .json(INPUT_PATH)
)

station_stream = (
    raw_stream
    .select(explode(col("data.stations")).alias("station"))
    .select(
        col("station.station_id").alias("station_id"),
        col("station.num_bikes_available").alias(
            "num_bikes_available"
        ),
        col("station.num_bikes_disabled").alias(
            "num_bikes_disabled"
        ),
        col("station.num_docks_available").alias(
            "num_docks_available"
        ),
        col("station.num_docks_disabled").alias(
            "num_docks_disabled"
        ),
        col("station.is_installed").alias("is_installed"),
        col("station.is_renting").alias("is_renting"),
        col("station.is_returning").alias("is_returning"),
        col("station.last_reported").alias("last_reported"),
        col("station.num_ebikes_available").alias(
            "num_ebikes_available"
        ),
    )
    .withColumn("snapshot_time", current_timestamp())
)

query = (
    station_stream.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 10)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(once=True)
    .start()
)

query.awaitTermination()
spark.stop()

