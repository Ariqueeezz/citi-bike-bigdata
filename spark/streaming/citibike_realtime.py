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
    unix_timestamp
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
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("CitiBike-Realtime-Streaming")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# MongoDB Configuration
# ============================================================

mongo_user = quote(os.getenv("MONGO_USER", "admin"), safe="")
mongo_pass = quote(os.getenv("MONGO_PASSWORD", "citibikemongo123"), safe="")
mongo_uri = os.getenv(
    "MONGO_URI",
    f"mongodb://{mongo_user}:{mongo_pass}@citibike-mongodb:27017/citibike?authSource=admin"
)

MONGO_DATABASE = "citibike"
MONGO_COLLECTION = "realtime_station_status"


# ============================================================
# Input Path
# ============================================================

INPUT_PATH = "/opt/spark/data/realtime"


# ============================================================
# JSON Schema
# ============================================================

vehicle_type_schema = StructType([
    StructField("vehicle_type_id", StringType(), True),
    StructField("count", IntegerType(), True),
])

station_schema = StructType([
    StructField("station_id", StringType(), True),

    StructField(
        "num_bikes_available",
        IntegerType(),
        True
    ),

    StructField(
        "num_bikes_disabled",
        IntegerType(),
        True
    ),

    StructField(
        "num_docks_available",
        IntegerType(),
        True
    ),

    StructField(
        "num_docks_disabled",
        IntegerType(),
        True
    ),

    StructField(
        "is_installed",
        IntegerType(),
        True
    ),

    StructField(
        "is_renting",
        IntegerType(),
        True
    ),

    StructField(
        "is_returning",
        IntegerType(),
        True
    ),

    StructField(
        "last_reported",
        LongType(),
        True
    ),

    StructField(
        "vehicle_types_available",
        ArrayType(vehicle_type_schema),
        True
    ),

    StructField(
        "num_ebikes_available",
        IntegerType(),
        True
    ),
])

root_schema = StructType([
    StructField(
        "data",
        StructType([
            StructField(
                "stations",
                ArrayType(station_schema),
                True
            )
        ]),
        True
    )
])


# ============================================================
# Read JSON as Streaming Data
# ============================================================

raw_stream = (
    spark.readStream
    .schema(root_schema)
    .json(INPUT_PATH)
)


# ============================================================
# Flatten stations[] & Data Quality Validation for last_reported
# ============================================================

station_stream = (
    raw_stream
    .select(
        explode(col("data.stations")).alias("station")
    )
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
        col("station.is_installed").alias(
            "is_installed"
        ),
        col("station.is_renting").alias(
            "is_renting"
        ),
        col("station.is_returning").alias(
            "is_returning"
        ),
        # Simpan epoch mentah untuk backward compatibility
        col("station.last_reported").alias(
            "last_reported"
        ),
        # VALIDASI KUALITAS DATA: Konversi epoch seconds ke timestamp nyata
        to_timestamp(
            from_unixtime(col("station.last_reported"))
        ).alias("last_reported_time"),
        # Validasi validitas nilai pelaporan
        when(
            col("station.last_reported").isNotNull() & (col("station.last_reported") > 0),
            True
        ).otherwise(False).alias("is_valid_reporting"),
        # Validasi freshness: Flag stasiun jika tidak melaporkan data > 1 jam (3600 detik)
        when(
            col("station.last_reported").isNull() | (col("station.last_reported") <= 0),
            True
        ).when(
            (unix_timestamp(current_timestamp()) - col("station.last_reported")) > 3600,
            True
        ).otherwise(False).alias("is_stale"),
        col("station.num_ebikes_available").alias(
            "num_ebikes_available"
        ),
    )
    .withColumn(
        "snapshot_time",
        current_timestamp()
    )
)


# ============================================================
# Write Each Micro-Batch to MongoDB
# ============================================================

def write_to_mongodb(batch_df, batch_id):

    print(
        f"=== PROCESSING REALTIME BATCH {batch_id} ==="
    )

    if batch_df.isEmpty():
        print("Batch is empty.")
        return

    total_count = batch_df.count()
    stale_count = batch_df.filter(col("is_stale") == True).count()
    print(
        f"Rows in batch: {total_count} (Stale/delayed stations: {stale_count})"
    )

    (
        batch_df.write
        .format("mongodb")
        .mode("append")
        .option(
            "spark.mongodb.write.connection.uri",
            mongo_uri
        )
        .option(
            "database",
            MONGO_DATABASE
        )
        .option(
            "collection",
            MONGO_COLLECTION
        )
        .save()
    )

    print(
        f"=== BATCH {batch_id} SAVED TO MONGODB ==="
    )


# ============================================================
# Start Streaming Query
# ============================================================

query = (
    station_stream.writeStream
    .foreachBatch(write_to_mongodb)
    .option(
        "checkpointLocation",
        "/opt/spark/data/realtime/checkpoint"
    )
    .trigger(processingTime="30 seconds")
    .start()
)


print("==============================================")
print("Citi Bike Real-Time Streaming Started")
print("Input  :", INPUT_PATH)
print("MongoDB:", MONGO_COLLECTION)
print("==============================================")


query.awaitTermination()
