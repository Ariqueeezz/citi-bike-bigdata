import os
import re
import json
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse

from pymongo import MongoClient, ReplaceOne

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    ArrayType,
)

# --------------------------------------------------
# 1. Spark configuration
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("CitiBike-Realtime-Streaming-v2")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 2. MongoDB configuration
# --------------------------------------------------

MONGO_URI = (
    "mongodb://admin:citibikemongo123"
    "@citibike-mongodb:27017/citibike"
    "?authSource=admin"
)

MONGO_DATABASE = "citibike"
MONGO_COLLECTION = "realtime_station_status"

# --------------------------------------------------
# 3. Input and checkpoint paths
# --------------------------------------------------

REALTIME_DIR = Path("/opt/spark/data/realtime")

INPUT_PATH = str(REALTIME_DIR / "streaming_status")
CHECKPOINT_PATH = str(REALTIME_DIR / "checkpoint_v2")

# --------------------------------------------------
# 4. GBFS station status schema
# --------------------------------------------------

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

# --------------------------------------------------
# 5. Read only new status snapshots
# --------------------------------------------------

raw_stream = (
    spark.readStream
    .schema(root_schema)
    .option("pathGlobFilter", "station_status_*.json")
    .option("maxFilesPerTrigger", 1)
    .json(INPUT_PATH)
)

station_stream = (
    raw_stream
    .select(
        input_file_name().alias("source_path"),
        explode(col("data.stations")).alias("station"),
    )
    .select(
        "source_path",
        col("station.station_id").cast("string").alias("station_id"),
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
        col("station.vehicle_types_available").alias(
            "vehicle_types_available"
        ),
        col("station.num_ebikes_available").alias(
            "num_ebikes_available"
        ),
    )
)

# --------------------------------------------------
# 6. Source filename -> UTC snapshot timestamp
# --------------------------------------------------

FILENAME_PATTERN = re.compile(
    r"^(station_status|station_information)_(\d{8}_\d{6})\.json$"
)


def parse_filename_timestamp(filename):
    match = FILENAME_PATTERN.match(filename)

    if not match:
        raise ValueError(
            f"Nama file tidak sesuai format snapshot: {filename}"
        )

    return datetime.strptime(
        match.group(2),
        "%Y%m%d_%H%M%S",
    ).replace(tzinfo=timezone.utc)


def utc_datetime(value):
    """Convert an aware UTC datetime to a BSON-compatible datetime."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


# --------------------------------------------------
# 7. Load eligible station-information snapshots
# --------------------------------------------------

def load_information_snapshots():
    snapshots = []

    for path in REALTIME_DIR.glob("station_information_*.json"):
        try:
            snapshot_time = parse_filename_timestamp(path.name)

            with path.open("r", encoding="utf-8") as file:
                payload = json.load(file)

            stations = payload.get("data", {}).get("stations", [])

            station_map = {
                str(station["station_id"]): station
                for station in stations
                if station.get("station_id") is not None
            }

            snapshots.append({
                "filename": path.name,
                "timestamp": snapshot_time,
                "stations": station_map,
            })

        except Exception as exc:
            print(
                f"[WARN] Metadata snapshot dilewati: "
                f"{path.name} | {exc}",
                flush=True,
            )

    snapshots.sort(key=lambda item: item["timestamp"])
    return snapshots


def select_metadata(status_time, station_id, info_snapshots):
    # Pertahankan prinsip historical enrichment:
    # metadata harus berasal dari hari UTC yang sama dan
    # tidak boleh lebih baru daripada snapshot status.
    eligible = [
        item for item in info_snapshots
        if item["timestamp"].date() == status_time.date()
        and item["timestamp"] <= status_time
    ]

    if not eligible:
        return {
            "match_status": "no_same_day_info_snapshot",
            "metadata": None,
            "metadata_snapshot_time": None,
        }

    selected = eligible[-1]
    metadata = selected["stations"].get(str(station_id))

    if metadata is None:
        return {
            "match_status": (
                "station_id_not_found_in_selected_info_snapshot"
            ),
            "metadata": None,
            "metadata_snapshot_time": selected["timestamp"],
        }

    return {
        "match_status": "matched",
        "metadata": metadata,
        "metadata_snapshot_time": selected["timestamp"],
    }


# --------------------------------------------------
# 8. Idempotent MongoDB upsert
# --------------------------------------------------

def write_to_mongodb(batch_df, batch_id):
    print(
        f"\n=== REALTIME BATCH {batch_id} ===",
        flush=True,
    )

    client = None

    try:
        # foreachBatch runs on the Spark driver.
        # Read rows on the driver and use PyMongo bulk upserts.
        rows = list(batch_df.toLocalIterator())

        if not rows:
            print("Batch kosong.", flush=True)
            return

        info_snapshots = load_information_snapshots()

        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=10000,
        )

        collection = client[MONGO_DATABASE][MONGO_COLLECTION]

        operations = []
        source_files = set()
        matched_count = 0
        unmatched_count = 0

        for row in rows:
            record = row.asDict(recursive=True)

            source_path = record.pop("source_path")
            source_filename = os.path.basename(
                urlparse(source_path).path
            )

            status_time = parse_filename_timestamp(source_filename)
            station_id = record.get("station_id")

            if station_id is None:
                print(
                    f"[WARN] station_id kosong di {source_filename}; "
                    "baris dilewati.",
                    flush=True,
                )
                continue

            selected = select_metadata(
                status_time,
                station_id,
                info_snapshots,
            )

            metadata = selected["metadata"]
            metadata_time = selected["metadata_snapshot_time"]

            # Deterministic _id: the same source snapshot and station
            # always replace the same MongoDB document.
            document_id = f"{source_filename}::{station_id}"

            document = {
                "_id": document_id,
                **record,
                "snapshot_time": utc_datetime(status_time),
                "source_file": source_filename,
                "match_status": selected["match_status"],
                "station_metadata_snapshot_time": (
                    utc_datetime(metadata_time)
                    if metadata_time is not None
                    else None
                ),
                "metadata_age_seconds": (
                    int((status_time - metadata_time).total_seconds())
                    if metadata_time is not None
                    else None
                ),
            }

            if metadata is not None:
                document.update({
                    "station_name": metadata.get("name"),
                    "station_short_name": metadata.get("short_name"),
                    "lon": metadata.get("lon"),
                    "lat": metadata.get("lat"),
                    "region_id": metadata.get("region_id"),
                    "capacity": metadata.get("capacity"),
                    "rental_uris": metadata.get("rental_uris"),
                    "is_charging": metadata.get("is_charging"),
                })
                matched_count += 1
            else:
                unmatched_count += 1

            operations.append(
                ReplaceOne(
                    {"_id": document_id},
                    document,
                    upsert=True,
                )
            )

            source_files.add(source_filename)

        if operations:
            result = collection.bulk_write(
                operations,
                ordered=False,
            )

            print(
                f"Source files: {sorted(source_files)}\n"
                f"Rows submitted: {len(operations)}\n"
                f"Inserted: {result.upserted_count}\n"
                f"Matched/replaced: {result.matched_count}\n"
                f"Metadata matched: {matched_count}\n"
                f"Metadata unmatched: {unmatched_count}",
                flush=True,
            )

        print(
            f"=== BATCH {batch_id} COMPLETE ===\n",
            flush=True,
        )

    finally:
        if client is not None:
            client.close()


# --------------------------------------------------
# 9. Start streaming query
# --------------------------------------------------

query = (
    station_stream.writeStream
    .foreachBatch(write_to_mongodb)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

print("Citi Bike Real-Time Streaming v2 started.", flush=True)
print(f"Input: {INPUT_PATH}", flush=True)
print(f"Checkpoint: {CHECKPOINT_PATH}", flush=True)

query.awaitTermination()
