import io
import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from minio import Minio

URL = (
    "https://gbfs.lyft.com/gbfs/2.3/bkn/en/"
    "station_status.json"
)

OUTPUT_DIR = "/app/data/realtime"
INTERVAL_SECONDS = int(os.getenv("COLLECTOR_INTERVAL_SECONDS", "60"))

# Konfigurasi Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "citibike-kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "citibike-station-status")

# Konfigurasi MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
# Failsafe: Jika berjalan di dalam kontainer Docker namun endpoint berisi localhost, arahkan ke internal DNS minio
if ("localhost" in MINIO_ENDPOINT or "127.0.0.1" in MINIO_ENDPOINT) and (os.path.exists("/.dockerenv") or os.getenv("IS_CONTAINER", "").lower() == "true" or sys.platform != "win32"):
    MINIO_ENDPOINT = "minio:9000"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_REALTIME = os.getenv("MINIO_BUCKET_REALTIME", "citibike-realtime")

minio_client = None
kafka_producer = None


def get_kafka_producer():
    """Menginisialisasi koneksi Kafka Producer dengan retry."""
    global kafka_producer
    if kafka_producer is not None:
        return kafka_producer

    for attempt in range(1, 15):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_request_size=10485760,
                compression_type="gzip",
                request_timeout_ms=10000,
            )
            kafka_producer = producer
            print(f"[Kafka] ✅ Berhasil terhubung ke Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS} (Topic: {KAFKA_TOPIC})")
            return kafka_producer
        except Exception as exc:
            print(f"[Kafka Warning] Menunggu Kafka Broker siap (Percobaan {attempt}/15): {exc}")
            time.sleep(3)

    print("[Kafka Error] Tidak dapat terhubung ke Kafka Broker setelah 15 percobaan.")
    return None


def get_minio_client():
    global minio_client
    if minio_client is not None:
        return minio_client

    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        if not client.bucket_exists(MINIO_BUCKET_REALTIME):
            client.make_bucket(MINIO_BUCKET_REALTIME)
            print(f"[MinIO] Bucket '{MINIO_BUCKET_REALTIME}' berhasil dibuat.")

        minio_client = client
        print(f"[MinIO] ✅ Terhubung ke MinIO di {MINIO_ENDPOINT}")
        return minio_client
    except Exception as exc:
        print(f"[MinIO Warning] Belum dapat terhubung ke MinIO ({exc}).")
        return None


def send_to_kafka(payload, timestamp):
    """Mengirim event telemetri stasiun ke Apache Kafka topic."""
    producer = get_kafka_producer()
    if producer is None:
        print("[Kafka Warning] Dilewati karena producer belum terhubung.")
        return

    try:
        station_count = len(payload.get("data", {}).get("stations", []))
        future = producer.send(
            KAFKA_TOPIC,
            key=timestamp,
            value=payload
        )
        record_metadata = future.get(timeout=10)
        print(
            f"[{timestamp}] [Kafka] 🚀 Berhasil dikirim ke Topic '{record_metadata.topic}' "
            f"[Partition {record_metadata.partition}, Offset {record_metadata.offset}] ({station_count} stasiun)"
        )
    except Exception as e:
        print(f"[Kafka Error] Gagal mengirim data ke Kafka: {e}")


def upload_to_minio(payload, timestamp):
    """Mengunggah data snapshot telemetri terbaru ke buffer MinIO citibike-realtime."""
    client = get_minio_client()
    if client is None:
        return

    try:
        raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        data_len = len(raw_bytes)

        realtime_filename = f"station_status_{timestamp}.json"
        client.put_object(
            bucket_name=MINIO_BUCKET_REALTIME,
            object_name=realtime_filename,
            data=io.BytesIO(raw_bytes),
            length=data_len,
            content_type="application/json"
        )

        print(f"[{timestamp}] [MinIO] 📦 Buffer Ingested -> s3://{MINIO_BUCKET_REALTIME}/{realtime_filename}")

    except Exception as e:
        print(f"[MinIO Error] Gagal upload ke MinIO realtime buffer: {e}")


def collect():
    response = requests.get(
        URL,
        timeout=30
    )
    response.raise_for_status()
    payload = response.json()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    station_count = len(payload.get("data", {}).get("stations", []))
    print(f"\n[{timestamp}] 📡 Collected {station_count} stations from GBFS API")

    # 1. Kirim ke Kafka Broker (Event-Driven Stream Buffer untuk Spark Streaming)
    send_to_kafka(payload, timestamp)

    # 2. Simpan ke MinIO S3 Real-Time Buffer
    upload_to_minio(payload, timestamp)

    # 3. Simpan ke Staging Disk Lokal (Sebagai sumber data untuk pengarsipan batch & quality check)
    try:
        output_file = os.path.join(OUTPUT_DIR, f"station_status_{timestamp}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        pass


os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"==================================================")
print(f"Citi Bike Real-Time Collector Starting (Interval: {INTERVAL_SECONDS}s)")
print(f"Kafka Broker  : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Kafka Topic   : {KAFKA_TOPIC}")
print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
print(f"==================================================")

# Inisialisasi awal
get_kafka_producer()
get_minio_client()

while True:
    try:
        collect()
    except Exception as e:
        print(f"Collector error: {e}")

    time.sleep(INTERVAL_SECONDS)
