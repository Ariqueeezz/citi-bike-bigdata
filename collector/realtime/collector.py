import io
import json
import os
import time
from datetime import datetime, timezone

import requests
from minio import Minio
from minio.error import S3Error

URL = (
    "https://gbfs.lyft.com/gbfs/2.3/bkn/en/"
    "station_status.json"
)

OUTPUT_DIR = "/app/data/realtime"
INTERVAL_SECONDS = int(os.getenv("COLLECTOR_INTERVAL_SECONDS", "60"))

# Konfigurasi MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_REALTIME = os.getenv("MINIO_BUCKET_REALTIME", "citibike-realtime")
MINIO_BUCKET_ARCHIVE = os.getenv("MINIO_BUCKET_ARCHIVE", "citibike-archive")

minio_client = None


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

        # Pastikan bucket penampung realtime dan arsip sudah ada
        for b in [MINIO_BUCKET_REALTIME, MINIO_BUCKET_ARCHIVE]:
            if not client.bucket_exists(b):
                client.make_bucket(b)
                print(f"[MinIO] Bucket '{b}' berhasil dibuat.")

        minio_client = client
        print(f"[MinIO] Terhubung ke MinIO di {MINIO_ENDPOINT}")
        return minio_client
    except Exception as exc:
        print(f"[MinIO Warning] Belum dapat terhubung ke MinIO ({exc}). Data tetap disimpan di disk lokal.")
        return None


def upload_to_minio(payload, timestamp):
    client = get_minio_client()
    if client is None:
        return

    try:
        raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        data_len = len(raw_bytes)

        # 1. Simpan ke bucket khusus REALTIME (stream buffer)
        # File bertanda waktu dan pointer latest
        realtime_filename = f"station_status_{timestamp}.json"
        client.put_object(
            bucket_name=MINIO_BUCKET_REALTIME,
            object_name=realtime_filename,
            data=io.BytesIO(raw_bytes),
            length=data_len,
            content_type="application/json"
        )
        client.put_object(
            bucket_name=MINIO_BUCKET_REALTIME,
            object_name="latest_station_status.json",
            data=io.BytesIO(raw_bytes),
            length=data_len,
            content_type="application/json"
        )

        # 2. Simpan juga ke bucket ARCHIVE dengan partisi tanggal
        date_prefix = datetime.now(timezone.utc).strftime("year=%Y/month=%m/day=%d")
        archive_path = f"{date_prefix}/station_status_{timestamp}.json"
        client.put_object(
            bucket_name=MINIO_BUCKET_ARCHIVE,
            object_name=archive_path,
            data=io.BytesIO(raw_bytes),
            length=data_len,
            content_type="application/json"
        )

        print(f"[{timestamp}] [MinIO] Ingested -> s3://{MINIO_BUCKET_REALTIME}/{realtime_filename} & s3://{MINIO_BUCKET_ARCHIVE}/{archive_path}")

    except Exception as e:
        print(f"[MinIO Error] Gagal upload ke MinIO: {e}")


def collect():
    response = requests.get(
        URL,
        timeout=30
    )
    response.raise_for_status()
    payload = response.json()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # 1. Simpan ke Disk Lokal (Backup / Local Stream)
    output_file = os.path.join(
        OUTPUT_DIR,
        f"station_status_{timestamp}.json"
    )
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    station_count = len(payload.get("data", {}).get("stations", []))
    print(f"[{timestamp}] Collected {station_count} stations from GBFS API")

    # 2. Kirim ke MinIO Bucket 'citibike-realtime' & 'citibike-archive'
    upload_to_minio(payload, timestamp)


os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Citi Bike Real-Time Collector started (Interval: {INTERVAL_SECONDS}s)")

# Coba inisialisasi koneksi MinIO di awal
get_minio_client()

while True:
    try:
        collect()
    except Exception as e:
        print(f"Collector error: {e}")

    time.sleep(INTERVAL_SECONDS)
