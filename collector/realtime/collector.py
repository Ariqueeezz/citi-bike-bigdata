import io
import json
import os
import time
from datetime import datetime, timezone

import requests
from minio import Minio
from minio.error import S3Error

BASE_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en"

FEEDS = {
    "station_status": f"{BASE_URL}/station_status.json",
    "station_information": f"{BASE_URL}/station_information.json",
}

OUTPUT_DIR = "/app/data/realtime"
INTERVAL_SECONDS = int(os.getenv("COLLECTOR_INTERVAL_SECONDS", "60"))

STATUS_INTERVAL_SECONDS = 60
INFORMATION_REFRESH_SECONDS = 6 * 60 * 60

# Timeout untuk request API
REQUEST_TIMEOUT = 30


def utc_timestamp():
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def fetch_json(url):
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def save_snapshot(feed_name, payload):
    timestamp = utc_timestamp()

    # Status baru masuk ke inbox khusus streaming.
    if feed_name == "station_status":
        output_dir = os.path.join(OUTPUT_DIR, "streaming_status")
    else:
        # Metadata tetap di folder utama.
        output_dir = OUTPUT_DIR

    os.makedirs(output_dir, exist_ok=True)

    filename = f"{feed_name}_{timestamp}.json"
    output_file = os.path.join(output_dir, filename)
    temp_file = f"{output_file}.tmp"

    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    os.replace(temp_file, output_file)
    return output_file, timestamp


def collect_station_information():
    payload = fetch_json(FEEDS["station_information"])

    stations = payload.get("data", {}).get("stations", [])
    output_file, timestamp = save_snapshot(
        "station_information", payload
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

    print(
        f"[{timestamp}] Station information collected: "
        f"{len(stations)} stations | {output_file}",
        flush=True,
    )


def collect_station_status():
    payload = fetch_json(FEEDS["station_status"])

    stations = payload.get("data", {}).get("stations", [])
    output_file, timestamp = save_snapshot(
        "station_status", payload
    )

    print(
        f"[{timestamp}] Station status collected: "
        f"{len(stations)} stations | {output_file}",
        flush=True,
    )


os.makedirs(OUTPUT_DIR, exist_ok=True)


# Ambil informasi stasiun ketika collector dimulai.
next_information_refresh = 0

while True:
    cycle_start = time.monotonic()

    if cycle_start >= next_information_refresh:
        try:
            collect_station_information()
            next_information_refresh = (
                time.monotonic() + INFORMATION_REFRESH_SECONDS
            )
        except Exception as e:
            print(
                f"Station information error: {e}",
                flush=True,
            )
            # Coba lagi pada siklus berikutnya.

    try:
        collect_station_status()
    except Exception as e:
        print(f"Station status error: {e}", flush=True)

    elapsed = time.monotonic() - cycle_start
    time.sleep(max(0, STATUS_INTERVAL_SECONDS - elapsed))
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
