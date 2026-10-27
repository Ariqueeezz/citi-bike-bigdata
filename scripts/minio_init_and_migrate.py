#!/usr/bin/env python3
"""
[STATUS: MIGRASI SELESAI / COMPLETED]
Script ini digunakan sebagai utilitas migrasi satu kali (one-off migration)
untuk memindahkan data mentah (raw tripdata) dan data kurasi (curated parquet)
dari klaster HDFS ke MinIO S3 Object Storage (citibike-raw & citibike-curated).

Karena proses migrasi data telah selesai dan diverifikasi, service normal
MinIO pada docker-compose.yml kini telah dikembalikan ke fungsi inisialisasi bucket standar.
Script ini tetap dipertahankan sebagai referensi / backup utilitas migrasi manual.
"""

import logging
import os
import re
import sys
import time
from pathlib import Path

import requests
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER") or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("MINIO_SECRET_KEY", "minioadmin")

BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "citibike-raw")
BUCKET_CURATED = os.getenv("MINIO_BUCKET_CURATED", "citibike-curated")
BUCKET_REALTIME = os.getenv("MINIO_BUCKET_REALTIME", "citibike-realtime")
BUCKET_ARCHIVE = os.getenv("MINIO_BUCKET_ARCHIVE", "citibike-archive")

ALL_BUCKETS = [BUCKET_RAW, BUCKET_CURATED, BUCKET_REALTIME, BUCKET_ARCHIVE]

WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://namenode:9870").rstrip("/")
LOCAL_RAW_DIR = Path(os.getenv("LOCAL_RAW_DIR", "/app/data/raw"))
LOCAL_PROCESSED_DIR = Path(os.getenv("LOCAL_PROCESSED_DIR", "/app/data/processed"))
LOCAL_CURATED_DIR = Path(os.getenv("LOCAL_CURATED_DIR", "/app/data/curated"))


def parse_endpoint(raw_endpoint: str):
    """Membersihkan skema protokol dari endpoint MinIO untuk SDK MinIO."""
    endpoint = raw_endpoint.strip()
    secure = False
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint[len("https://"):]
    elif endpoint.startswith("http://"):
        secure = False
        endpoint = endpoint[len("http://"):]
    return endpoint.rstrip("/"), secure


def get_content_type(filename: str) -> str:
    """Menentukan content type yang sesuai berdasarkan ekstensi file."""
    if filename.endswith(".csv"):
        return "text/csv"
    elif filename.endswith(".parquet") or ".snappy.parquet" in filename:
        return "application/vnd.apache.parquet"
    elif filename.endswith(".json"):
        return "application/json"
    elif filename == "_SUCCESS" or filename.endswith(".txt"):
        return "text/plain"
    return "application/octet-stream"


def get_minio_client(max_retries: int = 30, delay: int = 2) -> Minio:
    """Menghubungkan ke MinIO dengan mekanisme retry otomatis."""
    endpoint, secure = parse_endpoint(MINIO_ENDPOINT_RAW)
    logging.info("Menghubungkan ke MinIO di %s (secure=%s)...", endpoint, secure)

    for attempt in range(1, max_retries + 1):
        try:
            client = Minio(
                endpoint,
                access_key=MINIO_ROOT_USER,
                secret_key=MINIO_ROOT_PASSWORD,
                secure=secure,
            )
            # Cek konektivitas dengan list_buckets
            client.list_buckets()
            logging.info("Berhasil terhubung ke MinIO pada percobaan ke-%d.", attempt)
            return client
        except Exception as exc:
            logging.warning(
                "Percobaan %d/%d gagal terhubung ke MinIO (%s). Menunggu %ds...",
                attempt,
                max_retries,
                exc,
                delay,
            )
            time.sleep(delay)

    logging.error("Gagal terhubung ke MinIO setelah %d percobaan.", max_retries)
    sys.exit(1)


def ensure_buckets(client: Minio):
    """Memastikan semua bucket utama telah dibuat."""
    logging.info("Memeriksa status bucket MinIO...")
    for b in ALL_BUCKETS:
        try:
            if not client.bucket_exists(b):
                client.make_bucket(b)
                logging.info("[MinIO Init] Bucket '%s' berhasil dibuat.", b)
            else:
                logging.info("[MinIO Init] Bucket '%s' sudah tersedia.", b)
        except Exception as exc:
            logging.error("[MinIO Init] Gagal memeriksa/membuat bucket '%s': %s", b, exc)


def is_object_present_and_complete(client: Minio, bucket: str, object_name: str, expected_size: int) -> bool:
    """Memeriksa apakah objek sudah ada di MinIO dengan ukuran yang sesuai."""
    try:
        stat = client.stat_object(bucket, object_name)
        if stat.size == expected_size:
            return True
        logging.warning(
            "Objek '%s/%s' ada tetapi ukurannya berbeda (MinIO: %d bytes, Sumber: %d bytes). Mengunggah ulang...",
            bucket,
            object_name,
            stat.size,
            expected_size,
        )
        return False
    except S3Error as err:
        if err.code in ("NoSuchKey", "NotFound"):
            return False
        logging.warning("Error saat memeriksa objek '%s': %s", object_name, err)
        return False
    except Exception:
        return False


def migrate_hdfs_directory(
    client: Minio,
    hdfs_dir: str,
    target_bucket: str,
    prefix_in_bucket: str = "",
    strip_root_from_path: str = "",
) -> int:
    """
    Fungsi rekursif umum untuk memindai direktori WebHDFS dan mengalirkan
    file secara streaming ke bucket MinIO tujuan.
    """
    list_url = f"{WEBHDFS_URL}/webhdfs/v1{hdfs_dir}?op=LISTSTATUS"
    logging.info("Memeriksa WebHDFS [%s]: %s", target_bucket, list_url)

    try:
        resp = requests.get(list_url, timeout=10)
        if resp.status_code != 200:
            logging.warning("WebHDFS %s merespons status code %d", list_url, resp.status_code)
            return 0
        data = resp.json()
    except Exception as exc:
        logging.warning("Tidak dapat mengakses WebHDFS di %s (%s).", list_url, exc)
        return 0

    file_statuses = data.get("FileStatuses", {}).get("FileStatus", [])
    if not file_statuses:
        logging.info("Direktori HDFS %s kosong atau tidak memiliki file.", hdfs_dir)
        return 0

    migrated_count = 0

    for item in file_statuses:
        name = item.get("pathSuffix")
        item_type = item.get("type")
        size = item.get("length", 0)

        if not name:
            continue

        item_hdfs_path = f"{hdfs_dir.rstrip('/')}/{name}"

        if item_type == "DIRECTORY":
            sub_prefix = f"{prefix_in_bucket.rstrip('/')}/{name}".lstrip("/")
            migrated_count += migrate_hdfs_directory(
                client=client,
                hdfs_dir=item_hdfs_path,
                target_bucket=target_bucket,
                prefix_in_bucket=sub_prefix,
                strip_root_from_path=strip_root_from_path,
            )
            continue

        if item_type == "FILE":
            target_obj_name = f"{prefix_in_bucket.rstrip('/')}/{name}".lstrip("/") if prefix_in_bucket else name

            # Siapkan variasi path tujuan (misal standar tanpa prefix root dan dengan prefix root)
            target_keys = [target_obj_name]
            if strip_root_from_path and item_hdfs_path.startswith(strip_root_from_path):
                relative_path = item_hdfs_path.replace(strip_root_from_path, "").lstrip("/")
                if relative_path not in target_keys:
                    target_keys.append(relative_path)

            for target_key in target_keys:
                if is_object_present_and_complete(client, target_bucket, target_key, size):
                    logging.info(
                        "[Skip] '%s/%s' (%s MB) sudah ada di MinIO.",
                        target_bucket,
                        target_key,
                        round(size / (1024 * 1024), 2),
                    )
                    continue

                open_url = f"{WEBHDFS_URL}/webhdfs/v1{item_hdfs_path}?op=OPEN"
                logging.info(
                    "[Migrasi] Memulai streaming '%s' (%s MB) -> s3://%s/%s ...",
                    item_hdfs_path,
                    round(size / (1024 * 1024), 2),
                    target_bucket,
                    target_key,
                )

                try:
                    with requests.get(open_url, stream=True, allow_redirects=True, timeout=600) as stream_resp:
                        stream_resp.raise_for_status()
                        content_type = get_content_type(target_key)
                        client.put_object(
                            bucket_name=target_bucket,
                            object_name=target_key,
                            data=stream_resp.raw,
                            length=size,
                            content_type=content_type,
                        )
                    logging.info(
                        "[Sukses] Berhasil migrasi '%s' -> s3://%s/%s",
                        item_hdfs_path,
                        target_bucket,
                        target_key,
                    )
                    migrated_count += 1
                except Exception as exc:
                    logging.error(
                        "[Gagal] Gagal migrasi '%s' ke s3://%s/%s: %s",
                        item_hdfs_path,
                        target_bucket,
                        target_key,
                        exc,
                    )

    return migrated_count


def migrate_hdfs_raw_path(client: Minio, hdfs_dir: str = "/raw", prefix_in_bucket: str = "") -> int:
    """Migrasi data mentah dari HDFS (/raw) ke bucket 'citibike-raw'."""
    return migrate_hdfs_directory(
        client=client,
        hdfs_dir=hdfs_dir,
        target_bucket=BUCKET_RAW,
        prefix_in_bucket=prefix_in_bucket,
        strip_root_from_path="/raw/",
    )


def migrate_hdfs_curated_path(client: Minio, hdfs_dir: str = "/curated", prefix_in_bucket: str = "") -> int:
    """
    Migrasi data kurasi (Parquet & metadata) dari HDFS (/curated) ke bucket 'citibike-curated'.
    Mendukung path seperti:
      - /curated/2026/01/trips/*.parquet
      - /curated/realtime/gbfs/...
    """
    return migrate_hdfs_directory(
        client=client,
        hdfs_dir=hdfs_dir,
        target_bucket=BUCKET_CURATED,
        prefix_in_bucket=prefix_in_bucket,
        strip_root_from_path="/curated/",
    )


def migrate_local_raw_files(client: Minio) -> int:
    """Migrasi data CSV mentah lokal jika ada file di folder data/raw."""
    if not LOCAL_RAW_DIR.exists():
        return 0

    csv_files = list(LOCAL_RAW_DIR.glob("*.csv"))
    if not csv_files:
        return 0

    logging.info("Ditemukan %d file CSV di folder lokal %s", len(csv_files), LOCAL_RAW_DIR)
    migrated = 0

    for local_file in csv_files:
        size = local_file.stat().st_size
        filename = local_file.name

        match = re.search(r"(\d{4})(\d{2})", filename)
        if match:
            year, month = match.groups()
            target_key = f"{year}/{month}/{filename}"
        else:
            target_key = filename

        if is_object_present_and_complete(client, BUCKET_RAW, target_key, size):
            logging.info("[Skip Lokal Raw] '%s/%s' sudah ada di MinIO.", BUCKET_RAW, target_key)
            continue

        logging.info(
            "[Upload Lokal Raw] Mengunggah %s (%s MB) -> s3://%s/%s ...",
            local_file,
            round(size / (1024 * 1024), 2),
            BUCKET_RAW,
            target_key,
        )
        try:
            client.fput_object(
                bucket_name=BUCKET_RAW,
                object_name=target_key,
                file_path=str(local_file),
                content_type=get_content_type(filename),
            )
            logging.info("[Sukses Lokal Raw] %s berhasil diunggah.", target_key)
            migrated += 1
        except Exception as exc:
            logging.error("[Gagal Lokal Raw] Gagal mengunggah %s: %s", local_file, exc)

    return migrated


def migrate_local_curated_files(client: Minio) -> int:
    """
    Migrasi data curated lokal jika ada file di data/processed atau data/curated.
    Memindai secara rekursif seluruh file Parquet, JSON, atau CSV hasil olahan.
    """
    search_dirs = [LOCAL_PROCESSED_DIR, LOCAL_CURATED_DIR]
    migrated = 0

    for base_dir in search_dirs:
        if not base_dir.exists():
            continue

        local_files = [
            f for f in base_dir.rglob("*")
            if f.is_file() and not f.name.startswith(".")
        ]

        if not local_files:
            continue

        logging.info("Ditemukan %d file curated di direktori lokal %s", len(local_files), base_dir)

        for local_file in local_files:
            size = local_file.stat().st_size
            rel_path = local_file.relative_to(base_dir).as_posix()

            target_keys = [rel_path]
            if not rel_path.startswith("curated/"):
                target_keys.append(f"curated/{rel_path}")

            for target_key in target_keys:
                if is_object_present_and_complete(client, BUCKET_CURATED, target_key, size):
                    logging.info("[Skip Lokal Curated] '%s/%s' sudah ada di MinIO.", BUCKET_CURATED, target_key)
                    continue

                logging.info(
                    "[Upload Lokal Curated] Mengunggah %s (%s MB) -> s3://%s/%s ...",
                    local_file,
                    round(size / (1024 * 1024), 2),
                    BUCKET_CURATED,
                    target_key,
                )
                try:
                    client.fput_object(
                        bucket_name=BUCKET_CURATED,
                        object_name=target_key,
                        file_path=str(local_file),
                        content_type=get_content_type(local_file.name),
                    )
                    logging.info("[Sukses Lokal Curated] %s berhasil diunggah.", target_key)
                    migrated += 1
                except Exception as exc:
                    logging.error("[Gagal Lokal Curated] Gagal mengunggah %s: %s", local_file, exc)

    return migrated


def print_bucket_inventory(client: Minio):
    """Menampilkan ringkasan isi bucket MinIO setelah inisialisasi/migrasi."""
    logging.info("=== RINGKASAN INVENTARIS BUCKET MINIO ===")
    for b in ALL_BUCKETS:
        try:
            objects = list(client.list_objects(b, recursive=True))
            total_size = sum(obj.size for obj in objects)
            logging.info(
                "Bucket '%s': %d objek (Total: %s MB)",
                b,
                len(objects),
                round(total_size / (1024 * 1024), 2),
            )
            for obj in objects[:5]:
                logging.info("  - %s (%s MB)", obj.object_name, round(obj.size / (1024 * 1024), 2))
            if len(objects) > 5:
                logging.info("  - ... dan %d objek lainnya.", len(objects) - 5)
        except Exception as exc:
            logging.warning("Gagal membaca isi bucket '%s': %s", b, exc)
    logging.info("=========================================")


def main():
    logging.info("=== Citi Bike MinIO Inisialisasi & Migrasi Dimulai ===")
    client = get_minio_client()

    # 1. Pastikan semua bucket terbuat
    ensure_buckets(client)

    # 2. Migrasi data mentah dari HDFS (/raw) ke citibike-raw
    logging.info("Memulai pengecekan migrasi HDFS Raw -> MinIO bucket '%s'...", BUCKET_RAW)
    hdfs_raw_migrated = migrate_hdfs_raw_path(client, hdfs_dir="/raw", prefix_in_bucket="")
    logging.info("Migrasi HDFS Raw selesai: %d file baru dimigrasikan.", hdfs_raw_migrated)

    # 3. Migrasi data mentah lokal jika ada
    local_raw_migrated = migrate_local_raw_files(client)
    logging.info("Migrasi Raw lokal selesai: %d file baru diunggah.", local_raw_migrated)

    # 4. Migrasi data kurasi (curated) dari HDFS (/curated) ke citibike-curated
    logging.info("Memulai pengecekan migrasi HDFS Curated -> MinIO bucket '%s'...", BUCKET_CURATED)
    hdfs_curated_migrated = migrate_hdfs_curated_path(client, hdfs_dir="/curated", prefix_in_bucket="")
    logging.info("Migrasi HDFS Curated selesai: %d file baru dimigrasikan.", hdfs_curated_migrated)

    # 5. Migrasi data kurasi lokal jika ada
    local_curated_migrated = migrate_local_curated_files(client)
    logging.info("Migrasi Curated lokal selesai: %d file baru diunggah.", local_curated_migrated)

    # 6. Ringkasan inventaris seluruh bucket
    print_bucket_inventory(client)
    logging.info("=== Proses MinIO Inisialisasi & Migrasi Berhasil Selesai ===")


if __name__ == "__main__":
    main()
