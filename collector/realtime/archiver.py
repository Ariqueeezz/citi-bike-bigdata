import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data/realtime"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
FILE_STABILITY_SECONDS = int(os.getenv("FILE_STABILITY_SECONDS", "10"))

# Konfigurasi MinIO Object Storage
MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = (
    os.getenv("MINIO_ACCESS_KEY")
    or os.getenv("MINIO_ROOT_USER")
    or "minioadmin"
)
MINIO_SECRET_KEY = (
    os.getenv("MINIO_SECRET_KEY")
    or os.getenv("MINIO_ROOT_PASSWORD")
    or "minioadmin"
)
MINIO_BUCKET_REALTIME = os.getenv("MINIO_BUCKET_REALTIME", "citibike-realtime")
MINIO_BUCKET_ARCHIVE = os.getenv("MINIO_BUCKET_ARCHIVE", "citibike-archive")

SNAPSHOT_PATTERN = re.compile(
    r"^(station_status|station_information)_(\d{8})_(\d{6})\.json$"
)

minio_client = None


def parse_minio_endpoint(raw_endpoint: str):
    """Membersihkan skema protokol untuk SDK MinIO."""
    endpoint = raw_endpoint.strip()
    secure = False
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint[len("https://"):]
    elif endpoint.startswith("http://"):
        secure = False
        endpoint = endpoint[len("http://"):]
    return endpoint.rstrip("/"), secure


def get_minio_client():
    global minio_client
    if minio_client is not None:
        return minio_client

    endpoint, secure = parse_minio_endpoint(MINIO_ENDPOINT_RAW)
    try:
        client = Minio(
            endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=secure,
        )
        for b in [MINIO_BUCKET_REALTIME, MINIO_BUCKET_ARCHIVE]:
            if not client.bucket_exists(b):
                client.make_bucket(b)
                logging.info("[MinIO Archiver] Bucket '%s' berhasil dibuat.", b)

        minio_client = client
        logging.info("[MinIO Archiver] Terhubung ke MinIO di %s (secure=%s)", endpoint, secure)
        return minio_client
    except Exception as exc:
        logging.warning("[MinIO Archiver] Belum dapat terhubung ke MinIO (%s)", exc)
        return None


def get_archive_path(filename: str) -> str:
    """
    Menghasilkan path objek MinIO berpartisi tanggal:
    year=YYYY/month=MM/day=DD/{feed_name}/{filename}
    """
    match = SNAPSHOT_PATTERN.match(filename)
    if not match:
        return None

    feed_name, date_part, _ = match.groups()
    try:
        dt = datetime.strptime(date_part, "%Y%m%d")
        return f"year={dt.strftime('%Y')}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/{feed_name}/{filename}"
    except ValueError:
        return f"unparsed/{feed_name}/{filename}"


def sync_realtime_bucket_to_archive(client: Minio) -> int:
    """
    MEMINDAI BUCKET REALTIME -> ARCHIVE:
    Memindai seluruh snapshot yang sudah tersimpan di bucket 'citibike-realtime'
    dan menyalinnya (server-side copy) ke bucket 'citibike-archive' dengan partisi
    tanggal yang rapi.
    """
    try:
        if not client.bucket_exists(MINIO_BUCKET_REALTIME):
            logging.warning("[Sync Realtime->Archive] Bucket sumber '%s' belum ada.", MINIO_BUCKET_REALTIME)
            return 0

        objects = list(client.list_objects(MINIO_BUCKET_REALTIME, recursive=True))
        if not objects:
            return 0

        synced_count = 0
        for obj in objects:
            obj_name = obj.object_name

            # Lewati pointer latest (hanya salin snapshot berstempel waktu)
            if obj_name.startswith("latest_"):
                continue

            filename = obj_name.rsplit("/", 1)[-1]
            archive_path = get_archive_path(filename)
            if archive_path is None:
                continue

            # Cek apakah sudah ada di bucket archive dengan ukuran sama
            try:
                stat = client.stat_object(MINIO_BUCKET_ARCHIVE, archive_path)
                if stat.size == obj.size:
                    continue
            except S3Error as err:
                if err.code not in ("NoSuchKey", "NotFound"):
                    logging.warning("[Sync Realtime->Archive Stat Error] %s: %s", archive_path, err)

            # Lakukan server-side copy instan tanpa unduh ulang
            logging.info(
                "[Sync Realtime->Archive] Menyalin s3://%s/%s -> s3://%s/%s (%s KB)...",
                MINIO_BUCKET_REALTIME,
                obj_name,
                MINIO_BUCKET_ARCHIVE,
                archive_path,
                round(obj.size / 1024, 1),
            )
            client.copy_object(
                bucket_name=MINIO_BUCKET_ARCHIVE,
                object_name=archive_path,
                source=CopySource(MINIO_BUCKET_REALTIME, obj_name),
            )
            synced_count += 1

        if synced_count > 0:
            logging.info(
                "[Sync Realtime->Archive] Selesai: %d snapshot dari '%s' berhasil diarsipkan ke '%s'.",
                synced_count,
                MINIO_BUCKET_REALTIME,
                MINIO_BUCKET_ARCHIVE,
            )
        return synced_count

    except Exception as exc:
        logging.error(
            "[Sync Realtime->Archive Error] Gagal memindai/menyinkronkan bucket '%s': %s",
            MINIO_BUCKET_REALTIME,
            exc,
        )
        return 0


def archive_to_minio(client: Minio, source: Path, minio_path: str, size: int) -> bool:
    """Mengunggah file dari disk lokal ke bucket citibike-archive."""
    try:
        try:
            stat = client.stat_object(MINIO_BUCKET_ARCHIVE, minio_path)
            if stat.size == size:
                return True
        except S3Error as err:
            if err.code not in ("NoSuchKey", "NotFound"):
                logging.warning("[MinIO Archiver] Stat error %s: %s", minio_path, err)

        client.fput_object(
            bucket_name=MINIO_BUCKET_ARCHIVE,
            object_name=minio_path,
            file_path=str(source),
            content_type="application/json",
        )
        logging.info("[MinIO Disk Archived] %s -> s3://%s/%s", source.name, MINIO_BUCKET_ARCHIVE, minio_path)
        return True
    except Exception as exc:
        logging.error("[MinIO Archiver Error] Gagal mengarsipkan %s ke MinIO: %s", source.name, exc)
        return False


def archive_file(source: Path):
    minio_path = get_archive_path(source.name)
    if minio_path is None:
        return

    try:
        stat = source.stat()
        age_seconds = time.time() - stat.st_mtime

        if age_seconds < FILE_STABILITY_SECONDS:
            return

        client = get_minio_client()
        if client is not None:
            archive_to_minio(client, source, minio_path, stat.st_size)

    except Exception as exc:
        logging.exception("Gagal memproses file arsip %s: %s", source.name, exc)


def scan_snapshots():
    """Memindai direktori disk lokal /app/data/realtime."""
    if not DATA_DIR.exists():
        logging.warning("Direktori snapshot belum tersedia: %s", DATA_DIR)
        return

    for source in sorted(DATA_DIR.glob("*.json")):
        try:
            archive_file(source)
        except FileNotFoundError:
            continue
        except OSError as exc:
            logging.exception("Gagal memeriksa file: %s (%s)", source.name, exc)


def main():
    logging.info("Citi Bike Real-Time Archiver dimulai.")
    logging.info("  - Direktori Disk Lokal: %s", DATA_DIR)
    logging.info("  - Bucket Realtime Sumber: s3://%s", MINIO_BUCKET_REALTIME)
    logging.info("  - Bucket Arsip Tujuan: s3://%s", MINIO_BUCKET_ARCHIVE)

    # Inisialisasi awal koneksi
    get_minio_client()

    while True:
        try:
            client = get_minio_client()
            if client is not None:
                # 1. Otomatis sinkronkan snapshot yang sudah lama/tersimpan di bucket citibike-realtime ke citibike-archive
                sync_realtime_bucket_to_archive(client)

            # 2. Pindai juga file snapshot di disk lokal jika ada
            scan_snapshots()

        except Exception as exc:
            logging.exception("Kesalahan pada putaran scanning archiver: %s", exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
