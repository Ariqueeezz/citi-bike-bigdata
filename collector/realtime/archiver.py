
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

from hdfs import InsecureClient
from requests.exceptions import RequestException

DATA_DIR = Path("/app/data/realtime")
HDFS_ROOT = "/raw/realtime/gbfs"
WEBHDFS_URL = "http://namenode:9870"
HDFS_USER = "root"

POLL_INTERVAL_SECONDS = 30
FILE_STABILITY_SECONDS = 10
LEGACY_CUTOFF = "20261023"

SNAPSHOT_PATTERN = re.compile(
    r"^(station_status|station_information)_(\d{8})_(\d{6})\.json$"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

client = InsecureClient(WEBHDFS_URL, user=HDFS_USER)


def destination_for(filename):
    match = SNAPSHOT_PATTERN.match(filename)

    if not match:
        return None

    feed_name, date_part, _ = match.groups()

    if date_part < LEGACY_CUTOFF:
        return f"{HDFS_ROOT}/legacy/{feed_name}/{filename}"

    date_path = datetime.strptime(date_part, "%Y%m%d").strftime("%Y/%m/%d")
    return f"{HDFS_ROOT}/{date_path}/{feed_name}/{filename}"


def archive_file(source):
    destination = destination_for(source.name)

    if destination is None:
        return

    try:
        stat = source.stat()
        age_seconds = time.time() - stat.st_mtime

        if age_seconds < FILE_STABILITY_SECONDS:
            return

        # Cek apakah file sudah ada di HDFS.
        if client.status(destination, strict=False) is not None:
            logging.info("Sudah diarsipkan, dilewati: %s", source.name)
            return

        parent = destination.rsplit("/", 1)[0]
        temporary = f"{destination}.uploading"

        client.makedirs(parent)

        # Upload ke nama sementara terlebih dahulu.
        client.upload(
            temporary,
            str(source),
            overwrite=True,
        )

        # Periksa lagi agar tidak menimpa file final jika sudah ada.
        if client.status(destination, strict=False) is not None:
            client.delete(temporary, recursive=False)
            logging.info("File final sudah ada, dilewati: %s", source.name)
            return

        client.rename(temporary, destination)

        logging.info(
            "Berhasil diarsipkan: %s -> %s",
            source.name,
            destination,
        )

    except Exception:
        logging.exception("Gagal mengarsipkan %s", source.name)


def scan_snapshots():
    if not DATA_DIR.exists():
        logging.warning("Direktori snapshot belum tersedia: %s", DATA_DIR)
        return

    for source in sorted(DATA_DIR.glob("*.json")):
        try:
            archive_file(source)
        except FileNotFoundError:
            continue
        except OSError:
            logging.exception("Gagal memeriksa file: %s", source.name)


def main():
    logging.info("Archiver dimulai.")
    logging.info("Sumber lokal: %s", DATA_DIR)
    logging.info("Tujuan HDFS: %s", HDFS_ROOT)
    logging.info("WebHDFS: %s", WEBHDFS_URL)

    while True:
        try:
            scan_snapshots()
        except Exception:
            logging.exception("Kesalahan saat memindai snapshot.")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
