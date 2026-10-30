"""
Citi Bike MinIO Bucket & Data Lake Manager
Menangani:
1. Inisialisasi 4 Bucket:
   - citibike-realtime (penampung buffer data stream gbfs)
   - citibike-archive (arsip permanen data stream dengan partisi tanggal)
   - citibike-raw (data mentah batch csv)
   - citibike-curated (data olahan parquet & analitik)
2. Upload data raw CSV lokal ke MinIO Data Lake.
3. Pengarsipan Inkremental Stream Real-time (Periodic/On-Demand ke citibike-archive).
4. Menampilkan status dan daftar objek di MinIO.
"""

import os
import sys
import glob
import re
from datetime import datetime, timezone

# Pastikan UTF-8 di Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Muat variabel environment dari .env jika modul python-dotenv tersedia
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from minio import Minio
except ImportError:
    print("📦 Modul 'minio' belum terpasang di Python lokal.")
    print("   Mencoba menginstall otomatis via pip...")
    try:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "minio", "--retries", "1", "--timeout", "5"])
        from minio import Minio
        print("✅ Modul 'minio' berhasil dipasang otomatis.\n")
    except Exception as e:
        print(f"❌ Tidak dapat menginstall 'minio' secara otomatis: {e}")
        print("💡 Solusi alternatif:")
        print("   1. Jalankan via Docker:  .\\run_pipeline.ps1 -Docker minio-setup")
        print("   2. Install manual lokal: pip install -r requirements.txt")
        sys.exit(1)

# Konfigurasi akses MinIO (Auto-detect Host vs Docker Container)
is_in_container = os.path.exists("/.dockerenv") or os.getenv("IS_CONTAINER", "").lower() == "true"
default_minio = "minio:9000" if is_in_container else "localhost:9002"

MINIO_HOST = os.getenv("MINIO_HOST", default_minio)
if "://" in MINIO_HOST:
    MINIO_HOST = MINIO_HOST.split("://")[1]

# Failsafe: Jika dijalankan di Windows host lokal, pastikan tidak menggunakan internal hostname 'minio'
if sys.platform == "win32" and MINIO_HOST.startswith("minio"):
    MINIO_HOST = "localhost:9002"

MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

BUCKETS = [
    "citibike-realtime",
    "citibike-archive",
    "citibike-raw",
    "citibike-curated",
]


def get_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def init_buckets():
    print("=" * 65)
    print("  [MINIO SETUP] Inisialisasi Bucket Data Lake")
    print("=" * 65)

    client = get_client()
    try:
        # Cek koneksi
        client.list_buckets()
        print(f"✅ Berhasil terhubung ke MinIO di http://{MINIO_HOST}")
    except Exception as exc:
        print(f"❌ Tidak dapat terhubung ke MinIO di {MINIO_HOST}: {exc}")
        print("   Pastikan container MinIO sudah aktif (`docker compose up -d minio`).")
        return False

    for b in BUCKETS:
        if not client.bucket_exists(b):
            client.make_bucket(b)
            print(f"  ✨ Bucket baru dibuat: '{b}'")
        else:
            print(f"  ✅ Bucket sudah aktif: '{b}'")

    print("\nSeluruh 4 bucket MinIO siap digunakan! ✅\n")
    return True


def upload_raw_to_minio(local_raw_dir=None):
    """Mengunggah file CSV lokal ke bucket citibike-raw di MinIO."""
    print("=" * 65)
    print("  [MINIO INGESTION] Upload Raw CSV ke MinIO Data Lake")
    print("=" * 65)

    if local_raw_dir is None:
        local_raw_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data", "raw", "2026-01"
        )

    csv_files = glob.glob(os.path.join(local_raw_dir, "*.csv"))
    if not csv_files:
        print(f"⚠️  Tidak ada file CSV di {local_raw_dir}")
        return False

    client = get_client()
    init_buckets()

    target_bucket = "citibike-raw"
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        s3_object_name = f"2026/01/{file_name}"
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

        print(f"Mengunggah: {file_name} ({file_size_mb:.2f} MB) -> s3://{target_bucket}/{s3_object_name} ...")
        client.fput_object(
            bucket_name=target_bucket,
            object_name=s3_object_name,
            file_path=file_path
        )
        print(f"✅ Sukses: s3://{target_bucket}/{s3_object_name}")

    return True


def archive_realtime_streams(local_dir=None, days_limit=None):
    """
    Mengarsipkan file snapshot stream realtime ke bucket citibike-archive di MinIO secara terpisah.
    Proses bersifat inkremental:
    1. Mengecek objek yang sudah pernah diarsipkan ke citibike-archive.
    2. Mengidentifikasi file stream terbaru di data/realtime/ yang belum pernah diarsipkan.
    3. Mengurai tanggal dari nama file (station_status_YYYYMMDD_HHMMSS.json).
    4. Mengunggah ke MinIO dengan struktur partisi Hive: year=YYYY/month=MM/day=DD/...
    """
    print("=" * 65)
    print("  [MINIO ARCHIVE] Pengarsipan Inkremental Stream Real-Time")
    print("=" * 65)

    if local_dir is None:
        local_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data", "realtime"
        )

    pattern = os.path.join(local_dir, "station_status_*.json")
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"⚠️  Tidak ditemukan file snapshot stream di direktori: {local_dir}")
        return False

    client = get_client()
    init_buckets()
    target_bucket = "citibike-archive"

    # 1. Ambil daftar file yang sudah terarsip di bucket citibike-archive
    print("Memeriksa riwayat arsip di bucket 'citibike-archive' ...")
    try:
        existing_filenames = set()
        for obj in client.list_objects(target_bucket, recursive=True):
            existing_filenames.add(os.path.basename(obj.object_name))
        print(f"✅ Terdeteksi {len(existing_filenames):,} file yang sudah terarsip sebelumnya.")
    except Exception as exc:
        print(f"❌ Gagal membaca objek dari MinIO: {exc}")
        print("   Pastikan container MinIO sudah aktif (`docker compose up -d minio`).")
        return False

    # 2. Filter file stream yang belum terarsip
    pending_files = []
    regex = re.compile(r"station_status_(\d{4})(\d{2})(\d{2})_(\d{6})\.json")
    now_dt = datetime.now(timezone.utc)

    for f in files:
        fname = os.path.basename(f)
        if fname in existing_filenames:
            continue

        match = regex.match(fname)
        if not match:
            continue

        yyyy, mm, dd, hhmmss = match.groups()

        # Filter rentang hari jika parameter days_limit diberikan (misal: 14 hari)
        if days_limit:
            try:
                file_dt = datetime(int(yyyy), int(mm), int(dd), tzinfo=timezone.utc)
                if (now_dt - file_dt).days > days_limit:
                    continue
            except Exception:
                pass

        archive_obj_name = f"year={yyyy}/month={mm}/day={dd}/{fname}"
        pending_files.append((f, fname, archive_obj_name))

    print(f"\nRingkasan Pengecekan Stream:")
    print(f"  • Total file lokal terdeteksi : {len(files):,}")
    print(f"  • File sudah terarsip (skip)  : {len(files) - len(pending_files):,}")
    print(f"  • File baru siap diarsipkan   : {len(pending_files):,}")

    if not pending_files:
        print("\n✅ Seluruh data stream lokal sudah terarsip di MinIO! Tidak ada file baru yang perlu diunggah.\n")
        return True

    # 3. Lakukan pengunggahan inkremental ke MinIO
    print(f"\nMemulai proses pengarsipan ke s3://{target_bucket}/ ...")
    uploaded_count = 0
    total_bytes = 0

    for idx, (local_path, fname, archive_path) in enumerate(pending_files, 1):
        fsize = os.path.getsize(local_path)
        total_bytes += fsize
        fsize_kb = fsize / 1024

        client.fput_object(
            bucket_name=target_bucket,
            object_name=archive_path,
            file_path=local_path,
            content_type="application/json"
        )
        uploaded_count += 1
        if idx <= 3 or idx % 25 == 0 or idx == len(pending_files):
            print(f"  [{idx}/{len(pending_files)}] {fname} ({fsize_kb:.1f} KB) -> {archive_path}")

    total_mb = total_bytes / (1024 * 1024)
    print("\n" + "-" * 65)
    print("  RINGKASAN PENGARSIPAN DATA STREAM KE MINIO:")
    print("-" * 65)
    print(f"  • Berhasil diarsipkan : {uploaded_count:,} file")
    print(f"  • Total Volume Data   : {total_mb:.2f} MB")
    print(f"  • Bucket MinIO Target : s3://{target_bucket}/")
    print("-" * 65)
    print("  [STATUS] Pengarsipan Stream ke MinIO S3: SUCCESS ✅\n")
    return True


def list_minio_contents():
    """Menampilkan ringkasan isi bucket MinIO."""
    print("=" * 65)
    print("  [MINIO STATUS] Ringkasan Isi Bucket Data Lake")
    print("=" * 65)

    client = get_client()
    try:
        for b in BUCKETS:
            print(f"\n📁 Bucket: [{b}]")
            if not client.bucket_exists(b):
                print("   (Belum dibuat)")
                continue

            objects = list(client.list_objects(b, recursive=True))
            if not objects:
                print("   (Kosong)")
            else:
                for obj in objects[:10]:
                    size_kb = obj.size / 1024
                    print(f"   • {obj.object_name} ({size_kb:.2f} KB)")
                if len(objects) > 10:
                    print(f"   ... dan {len(objects) - 10} objek lainnya.")
    except Exception as e:
        print(f"Error membaca objek: {e}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Citi Bike MinIO Manager & Archiver")
    parser.add_argument("action", choices=["init", "upload", "archive", "list"], default="init", nargs="?")
    parser.add_argument("--days", type=int, default=None, help="Batasan hari ke belakang (misal 14 untuk 2 minggu).")
    args = parser.parse_args()

    if args.action == "init":
        init_buckets()
    elif args.action == "upload":
        upload_raw_to_minio()
    elif args.action == "archive":
        archive_realtime_streams(days_limit=args.days)
    elif args.action == "list":
        list_minio_contents()
