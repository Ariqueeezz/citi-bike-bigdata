"""
Citi Bike MinIO Bucket & Data Lake Manager
Menangani:
1. Inisialisasi 4 Bucket:
   - citibike-realtime (penampung data stream gbfs)
   - citibike-archive (arsip historis data stream)
   - citibike-raw (data mentah batch csv)
   - citibike-curated (data olahan parquet & analitik)
2. Upload data raw CSV lokal ke MinIO Data Lake.
3. Menampilkan status dan daftar objek di MinIO.
"""

import os
import sys
import glob

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
        subprocess.check_call([sys.executable, "-m", "pip", "install", "minio"])
        from minio import Minio
        print("✅ Modul 'minio' berhasil dipasang otomatis.\n")
    except Exception as e:
        print(f"❌ Tidak dapat menginstall 'minio' secara otomatis: {e}")
        print("💡 Solusi alternatif:")
        print("   1. Jalankan via Docker:  .\\run_pipeline.ps1 -Docker minio-setup")
        print("   2. Install manual lokal: pip install -r requirements.txt")
        sys.exit(1)

# Konfigurasi akses MinIO dari host/laptop
MINIO_HOST = os.getenv("MINIO_HOST", os.getenv("MINIO_ENDPOINT", "localhost:9002"))
# Bersihkan awalan http:// atau https:// jika ada
if "://" in MINIO_HOST:
    MINIO_HOST = MINIO_HOST.split("://")[1]

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
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["init", "upload", "list"], default="init", nargs="?")
    args = parser.parse_args()

    if args.action == "init":
        init_buckets()
    elif args.action == "upload":
        upload_raw_to_minio()
    elif args.action == "list":
        list_minio_contents()
