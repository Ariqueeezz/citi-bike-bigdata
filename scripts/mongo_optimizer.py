"""
Citi Bike MongoDB Indexing & TTL Retention Optimizer
Menerapkan:
1. TTL (Time-To-Live) Index untuk otomatis menghapus data streaming yang berumur > 24 jam.
2. Performance Indexing pada field snapshot_time, station_id, dan field analitik historis.
3. Menampilkan statistik ukuran database dan koleksi.
"""

import os
import sys
from urllib.parse import quote
from pymongo import MongoClient, ASCENDING, DESCENDING

# Pastikan UTF-8 di Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def get_mongo_client():
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        user = quote(os.getenv("MONGO_USER", "admin"), safe="")
        pwd = quote(os.getenv("MONGO_PASSWORD", "citibikemongo123"), safe="")
        host = os.getenv("MONGO_HOST", "localhost")
        port = os.getenv("MONGO_PORT", "27017")
        mongo_uri = f"mongodb://{user}:{pwd}@{host}:{port}/citibike?authSource=admin"

    return MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)


def optimize_mongodb(retention_hours=24):
    print("=" * 65)
    print("  [OPTIMASI MONGODB] Indexing & Data Retention Setup")
    print("=" * 65)

    try:
        client = get_mongo_client()
        # Test connection
        client.admin.command("ping")
        print("✅ Berhasil terhubung ke MongoDB.")
    except Exception as exc:
        print(f"⚠️  Tidak dapat terhubung ke MongoDB: {exc}")
        print("   Pastikan container MongoDB sudah menyala (`docker compose up -d mongodb`).")
        return False

    db = client["citibike"]
    ttl_seconds = int(retention_hours * 3600)

    print(f"\n1. Mengatur TTL Index pada koleksi 'realtime_station_status':")
    print(f"   Data real-time akan otomatis terhapus setelah {retention_hours} jam ({ttl_seconds:,} detik).")
    try:
        # Hapus index TTL lama jika ada dengan durasi berbeda
        existing_indexes = db.realtime_station_status.index_information()
        if "ttl_snapshot_retention" in existing_indexes:
            db.realtime_station_status.drop_index("ttl_snapshot_retention")

        db.realtime_station_status.create_index(
            [("snapshot_time", ASCENDING)],
            expireAfterSeconds=ttl_seconds,
            name="ttl_snapshot_retention"
        )
        print("   ✅ TTL Index 'ttl_snapshot_retention' berhasil dipasang.")
    except Exception as e:
        print(f"   ❌ Gagal membuat TTL Index: {e}")

    print("\n2. Mengatur Index Performa untuk Query Dashboard:")
    indexes_to_create = [
        ("realtime_station_status", [("snapshot_time", DESCENDING)], "idx_snapshot_time_desc"),
        ("realtime_station_status", [("station_id", ASCENDING), ("snapshot_time", DESCENDING)], "idx_station_snapshot"),
        ("realtime_station_status", [("is_installed", ASCENDING), ("is_renting", ASCENDING)], "idx_installed_renting"),
        ("historical_hourly_stats", [("ride_date", ASCENDING), ("ride_hour", ASCENDING)], "idx_exp1_hourly"),
        ("exp2_hourly_stats", [("ride_date", ASCENDING), ("ride_hour", ASCENDING)], "idx_exp2_hourly"),
        ("exp2_station_stats", [("total_rides", DESCENDING)], "idx_exp2_station_rides"),
    ]

    for col_name, idx_spec, idx_name in indexes_to_create:
        try:
            col = db[col_name]
            col.create_index(idx_spec, name=idx_name)
            print(f"   ✅ Index '{idx_name}' pada koleksi '{col_name}' siap.")
        except Exception as e:
            print(f"   ⚠️  Index '{idx_name}' pada '{col_name}': {e}")

    print("\n3. Statistik Ukuran Database:")
    try:
        stats = db.command("dbstats")
        data_size_mb = stats.get("dataSize", 0) / (1024 * 1024)
        storage_size_mb = stats.get("storageSize", 0) / (1024 * 1024)
        objects_count = stats.get("objects", 0)
        print(f"   • Jumlah Dokumen (Objects) : {objects_count:,}")
        print(f"   • Data Size                : {data_size_mb:.2f} MB")
        print(f"   • Storage Allocated Size   : {storage_size_mb:.2f} MB")
    except Exception as e:
        print(f"   (Statistik ringkas tidak dapat dibaca: {e})")

    print("\n" + "=" * 65)
    print("  [OPTIMASI SELESAI] Database MongoDB Teroptimasi! ✅")
    print("=" * 65)
    return True


if __name__ == "__main__":
    optimize_mongodb()
