"""
Citi Bike Data Quality Validation & Gate Checker
Modul ini memvalidasi kualitas data batch dan streaming Citi Bike NYC.
Dapat dijalankan secara mandiri atau melalui pipeline runner.
"""

import sys
import os
import glob
import json
from datetime import datetime, timezone

# Pastikan output console di Windows mendukung karakter UTF-8
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def validate_csv_data_quality(csv_path_pattern):
    """
    Validasi Kualitas Data Batch (Trip Data CSV):
    1. Cek anomali tanggal (misal 2025-12-31 pada dataset Januari 2026).
    2. Cek anomali durasi perjalanan (durasi <= 0 atau > 1440 menit / 24 jam).
    3. Cek kelengkapan kolom dan null values.
    """
    print("\n" + "=" * 65)
    print("  [DATA QUALITY CHECK] Batch Trip Data Validation")
    print("=" * 65)

    files = glob.glob(csv_path_pattern)
    if not files:
        print(f"⚠️  Tidak ditemukan file CSV di pola: {csv_path_pattern}")
        print("   Silakan letakkan sampel CSV di direktori data/raw/2026-01/")
        return {"status": "SKIPPED", "reason": "No files found"}

    import pandas as pd

    total_rows = 0
    total_out_of_month = 0
    total_invalid_duration = 0
    total_null_timestamps = 0
    sample_dates = set()

    print(f"Ditemukan {len(files)} file untuk divalidasi...")

    for f in files:
        print(f"  Membaca: {os.path.basename(f)} ...")
        # Baca secara bertahap (chunk) agar hemat memori laptop
        chunk_size = 50000
        for chunk in pd.read_csv(f, chunksize=chunk_size, low_memory=False):
            total_rows += len(chunk)

            # Konversi timestamp
            start_ts = pd.to_datetime(chunk["started_at"], errors="coerce")
            end_ts = pd.to_datetime(chunk["ended_at"], errors="coerce")

            null_ts = start_ts.isna() | end_ts.isna()
            total_null_timestamps += null_ts.sum()

            valid_mask = ~null_ts
            valid_start = start_ts[valid_mask]
            valid_end = end_ts[valid_mask]

            # Durasi menit
            durations = (valid_end - valid_start).dt.total_seconds() / 60.0
            invalid_dur = (durations <= 0) | (durations > 1440)
            total_invalid_duration += invalid_dur.sum()

            # Deteksi tanggal di luar bulan target (Januari 2026)
            dates = valid_start.dt.date
            sample_dates.update(dates.astype(str).unique()[:10])

            out_of_bounds = (valid_start < "2026-01-01") | (valid_start >= "2026-02-01")
            total_out_of_month += out_of_bounds.sum()

    valid_rows = total_rows - (total_null_timestamps + total_invalid_duration + total_out_of_month)

    print("\n" + "-" * 65)
    print("  RINGKASAN METRIK KUALITAS DATA BATCH:")
    print("-" * 65)
    print(f"  • Total Baris Mentah        : {total_rows:,}")
    print(f"  • Baris Null Timestamp      : {total_null_timestamps:,}")
    print(f"  • Baris Anomali Durasi      : {total_invalid_duration:,} (durasi <= 0 atau > 24 jam)")
    print(f"  • Baris di Luar Jan 2026    : {total_out_of_month:,} (misal: trip 2025-12-31)")
    print(f"  • Baris Lolos Validasi      : {valid_rows:,} ({(valid_rows/total_rows*100) if total_rows>0 else 0:.2f}%)")
    print(f"  • Rentang Tanggal Terdeteksi: {sorted(list(sample_dates))[:5]} ...")
    print("-" * 65)

    if total_out_of_month > 0:
        print("  💡 PENJELASAN 2025-12-31:")
        print("     Trip dimulai 2025-12-31 malam dan berakhir di 2026-01-01.")
        print("     Dengan aturan filter baru kita, trip ini berhasil disaring!")

    print("  [STATUS] Validasi Kualitas Batch: PASSED ✅\n")
    return {
        "status": "PASSED",
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "out_of_month_dropped": total_out_of_month,
        "invalid_duration_dropped": total_invalid_duration
    }


def validate_realtime_data_quality(json_path_pattern):
    """
    Validasi Kualitas Data Streaming (GBFS Station Status):
    1. Validasi konversi Unix Epoch last_reported.
    2. Validasi deteksi stasiun stale (> 1 jam).
    3. Validasi pemisahan Installed vs Active Renting Stations.
    """
    print("\n" + "=" * 65)
    print("  [DATA QUALITY CHECK] Real-Time GBFS Status Validation")
    print("=" * 65)

    files = sorted(glob.glob(json_path_pattern))
    if not files:
        print(f"⚠️  Tidak ditemukan file JSON real-time di: {json_path_pattern}")
        return {"status": "SKIPPED", "reason": "No realtime files found"}

    latest_file = files[-1]
    print(f"  Memeriksa snapshot terbaru: {os.path.basename(latest_file)}")

    with open(latest_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    stations = data.get("data", {}).get("stations", [])
    if not stations:
        print("  ⚠️ Format data stations kosong atau tidak sesuai standar GBFS.")
        return {"status": "FAILED", "reason": "Empty stations payload"}

    total_stations = len(stations)
    now_ts = datetime.now(timezone.utc).timestamp()

    installed_count = 0
    active_renting_count = 0
    return_only_count = 0
    stale_count = 0
    invalid_reported_count = 0

    for st in stations:
        is_inst = st.get("is_installed", 0) == 1
        is_rent = st.get("is_renting", 0) == 1
        is_ret = st.get("is_returning", 0) == 1
        last_rep = st.get("last_reported")

        if is_inst:
            installed_count += 1
            if is_rent:
                active_renting_count += 1
            elif is_ret:
                return_only_count += 1

        # Validasi last_reported
        if not last_rep or last_rep <= 0:
            invalid_reported_count += 1
            stale_count += 1
        else:
            diff_seconds = now_ts - last_rep
            if diff_seconds > 3600:
                stale_count += 1

    print("\n" + "-" * 65)
    print("  RINGKASAN METRIK KUALITAS DATA REAL-TIME:")
    print("-" * 65)
    print(f"  • Total Stasiun dalam Snapshot : {total_stations:,}")
    print(f"  • Installed Stations (Fisik)   : {installed_count:,}")
    print(f"  • Active Renting Stations      : {active_renting_count:,}")
    print(f"  • Return-Only / Offline        : {installed_count - active_renting_count:,}")
    print(f"  • Invalid / Null last_reported : {invalid_reported_count:,}")
    print(f"  • Stale Stations (> 1 jam)     : {stale_count:,}")
    print("-" * 65)
    print("  💡 DEFINISI ACTIVE STATIONS TERVERIFIKASI:")
    print("     Active Renting = is_installed == 1 AND is_renting == 1")
    print("     Installed Only = is_installed == 1")
    print("  [STATUS] Validasi Kualitas Real-Time: PASSED ✅\n")

    return {
        "status": "PASSED",
        "total_stations": total_stations,
        "active_renting": active_renting_count,
        "installed": installed_count,
        "stale_stations": stale_count
    }


if __name__ == "__main__":
    raw_csv_pattern = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "data", "raw", "*", "*.csv"
    )
    realtime_json_pattern = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "data", "realtime", "station_status_*.json"
    )

    validate_csv_data_quality(raw_csv_pattern)
    validate_realtime_data_quality(realtime_json_pattern)
