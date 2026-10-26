import json
import os
import time
from datetime import datetime, timezone

import requests


BASE_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en"

FEEDS = {
    "station_status": f"{BASE_URL}/station_status.json",
    "station_information": f"{BASE_URL}/station_information.json",
}

OUTPUT_DIR = "/app/data/realtime"

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
    )

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
