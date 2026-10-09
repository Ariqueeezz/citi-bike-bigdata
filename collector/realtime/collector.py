import json
import os
import time
from datetime import datetime, timezone

import requests


URL = (
    "https://gbfs.lyft.com/gbfs/2.3/bkn/en/"
    "station_status.json"
)

OUTPUT_DIR = "/app/data/realtime"

INTERVAL_SECONDS = 60


def collect():

    response = requests.get(
        URL,
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d_%H%M%S")

    output_file = os.path.join(
        OUTPUT_DIR,
        f"station_status_{timestamp}.json"
    )

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            payload,
            f,
            ensure_ascii=False
        )

    station_count = len(
        payload.get("data", {}).get("stations", [])
    )

    print(
        f"[{timestamp}] "
        f"Collected {station_count} stations"
    )


os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)


while True:

    try:

        collect()

    except Exception as e:

        print(
            f"Collector error: {e}"
        )

    time.sleep(
        INTERVAL_SECONDS
    )
