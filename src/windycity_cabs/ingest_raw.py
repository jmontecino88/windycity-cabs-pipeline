import gzip
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

DATASET_URL = "https://data.cityofchicago.org/resource/ajtu-isnz.json"
PAGE_LIMIT = 5000
FIRST_RUN_DAYS = 60
LOOKBACK_HOURS = 6

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
STATE_PATH = PROJECT_ROOT / "data" / "state" / "ingest_state.json"

DEFAULT_STATE = {
    "last_watermark": None,
    "last_run_utc": None,
    "rows_downloaded": 0,
}


class RetryableAPIError(Exception):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_utc_iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def to_soql_utc_literal(ts: datetime) -> str:
    # Socrata comparisons for this column accept literals without timezone suffix.
    return ts.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")


def parse_any_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_trip_timestamp(value: str) -> datetime:
    if not value:
        raise ValueError("Missing trip_start_timestamp")
    return parse_any_iso(value)


def ensure_state_file() -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not STATE_PATH.exists():
        STATE_PATH.write_text(json.dumps(DEFAULT_STATE, indent=2), encoding="utf-8")


def load_state() -> dict[str, Any]:
    ensure_state_file()
    raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    state = DEFAULT_STATE.copy()
    state.update(raw)
    return state


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def compute_start_watermark(state: dict[str, Any], now_utc: datetime) -> datetime:
    last_watermark = state.get("last_watermark")
    if last_watermark:
        return parse_any_iso(last_watermark) - timedelta(hours=LOOKBACK_HOURS)
    return now_utc - timedelta(days=FIRST_RUN_DAYS)


def next_part_index(raw_root: Path) -> int:
    highest = 0
    if raw_root.exists():
        for path in raw_root.glob("dt=*/part-*.jsonl.gz"):
            stem = path.name.split(".")[0]
            try:
                number = int(stem.split("-")[1])
            except (IndexError, ValueError):
                continue
            highest = max(highest, number)
    return highest + 1


def write_partitioned_page(rows: list[dict[str, Any]], part_counter: int) -> int:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        ts = parse_trip_timestamp(row.get("trip_start_timestamp"))
        partition = ts.date().isoformat()
        grouped[partition].append(row)

    for partition in sorted(grouped.keys()):
        partition_dir = RAW_ROOT / f"dt={partition}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        target = partition_dir / f"part-{part_counter:05d}.jsonl.gz"
        with gzip.open(target, mode="wt", encoding="utf-8") as handle:
            for row in grouped[partition]:
                handle.write(json.dumps(row, ensure_ascii=False))
                handle.write("\n")
        part_counter += 1

    return part_counter


@retry(
    retry=retry_if_exception_type((RetryableAPIError, requests.RequestException)),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    stop=stop_after_attempt(7),
    reraise=True,
)
def fetch_page(session: requests.Session, params: dict[str, Any], headers: dict[str, str]) -> list[dict[str, Any]]:
    response = session.get(DATASET_URL, params=params, headers=headers, timeout=60)
    if response.status_code == 429 or 500 <= response.status_code < 600:
        raise RetryableAPIError(f"Retryable status: {response.status_code}")
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Unexpected API payload type")
    return payload


def run_ingest() -> int:
    load_dotenv(PROJECT_ROOT / ".env")

    state = load_state()
    started_at = utc_now()
    start_watermark = compute_start_watermark(state, started_at)
    start_watermark_iso = to_utc_iso(start_watermark)
    start_watermark_soql = to_soql_utc_literal(start_watermark)

    headers: dict[str, str] = {}
    app_token = os.getenv("SOCRATA_APP_TOKEN", "").strip()
    if app_token:
        headers["X-App-Token"] = app_token

    RAW_ROOT.mkdir(parents=True, exist_ok=True)

    offset = 0
    total_pages = 0
    total_rows = 0
    max_watermark: datetime | None = None
    part_counter = next_part_index(RAW_ROOT)

    with requests.Session() as session:
        while True:
            params = {
                "$limit": PAGE_LIMIT,
                "$offset": offset,
                "$where": f"trip_start_timestamp >= '{start_watermark_soql}'",
                "$order": "trip_start_timestamp ASC",
            }
            rows = fetch_page(session, params, headers)
            if not rows:
                break

            part_counter = write_partitioned_page(rows, part_counter)
            total_pages += 1
            total_rows += len(rows)

            for row in rows:
                ts_value = row.get("trip_start_timestamp")
                if not ts_value:
                    continue
                parsed = parse_trip_timestamp(ts_value)
                if max_watermark is None or parsed > max_watermark:
                    max_watermark = parsed

            offset += PAGE_LIMIT

    ended_at = utc_now()
    if max_watermark is None:
        if state.get("last_watermark"):
            end_watermark_iso = state["last_watermark"]
        else:
            end_watermark_iso = start_watermark_iso
    else:
        end_watermark_iso = to_utc_iso(max_watermark)

    updated_state = {
        "last_watermark": end_watermark_iso,
        "last_run_utc": to_utc_iso(ended_at),
        "rows_downloaded": total_rows,
    }
    save_state(updated_state)

    runtime_seconds = (ended_at - started_at).total_seconds()
    print(f"start watermark: {start_watermark_iso}")
    print(f"end watermark: {end_watermark_iso}")
    print(f"total pages: {total_pages}")
    print(f"total rows: {total_rows}")
    print(f"runtime seconds: {runtime_seconds:.2f}")

    return 0


def main() -> int:
    return run_ingest()


if __name__ == "__main__":
    raise SystemExit(main())