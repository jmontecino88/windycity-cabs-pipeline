import hashlib
import os
import re
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
STAGING_ROOT = PROJECT_ROOT / "data" / "staging"

DAYS_TO_PROCESS = 7
BUSINESS_KEY_FIELDS = [
    "trip_start_timestamp",
    "trip_end_timestamp",
    "taxi_id",
    "pickup_community_area",
    "dropoff_community_area",
    "trip_miles",
    "fare",
]


def to_snake_case(value: str) -> str:
    text = value.strip()
    text = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def discover_recent_raw_partitions(days: int) -> list[tuple[date, Path]]:
    partitions: list[tuple[date, Path]] = []
    if not RAW_ROOT.exists():
        return partitions

    for path in RAW_ROOT.glob("dt=*"):
        if not path.is_dir():
            continue
        dt_text = path.name.replace("dt=", "", 1)
        try:
            dt_value = date.fromisoformat(dt_text)
        except ValueError:
            continue
        partitions.append((dt_value, path))

    partitions.sort(key=lambda item: item[0])
    if len(partitions) <= days:
        return partitions
    return partitions[-days:]


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = {col: to_snake_case(str(col)).lower() for col in frame.columns}
    frame = frame.rename(columns=renamed)
    frame.columns = [str(col).lower() for col in frame.columns]
    if frame.columns.duplicated().any():
        frame = frame.loc[:, ~frame.columns.duplicated(keep="first")]
    return frame


def cast_types(frame: pd.DataFrame) -> pd.DataFrame:
    datetime_columns = ["trip_start_timestamp", "trip_end_timestamp"]
    float_columns = ["trip_miles", "fare", "tips", "trip_seconds"]

    for col in datetime_columns:
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col], errors="coerce", utc=True)

    for col in float_columns:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

    return frame


def normalize_key_part(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""

    if isinstance(value, pd.Timestamp):
        ts = value.tz_convert("UTC") if value.tzinfo else value.tz_localize("UTC")
        return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if isinstance(value, float):
        if pd.isna(value):
            return ""
        return format(value, ".15g")

    text = str(value).strip().lower()
    if text in {"none", "nan", "nat"}:
        return ""
    return text


def build_business_key(frame: pd.DataFrame) -> pd.DataFrame:
    columns: list[list[Any]] = []
    row_count = len(frame)

    for name in BUSINESS_KEY_FIELDS:
        if name in frame.columns:
            columns.append(frame[name].tolist())
        else:
            columns.append([""] * row_count)

    hashes: list[str] = []
    for values in zip(*columns):
        normalized = [normalize_key_part(value) for value in values]
        payload = "||".join(normalized).encode("utf-8")
        hashes.append(hashlib.sha256(payload).hexdigest())

    frame["business_key"] = hashes
    return frame


def add_derived_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if "trip_start_timestamp" in frame.columns:
        ts = frame["trip_start_timestamp"]
        frame["trip_date"] = ts.dt.date
        frame["trip_hour"] = ts.dt.hour.astype("Int64")
        frame["weekday"] = ts.dt.weekday.astype("Int64")
        weekend = (ts.dt.weekday >= 5).fillna(False)
        frame["is_weekend"] = weekend.astype(bool)
    else:
        frame["trip_date"] = [None] * len(frame)
        frame["trip_hour"] = pd.Series([pd.NA] * len(frame), dtype="Int64")
        frame["weekday"] = pd.Series([pd.NA] * len(frame), dtype="Int64")
        frame["is_weekend"] = False

    return frame


def add_outlier_flags(frame: pd.DataFrame) -> pd.DataFrame:
    for col in ["trip_miles", "fare", "trip_seconds"]:
        flag_col = f"outlier_{col}"
        if col not in frame.columns:
            continue

        numeric = pd.to_numeric(frame[col], errors="coerce")
        threshold = numeric.quantile(0.999)
        if pd.isna(threshold):
            frame[flag_col] = False
        else:
            frame[flag_col] = (numeric > threshold).fillna(False).astype(bool)

    return frame


def read_partition_raw(partition_path: Path) -> tuple[pd.DataFrame, int]:
    files = sorted(partition_path.glob("part-*.jsonl.gz"))
    if not files:
        return pd.DataFrame(), 0

    frames: list[pd.DataFrame] = []
    for file_path in files:
        frame = pd.read_json(file_path, lines=True, compression="gzip")
        frames.append(frame)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return combined, len(files)


def atomic_write_partition(frame: pd.DataFrame, dt_value: str) -> None:
    target_dir = STAGING_ROOT / f"dt={dt_value}"
    target_dir.mkdir(parents=True, exist_ok=True)

    target_file = target_dir / "trips.parquet"
    temp_file = target_dir / f".trips.{os.getpid()}.tmp.parquet"

    frame.to_parquet(temp_file, index=False)
    os.replace(temp_file, target_file)


def process_partition(dt_value: date, raw_path: Path) -> tuple[int, int, int]:
    raw_frame, file_count = read_partition_raw(raw_path)
    if file_count == 0:
        return 0, 0, 0

    raw_frame = normalize_columns(raw_frame)
    raw_frame = cast_types(raw_frame)
    raw_frame = build_business_key(raw_frame)

    before_dedupe = len(raw_frame)
    staged_frame = raw_frame.drop_duplicates(subset=["business_key"], keep="first").copy()
    duplicates_removed = before_dedupe - len(staged_frame)

    staged_frame = add_derived_columns(staged_frame)
    staged_frame = add_outlier_flags(staged_frame)

    atomic_write_partition(staged_frame, dt_value.isoformat())

    print(
        f"dt={dt_value.isoformat()} files={file_count} rows_in={before_dedupe} "
        f"rows_out={len(staged_frame)} duplicates_removed={duplicates_removed}"
    )
    return before_dedupe, len(staged_frame), duplicates_removed


def run_stage() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    start = time.time()

    partitions = discover_recent_raw_partitions(DAYS_TO_PROCESS)
    if not partitions:
        print("No raw partitions found to stage")
        return 0

    STAGING_ROOT.mkdir(parents=True, exist_ok=True)

    total_in = 0
    total_out = 0
    total_dupes = 0

    print(f"processing_partitions={len(partitions)}")
    for dt_value, raw_path in partitions:
        rows_in, rows_out, dupes = process_partition(dt_value, raw_path)
        total_in += rows_in
        total_out += rows_out
        total_dupes += dupes

    runtime = time.time() - start
    print(f"total_rows_in={total_in}")
    print(f"total_rows_out={total_out}")
    print(f"total_duplicates_removed={total_dupes}")
    print(f"runtime_seconds={runtime:.2f}")

    return 0


def main() -> int:
    return run_stage()


if __name__ == "__main__":
    raise SystemExit(main())