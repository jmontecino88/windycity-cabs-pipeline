import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STAGING_ROOT = PROJECT_ROOT / "data" / "staging"
SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"
BATCH_SIZE = 1000

TARGET_COLUMNS = [
    "business_key",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_date",
    "trip_hour",
    "weekday",
    "is_weekend",
    "trip_miles",
    "fare",
    "tips",
    "trip_seconds",
    "pickup_community_area",
    "dropoff_community_area",
    "taxi_id",
    "payment_type",
    "company",
    "trip_id",
    "trip_total",
    "extras",
    "tolls",
    "outlier_trip_miles",
    "outlier_fare",
    "outlier_trip_seconds",
]


def to_bool_or_none(value: Any) -> bool | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    text_value = str(value).strip().lower()
    if text_value in {"true", "1", "t", "yes", "y"}:
        return True
    if text_value in {"false", "0", "f", "no", "n"}:
        return False
    return None


def normalize_db_value(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def to_text_or_none(value: Any, max_len: int | None = None) -> str | None:
    if value is None or pd.isna(value):
        return None
    text_value = str(value).strip()
    if text_value == "":
        return None
    if max_len is not None and len(text_value) > max_len:
        return text_value[:max_len]
    return text_value


def find_staging_partitions() -> list[Path]:
    if not STAGING_ROOT.exists():
        return []
    return sorted(STAGING_ROOT.glob("dt=*/trips.parquet"), key=lambda p: p.parent.name)


def make_engine() -> Engine:
    load_dotenv(PROJECT_ROOT / ".env")

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    user = os.getenv("DB_USER", "windy")
    password = os.getenv("DB_PASSWORD", "windy")
    database = os.getenv("DB_NAME", "windycity")

    url = (
        f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{quote_plus(database)}?charset=utf8mb4"
    )
    return create_engine(url, future=True)


def ensure_schema(connection: Connection) -> None:
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8").strip()
    if schema_sql:
        connection.exec_driver_sql(schema_sql)


def column_exists(connection: Connection, table_name: str, column_name: str) -> bool:
    query = text(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = :table_name
          AND column_name = :column_name
        """
    )
    found = int(
        connection.execute(
            query,
            {"table_name": table_name, "column_name": column_name},
        ).scalar_one()
    )
    return found > 0


def ensure_extended_columns(connection: Connection) -> None:
    expected = {
        "taxi_id": "VARCHAR(64) NULL",
        "payment_type": "VARCHAR(64) NULL",
        "company": "VARCHAR(128) NULL",
        "trip_id": "VARCHAR(64) NULL",
        "trip_total": "DOUBLE NULL",
        "extras": "DOUBLE NULL",
        "tolls": "DOUBLE NULL",
    }
    for column_name, ddl_type in expected.items():
        if not column_exists(connection, "stg_trips", column_name):
            connection.execute(text(f"ALTER TABLE stg_trips ADD COLUMN {column_name} {ddl_type}"))


def get_row_count(connection: Connection) -> int:
    return int(connection.execute(text("SELECT COUNT(*) FROM stg_trips")).scalar_one())


def frame_to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    df = frame.copy()

    for column in TARGET_COLUMNS:
        if column not in df.columns:
            df[column] = None

    df = df[TARGET_COLUMNS]

    df["business_key"] = df["business_key"].fillna("").astype(str).str.strip().str.lower()
    df = df[df["business_key"] != ""].copy()

    for column in ["trip_start_timestamp", "trip_end_timestamp"]:
        dt_series = pd.to_datetime(df[column], errors="coerce", utc=True)
        dt_series = dt_series.dt.tz_convert("UTC").dt.tz_localize(None)
        df[column] = dt_series.apply(lambda value: value.to_pydatetime() if pd.notna(value) else None)

    df["trip_date"] = pd.to_datetime(df["trip_date"], errors="coerce").dt.date

    for column in ["trip_hour", "weekday"]:
        number_series = pd.to_numeric(df[column], errors="coerce").astype("Int64")
        df[column] = number_series.apply(lambda value: int(value) if pd.notna(value) else None)

    for column in ["trip_miles", "fare", "tips", "trip_seconds"]:
        number_series = pd.to_numeric(df[column], errors="coerce")
        df[column] = number_series.apply(lambda value: float(value) if pd.notna(value) else None)

    for column in ["trip_total", "extras", "tolls"]:
        number_series = pd.to_numeric(df[column], errors="coerce")
        df[column] = number_series.apply(lambda value: float(value) if pd.notna(value) else None)

    for column in ["pickup_community_area", "dropoff_community_area"]:
        df[column] = df[column].apply(lambda value: None if value is None or pd.isna(value) else str(value).strip())

    df["taxi_id"] = df["taxi_id"].apply(lambda value: to_text_or_none(value, max_len=64))
    df["payment_type"] = df["payment_type"].apply(lambda value: to_text_or_none(value, max_len=64))
    df["company"] = df["company"].apply(lambda value: to_text_or_none(value, max_len=128))
    df["trip_id"] = df["trip_id"].apply(lambda value: to_text_or_none(value, max_len=64))

    for column in ["is_weekend", "outlier_trip_miles", "outlier_fare", "outlier_trip_seconds"]:
        df[column] = df[column].apply(to_bool_or_none)

    records = []
    for row in df.to_dict(orient="records"):
        normalized_row = {key: normalize_db_value(value) for key, value in row.items()}
        records.append(normalized_row)
    return records


def upsert_batches(connection: Connection, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0

    upsert_sql = text(
        """
        INSERT INTO stg_trips (
            business_key,
            trip_start_timestamp,
            trip_end_timestamp,
            trip_date,
            trip_hour,
            weekday,
            is_weekend,
            trip_miles,
            fare,
            tips,
            trip_seconds,
            pickup_community_area,
            dropoff_community_area,
            taxi_id,
            payment_type,
            company,
            trip_id,
            trip_total,
            extras,
            tolls,
            outlier_trip_miles,
            outlier_fare,
            outlier_trip_seconds
        ) VALUES (
            :business_key,
            :trip_start_timestamp,
            :trip_end_timestamp,
            :trip_date,
            :trip_hour,
            :weekday,
            :is_weekend,
            :trip_miles,
            :fare,
            :tips,
            :trip_seconds,
            :pickup_community_area,
            :dropoff_community_area,
            :taxi_id,
            :payment_type,
            :company,
            :trip_id,
            :trip_total,
            :extras,
            :tolls,
            :outlier_trip_miles,
            :outlier_fare,
            :outlier_trip_seconds
        )
        ON DUPLICATE KEY UPDATE
            trip_start_timestamp = VALUES(trip_start_timestamp),
            trip_end_timestamp = VALUES(trip_end_timestamp),
            trip_date = VALUES(trip_date),
            trip_hour = VALUES(trip_hour),
            weekday = VALUES(weekday),
            is_weekend = VALUES(is_weekend),
            trip_miles = VALUES(trip_miles),
            fare = VALUES(fare),
            tips = VALUES(tips),
            trip_seconds = VALUES(trip_seconds),
            pickup_community_area = VALUES(pickup_community_area),
            dropoff_community_area = VALUES(dropoff_community_area),
            taxi_id = VALUES(taxi_id),
            payment_type = VALUES(payment_type),
            company = VALUES(company),
            trip_id = VALUES(trip_id),
            trip_total = VALUES(trip_total),
            extras = VALUES(extras),
            tolls = VALUES(tolls),
            outlier_trip_miles = VALUES(outlier_trip_miles),
            outlier_fare = VALUES(outlier_fare),
            outlier_trip_seconds = VALUES(outlier_trip_seconds),
            loaded_at = CURRENT_TIMESTAMP
        """
    )

    affected_rows = 0
    for start in range(0, len(records), BATCH_SIZE):
        batch = records[start : start + BATCH_SIZE]
        result = connection.execute(upsert_sql, batch)
        if result.rowcount is None or result.rowcount < 0:
            affected_rows += len(batch)
        else:
            affected_rows += result.rowcount
    return affected_rows


def run_load() -> int:
    start = time.time()
    partitions = find_staging_partitions()

    if not partitions:
        print("partitions processed: 0")
        print("rows attempted: 0")
        print("rows inserted/updated: 0")
        print("runtime seconds: 0.00")
        return 0

    rows_attempted = 0
    mysql_affected_rows = 0

    engine = make_engine()
    with engine.begin() as connection:
        ensure_schema(connection)
        ensure_extended_columns(connection)
        before_count = get_row_count(connection)

        for parquet_path in partitions:
            frame = pd.read_parquet(parquet_path)
            records = frame_to_records(frame)
            rows_attempted += len(records)
            mysql_affected_rows += upsert_batches(connection, records)

        after_count = get_row_count(connection)

    runtime = time.time() - start
    inserted = max(after_count - before_count, 0)
    updated_estimate = max(rows_attempted - inserted, 0)

    print(f"partitions processed: {len(partitions)}")
    print(f"rows attempted: {rows_attempted}")
    print(f"rows inserted/updated: {rows_attempted}")
    print(f"rows inserted: {inserted}")
    print(f"rows updated (estimate): {updated_estimate}")
    print(f"mysql affected rows: {mysql_affected_rows}")
    print(f"runtime seconds: {runtime:.2f}")

    return 0


def main() -> int:
    return run_load()


if __name__ == "__main__":
    raise SystemExit(main())
