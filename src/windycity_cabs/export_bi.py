import os
import time
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPORT_ROOT = PROJECT_ROOT / "bi_exports"
TABLES_TO_EXPORT = [
    "daily_kpis",
    "hourly_kpis",
    "taxi_daily_kpis",
    "zone_daily_kpis",
    "executive_kpis",
    "payment_mix_daily",
]


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


def run_export() -> int:
    start = time.time()
    EXPORT_ROOT.mkdir(parents=True, exist_ok=True)

    engine = make_engine()
    with engine.connect() as connection:
        for table_name in TABLES_TO_EXPORT:
            query = f"SELECT * FROM {table_name}"
            frame = pd.read_sql(query, connection)
            output_path = EXPORT_ROOT / f"{table_name}.csv"
            frame.to_csv(output_path, index=False)
            print(f"{table_name}: {len(frame)} rows exported")

    runtime = time.time() - start
    print(f"runtime seconds: {runtime:.2f}")
    return 0


def main() -> int:
    return run_export()


if __name__ == "__main__":
    raise SystemExit(main())
