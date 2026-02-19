import os
import time
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_TABLE = "stg_trips"
MART_TABLES = [
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
    count = int(
        connection.execute(
            query,
            {"table_name": table_name, "column_name": column_name},
        ).scalar_one()
    )
    return count > 0


def ensure_required_columns(connection: Connection) -> None:
    if not column_exists(connection, SOURCE_TABLE, "taxi_id"):
        connection.execute(text("ALTER TABLE stg_trips ADD COLUMN taxi_id VARCHAR(64) NULL"))
    if not column_exists(connection, SOURCE_TABLE, "payment_type"):
        connection.execute(text("ALTER TABLE stg_trips ADD COLUMN payment_type VARCHAR(64) NULL"))


def rebuild_marts(connection: Connection) -> list[str]:
    ensure_required_columns(connection)

    for table in MART_TABLES:
        connection.execute(text(f"DROP TABLE IF EXISTS {table}"))

    connection.execute(
        text(
            f"""
            CREATE TABLE daily_kpis AS
            SELECT
                trip_date,
                COUNT(*) AS total_trips,
                SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) AS total_revenue,
                SUM(COALESCE(trip_seconds, 0)) AS total_trip_seconds,
                SUM(COALESCE(trip_seconds, 0)) / 3600.0 AS total_trip_hours,
                CASE
                    WHEN SUM(COALESCE(trip_seconds, 0)) > 0
                    THEN SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) / (SUM(COALESCE(trip_seconds, 0)) / 3600.0)
                    ELSE NULL
                END AS revenue_per_active_hour,
                CASE
                    WHEN COUNT(*) > 0
                    THEN SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) / COUNT(*)
                    ELSE NULL
                END AS avg_revenue_per_trip,
                CASE
                    WHEN COUNT(*) > 0
                    THEN (SUM(COALESCE(trip_seconds, 0)) / 60.0) / COUNT(*)
                    ELSE NULL
                END AS avg_trip_minutes,
                CASE
                    WHEN SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) > 0
                    THEN SUM(COALESCE(tips, 0)) / SUM(COALESCE(fare, 0) + COALESCE(tips, 0))
                    ELSE NULL
                END AS tips_ratio
            FROM {SOURCE_TABLE}
            WHERE trip_date IS NOT NULL
            GROUP BY trip_date
            """
        )
    )

    connection.execute(
        text(
            f"""
            CREATE TABLE hourly_kpis AS
            SELECT
                trip_date,
                trip_hour,
                COUNT(*) AS total_trips,
                SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) AS total_revenue,
                SUM(COALESCE(trip_seconds, 0)) AS total_trip_seconds,
                SUM(COALESCE(trip_seconds, 0)) / 3600.0 AS total_trip_hours,
                CASE
                    WHEN SUM(COALESCE(trip_seconds, 0)) > 0
                    THEN SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) / (SUM(COALESCE(trip_seconds, 0)) / 3600.0)
                    ELSE NULL
                END AS revenue_per_active_hour,
                CASE
                    WHEN COUNT(*) > 0
                    THEN SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) / COUNT(*)
                    ELSE NULL
                END AS avg_revenue_per_trip,
                CASE
                    WHEN COUNT(*) > 0
                    THEN (SUM(COALESCE(trip_seconds, 0)) / 60.0) / COUNT(*)
                    ELSE NULL
                END AS avg_trip_minutes,
                CASE
                    WHEN SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) > 0
                    THEN SUM(COALESCE(tips, 0)) / SUM(COALESCE(fare, 0) + COALESCE(tips, 0))
                    ELSE NULL
                END AS tips_ratio
            FROM {SOURCE_TABLE}
            WHERE trip_date IS NOT NULL
            GROUP BY trip_date, trip_hour
            """
        )
    )

    connection.execute(
        text(
            f"""
            CREATE TABLE taxi_daily_kpis AS
            SELECT
                trip_date,
                COALESCE(NULLIF(CAST(taxi_id AS CHAR), ''), 'unknown') AS taxi_id,
                COUNT(*) AS total_trips,
                SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) AS total_revenue,
                SUM(COALESCE(trip_seconds, 0)) / 3600.0 AS total_trip_hours,
                CASE
                    WHEN SUM(COALESCE(trip_seconds, 0)) > 0
                    THEN SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) / (SUM(COALESCE(trip_seconds, 0)) / 3600.0)
                    ELSE NULL
                END AS revenue_per_active_hour
            FROM {SOURCE_TABLE}
            WHERE trip_date IS NOT NULL
            GROUP BY trip_date, COALESCE(NULLIF(CAST(taxi_id AS CHAR), ''), 'unknown')
            """
        )
    )

    connection.execute(
        text(
            f"""
            CREATE TABLE zone_daily_kpis AS
            SELECT
                trip_date,
                COALESCE(NULLIF(CAST(pickup_community_area AS CHAR), ''), 'unknown') AS pickup_community_area,
                COUNT(*) AS total_trips,
                SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) AS total_revenue,
                SUM(COALESCE(trip_seconds, 0)) / 3600.0 AS total_trip_hours,
                CASE
                    WHEN SUM(COALESCE(trip_seconds, 0)) > 0
                    THEN SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) / (SUM(COALESCE(trip_seconds, 0)) / 3600.0)
                    ELSE NULL
                END AS revenue_per_active_hour
            FROM {SOURCE_TABLE}
            WHERE trip_date IS NOT NULL
            GROUP BY trip_date, COALESCE(NULLIF(CAST(pickup_community_area AS CHAR), ''), 'unknown')
            """
        )
    )

    connection.execute(
        text(
            """
            CREATE TABLE executive_kpis AS
            SELECT
                trip_date,
                SUM(total_revenue) AS total_revenue,
                SUM(total_trip_hours) AS total_trip_hours,
                SUM(total_revenue) / NULLIF(SUM(total_trip_hours), 0) AS revenue_per_active_hour,
                (
                    SELECT SUM(total_revenue)
                    FROM (
                        SELECT total_revenue
                        FROM taxi_daily_kpis t2
                        WHERE t2.trip_date = t.trip_date
                        ORDER BY total_revenue DESC
                        LIMIT 10
                    ) top10
                ) / NULLIF(SUM(total_revenue), 0) AS revenue_share_top_10_taxis,
                (
                    SELECT SUM(total_revenue)
                    FROM (
                        SELECT total_revenue
                        FROM zone_daily_kpis z2
                        WHERE z2.trip_date = t.trip_date
                        ORDER BY total_revenue DESC
                        LIMIT 3
                    ) top3
                ) / NULLIF(SUM(total_revenue), 0) AS revenue_share_top_3_zones
            FROM daily_kpis t
            GROUP BY trip_date
            """
        )
    )

    connection.execute(
        text(
            f"""
            CREATE TABLE payment_mix_daily AS
            SELECT
                trip_date,
                COALESCE(NULLIF(CAST(payment_type AS CHAR), ''), 'unknown') AS payment_type,
                COUNT(*) AS total_trips,
                SUM(COALESCE(fare, 0) + COALESCE(tips, 0)) AS total_revenue
            FROM {SOURCE_TABLE}
            WHERE trip_date IS NOT NULL
            GROUP BY trip_date, COALESCE(NULLIF(CAST(payment_type AS CHAR), ''), 'unknown')
            """
        )
    )

    return MART_TABLES


def run_build_marts() -> int:
    start = time.time()
    engine = make_engine()

    with engine.begin() as connection:
        created = rebuild_marts(connection)

    runtime = time.time() - start
    print(f"tables rebuilt: {', '.join(created)}")
    print(f"runtime seconds: {runtime:.2f}")
    return 0


def main() -> int:
    return run_build_marts()


if __name__ == "__main__":
    raise SystemExit(main())
