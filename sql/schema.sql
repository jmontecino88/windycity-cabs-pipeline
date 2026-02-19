CREATE TABLE IF NOT EXISTS stg_trips (
    business_key VARCHAR(64) PRIMARY KEY,
    trip_start_timestamp DATETIME NULL,
    trip_end_timestamp DATETIME NULL,
    trip_date DATE NULL,
    trip_hour INT NULL,
    weekday INT NULL,
    is_weekend BOOLEAN NULL,
    trip_miles DOUBLE NULL,
    fare DOUBLE NULL,
    tips DOUBLE NULL,
    trip_seconds DOUBLE NULL,
    pickup_community_area VARCHAR(32) NULL,
    dropoff_community_area VARCHAR(32) NULL,
    outlier_trip_miles BOOLEAN NULL,
    outlier_fare BOOLEAN NULL,
    outlier_trip_seconds BOOLEAN NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);