CREATE TABLE IF NOT EXISTS daily_weather (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    date            DATE NOT NULL,
    temp_max_c      DOUBLE PRECISION,
    temp_min_c      DOUBLE PRECISION,
    temp_mean_c     DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    windspeed_max_kmh DOUBLE PRECISION,
    sunrise         TIMESTAMP,
    sunset          TIMESTAMP,
    ingested_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE(city, date)
);

CREATE TABLE IF NOT EXISTS weather_summary (
    id              SERIAL PRIMARY KEY,
    report_date     DATE NOT NULL,
    city            VARCHAR(100) NOT NULL,
    avg_temp_7d     DOUBLE PRECISION,
    total_precip_7d DOUBLE PRECISION,
    max_wind_7d     DOUBLE PRECISION,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(city, report_date)
);
