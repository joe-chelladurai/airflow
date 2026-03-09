"""
Weather ETL Pipeline — Real Data from Open-Meteo API (free, no API key)

This DAG fetches 7-day weather forecasts for major world cities,
transforms the data, loads it into Postgres, and generates a summary report.

Data source: https://open-meteo.com (free and open-source weather API)
"""

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

CITIES = {
    "New York":    {"lat": 40.71, "lon": -74.01},
    "London":      {"lat": 51.51, "lon": -0.13},
    "Tokyo":       {"lat": 35.68, "lon": 139.69},
    "Sydney":      {"lat": -33.87, "lon": 151.21},
    "Lagos":       {"lat": 6.52,  "lon": 3.38},
    "São Paulo":   {"lat": -23.55, "lon": -46.63},
    "Mumbai":      {"lat": 19.08, "lon": 72.88},
    "Berlin":      {"lat": 52.52, "lon": 13.41},
}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


@dag(
    dag_id="weather_etl_pipeline",
    description="Fetch real weather data from Open-Meteo, transform, and load into Postgres",
    schedule="@daily",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["etl", "weather", "real-data"],
)
def weather_etl_pipeline():

    # ── STEP 1: EXTRACT ─────────────────────────────────────────────────
    @task
    def extract_weather(city: str, coords: dict) -> dict:
        """Fetch 7-day forecast from Open-Meteo for a single city."""
        import requests

        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "daily": ",".join([
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "precipitation_sum",
                "windspeed_10m_max",
                "sunrise",
                "sunset",
            ]),
            "timezone": "auto",
            "forecast_days": 7,
        }

        response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        return {
            "city": city,
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "timezone": data.get("timezone", "UTC"),
            "daily": data["daily"],
        }

    # ── STEP 2: TRANSFORM ───────────────────────────────────────────────
    @task
    def transform_weather(raw: dict) -> list:
        """Flatten the nested API response into clean row-per-day records."""
        daily = raw["daily"]
        rows = []

        for i in range(len(daily["time"])):
            rows.append({
                "city":              raw["city"],
                "latitude":          raw["latitude"],
                "longitude":         raw["longitude"],
                "date":              daily["time"][i],
                "temp_max_c":        daily["temperature_2m_max"][i],
                "temp_min_c":        daily["temperature_2m_min"][i],
                "temp_mean_c":       daily["temperature_2m_mean"][i],
                "precipitation_mm":  daily["precipitation_sum"][i],
                "windspeed_max_kmh": daily["windspeed_10m_max"][i],
                "sunrise":           daily["sunrise"][i],
                "sunset":            daily["sunset"][i],
            })

        return rows

    # ── STEP 3: LOAD ────────────────────────────────────────────────────
    @task
    def load_to_postgres(records: list):
        """Insert transformed records into the daily_weather table (upsert)."""
        hook = PostgresHook(postgres_conn_id="weather_postgres")

        insert_sql = """
            INSERT INTO daily_weather
                (city, latitude, longitude, date,
                 temp_max_c, temp_min_c, temp_mean_c,
                 precipitation_mm, windspeed_max_kmh, sunrise, sunset)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city, date) DO UPDATE SET
                temp_max_c        = EXCLUDED.temp_max_c,
                temp_min_c        = EXCLUDED.temp_min_c,
                temp_mean_c       = EXCLUDED.temp_mean_c,
                precipitation_mm  = EXCLUDED.precipitation_mm,
                windspeed_max_kmh = EXCLUDED.windspeed_max_kmh,
                sunrise           = EXCLUDED.sunrise,
                sunset            = EXCLUDED.sunset,
                ingested_at       = NOW();
        """

        for r in records:
            hook.run(insert_sql, parameters=(
                r["city"], r["latitude"], r["longitude"], r["date"],
                r["temp_max_c"], r["temp_min_c"], r["temp_mean_c"],
                r["precipitation_mm"], r["windspeed_max_kmh"],
                r["sunrise"], r["sunset"],
            ))

        return f"Loaded {len(records)} rows for {records[0]['city']}"

    # ── STEP 4: GENERATE SUMMARY ────────────────────────────────────────
    build_summary = PostgresOperator(
        task_id="build_weather_summary",
        postgres_conn_id="weather_postgres",
        sql="""
            INSERT INTO weather_summary (report_date, city, avg_temp_7d, total_precip_7d, max_wind_7d)
            SELECT
                CURRENT_DATE                  AS report_date,
                city,
                ROUND(AVG(temp_mean_c)::numeric, 1)  AS avg_temp_7d,
                ROUND(SUM(precipitation_mm)::numeric, 1) AS total_precip_7d,
                ROUND(MAX(windspeed_max_kmh)::numeric, 1) AS max_wind_7d
            FROM daily_weather
            WHERE date >= CURRENT_DATE
              AND date <  CURRENT_DATE + INTERVAL '7 days'
            GROUP BY city
            ON CONFLICT (city, report_date) DO UPDATE SET
                avg_temp_7d     = EXCLUDED.avg_temp_7d,
                total_precip_7d = EXCLUDED.total_precip_7d,
                max_wind_7d     = EXCLUDED.max_wind_7d,
                created_at      = NOW();
        """,
    )

    # ── STEP 5: LOG REPORT ──────────────────────────────────────────────
    @task
    def print_report():
        """Query the summary table and print a human-readable report."""
        hook = PostgresHook(postgres_conn_id="weather_postgres")
        records = hook.get_records("""
            SELECT city, avg_temp_7d, total_precip_7d, max_wind_7d
            FROM weather_summary
            WHERE report_date = CURRENT_DATE
            ORDER BY avg_temp_7d DESC;
        """)

        report = "\n╔══════════════════════════════════════════════════════════╗\n"
        report += "║         7-DAY WEATHER FORECAST SUMMARY                  ║\n"
        report += f"║         Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}                    ║\n"
        report += "╠══════════════════════════════════════════════════════════╣\n"
        report += f"║ {'City':<14} {'Avg °C':>8} {'Rain mm':>10} {'Wind km/h':>11}  ║\n"
        report += "╠══════════════════════════════════════════════════════════╣\n"

        for city, avg_t, precip, wind in records:
            report += f"║ {city:<14} {avg_t:>8.1f} {precip:>10.1f} {wind:>11.1f}  ║\n"

        report += "╚══════════════════════════════════════════════════════════╝\n"
        print(report)

    # ── DAG WIRING ──────────────────────────────────────────────────────
    load_tasks = []
    for city_name, city_coords in CITIES.items():
        raw_data = extract_weather(city=city_name, coords=city_coords)
        clean_data = transform_weather(raw_data)
        loaded = load_to_postgres(clean_data)
        load_tasks.append(loaded)

    load_tasks >> build_summary >> print_report()


weather_etl_pipeline()
