# Apache Airflow

## Extract-Transform-Load Pipeline with Open-source Weather Data

An automated ETL pipeline that fetches **real-time weather forecasts** for 8 major cities around the world, transforms the data, and loads it into PostgreSQL — orchestrated with Apache Airflow.

## Data Source

[Open-Meteo API](https://open-meteo.com) — free, open-source weather API. No API key required.

## Pipeline Overview

```
Open-Meteo API
     │
     ▼
 ┌─────────────────────────────────────────────┐
 │  EXTRACT   8 cities fetched in parallel     │
 │  ──────    New York · London · Tokyo        │
 │            Sydney · Lagos · São Paulo       │
 │            Mumbai · Berlin                  │
 └──────────────────┬──────────────────────────┘
                    ▼
 ┌─────────────────────────────────────────────┐
 │  TRANSFORM  Flatten nested JSON into        │
 │  ─────────  clean row-per-day records       │
 └──────────────────┬──────────────────────────┘
                    ▼
 ┌─────────────────────────────────────────────┐
 │  LOAD       Upsert into PostgreSQL          │
 │  ────       (daily_weather table)           │
 └──────────────────┬──────────────────────────┘
                    ▼
 ┌─────────────────────────────────────────────┐
 │  SUMMARIZE  Aggregate 7-day stats per city  │
 │  ─────────  (weather_summary table)         │
 └──────────────────┬──────────────────────────┘
                    ▼
 ┌─────────────────────────────────────────────┐
 │  REPORT     Log formatted summary to stdout │
 └─────────────────────────────────────────────┘
```

## Sample Output

```
 city      | avg_temp_7d | total_precip_7d | max_wind_7d
-----------+-------------+-----------------+------------
 Lagos     |        29.9 |            12.5 |        19.5
 Mumbai    |        28.0 |             0.0 |        18.0
 São Paulo |        21.7 |            73.9 |        13.8
 Sydney    |        21.0 |             5.2 |        19.2
 Berlin    |         9.6 |             3.4 |        17.3
 London    |         9.2 |            11.2 |        32.4
 New York  |         9.1 |             1.0 |        35.1
 Tokyo     |         7.0 |             1.2 |        16.3
```

## Airflow Concepts Used

- **TaskFlow API** — `@dag` and `@task` decorators for clean Python-native DAG definitions
- **Fan-out / Fan-in** — 8 parallel extraction branches converge into a single summary step
- **XComs** — automatic data passing between tasks
- **PostgresOperator** — runs SQL aggregation queries directly
- **PostgresHook** — programmatic row-level upserts
- **Connections** — named `weather_postgres` connection for secure DB access
- **Retries** — failed tasks automatically retry twice with a 2-minute delay
- **Upsert pattern** — `ON CONFLICT DO UPDATE` prevents duplicate rows across re-runs

## Project Structure

```
airflow/
├── dags/
│   └── weather_etl.py        # Pipeline logic (extract → transform → load → summarize → report)
├── sql/
│   └── init.sql              # Creates daily_weather and weather_summary tables
├── docker-compose.yml        # Airflow webserver, scheduler, and Postgres
├── .env                      # Environment config
└── .gitignore
```

## Setup

Requires [Docker Desktop](https://www.docker.com/products/docker-desktop/).

```bash
# Initialize the database, admin user, and Postgres connection
docker compose up airflow-init

# Start all services
docker compose up -d
```

Open **http://localhost:8080** and log in with `admin` / `admin`.

## Usage

**From the UI:** Unpause `weather_etl_pipeline` and click the play button.

**From the CLI:**

```bash
# Trigger a run
docker exec airflow-airflow-scheduler-1 airflow dags trigger weather_etl_pipeline

# Check run status
docker exec airflow-airflow-scheduler-1 airflow dags list-runs -d weather_etl_pipeline

# Query the results
docker exec airflow-postgres-1 psql -U airflow -d airflow \
  -c "SELECT * FROM weather_summary ORDER BY avg_temp_7d DESC;"
```

## Teardown

```bash
docker compose down       # Stop containers (data is preserved)
docker compose down -v    # Stop containers and delete all data
```

## Tech Stack

- **Apache Airflow 2.10** — workflow orchestration
- **PostgreSQL 15** — data storage
- **Open-Meteo API** — real-time weather data
- **Docker Compose** — containerized infrastructure
