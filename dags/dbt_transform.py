"""
dbt Weather Transformation Pipeline

Runs dbt models after the weather ETL pipeline completes,
transforming raw weather data into analytics-ready tables.

Models: staging -> intermediate -> marts
"""

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"
PROFILES = f"--profiles-dir {DBT_PROJECT_DIR}"


@dag(
    dag_id="dbt_weather_transform",
    description="Run dbt transformations on raw weather data",
    schedule="@daily",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["dbt", "transform", "weather"],
)
def dbt_weather_transform():

    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_weather_etl",
        external_dag_id="weather_etl_pipeline",
        external_task_id=None,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_CMD} deps {PROFILES}",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_CMD} seed {PROFILES}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_CMD} run {PROFILES}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test {PROFILES}",
    )

    wait_for_etl >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test


dbt_weather_transform()
