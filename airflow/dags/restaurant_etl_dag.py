"""
Airflow DAG: orchestrates the restaurant analytics ETL pipeline.

  extract/clean (Pandas) -> load raw to Postgres -> Spark transform -> done

Place this file in your Airflow DAGs folder. Adjust PROJECT_DIR to the
absolute path of this repo on the Airflow worker.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/restaurant_data_platform"

default_args = {
    "owner": "restaurant-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="restaurant_etl_pipeline",
    default_args=default_args,
    description="End-to-end ETL for the Restaurant Data Engineering Analytics Platform",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["restaurant", "etl", "analytics"],
) as dag:

    clean_transform = BashOperator(
        task_id="clean_transform",
        bash_command=f"python {PROJECT_DIR}/etl/clean_transform.py",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"python {PROJECT_DIR}/etl/load_to_postgres.py",
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            f"spark-submit --jars {PROJECT_DIR}/spark/postgresql-42.7.3.jar "
            f"{PROJECT_DIR}/spark/spark_transform.py"
        ),
    )

    clean_transform >> load_to_postgres >> spark_transform
