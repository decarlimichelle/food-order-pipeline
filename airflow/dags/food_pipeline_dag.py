"""
Airflow DAG — Food Order Pipeline Orchestration

This DAG orchestrates the data pipeline:
  1. Check that Postgres and Kafka are healthy
  2. Run dbt models (staging → marts)
  3. Run dbt tests
  4. Export analytics data to Parquet for external use

Schedule: Runs every hour (in production).
For learning, trigger it manually from the Airflow UI.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ─── DAG Configuration ───────────────────────────────────────

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="food_order_pipeline",
    default_args=default_args,
    description="Orchestrates the food order data pipeline: dbt transforms and data quality checks",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-engineering", "food-orders", "dbt"],
) as dag:

    # ─── Task 1: Health Check ────────────────────────────────
    check_postgres = BashOperator(
        task_id="check_postgres_connection",
        bash_command=(
            "python -c \""
            "import psycopg2; "
            "conn = psycopg2.connect("
            "  host='postgres', port=5432, user='pipeline', "
            "  password='pipeline123', dbname='food_orders'"
            "); "
            "print('Postgres OK'); "
            "conn.close()"
            "\""
        ),
    )

    # ─── Task 2: dbt Debug (validate connection) ─────────────
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="cd /usr/app && dbt debug",
    )

    # ─── Task 3: dbt Run — Staging Models ────────────────────
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /usr/app && dbt run --select staging",
    )

    # ─── Task 4: dbt Run — Mart Models ───────────────────────
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /usr/app && dbt run --select marts",
    )

    # ─── Task 5: dbt Test — Data Quality ─────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /usr/app && dbt test",
    )

    # ─── Task 6: Generate dbt Docs ───────────────────────────
    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command="cd /usr/app && dbt docs generate",
    )

    # ─── Task Dependencies (the pipeline order) ──────────────
    # This defines the execution order:
    #
    #   check_postgres → dbt_debug → dbt_run_staging
    #                                       ↓
    #                                 dbt_run_marts
    #                                       ↓
    #                                   dbt_test
    #                                       ↓
    #                                   dbt_docs

    check_postgres >> dbt_debug >> dbt_run_staging >> dbt_run_marts >> dbt_test >> dbt_docs
