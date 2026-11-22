"""Airflow DAG: landing -> bronze -> silver -> gold, each step a Spark job."""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS_DIR = Path(__file__).resolve().parent
# Local stack uses spark_default (master local[*]); the course cluster names it spark-default
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark_default")

with DAG(
    dag_id="goit_de_fp_datalake",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["goit-de-fp"],
) as dag:
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=str(JOBS_DIR / "landing_to_bronze.py"),
        conn_id=SPARK_CONN_ID,
        verbose=True,
    )
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=str(JOBS_DIR / "bronze_to_silver.py"),
        conn_id=SPARK_CONN_ID,
        verbose=True,
    )
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=str(JOBS_DIR / "silver_to_gold.py"),
        conn_id=SPARK_CONN_ID,
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
