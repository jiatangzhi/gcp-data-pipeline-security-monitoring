"""
dags/pipeline_dag.py
--------------------
Airflow DAG for the GCP Security Monitoring pipeline.
Defines three tasks that mirror the orchestrator phases:
  extract_task → transform_task → load_task

To run with Airflow:
  1. Copy this file to your Airflow dags/ folder (or set AIRFLOW__CORE__DAGS_FOLDER)
  2. airflow db init && airflow scheduler &
  3. airflow dags trigger security_monitoring_pipeline
"""

import sys
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make sure the project src is importable from within Airflow
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 1, 1),
}

# ─────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────

def extract_task(**context):
    """
    EXTRACT: Load raw CSV data and simulate API ingestion.
    Stores the result in Airflow XCom so transform_task can use it.
    """
    from src.ingestion.ingest import ingest_all

    logger.info("Starting extract task")
    raw_data = ingest_all()

    # Serialise DataFrames to JSON for XCom (Airflow's inter-task communication)
    serialised = {
        key: df.to_json(orient="records", date_format="iso")
        for key, df in raw_data.items()
    }

    # Push to XCom so transform_task can read it
    context["ti"].xcom_push(key="raw_data_json", value=serialised)
    logger.info("Extract task complete")
    return "extract_done"


def transform_task(**context):
    """
    TRANSFORM: Pull raw data from XCom, clean it, compute metrics.
    Pushes cleaned + aggregated data back to XCom.
    """
    import json
    import pandas as pd
    from src.transformation.transform import transform_all
    from src.validation.validate import validate_all

    logger.info("Starting transform task")

    # Pull raw data from XCom
    serialised = context["ti"].xcom_pull(key="raw_data_json", task_ids="extract")
    raw_data = {key: pd.read_json(val) for key, val in serialised.items()}

    # Transform
    transformed = transform_all(raw_data)

    # Validate
    validation_report = validate_all(transformed["cleaned"])
    logger.info(
        f"Validation: {validation_report['passed']}/{validation_report['total_checks']} checks passed"
    )

    # Serialise for XCom
    cleaned_json = {
        key: df.to_json(orient="records", date_format="iso")
        for key, df in transformed["cleaned"].items()
    }
    aggregated_json = {
        key: df.to_json(orient="records", date_format="iso")
        for key, df in transformed["aggregated"].items()
    }

    context["ti"].xcom_push(key="cleaned_json", value=cleaned_json)
    context["ti"].xcom_push(key="aggregated_json", value=aggregated_json)
    context["ti"].xcom_push(key="validation_report", value=validation_report)

    logger.info("Transform task complete")
    return "transform_done"


def load_task(**context):
    """
    LOAD: Pull cleaned + aggregated data from XCom and write to the warehouse.
    """
    import pandas as pd
    from src.warehouse.warehouse import DataWarehouse

    logger.info("Starting load task")

    # Pull serialised DataFrames from XCom
    cleaned_json = context["ti"].xcom_pull(key="cleaned_json", task_ids="transform")
    aggregated_json = context["ti"].xcom_pull(key="aggregated_json", task_ids="transform")

    cleaned = {key: pd.read_json(val) for key, val in cleaned_json.items()}
    aggregated = {key: pd.read_json(val) for key, val in aggregated_json.items()}

    # Load into the warehouse
    with DataWarehouse() as wh:
        wh.load_cleaned(cleaned)
        wh.load_aggregated(aggregated)
        wh.print_summary()

    logger.info("Load task complete")
    return "load_done"


# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────

with DAG(
    dag_id="security_monitoring_pipeline",
    description="End-to-end security monitoring pipeline: ingest → transform → load",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Run daily at 06:00 UTC
    catchup=False,
    tags=["security", "data-engineering", "gcp"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
        provide_context=True,
    )

    # Define the task order: extract → transform → load
    extract >> transform >> load
