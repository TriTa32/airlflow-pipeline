from airflow import DAG
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests

REPO_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SPEC_FILE = os.path.join(REPO_PATH, 'druid-ingestion-spec.json')

OVERLORD_URL = "http://druid-overlord.superset-prod.svc.cluster.local:8081/status"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def verify_overlord():
    try:
        response = requests.get(OVERLORD_URL, timeout=10)
        response.raise_for_status()
        status = response.json()
        if 'version' in status and 'modules' in status:
            print(f"Druid Overlord is active. Version: {status['version']}")
        else:
            raise ValueError("Unexpected Overlord status response.")
    except Exception as e:
        raise RuntimeError(f"Failed to verify Druid Overlord: {e}")

# Define the DAG
with DAG(
    "druid_ingestion_operator_with_verification",
    default_args=default_args,
    description="DAG to verify Druid Overlord and ingest data using DruidOperator",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    template_searchpath=[REPO_PATH],
    catchup=False,
) as dag:

    verify_overlord_task = PythonOperator(
        task_id="verify_overlord",
        python_callable=verify_overlord,
    )

    ingest_data_to_druid = DruidOperator(
        task_id="ingest_data_to_druid",
        json_index_file="druid-ingestion-spec.json",
        druid_ingest_conn_id="druid_default",
        do_xcom_push=True,
        on_failure_callback=lambda context: print(f"Task failed with context: {context}")
    )

    # Task dependencies
    verify_overlord_task >> ingest_data_to_druid
