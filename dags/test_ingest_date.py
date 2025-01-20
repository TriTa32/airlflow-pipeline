from airflow import DAG
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests

REPO_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SPEC_FILE = os.path.join(REPO_PATH, 'druid-ingestion-spec.json')

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def verify_overlord_status():
    """Verify the Druid Overlord status before submitting the ingestion spec."""
    druid_overlord_url = "http://druid-overlord.superset-prod.svc.cluster.local:8081/status"  
    try:
        response = requests.get(druid_overlord_url, timeout=10)
        response.raise_for_status()
        status = response.json()
        print("Druid Overlord status:", status)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to connect to Druid Overlord: {e}")

# Define the DAG
with DAG(
    "druid_ingestion_operator_test",
    default_args=default_args,
    description="DAG to ingest data into Druid using DruidOperator",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    template_searchpath=[REPO_PATH],
    catchup=False,
) as dag:

    verify_overlord = PythonOperator(
        task_id="verify_overlord",
        python_callable=verify_overlord_status
    )

    ingest_data_to_druid = DruidOperator(
        task_id="ingest_data_to_druid",
        json_index_file="druid-ingestion-spec.json",
        druid_ingest_conn_id="druid_default",
        do_xcom_push=True
    )

    verify_overlord >> ingest_data_to_druid
