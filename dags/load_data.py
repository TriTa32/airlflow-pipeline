from airflow import DAG
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
import json
import requests
import os

logger = logging.getLogger(__name__)

REPO_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SPEC_FILE = os.path.join(REPO_PATH, 'wikipedia-index.json')

def check_druid_connection(druid_conn_id: str = 'druid_default') -> None:
    conn = BaseHook.get_connection(druid_conn_id)
    url = f'http://{conn.host}:{conn.port or 8082}/status'
    
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        raise Exception(f"Druid health check failed: {response.status_code}")
    logger.info("Druid connection successful")

def validate_spec(json_index_file: str) -> None:
    try:
        if not os.path.exists(json_index_file):
            raise FileNotFoundError(f"Spec file not found at: {json_index_file}")
            
        with open(json_index_file, 'r') as f:
            spec = json.load(f)
            if not all(field in spec for field in ['spec', 'type']):
                raise ValueError("Missing required fields in spec")
            logger.info(f"Spec file found and validated at: {json_index_file}")
    except Exception as e:
        logger.error(f"Spec validation failed: {str(e)}")
        raise

def monitor_status(**context) -> None:
    status = context['task_instance'].xcom_pull(task_ids='druid_ingest')
    if not status:
        raise Exception("No ingestion status received")
    
    if isinstance(status, str):
        status = json.loads(status)
    
    if status.get('status') != 'SUCCESS':
        raise Exception(f"Ingestion failed: {status.get('status')}")
    
    logger.info(f"Ingestion completed successfully. Processed: {status.get('processedRows', 0)} rows")

with DAG(
    'druid-ingest-monitored-test',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['druid'],
    template_searchpath=[REPO_PATH]
) as dag:

    check_connection = PythonOperator(
        task_id='check_connection',
        python_callable=check_druid_connection,
        op_kwargs={'druid_conn_id': 'druid_default'}
    )

    validate_ingestion = PythonOperator(
        task_id='validate_ingestion',
        python_callable=validate_spec,
        op_kwargs={'json_index_file': SPEC_FILE}
    )

    ingest_data = DruidOperator(
        task_id='druid_ingest',
        json_index_file='wikipedia-index.json',
        druid_ingest_conn_id='druid_default',
    )

    check_status = PythonOperator(
        task_id='check_status',
        python_callable=monitor_status,
        provide_context=True
    )

    check_connection >> validate_ingestion >> ingest_data >> check_status