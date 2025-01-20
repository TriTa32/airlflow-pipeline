from airflow import DAG
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.providers.apache.druid.hooks.druid import DruidHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}
REPO_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
def debug_druid_submission(**kwargs):
    hook = DruidHook(druid_ingest_conn_id='druid_default')
    
    ingestion_spec_path = '/opt/airflow/dags/repo/druid-ingestion-spec.json'
    
    print(f"Submitting ingestion spec from file: {ingestion_spec_path}")
    
    try:
        with open(ingestion_spec_path, 'r') as f:
            ingestion_spec = json.load(f)
        print(f"Ingestion spec loaded successfully: {ingestion_spec}")
    except Exception as e:
        print(f"Error reading ingestion spec file: {e}")
        raise

    try:
        response = hook.submit_indexing_job(ingestion_spec)
        print(f"Druid Overlord API Response: {response.text}")
        return response.text
    except Exception as e:
        print(f"Error during Druid job submission: {e}")
        raise

# Define DAG
with DAG(
    dag_id='druid_ingestion_operator_test',
    default_args=default_args,
    description='DAG for ingesting data into Druid with debugging',
    schedule_interval=None,
    template_searchpath=[REPO_PATH],
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    ingest_data_to_druid = DruidOperator(
        task_id='ingest_data_to_druid',
        json_index_file='/opt/airflow/dags/repo/druid-ingestion-spec.json',
    )

    # debug_druid_task = PythonOperator(
    #     task_id='debug_druid_submission',
    #     python_callable=debug_druid_submission,
    #     provide_context=True,
    # )

    ingest_data_to_druid
