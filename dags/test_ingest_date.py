from airflow import DAG
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.providers.apache.druid.hooks.druid import DruidHook
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def debug_druid_submission(**kwargs):
    hook = DruidHook(druid_ingest_conn_id='druid_ingest_default')
    
    ingestion_spec_path = '/opt/airflow/dags/repo/druid-ingestion-spec.json'
    
    print(f"Submitting ingestion spec: {ingestion_spec_path}")
    
    # Submit the job and log the response
    try:
        response = hook.submit_indexing_job(json_index_file=ingestion_spec_path)
        print(f"Druid Overlord API Response: {response.text}")
        return response.text
    except Exception as e:
        print(f"Error during Druid job submission: {e}")
        raise

# Define DAG
with DAG(
    dag_id='druid_ingestion_operator_with_verification',
    default_args=default_args,
    description='DAG for ingesting data into Druid with debugging',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    ingest_data_to_druid = DruidOperator(
        task_id='ingest_data_to_druid',
        json_index_file='/opt/airflow/dags/repo/druid-ingestion-spec.json',
        druid_ingest_conn_id='druid_ingest_default',
        do_xcom_push=False,
    )

    debug_druid_task = PythonOperator(
        task_id='debug_druid_submission',
        python_callable=debug_druid_submission,
        provide_context=True,
    )

    debug_druid_task >> ingest_data_to_druid
