from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.apache.druid.operators.druid import DruidOperator
import pandas as pd
import logging
import json
import os 

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
SPEC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../spec_file'))

def extract_data_from_postgres(**context):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql_query = """SELECT * FROM public.sat_employee;"""
    
    df = pg_hook.get_pandas_df(sql_query)
    df['load_datetime'] = pd.to_datetime(df['load_datetime']).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Convert records to JSON strings
    records_json = [json.dumps(record) for record in df.to_dict('records')]
    
    # Push JSON records to XCom
    context['task_instance'].xcom_push(key='employee_records_json', value=records_json)
    
    return records_json

def cleanup_temp_file(**context):
    """Remove temporary JSON file after Druid ingestion"""
    temp_file = context['task_instance'].xcom_pull(key='data_file', task_ids='extract_data')
    try:
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)
            logging.info(f"Removed temporary file: {temp_file}")
    except Exception as e:
        logging.error(f"Error removing temporary file: {str(e)}")


def log_ingestion_status(**context):
    """Log the status of data ingestion"""
    record_count = context['task_instance'].xcom_pull(key='record_count', task_ids='extract_data')
    logging.info(f"Completed ingestion of {record_count} records into Druid")
    return True

with DAG(
        dag_id='postgres_to_druid_pipeline',
        default_args=default_args,
        description='Extract data from PostgreSQL and ingest into Druid using DruidOperator',
        schedule_interval=None, 
        catchup=False,
        tags=["postgres", "druid"],
        template_searchpath=[SPEC_PATH]
    ) as dag:

    # Define tasks
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_from_postgres,
        provide_context=True,
    )

    ingest_to_druid = DruidOperator(
        task_id='ingest_to_druid',
        json_index_file='sat_employee_index.json',
        druid_ingest_conn_id='druid_default',
        max_ingestion_time=3600,
        params=dict(
            DATA_SOURCE='sat_employee',
            INLINE_DATA="{{ task_instance.xcom_pull(key='employee_records_json', task_ids='extract_data') | join('\n') }}"
        )
    )

    log_completion = PythonOperator(
        task_id='log_completion',
        python_callable=log_ingestion_status,
        provide_context=True,
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_temp_file',
        python_callable=cleanup_temp_file,
        provide_context=True,
        trigger_rule='all_success',  # Run even if previous tasks succeeded
    )

    # Define task dependencies
    extract_data >> ingest_to_druid >> log_completion >> cleanup_task
