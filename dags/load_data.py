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
    table_name = "sat_employee"
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql_query = f"SELECT * FROM public.{table_name};"
    df = pg_hook.get_pandas_df(sql_query)
    
    # Ensure `load_datetime` is in ISO 8601 format
    df['load_datetime'] = pd.to_datetime(df['load_datetime']).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Convert the DataFrame to a JSON array string
    records_json = df.to_dict(orient='records')  # List of dicts
    records_json_str = json.dumps(records_json)  # JSON array as a string

    # Push the JSON array string to XCom
    context['task_instance'].xcom_push(key='sat_employee_records_json', value=records_json_str)
    records_json = context['task_instance'].xcom_pull(key='sat_employee_records_json')

    logging.info(f"JSON Data for Ingestion: {records_json}")
    return records_json_str

def log_ingestion_status(**context):
    """Log the status of data ingestion"""
    table_name = "sat_employee"
    records_json_str = context['task_instance'].xcom_pull(key=f'{table_name}_records_json')
    record_count = len(json.loads(records_json_str))  # Count number of records in the JSON array
    logging.info(f"Completed ingestion of {record_count} records into Druid for table: {table_name}")
    return True

with DAG(
        dag_id='postgres_to_druid_sat_employee',
        default_args=default_args,
        description='Extract data from PostgreSQL and ingest into Druid',
        schedule_interval=None,
        catchup=False,
        tags=["postgres", "druid"],
        template_searchpath=[SPEC_PATH]
    ) as dag:

    extract_data = PythonOperator(
        task_id='extract_data_sat_employee',
        python_callable=extract_data_from_postgres,
    )

    ingest_to_druid = DruidOperator(
        task_id='ingest_to_druid_sat_employee',
        json_index_file='sat_employee_index.json',
        druid_ingest_conn_id='druid_default',
        max_ingestion_time=3600,
        params=dict(
            DATA_SOURCE='sat_employee',  # Dynamic data source
            INLINE_DATA="{{ task_instance.xcom_pull(key='sat_employee_records_json') }}"
        )
    )

    log_completion = PythonOperator(
        task_id='log_completion_sat_employee',
        python_callable=log_ingestion_status,
    )

    # Task dependencies
    extract_data >> ingest_to_druid >> log_completion 
