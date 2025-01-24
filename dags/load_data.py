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

# Task to extract data from PostgreSQL
def extract_data_from_postgres(**context):
    table_name = "sat_employee"
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql_query = f"SELECT * FROM public.{table_name};"
    logging.info(f"Executing query: {sql_query}")
    
    try:
        df = pg_hook.get_pandas_df(sql_query)
        logging.info(f"Data fetched from table {table_name}: {df.head()}")  # Log the first few rows
    except Exception as e:
        logging.error(f"Error fetching data from PostgreSQL: {str(e)}")
        raise
    
    # Ensure `load_datetime` is in ISO 8601 format
    try:
        df['load_datetime'] = pd.to_datetime(df['load_datetime']).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        logging.info("Converted `load_datetime` to ISO 8601 format")
    except Exception as e:
        logging.error(f"Error processing `load_datetime`: {str(e)}")
        raise

    # Convert the DataFrame to a JSON array string
    try:
        records_json = df.to_dict(orient='records')  # List of dicts
        records_json_str = json.dumps(records_json)  # JSON array as a string
        logging.info(f"Prepared JSON data: {records_json_str[:500]}...")  # Log only the first 500 characters
    except Exception as e:
        logging.error(f"Error converting data to JSON: {str(e)}")
        raise

    # Push the JSON array string to XCom
    context['task_instance'].xcom_push(key=f'{table_name}_records_json', value=records_json_str)
    return records_json_str

# Task to log ingestion status
def log_ingestion_status(**context):
    table_name = "sat_employee"
    records_json_str = context['task_instance'].xcom_pull(key=f'{table_name}_records_json')
    
    if not records_json_str:
        logging.error(f"No records found in XCom for table: {table_name}")
        return False
    
    try:
        record_count = len(json.loads(records_json_str))  # Count number of records in the JSON array
        logging.info(f"Completed ingestion of {record_count} records into Druid for table: {table_name}")
    except Exception as e:
        logging.error(f"Error reading JSON data from XCom: {str(e)}")
        raise
    
    return True

# DAG Definition
with DAG(
        dag_id='postgres_to_druid_sat_employee',
        default_args=default_args,
        description='Extract data from PostgreSQL and ingest into Druid',
        schedule_interval=None,
        catchup=False,
        tags=["postgres", "druid"],
        template_searchpath=[SPEC_PATH]
    ) as dag:

    # Extract data task
    extract_data = PythonOperator(
        task_id='extract_data_sat_employee',
        python_callable=extract_data_from_postgres,
    )

    # Ingest data into Druid
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

    # Log completion task
    log_completion = PythonOperator(
        task_id='log_completion_sat_employee',
        python_callable=log_ingestion_status,
    )

    # Task dependencies
    extract_data >> ingest_to_druid >> log_completion
