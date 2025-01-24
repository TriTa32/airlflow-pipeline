from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.models import Variable
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

    # Query the data
    sql_query = f"SELECT * FROM public.{table_name};"
    df = pg_hook.get_pandas_df(sql_query)

    # Ensure `load_datetime` is in ISO 8601 format
    df['load_datetime'] = pd.to_datetime(df['load_datetime']).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Convert the DataFrame to NDJSON
    ndjson_data = df.to_json(orient='records', lines=True)

    # Log and push the NDJSON to XCom
    logging.info(f"NDJSON Data for Ingestion (first 500 chars): {ndjson_data[:500]}")
    context['task_instance'].xcom_push(key='sat_employee_records_json', value=ndjson_data)

    return ndjson_data


def log_ingestion_status(**context):
    """Log the status of data ingestion"""
    table_name = "sat_employee"
    records_json_str = context['task_instance'].xcom_pull(key=f'{table_name}_records_json')
    record_count = len(json.loads(records_json_str))  # Count number of records in the JSON array
    logging.info(f"Completed ingestion of {record_count} records into Druid for table: {table_name}")
    return True

def log_inline_data(**context):
    # Retrieve INLINE_DATA from XCom
    task_instance = context['task_instance']
    inline_data = task_instance.xcom_pull(key='sat_employee_records_json')
    
    # Log the INLINE_DATA
    if inline_data:
        logging.info(f"INLINE_DATA retrieved from XCom (first 500 chars): {inline_data[:500]}...")
    else:
        logging.error("INLINE_DATA is empty or missing. Check the upstream task for issues.")
        raise ValueError("No INLINE_DATA found to pass to Druid.")

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
   
    log_inline_data_task = PythonOperator(
        task_id='log_inline_data',
        python_callable=log_inline_data,
    )

    ingest_to_druid = DruidOperator(
        task_id='ingest_to_druid_sat_employee',
        json_index_file='sat_employee_index.json',
        druid_ingest_conn_id='druid_default',
        max_ingestion_time=3600,
        params=dict(
            DATA_SOURCE='sat_employee',
            INLINE_DATA="{\"sat_employee_hk\":\"SAT_EMP_HASH_001\",\"hub_employee_hk\":\"EMP_HASH_001\",\"load_datetime\":\"2025-01-09T05:00:00.000Z\",\"record_source\":\"SYSTEM\",\"hash_diff\":\"HD_EMP_001\",\"employee_account\":\"acct001\",\"employee_name\":\"Alice Johnson\",\"employee_email\":\"alice.johnson@example.com\",\"employee_location\":\"New York\"}\n{\"sat_employee_hk\":\"SAT_EMP_HASH_002\",\"hub_employee_hk\":\"EMP_HASH_002\",\"load_datetime\":\"2025-01-09T05:01:00.000Z\",\"record_source\":\"CRM\",\"hash_diff\":\"HD_EMP_002\",\"employee_account\":\"acct002\",\"employee_name\":\"Bob Smith\",\"employee_email\":\"bob.smith@example.com\",\"employee_location\":\"Chicago\"}\n{\"sat_employee_hk\":\"SAT_EMP_HASH_003\",\"hub_employee_hk\":\"EMP_HASH_003\",\"load_datetime\":\"2025-01-09T05:02:00.000Z\",\"record_source\":\"HR_FEED\",\"hash_diff\":\"HD_EMP_003\",\"employee_account\":\"acct003\",\"employee_name\":\"Charlie Brown\",\"employee_email\":\"charlie.brown@example.com\",\"employee_location\":\"Seattle\"}\n{\"sat_employee_hk\":\"SAT_EMP_HASH_004\",\"hub_employee_hk\":\"EMP_HASH_004\",\"load_datetime\":\"2025-01-09T05:03:00.000Z\",\"record_source\":\"SYSTEM\",\"hash_diff\":\"HD_EMP_004\",\"employee_account\":\"acct004\",\"employee_name\":\"Diana Prince\",\"employee_email\":\"diana.prince@example.com\",\"employee_location\":\"Miami\"}\n{\"sat_employee_hk\":\"SAT_EMP_HASH_005\",\"hub_employee_hk\":\"EMP_HASH_005\",\"load_datetime\":\"2025-01-09T05:04:00.000Z\",\"record_source\":\"HR_FEED\",\"hash_diff\":\"HD_EMP_005\",\"employee_account\":\"acct005\",\"employee_name\":\"Ethan Hunt\",\"employee_email\":\"ethan.hunt@example.com\",\"employee_location\":\"Los Angeles\"}\n{\"sat_employee_hk\":\"SAT_EMP_HASH_006\",\"hub_employee_hk\":\"EMP_HASH_006\",\"load_datetime\":\"2025-01-09T05:05:00.000Z\",\"record_source\":\"CRM\",\"hash_diff\":\"HD_EMP_006\",\"employee_account\":\"acct006\",\"employee_name\":\"Fiona Chen\",\"employee_email\":\"fiona.chen@example.com\",\"employee_location\":\"Boston\"}\n{\"sat_employee_hk\":\"SAT_EMP_HASH_007\",\"hub_employee_hk\":\"EMP_HASH_007\",\"load_datetime\":\"2025-01-09T05:06:00.000Z\",\"record_source\":\"SYSTEM\",\"hash_diff\":\"HD_EMP_007\",\"employee_account\":\"acct007\",\"employee_name\":\"George Patel\",\"employee_email\":\"george.patel@example.com\",\"employee_location\":\"San Francisco\"}\n{\"sat_employee_hk\":\"SAT_EMP_HASH_008\",\"hub_employee_hk\":\"EMP_HASH_008\",\"load_datetime\":\"2025-01-09T05:07:00.000Z\",\"record_source\":\"EXCEL_IMPORT\",\"hash_diff\":\"HD_EMP_008\",\"employee_account\":\"acct008\",\"employee_name\":\"Hannah Lee\",\"employee_email\":\"hannah.lee@example.com\",\"employee_location\":\"Chicago\"}\n{\"sat_employee_hk\":\"SAT_EMP_HASH_009\",\"hub_employee_hk\":\"EMP_HASH_009\",\"load_datetime\":\"2025-01-09T05:08:00.000Z\",\"record_source\":\"HR_FEED\",\"hash_diff\":\"HD_EMP_009\",\"employee_account\":\"acct009\",\"employee_name\":\"Isaac Newton\",\"employee_email\":\"isaac.newton@example.com\",\"employee_location\":\"Austin\"}\n{\"sat_employee_hk\":\"SAT_EMP_HASH_010\",\"hub_employee_hk\":\"EMP_HASH_010\",\"load_datetime\":\"2025-01-09T05:09:00.000Z\",\"record_source\":\"SYSTEM\",\"hash_diff\":\"HD_EMP_010\",\"employee_account\":\"acct010\",\"employee_name\":\"Julia Roberts\",\"employee_email\":\"julia.roberts@example.com\",\"employee_location\":\"Denver\"}"
        )
    )

    log_completion = PythonOperator(
        task_id='log_completion_sat_employee',
        python_callable=log_ingestion_status,
    )

    # Task dependencies
    extract_data >> log_inline_data_task >> ingest_to_druid >> log_completion
