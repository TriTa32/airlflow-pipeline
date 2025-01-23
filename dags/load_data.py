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
print(SPEC_PATH)
def extract_data_from_postgres(**context):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql_query = """
        SELECT 
            *
        FROM public.sat_employee;
    """
    
    try:
        df = pg_hook.get_pandas_df(sql_query)
        
        df['load_datetime'] = pd.to_datetime(df['load_datetime'])
        
        # Save to temporary file for Druid ingestion
        temp_file = f"/tmp/employees_data_{context['execution_date'].strftime('%Y%m%d')}.json"
        
        # Convert DataFrame to Druid-compatible JSON format
        records = df.to_dict('records')
        with open(temp_file, 'w') as f:
            for record in records:
                record['load_datetime'] = record['load_datetime'].isoformat()
                f.write(json.dumps(record) + '\n')
        
        # Store the file path and record count in XCom
        context['task_instance'].xcom_push(key='data_file', value=temp_file)
        context['task_instance'].xcom_push(key='record_count', value=len(df))
        
        return temp_file
        
    except Exception as e:
        logging.error(f"Error extracting data from PostgreSQL: {str(e)}")
        raise

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
        max_ingestion_time=3600,  # Maximum time to wait for ingestion (in seconds)
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
