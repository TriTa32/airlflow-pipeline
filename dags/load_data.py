from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
import json
import os

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Druid ingestion spec (parameterized)
def get_ingestion_spec(inline_data):
    return {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": "sat_employee",
                "timestampSpec": {"column": "load_datetime", "format": "iso"},
                "dimensionsSpec": {
                    "dimensions": [
                        {"type": "string", "name": "sat_employee_hk"},
                        {"type": "string", "name": "hub_employee_hk"},
                        {"type": "string", "name": "record_source"},
                        {"type": "string", "name": "hash_diff"},
                        {"type": "string", "name": "employee_account"},
                        {"type": "string", "name": "employee_name"},
                        {"type": "string", "name": "employee_email"},
                        {"type": "string", "name": "employee_location"},
                    ]
                },
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": "NONE",
                    "rollup": False,
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {"type": "inline", "data": inline_data},
                "inputFormat": {"type": "json"},
            },
            "tuningConfig": {
                "type": "index_parallel",
                "partitionsSpec": {"type": "dynamic", "maxRowsPerSegment": 5000000},
                "maxRowsInMemory": 1000000,
            },
        },
    }

# Extract data from PostgreSQL
def extract_data_from_postgres(**context):
    try:
        table_name = "sat_employee"
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        sql_query = f"SELECT * FROM public.{table_name};"
        df = pg_hook.get_pandas_df(sql_query)

        if df.empty:
            raise ValueError(f"No data retrieved from {table_name}")

        # Format datetime
        df["load_datetime"] = pd.to_datetime(df["load_datetime"]).dt.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )

        # Convert to JSON string
        json_data = df.to_json(orient="records")

        # Log the actual data being pushed
        logging.info(f"Pushing data to XCom (first 500 chars): {json_data[:500]}")
        logging.info(f"Total records: {len(df)}")

        # Push to XCom
        context["task_instance"].xcom_push(key="sat_employee_records_json", value=json_data)

        return json_data

    except Exception as e:
        logging.error(f"Error in extract_data_from_postgres: {str(e)}")
        raise

# Debug XCom data
def debug_xcom_data(**context):
    data = context["task_instance"].xcom_pull(key="sat_employee_records_json")
    logging.info(f"XCom data type: {type(data)}")
    logging.info(f"XCom data preview: {data[:500]}")
    try:
        parsed = json.loads(data)
        logging.info(f"Valid JSON with {len(parsed)} records")
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {str(e)}")
        raise

# Log ingestion status
def log_ingestion_status(**context):
    """Log the status of data ingestion"""
    table_name = "sat_employee"
    records_json_str = context["task_instance"].xcom_pull(key="sat_employee_records_json")
    if records_json_str:
        record_count = len(json.loads(records_json_str))
        logging.info(f"Completed ingestion of {record_count} records into Druid for table: {table_name}")
    else:
        logging.error("No data found in XCom")
    return True

# Define DAG
with DAG(
    dag_id="postgres_to_druid_sat_employee",
    default_args=default_args,
    description="Extract data from PostgreSQL and ingest into Druid",
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "druid"],
) as dag:

    extract_data = PythonOperator(
        task_id="extract_data_sat_employee",
        python_callable=extract_data_from_postgres,
    )

    debug_task = PythonOperator(
        task_id="debug_xcom_data",
        python_callable=debug_xcom_data,
    )

    def prepare_druid_ingestion(**context):
        inline_data = context["task_instance"].xcom_pull(key="sat_employee_records_json")
        if not inline_data:
            raise ValueError("No data found in XCom for ingestion")
        return get_ingestion_spec(inline_data)

    prepare_druid_spec = PythonOperator(
        task_id="prepare_druid_spec",
        python_callable=prepare_druid_ingestion,
        provide_context=True,
    )

    ingest_to_druid = DruidOperator(
        task_id="ingest_to_druid_sat_employee",
        json_index_file="{{ task_instance.xcom_pull(task_ids='prepare_druid_spec') }}",
        druid_ingest_conn_id="druid_default",
        max_ingestion_time=3600,
    )
    
    log_completion = PythonOperator(
        task_id="log_completion_sat_employee",
        python_callable=log_ingestion_status,
    )

    # Task dependencies
    extract_data >> debug_task >> prepare_druid_spec >> ingest_to_druid >> log_completion
