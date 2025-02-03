from airflow import DAG
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import logging

# Set data directory and file path
DATA_DIR = "/tmp/druid_ingestion/"
DATA_FILE = os.path.join(DATA_DIR, "sat_employee.json")

# Ensure directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Function to extract PostgreSQL data and save as JSON
def extract_data_and_save():
    try:
        table_name = "sat_employee"
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        sql_query = f"SELECT * FROM public.{table_name};"
        df = pg_hook.get_pandas_df(sql_query)

        if df.empty:
            raise ValueError(f"No data retrieved from {table_name}")

        # Format datetime properly
        df["load_datetime"] = pd.to_datetime(df["load_datetime"]).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Convert to JSON and save (newline-delimited JSON format for Druid)
        df.to_json(DATA_FILE, orient="records", lines=True)

        logging.info(f"Data saved to {DATA_FILE}, total records: {len(df)}")

    except Exception as e:
        logging.error(f"Error in extract_data_and_save: {str(e)}")
        raise

# Define ingestion spec (static)
ingestion_spec = {
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
            "inputSource": {
                "type": "local",
                "baseDir": DATA_DIR,
                "filter": "sat_employee.json",
            },
            "inputFormat": {"type": "json"},
        },
        "tuningConfig": {"type": "index_parallel"},
    },
}

# Define DAG
with DAG(
    dag_id="postgres_to_druid_file_ingestion",
    default_args=default_args,
    description="Extract PostgreSQL data, save as file, and ingest into Druid",
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "druid"],
) as dag:

    extract_data = PythonOperator(
        task_id="extract_data_and_save",
        python_callable=extract_data_and_save,
    )

    ingest_to_druid = DruidOperator(
        task_id="ingest_to_druid",
        json_index_file=json.dumps(ingestion_spec),  # Pass JSON directly
        druid_ingest_conn_id="druid_default",
    )

    # Define task dependency
    extract_data >> ingest_to_druid
