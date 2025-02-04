from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_prepare_druid_spec(**context):
    try:
        # Extract data from PostgreSQL
        table_name = "sat_employee"
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        # Execute query and fetch data
        sql_query = f"SELECT * FROM public.{table_name};"
        df = pg_hook.get_pandas_df(sql_query)

        if df.empty:
            raise ValueError(f"No data retrieved from {table_name}")

        # Ensure load_datetime is in ISO format
        df['load_datetime'] = pd.to_datetime(df['load_datetime']).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Convert DataFrame to newline-delimited JSON string
        inline_data = '\n'.join(df.to_json(orient='records', lines=True).splitlines())

        # Prepare ingestion specification
        ingestion_spec = {
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "sat_employee",
                    "timestampSpec": {
                        "column": "load_datetime", 
                        "format": "iso"
                    },
                    "dimensionsSpec": {
                        "dimensions": [
                            "sat_employee_hk",
                            "hub_employee_hk",
                            "record_source",
                            "hash_diff",
                            "employee_account",
                            "employee_name",
                            "employee_email",
                            "employee_location"
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
                        "type": "inline",
                        "data": inline_data
                    },
                    "inputFormat": {"type": "json"},
                },
                "tuningConfig": {
                    "type": "index_parallel",
                    "maxRowsPerSegment": 5000000,
                    "maxRowsInMemory": 100000
                }
            }
        }
        
        # Return the specification as a JSON string
        return json.dumps(ingestion_spec)

    except Exception as e:
        print(f"Error in extract_and_prepare_druid_spec: {str(e)}")
        raise

# Define DAG
with DAG(
    dag_id="postgres_to_druid_inline_ingestion",
    default_args=default_args,
    description="Extract PostgreSQL data and ingest into Druid with inline data",
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "druid"],
) as dag:
    
    # Prepare data and ingestion specification
    prepare_ingestion = PythonOperator(
        task_id="prepare_data_and_spec",
        python_callable=extract_and_prepare_druid_spec,
        provide_context=True,
    )

    # Ingest data to Druid
    ingest_to_druid = DruidOperator(
        task_id="ingest_to_druid",
        json_index_file="{{ task_instance.xcom_pull(task_ids='prepare_data_and_spec') }}",
        druid_ingest_conn_id="druid_default",
    )

    # Define task dependency
    prepare_ingestion >> ingest_to_druid