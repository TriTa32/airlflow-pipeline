from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json

def prepare_druid_spec(table_name, **context):
    try:
        # PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        # Dynamic query for each table
        sql_query = f"SELECT * FROM public.{table_name};"
        df = pg_hook.get_pandas_df(sql_query)

        if df.empty:
            raise ValueError(f"No data retrieved from {table_name}")

        # Prepare table-specific dimensions
        dimensions = list(df.columns)

        # Ensure timestamp is in ISO format (adjust column name as needed)
        if 'load_datetime' in df.columns:
            df['load_datetime'] = pd.to_datetime(df['load_datetime']).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        # Convert DataFrame to newline-delimited JSON string
        inline_data = '\n'.join(df.to_json(orient='records', lines=True).splitlines())

        # Prepare ingestion specification
        ingestion_spec = {
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": table_name,
                    "timestampSpec": {
                        "column": "load_datetime", 
                        "format": "iso"
                    },
                    "dimensionsSpec": {
                        "dimensions": dimensions
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
        print(f"Error preparing spec for {table_name}: {str(e)}")
        raise

def create_postgres_to_druid_dag(
    tables_to_ingest,
    dag_id='postgres_to_druid_dynamic_ingestion',
    schedule_interval=None
):
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description="Dynamic PostgreSQL to Druid ingestion",
        schedule_interval=schedule_interval,
        catchup=False,
        tags=["postgres", "druid", "dynamic_ingest"]
    ) as dag:
        
        for table in tables_to_ingest:
            # Prepare data and ingestion specification
            prepare_ingestion = PythonOperator(
                task_id=f"prepare_{table}_spec",
                python_callable=prepare_druid_spec,
                op_kwargs={'table_name': table},
                provide_context=True
            )

            # Ingest data to Druid
            ingest_to_druid = DruidOperator(
                task_id=f"ingest_{table}_to_druid",
                json_index_file="{{ task_instance.xcom_pull(task_ids='prepare_" + table + "_spec') }}",
                druid_ingest_conn_id="druid_default",
            )

            # Define task dependency
            prepare_ingestion >> ingest_to_druid

    return dag

# List of tables to ingest
tables = [
    'sat_employee', 
    'link_employee_unit', 
    'hub_employee',
    'sat_employee_level'
]

# Generate the DAG
globals()['postgres_to_druid_dynamic_dag'] = create_postgres_to_druid_dag(tables)