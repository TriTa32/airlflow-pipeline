from airflow import DAG
from airflow.providers.apache.druid.operators.druid import DruidOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "druid_ingestion_operator_test",
    default_args=default_args,
    description="DAG to ingest data into Druid using DruidOperator",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    ingest_data_to_druid = DruidOperator(
        task_id="ingest_data_to_druid",
        json_index_file="druid-ingestion-spec.json",
        druid_ingest_conn_id="druid_default",
    )


    ingest_data_to_druid
