from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Define a function to simulate pipeline execution
def run_pipeline(**kwargs):
    # Simulate some pipeline logic
    print("Running the pipeline...")
    # Simulate success or failure
    success = True  # Change to False to simulate failure
    if not success:
        raise ValueError("Pipeline failed!")
    print("Pipeline executed successfully.")

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    dag_id="pipeline_health_check",
    default_args=default_args,
    description="A DAG to check the pipeline running success",
    schedule_interval=None,  # Runs daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "pipeline-check"],
) as dag:

    # Task to run the pipeline
    run_pipeline_task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )

    # Task to indicate pipeline success
    success_task = DummyOperator(
        task_id="pipeline_success"
    )

    # Task dependencies
    run_pipeline_task >> success_task
