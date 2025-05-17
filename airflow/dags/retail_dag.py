from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os


project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, project_root)


# Import the job functions
from dw.scripts.data_generator.generator import main as job1_main
from dw.scripts.lambda_function.json_to_parquet import main as job2_main

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "retail_dag",
    default_args=default_args,
    description="Run two jobs sequentially",
    schedule=timedelta(days=1),  # Runs daily
    start_date=datetime(2025, 5, 10),
    catchup=False,
    tags=["jobs"],
) as dag:

    run_job1 = PythonOperator(
        task_id="run_job1",
        python_callable=job1_main,
    )

    run_job2 = PythonOperator(
        task_id="run_job2",
        python_callable=job2_main,
    )

    # Set the dependency - job2 runs after job1 completes
    run_job1 >> run_job2

