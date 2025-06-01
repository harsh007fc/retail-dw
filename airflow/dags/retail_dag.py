from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os


project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, project_root)


# Import the job functions
from dw.scripts.data_generator.generator import main as generate
from dw.scripts.lambda_function.json_to_parquet import main as convert
from dw.duckdb.create_external_tables_duckdb import main as update_table
from dw.scripts.silver_scripts.scd2_tables_load import main as scd2_tables
from dw.scripts.silver_scripts.static_table_load import main as static_tables
from dw.scripts.silver_scripts.fact_table_load import main as fact_table

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

    generate_raw = PythonOperator(
        task_id="generate",
        python_callable=generate,
    )

    convert_raw = PythonOperator(
        task_id="convert",
        python_callable=convert,
    )

    update_table = PythonOperator(
        task_id="update_table",
        python_callable=update_table,
    )

    scd2_tables = PythonOperator(
        task_id="scd2_tables",
        python_callable=scd2_tables,
    )

    static_tables = PythonOperator(
        task_id="static_tables",
        python_callable=static_tables,
    )

    fact_table = PythonOperator(
        task_id="fact_table",
        python_callable=fact_table,
    )

    # Set the dependency - job2 runs after job1 completes
    (
        generate_raw
        >> convert_raw
        >> update_table
        >> scd2_tables
        >> static_tables
        >> fact_table
    )
