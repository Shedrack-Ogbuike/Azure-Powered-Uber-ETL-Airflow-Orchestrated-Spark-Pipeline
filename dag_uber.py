from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from Extraction import run_extraction
from Transformation import run_transformation
from Loading import run_loading
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Default DAG arguments
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define DAG
dag = DAG(
    dag_id="uber_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for Uber ride bookings",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
)

# Define a single Python callable that runs the full ETL
def etl_task():
    # Extract
    df = run_extraction("/home/wonder1844/airflow/uber_project_dag/raw_data/ncr_ride_bookings.csv")
    
    if df is None:
        raise ValueError("Extraction failed, DataFrame is None")
    
    # Transform
    bookings, customers, drivers, locations = run_transformation(df)
    
    # Load
    run_loading(bookings, customers, drivers, locations)

# Airflow operator
etl_operator = PythonOperator(
    task_id="uber_etl",
    python_callable=etl_task,
    dag=dag
)

