import sys
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

sys.path.append("/opt/airflow/script_etl")

from etl_to_dwh import run_etl_to_dwh
from dwh_to_mart import run_dwh_to_mart
from mart_report import run_mart_report


with DAG(
    dag_id='etl_stg_to_dwh_to_mart',
    start_date=datetime(2025, 4, 19),
    schedule_interval='30 3 * * *',  # Runs once a day: at 3.30 AM
    catchup=False
) as dag:
    

    start_task = EmptyOperator(
        task_id='start',
        owner='Gugus'
    )

    task_etl_to_dwh = PythonOperator(
        task_id='task_etl_to_dwh',
        python_callable=run_etl_to_dwh,
    )

    task_dwh_to_mart = PythonOperator(
        task_id='task_dwh_to_mart',
        python_callable=run_dwh_to_mart,
    )

    task_mart_report = PythonOperator(
        task_id='task_mart_report',
        python_callable=run_mart_report,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> task_etl_to_dwh >> task_dwh_to_mart >> task_mart_report >> end_task
