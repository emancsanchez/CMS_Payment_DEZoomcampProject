from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

default_args = {
    'owner': 'eman',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id = 'weather_dag',
    default_args = default_args,
    description = 'weather related dag that runs every hour',
    start_date=datetime(2025, 4, 6),
    schedule_interval='@hourly'
) as dag:
    
    script_path = "/home/eman/virtual_env/scripts/DataProjectScript.py"

    task1 = BashOperator(
    task_id = 'first_task',
    bash_command =f'python3 {script_path}'
    )

    task1