from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import sys


sys.path.append(os.path.join(os.path.dirname(__file__), "scripts"))

import cms_script

bucket = 'zoomcamp-bucket-storage'
blob_name = 'cms_data/'
project_id = 'DEzoomcamp-project1'
#csv_correct_input = f'/home/eman/virtual_env/files/cms_open/OWNRSHP_{year}.csv'
#csv_corrected = f'home/eman/virtual_env/files/cms_open/OWNRSHP_{year}_corrected.csv'

default_args = {
    'owner': 'eman',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id = 'cms_dag',
    default_args = default_args,
    description = 'uploads CMS Openpayments to GCS',
    start_date=datetime(2025, 4, 6),
    schedule_interval='@once',
    catchup=False
) as dag:

    download_local = PythonOperator(
    task_id = 'first_task',
    python_callable = cms_script.down_up_gcs
    )
    
    correct_csv = PythonOperator(
    task_id = 'correct_csv',
    python_callable = cms_script.correct_csv_format,
    op_args = [2017, 2024]
    )
    
    upload_GCS = PythonOperator(
    task_id = 'uploadgcs',
    python_callable = cms_script.upload_to_gcs,    
    op_args = [bucket, blob_name]    
    )
    
    GCS_to_BQ = PythonOperator(
    task_id = 'GCS_to_BQ',
    python_callable = cms_script.GCS_to_BQ,
    op_args = [bucket, project_id]   
    )

    download_local >> correct_csv >> upload_GCS >> GCS_to_BQ