
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from check_jobs import run_job1, run_job2
from general.custom_config import cache_folder


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 60,
}

raw_dir = os.path.join(cache_folder, "raw", "sales", '{{ ds }}')
stg_dir = os.path.join(cache_folder, "stg", "sales", '{{ ds }}')


dag = DAG(
    dag_id='process_sales',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    schedule_interval='0 1 * * *',
    catchup=True,
    default_args=DEFAULT_ARGS,
)

extract_data_from_api = PythonOperator(
    task_id='extract_data_from_api',
    python_callable=run_job1,
    op_kwargs={
        'date': '{{ ds }}',
        'raw_dir': raw_dir,
    },
    dag=dag
)

convert_to_avro = PythonOperator(
    task_id='convert_to_avro',
    python_callable=run_job2,
    op_kwargs={
        'date': '{{ ds }}',
        'raw_dir': raw_dir,
        'stg_dir': stg_dir,
    },
    dag=dag
)

extract_data_from_api >> convert_to_avro
