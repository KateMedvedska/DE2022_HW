
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

from general.custom_config import cache_folder

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['k.o.medvedska@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': 60,
}

raw_dir = os.path.join(cache_folder, "raw", "sales", '{{ ds }}')
stg_dir = os.path.join(cache_folder, "stg", "sales", '{{ ds }}')


dag = DAG(
    dag_id='process_sales_http',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    schedule_interval='0 1 * * *',
    catchup=True,
    default_args=DEFAULT_ARGS,
)

extract_data_from_api = SimpleHttpOperator(
    task_id='extract_data_from_api',
    http_conn_id='extract_sales_data_from_api_job',
    method='POST',
    data={
        'date': '{{ ds }}',
        'raw_dir': raw_dir,
    },
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.status_code == 201,
    dag=dag
)

convert_to_avro = SimpleHttpOperator(
    task_id='convert_to_avro',
    http_conn_id='convert_to_avro_job',
    method='POST',
    data={
        'date': '{{ ds }}',
        'raw_dir': raw_dir,
        'stg_dir': stg_dir,
    },
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.status_code == 201,
    dag=dag
)


extract_data_from_api >> convert_to_avro
