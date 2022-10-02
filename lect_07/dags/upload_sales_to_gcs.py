
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from general.custom_config import cache_folder
from upload_to_gcs import upload_folder

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 60,
}

sales_dir = os.path.join(cache_folder, 'sales', '{{ ds }}')
year = "{{ execution_date.strftime('%Y') }}"
month = "{{ execution_date.strftime('%m') }}"
day = "{{ execution_date.strftime('%d') }}"


dag = DAG(
    dag_id='upload_sales_to_gcs',
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 3),
    schedule_interval='0 10 * * *',
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=['de2022'],
    max_active_runs=1,
)

upload_files = PythonOperator(
    task_id='upload_files',
    python_callable=upload_folder,
    op_kwargs={
        'bucket_name': 'de2022-hw-lect-10',
        'source_folder_path': sales_dir,
        'destination_folder_name': os.path.join(
            'src1/sales/v1',
            f'year={year}',
            f'month={month}',
            f'day={day}'
        ),
    },
    dag=dag
)


upload_files