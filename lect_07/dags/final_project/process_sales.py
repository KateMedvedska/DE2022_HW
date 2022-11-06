"""
Sales processing pipeline
"""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from final_project.table_defs.sales_csv import sales_csv

project_id = 'de2022-kate-medvedska'


DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="process_sales",
    description="Process sales data",
    schedule_interval='0 7 * * *',
    start_date=dt.datetime(2022, 9, 1),
    end_date=dt.datetime(2022, 10, 1),
    catchup=True,
    tags=['sales'],
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
)

dag.doc_md = __doc__


sales_from_dl_to_bronze = BigQueryInsertJobOperator(
    task_id='sales_from_dl_to_bronze',
    dag=dag,
    location='us-east1',
    project_id=project_id,
    configuration={
        "query": {
            "query": "{% include '/sql/sales_from_dl_raw_to_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "sales_csv": sales_csv,
            },
        }
    },
    params={
        'data_lake_raw_bucket': "de2022-final-project-raw",
        'project_id': project_id
    }
)


sales_from_bronze_to_silver = BigQueryInsertJobOperator(
    task_id='sales_from_bronze_to_silver',
    dag=dag,
    location='us-east1',
    project_id=project_id,
    configuration={
        "query": {
            "query": "{% include 'sql/sales_from_bronze_to_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': project_id
    }
)


sales_from_dl_to_bronze >> sales_from_bronze_to_silver
