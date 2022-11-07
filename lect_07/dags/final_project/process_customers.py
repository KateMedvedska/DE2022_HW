"""
Customers processing pipeline
"""
import datetime as dt

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from final_project.table_defs.customers_csv import customers_csv

project_id = 'de2022-kate-medvedska'


DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="process_customers",
    description="Process customers data",
    schedule_interval='0 8 * * *',
    start_date=dt.datetime(2022, 8, 1),
    end_date=dt.datetime(2022, 8, 6),
    catchup=True,
    tags=['customers', 'de2022'],
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
)

dag.doc_md = __doc__

customers_from_dl_to_bronze = BigQueryInsertJobOperator(
    task_id='customers_from_dl_to_bronze',
    dag=dag,
    location='us-east1',
    project_id=project_id,
    configuration={
        "query": {
            "query": "{% include '/sql/customers_from_dl_raw_to_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "customers_csv": customers_csv,
            },
        }
    },
    params={
        'data_lake_raw_bucket': "de2022-final-project-raw",
        'project_id': project_id
    }
)

customers_from_bronze_to_silver = BigQueryInsertJobOperator(
    task_id='customers_from_bronze_to_silver',
    dag=dag,
    location='us-east1',
    project_id=project_id,
    configuration={
        "query": {
            "query": "{% include '/sql/customers_from_bronze_to_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': project_id
    }
)

trigger_enrich_user_profiles = TriggerDagRunOperator(
    task_id='trigger_enrich_user_profiles',
    trigger_dag_id='enrich_user_profiles',
)

customers_from_dl_to_bronze >> customers_from_bronze_to_silver >> trigger_enrich_user_profiles
