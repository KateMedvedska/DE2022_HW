"""
Customers processing pipeline
"""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from final_project.table_defs.user_profiles_jsonl import user_profiles_jsonl

project_id = 'de2022-kate-medvedska'


DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="process_user_profiles",
    description="Process user profiles data",
    schedule_interval='@once',
    start_date=dt.datetime(2022, 10, 1),
    catchup=True,
    tags=['customers', 'de2022'],
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
)

dag.doc_md = __doc__

user_profiles_from_dl_to_silver = BigQueryInsertJobOperator(
    task_id='user_profiles_from_dl_to_silver',
    dag=dag,
    location='us-east1',
    project_id=project_id,
    configuration={
        "query": {
            "query": "{% include '/sql/user_profiles_from_dl_raw_to_silver.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "user_profiles_jsonl": user_profiles_jsonl,
            },
        }
    },
    params={
        'data_lake_raw_bucket': "de2022-final-project-raw",
        'project_id': project_id
    }
)

user_profiles_from_dl_to_silver
