"""
Enrich customers data pipeline
"""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


project_id = 'de2022-kate-medvedska'


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="enrich_user_profiles",
    description="Enrich customers data by user profiles",
    schedule_interval=None,
    start_date=dt.datetime(2022, 10, 1),
    catchup=False,
    tags=['customers', 'de2022'],
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
)

dag.doc_md = __doc__

enrich_customers_to_gold = BigQueryInsertJobOperator(
    task_id='enrich_customers_to_gold',
    dag=dag,
    location='us-east1',
    project_id=project_id,
    configuration={
        "query": {
            "query": "{% include '/sql/customers_enrich_to_gold.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': project_id
    }
)


enrich_customers_to_gold
