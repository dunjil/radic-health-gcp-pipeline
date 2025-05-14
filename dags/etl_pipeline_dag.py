from datetime import datetime, timedelta
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePipelineOperator

default_args = {
    'owner': 'radichealth',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def make_dataflow_task(task_id, script_name):
    return DataflowCreatePipelineOperator(
        task_id=task_id,
        body={
            "name": f'{task_id}-{{{{ ds_nodash }}}}',  # Adding the required 'name' field
            "jobName": f'{task_id}-{{{{ ds_nodash }}}}',
            "projectId": 'radic-healthcare',
            "location": 'us-central1',
            "environment": {
                "tempLocation": 'gs://bucket-radic-healthcare/temp/',
                "stagingLocation": 'gs://bucket-radic-healthcare/staging/',
            },
            "pipeline": {
                "script": f'gs://bucket-radic-healthcare/etl/{script_name}.py',
                "options": {
                    'runner': 'DataflowRunner',
                    # Add other options as needed
                }
            }
        },
        project_id='radic-healthcare',  # Adding explicit project_id
        location='us-central1',  # Adding explicit location
    )

with models.DAG(
    dag_id='radichealth_etl_daily',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    description='Daily ETL from PostgreSQL to BigQuery using Dataflow',
    tags=['radichealth', 'etl', 'dataflow', 'bigquery']
) as dag:

    start = EmptyOperator(task_id='start')

    create_schema = BigQueryInsertJobOperator(
        task_id='create_star_schema',
        configuration={
            "query": {
                "query": "gs://bucket-radic-healthcare/sql/create_star_schema.sql",
                "useLegacySql": False
            }
        },
        location="US"
    )

    # Define ETL tasks
    etl_tasks = [
        make_dataflow_task('etl_diagnosis', 'etl_diagnosis'),
        make_dataflow_task('etl_facility', 'etl_facility'),
        make_dataflow_task('etl_fact_encounter', 'etl_fact_encounter'),
        make_dataflow_task('etl_patient', 'etl_patient'),
        make_dataflow_task('etl_provider', 'etl_provider'),
    ]

    end = EmptyOperator(task_id='end')

    # DAG dependencies
    start >> create_schema >> etl_tasks >> end