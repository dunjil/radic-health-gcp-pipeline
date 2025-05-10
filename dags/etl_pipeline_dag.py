from datetime import datetime, timedelta
from airflow import models
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator

default_args = {
    'owner': 'radichealth',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def make_dataflow_task(task_id, script_name):
    return DataflowTemplatedJobStartOperator(
        task_id=task_id,
        template='gs://bucket-radic-healthcare/templates/postgres_to_bq_template',
        project_id='radic-healthcare',
        region='us-central1',
        job_name=f'{task_id}-{{{{ ds_nodash }}}}',
        parameters={
            'inputDatabase': 'radichealthcare_rearburied',
            'inputHost': 'olye3.h.filess.io',
            'inputPort': '5433',
            'inputUser': 'radichealthcare_rearburied',
            'inputPassword': '0faa3d7a3228960d4e6049300dfce8887de942b2',
            'outputTable': f'radic-healthcare:healthcare_dataset.{script_name}',
            'runner': 'DataflowRunner',
            'etlScriptsPath': f'gs://bucket-radic-healthcare/etl/{script_name}.py'
        }
    )

with models.DAG(
    dag_id='radichealth_etl_daily',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    description='Daily ETL from PostgreSQL to BigQuery using Dataflow',
    tags=['radichealth', 'etl', 'dataflow', 'bigquery']
) as dag:

    start = DummyOperator(task_id='start')

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
        make_dataflow_task('etl_date', 'etl_date'),
        make_dataflow_task('etl_diagnosis', 'etl_diagnosis'),
        make_dataflow_task('etl_facility', 'etl_facility'),
        make_dataflow_task('etl_fact_encounter', 'etl_fact_encounter'),
        make_dataflow_task('etl_patient', 'etl_patient'),
        make_dataflow_task('etl_provider', 'etl_provider'),
    ]

    end = DummyOperator(task_id='end')

    # DAG dependencies
    start >> create_schema >> etl_tasks >> end
