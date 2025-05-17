from datetime import datetime, timedelta
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePipelineOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

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
            "name": f'{task_id}-{{{{ ds_nodash }}}}', 
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
        project_id='radic-healthcare',
        location='us-central1',
    )

# Function to read SQL from GCS
def get_sql_from_gcs(**context):
    gcs_hook = GCSHook()
    sql_content = gcs_hook.download_as_byte_array(
        bucket_name='bucket-radic-healthcare',
        object_name='sql/create_star_schema.sql'
    ).decode('utf-8')
    return sql_content

with models.DAG(
    dag_id='radichealth_etl_daily',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    description='Daily ETL from PostgreSQL to BigQuery using Dataflow',
    tags=['radichealth', 'etl', 'dataflow', 'bigquery']
) as dag:

    start = EmptyOperator(task_id='start')
    
    # First, get the SQL content from GCS
    get_sql = PythonOperator(
        task_id='get_sql_content',
        python_callable=get_sql_from_gcs,
        provide_context=True,
    )
    
    # Then execute the SQL in BigQuery
    create_schema = BigQueryInsertJobOperator(
        task_id='create_star_schema',
        configuration={
            "query": {
                "query": "{{ task_instance.xcom_pull(task_ids='get_sql_content') }}",
                "useLegacySql": False
            }
        },
        location="us-central1",
        project_id='radic-healthcare',  # Explicitly specify the project ID
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
    start >> get_sql >> create_schema >> etl_tasks >> end