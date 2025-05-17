from datetime import datetime, timedelta
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowCreatePipelineOperator,
    DataflowRunPipelineOperator
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Constants
GCP_PROJECT_ID = 'radic-healthcare'
GCP_LOCATION = 'us-east1'
TEMP_LOCATION = 'gs://bucket-radic-healthcare/temp/'
STAGING_LOCATION = 'gs://bucket-radic-healthcare/staging/'
PIPELINE_ROOT = 'gs://bucket-radic-healthcare/pipeline-root/'

default_args = {
    'owner': 'radichealth',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def make_dataflow_pipeline_tasks(task_id, template_name):
    """
    Creates a pair of operators to create and run a Dataflow pipeline using the Pipelines API.
    Assumes you have a Dataflow Flex Template JSON spec stored in GCS at:
    gs://bucket-radic-healthcare/templates/{template_name}.json
    """

    pipeline_name = f"{task_id}-pipeline"
    job_name = f"{task_id}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    # Create pipeline operator - creates a pipeline resource in Dataflow
    create_pipeline = DataflowCreatePipelineOperator(
        task_id=f'create_{task_id}_pipeline',
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        pipeline_id=pipeline_name,
        body={
            "type": "PIPELINE_TYPE_BATCH",
            "displayName": f"{task_id} pipeline",
            "labels": {
                "env": "prod",
                "team": "data"
            },
            "workload": {
                "dataflowFlexTemplateRequest": {
                    "launchParameter": {
                        "containerSpecGcsPath": f"gs://bucket-radic-healthcare/templates/{template_name}.json",
                        "jobName": job_name,
                        "parameters": {
                            "tempLocation": TEMP_LOCATION,
                            "stagingLocation": STAGING_LOCATION,
                            # Add other pipeline-specific parameters here if needed
                        },
                        "environment": {
                            "tempLocation": TEMP_LOCATION,
                            "stagingLocation": STAGING_LOCATION,
                        }
                    }
                }
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    # Run pipeline operator - triggers execution of the created pipeline
    run_pipeline = DataflowRunPipelineOperator(
        task_id=f'run_{task_id}_pipeline',
        pipeline_id=pipeline_name,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        gcp_conn_id='google_cloud_default'
    )

    return create_pipeline, run_pipeline

def get_sql_from_gcs(**kwargs):
    """Downloads SQL from GCS"""
    gcs_hook = GCSHook()
    # Use download method (returns bytes), then decode
    sql_bytes = gcs_hook.download(
        bucket_name='bucket-radic-healthcare',
        object_name='sql/create_star_schema.sql'
    )
    return sql_bytes.decode('utf-8')

with models.DAG(
    dag_id='radichealth_etl_daily',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    description='Daily ETL using Dataflow Pipelines API',
    tags=['radichealth', 'etl', 'dataflow']
) as dag:

    start = EmptyOperator(task_id='start')

    get_sql = PythonOperator(
        task_id='get_sql_content',
        python_callable=get_sql_from_gcs,
    )

    create_schema = BigQueryInsertJobOperator(
        task_id='create_star_schema',
        configuration={
            "query": {
                "query": "{{ task_instance.xcom_pull(task_ids='get_sql_content') }}",
                "useLegacySql": False
            }
        },
        location="us-central1",
        project_id=GCP_PROJECT_ID,
    )

    etl_operations = []
    for task_id, template_name in [
        ('etl_diagnosis', 'diagnosis-template'),
        ('etl_facility', 'facility-template'),
        ('etl_fact_encounter', 'encounter-template'),
        ('etl_patient', 'patient-template'),
        ('etl_provider', 'provider-template')
    ]:
        create_task, run_task = make_dataflow_pipeline_tasks(task_id, template_name)
        etl_operations.extend([create_task, run_task])

    end = EmptyOperator(task_id='end')

    # Define dependencies
    start >> get_sql >> create_schema >> etl_operations >> end
