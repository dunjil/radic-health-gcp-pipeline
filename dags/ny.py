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
GCP_LOCATION = 'us-central1'
TEMP_LOCATION = 'gs://bucket-radic-healthcare/temp/'
STAGING_LOCATION = 'gs://bucket-radic-healthcare/staging/'
DATAFLOW_TEMPLATE_BUCKET = 'gs://bucket-radic-healthcare/dataflow-templates/'

default_args = {
    'owner': 'radichealth',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dataflow_default_options': {
        'project': GCP_PROJECT_ID,
        'region': GCP_LOCATION,
        'tempLocation': TEMP_LOCATION,
    }
}

def make_dataflow_task(task_id, template_name):
    """Creates a pair of operators to create and run a Dataflow pipeline"""
    pipeline_name = f"{task_id}-pipeline"
    job_name = f"{task_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    return [
        # Create the pipeline definition
        DataflowCreatePipelineOperator(
            task_id=f'create_{task_id}_pipeline',
            project_id=GCP_PROJECT_ID,
            location=GCP_LOCATION,
            body={
                "name": f"projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}/pipelines/{pipeline_name}",
                "type": "PIPELINE_TYPE_BATCH",
                "workload": {
                    "dataflowFlexTemplateRequest": {
                        "launchParameter": {
                            "containerSpecGcsPath": f"{DATAFLOW_TEMPLATE_BUCKET}{template_name}.json",
                            "jobName": job_name,
                            "environment": {
                                "tempLocation": TEMP_LOCATION,
                                "stagingLocation": STAGING_LOCATION,
                                "serviceAccountEmail": f"dataflow-sa@{GCP_PROJECT_ID}.iam.gserviceaccount.com"
                            },
                            "parameters": {
                                "input": f"gs://bucket-radic-healthcare/input/{task_id}/",
                                "output": f"gs://bucket-radic-healthcare/output/{task_id}/"
                            },
                        },
                        "projectId": GCP_PROJECT_ID,
                        "location": GCP_LOCATION,
                    }
                },
            },
            gcp_conn_id='google_cloud_default'
        ),
        # Execute the pipeline
        DataflowRunPipelineOperator(
            task_id=f'run_{task_id}_pipeline',
            pipeline_name=pipeline_name,
            project_id=GCP_PROJECT_ID,
            gcp_conn_id='google_cloud_default'
        )
    ]

def get_sql_from_gcs(**context):
    """Downloads SQL from GCS"""
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
    description='Daily ETL using Dataflow Pipelines API',
    tags=['radichealth', 'etl', 'dataflow']
) as dag:

    start = EmptyOperator(task_id='start')
    
    # Get SQL content from GCS
    get_sql = PythonOperator(
        task_id='get_sql_content',
        python_callable=get_sql_from_gcs,
        provide_context=True,
    )
    
    # Execute the SQL in BigQuery
    create_schema = BigQueryInsertJobOperator(
        task_id='create_star_schema',
        configuration={
            "query": {
                "query": "{{ task_instance.xcom_pull(task_ids='get_sql_content') }}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": 'radic_healthcare',
                    "tableId": 'star_schema'
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        },
        location=GCP_LOCATION,
        project_id=GCP_PROJECT_ID,
        gcp_conn_id='google_cloud_default'
    )

    # Create all Dataflow tasks
    etl_operations = []
    for task_id, template_name in [
        ('etl_diagnosis', 'diagnosis-template'),
        ('etl_facility', 'facility-template'),
        ('etl_fact_encounter', 'encounter-template'),
        ('etl_patient', 'patient-template'),
        ('etl_provider', 'provider-template')
    ]:
        create_task, run_task = make_dataflow_task(task_id, template_name)
        etl_operations.extend([create_task, run_task])

    end = EmptyOperator(task_id='end')

    # Define workflow
    start >> get_sql >> create_schema >> etl_operations >> end