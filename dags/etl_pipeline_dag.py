from datetime import datetime, timedelta
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator, DataflowConfiguration

# Constants
GCP_PROJECT_ID = 'radic-healthcare'
GCP_LOCATION = 'us-east1'
TEMP_LOCATION = 'gs://bucket-radic-healthcare/temp/'
STAGING_LOCATION = 'gs://bucket-radic-healthcare/staging/'
ETL_SCRIPTS_PATH = 'gs://bucket-radic-healthcare/etl/'

default_args = {
    'owner': 'radichealth',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_sql_from_gcs(**kwargs):
    """Downloads SQL from GCS"""
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
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
    description='Daily ETL using Dataflow Python jobs',
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
        gcp_conn_id='google_cloud_default',
    )

    etl_tasks = []
    for etl_script in [
        'etl_diagnosis',
        'etl_facility',
        'etl_fact_encounter',
        'etl_patient',
        'etl_provider'
    ]:
        dataflow_task = BeamRunPythonPipelineOperator(
            py_file=f"{ETL_SCRIPTS_PATH}{etl_script}.py",
            dataflow_config=DataflowConfiguration(job_name=f"{etl_script}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",project_id=GCP_PROJECT_ID, location=GCP_LOCATION,),
            gcp_conn_id='google_cloud_default',
            runner='DataflowRunner',
            pipeline_options={
                "tempLocation": TEMP_LOCATION,
                "stagingLocation": STAGING_LOCATION,
                "project": GCP_PROJECT_ID,
                "region": GCP_LOCATION,
            },
            # py_requirements=['apache-beam[gcp]'],  # Add your dependencies here
            py_interpreter='python3',
            py_system_site_packages=False,
        )
        etl_tasks.append(dataflow_task)

    end = EmptyOperator(task_id='end')

    # Set dependencies
    start >> get_sql >> create_schema >> etl_tasks >> end
