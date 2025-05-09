from datetime import datetime, timedelta
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.operators.dummy import DummyOperator

# Define default arguments
default_args = {
    'owner': 'radichealth',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with models.DAG(
    dag_id='radichealth_etl_daily',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    description='Daily ETL from PostgreSQL to BigQuery using Dataflow',
    tags=['radichealth', 'etl', 'dataflow', 'bigquery']
) as dag:

    start = DummyOperator(task_id='start')

    # Launch Dataflow job from template (you can also use Python/Java code instead of template)
    run_dataflow_etl = DataflowTemplatedJobStartOperator(
        task_id='run_dataflow_etl',
        template='gs://bucket-radic-healthcare/templates/postgres_to_bq_template',
        project_id='radic-healthcare',
        region='us-central1',
        job_name='radichealth-etl-{{ ds_nodash }}',
        parameters={
            'inputDatabase': 'radichealthcare_rearburied',
            'inputHost': 'olye3.h.filess.io',
            'inputPort': '5433',
            'inputUser': 'radichealthcare_rearburied',
            'inputPassword': '0faa3d7a3228960d4e6049300dfce8887de942b2',
            'outputTable': 'your_project:healthcare_dataset.fact_encounter',
            'runner': 'DataflowRunner',
        }
    )

    end = DummyOperator(task_id='end')

    # DAG structure
    start >> run_dataflow_etl >> end
