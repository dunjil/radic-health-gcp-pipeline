import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReadDiagnoses(beam.DoFn):
    def process(self, element):
        try:
            conn = psycopg2.connect(
                dbname="radichealthcare_rearburied",
                user="radichealthcare_rearburied",
                password="0faa3d7a3228960d4e6049300dfce8887de942b2",
                host="olye3.h.filess.io",
                port="5433"
            )
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM healthcare.diagnoses")
                    columns = [desc[0] for desc in cursor.description]
                    for row in cursor.fetchall():
                        yield dict(zip(columns, row))
        except Exception as e:
            logger.error(f"Error reading from database: {e}")
            raise

class TransformDiagnosis(beam.DoFn):
    def process(self, record: Dict[str, Any]):
        try:
            record['is_current'] = True if record.get('expiration_date') is None else False
            return [record]
        except Exception as e:
            logger.error(f"Error transforming record: {e}")
            raise

def run_pipeline():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='radic-healthcare',
        temp_location='gs://bucket-radic-healthcare/tmp/',
        region='us-central1'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Start' >> beam.Create([None])
            | 'ReadDiagnoses' >> beam.ParDo(ReadDiagnoses())
            | 'TransformDiagnosis' >> beam.ParDo(TransformDiagnosis())
            | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                table='radic-healthcare.healthcare_dataset.diagnoses',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == '__main__':
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"Pipeline failed with exception: {e}")
