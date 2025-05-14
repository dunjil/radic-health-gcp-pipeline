import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2

class ReadDiagnoses(beam.DoFn):
    def process(self, element):
        conn = psycopg2.connect(
            dbname="radichealthcare_rearburied",
            user="radichealthcare_rearburied",
            password="0faa3d7a3228960d4e6049300dfce8887de942b2",
            host="olye3.h.filess.io",
            port="5433"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM healthcare.diagnosis")
        columns = [desc[0] for desc in cursor.description]
        for row in cursor.fetchall():
            yield dict(zip(columns, row))
        cursor.close()
        conn.close()

class TransformDiagnosis(beam.DoFn):
    def process(self, record):
        record['is_current'] = True if record.get('expiration_date') is None else False
        return [record]

def run():
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
            | 'Transform' >> beam.ParDo(TransformDiagnosis())
            | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                'radic-healthcare.healthcare_dataset.dim_diagnosis',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == '__main__':
    run()
