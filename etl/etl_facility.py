import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2
import logging

class ReadFacilities(beam.DoFn):
    def process(self, element):
        try:
            conn = psycopg2.connect(
                dbname="radichealthcare_rearburied",
                user="radichealthcare_rearburied",
                password="0faa3d7a3228960d4e6049300dfce8887de942b2",
                host="olye3.h.filess.io",
                port="5433"
            )
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM healthcare.facilities")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
        
            for row in rows:
                yield dict(zip([desc[0] for desc in cursor.description], row))

            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error reading from PostgreSQL: {e}")
            raise

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
            | 'ReadFacilities' >> beam.ParDo(ReadFacilities())
            | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                'radic-healthcare.healthcare_dataset.facilities',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == '__main__':
    run()
