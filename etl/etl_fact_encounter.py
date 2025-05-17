import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2
import logging
from datetime import datetime

class ReadFromPostgres(beam.DoFn):
    def process(self, element):
        try:
            conn = psycopg2.connect(
                database="radichealthcare_rearburied",
                user="radichealthcare_rearburied",
                password="0faa3d7a3228960d4e6049300dfce8887de942b2",
                host="olye3.h.filess.io",
                port="5433"
            )
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM healthcare.encounters")
            columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                yield dict(zip(columns, row))
            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error reading from PostgreSQL: {e}")
            raise

class TransformEncounter(beam.DoFn):
    def process(self, record):
        try:
            # Calculate length_of_stay
            admission = datetime.fromisoformat(str(record["admission_date"]))
            discharge = datetime.fromisoformat(str(record["discharge_date"]))
            record["length_of_stay"] = (discharge - admission).days

            # Transform the record to match the BigQuery schema
            transformed_record = {
                'encounter_id': str(record['id']),
                'patient_id': str(record['patient_id']),
                'provider_id': str(record['provider_id']),
                'facility_id': str(record['facility_id']),
                'diagnosis_code': str(record['primary_diagnosis_id']),
                'admission_date': record['admission_date'].date(),
                'discharge_date': record['discharge_date'].date(),
                'date_key': int(record['admission_date'].strftime('%Y%m%d')),  # Example date_key format
                'length_of_stay': record['length_of_stay'],
                'total_charges': float(record['total_charges']),
                'payments_received': float(record['total_payments']),
                'insurance_type': record['insurance_type'],
                'referral_provider_id': None  # Assuming no referral_provider_id in the source data
            }
            return [transformed_record]
        except Exception as e:
            logging.error(f"Error transforming record: {e}")
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
            | 'ReadFromPostgres' >> beam.ParDo(ReadFromPostgres())
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                'radic-healthcare.healthcare_dataset.fact_encounter',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
