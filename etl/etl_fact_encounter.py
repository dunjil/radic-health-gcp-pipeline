import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2

class ReadFromPostgres(beam.DoFn):
    def process(self, element):
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

class TransformEncounter(beam.DoFn):
    def process(self, record):
        # Optionally apply any transformation logic to the encounter record
        # For example: calculate length_of_stay if not present
        if record.get("admission_date") and record.get("discharge_date") and not record.get("length_of_stay"):
            from datetime import datetime
            try:
                admission = datetime.fromisoformat(str(record["admission_date"]))
                discharge = datetime.fromisoformat(str(record["discharge_date"]))
                record["length_of_stay"] = (discharge - admission).days
            except Exception:
                record["length_of_stay"] = None
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
            | 'ReadFromPostgres' >> beam.ParDo(ReadFromPostgres())
            | 'TransformEncounter' >> beam.ParDo(TransformEncounter())
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                'radic-healthcare.healthcare_dataset.fact_encounter',
                schema='encounter_id:INTEGER,date_id:DATE,patient_id:INTEGER,provider_id:INTEGER,facility_id:INTEGER,primary_diagnosis_id:INTEGER,encounter_type:STRING,admission_date:TIMESTAMP,discharge_date:TIMESTAMP,length_of_stay:INTEGER,total_charges:FLOAT,total_payments:FLOAT,insurance_type:STRING,load_timestamp:TIMESTAMP',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
