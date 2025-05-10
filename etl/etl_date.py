# import argparse
# import logging
# import calendar
# from datetime import datetime, timedelta

# import apache_beam as beam
# from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

# # Helper to generate date dimension rows
# def generate_dates(start_date: str, end_date: str):
#     start = datetime.strptime(start_date, "%Y-%m-%d")
#     end = datetime.strptime(end_date, "%Y-%m-%d")
#     delta = timedelta(days=1)

#     while start <= end:
#         yield {
#             'date_id': start.strftime("%Y-%m-%d"),
#             'day': start.day,
#             'month': start.month,
#             'year': start.year,
#             'quarter': (start.month - 1) // 3 + 1,
#             'day_of_week': start.weekday(),
#             'day_name': calendar.day_name[start.weekday()],
#             'month_name': calendar.month_name[start.month],
#             'is_weekend': start.weekday() >= 5,
#         }
#         start += delta

# def run():
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--start_date', required=True)
#     parser.add_argument('--end_date', required=True)
#     parser.add_argument('--output_table', required=True)
#     parser.add_argument('--project', required=True)
#     parser.add_argument('--region', required=True)
#     parser.add_argument('--temp_location', required=True)
#     known_args, pipeline_args = parser.parse_known_args()

#     pipeline_options = PipelineOptions(pipeline_args)
#     pipeline_options.view_as(GoogleCloudOptions).project = known_args.project
#     pipeline_options.view_as(GoogleCloudOptions).region = known_args.region
#     pipeline_options.view_as(GoogleCloudOptions).temp_location = known_args.temp_location
#     pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

#     with beam.Pipeline(options=pipeline_options) as p:
#         (
#             p
#             | 'Generate Dates' >> beam.Create(generate_dates(known_args.start_date, known_args.end_date))
#             | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
#                 known_args.output_table,
#                 schema='date_id:DATE,day:INTEGER,month:INTEGER,year:INTEGER,quarter:INTEGER,day_of_week:INTEGER,day_name:STRING,month_name:STRING,is_weekend:BOOLEAN',
#                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
#                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
#             )
#         )

# if __name__ == '__main__':
#     logging.getLogger().setLevel(logging.INFO)
#     run()
