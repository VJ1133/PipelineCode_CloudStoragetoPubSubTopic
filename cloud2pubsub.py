import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.pubsub import WriteToPubSub
import os
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/2023 Plan B Data Engineering and Data Analysis/Final Project/pipeline-1133-96e19741fd40.json'

# Set the GCP project ID and Pub/Sub topic name
project_id = 'pipeline-1133'
topic_name = 'uber-topic-new'

# Define the pipeline options
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.region = 'us-central1'
google_cloud_options.job_name = 'json-to-pubsub-test6'
google_cloud_options.staging_location = 'gs://databucket_uber/staging'
google_cloud_options.temp_location = 'gs://databucket_uber/temp'
options.view_as(StandardOptions).streaming = True  # set the pipeline to streaming mode
options.view_as(StandardOptions).runner = 'DataflowRunner'
options.view_as(SetupOptions).save_main_session = True

pipeline = beam.Pipeline(options=options)

# Read JSON file and parse each line to JSON object
json_lines = (pipeline
              | 'Read JSON' >> ReadFromText('gs://databucket_uber/uber_dataset.json')
              | 'Parse JSON' >> beam.Map(json.loads)
             )

# Convert JSON objects to bytes and write them to Pub/Sub
json_messages = (json_lines
                 | 'Convert to Bytes' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
                 | 'Write to Pub/Sub' >> WriteToPubSub(topic='projects/{project_id}/topics/{topic_name}'.format(project_id=project_id, topic_name=topic_name))
                )

result = pipeline.run()
