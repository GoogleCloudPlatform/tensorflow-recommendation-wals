import argparse
import datetime
import os
import os.path
import tempfile

import tensorflow as tf
import apache_beam as beam
from apache_beam.io import tfrecordio, textio
import tensorflow_transform as tft
from tensorflow_transform.beam import impl as beam_impl
from tensorflow_transform.coders import example_proto_coder
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import dataset_schema
from tensorflow_transform.beam.tft_beam_io import transform_fn_io

BUCKET = "recserve-tft-test"
PROJECT_ID = "lramsey-recserve-composer"
DATASET = 'GA360_test'
TABLE_NAME = 'ga_sessions_sample'
ARTICLE_CUSTOM_DIMENSION = '10'


QUERY = '''
#legacySql
SELECT
 fullVisitorId as clientId,
 ArticleID as contentId,
 (nextTime - hits.time) as timeOnPage,
FROM(
  SELECT
    fullVisitorId,
    hits.time,
    MAX(IF(hits.customDimensions.index={0},
           hits.customDimensions.value,NULL)) WITHIN hits AS ArticleID,
    LEAD(hits.time, 1) OVER (PARTITION BY fullVisitorId, visitNumber
                             ORDER BY hits.time ASC) as nextTime
  FROM [{1}.{2}.{3}]
  WHERE hits.type = "PAGE"
) HAVING timeOnPage is not null and contentId is not null;
'''


def preprocess_tft(rowdict):
  median = 57937 #tft.quantiles(rowdict['session_duration'], 11, epsilon=0.001)[5]
  result = {
    'userId' : tft.string_to_int(rowdict['clientId'], vocab_filename='vocab_users'),
    'itemId' : tft.string_to_int(rowdict['contentId'], vocab_filename='vocab_items'),
    'rating' : rowdict['timeOnPage']
  }

  # 'rating' : 0.3 * (1 + (rowdict['timeOnPage'] - median)/median)
  # cap the rating at 1.0
  #result['rating'] = tf.where(tf.less(result['rating'], tf.ones(tf.shape(result['rating']))),
  #                           result['rating'], tf.ones(tf.shape(result['rating'])))

  return result

def create_vocab_and_rating_files(output_dir, raw_data, raw_data_metadata):
  raw_dataset = (raw_data, raw_data_metadata)
  transformed_dataset, transform_fn = (
      raw_dataset | beam_impl.AnalyzeAndTransformDataset(preprocess_tft))
  transformed_data, transformed_metadata = transformed_dataset

  # write vocab files
  _ = (transform_fn
       | 'WriteTransformFn' >>
       transform_fn_io.WriteTransformFn(os.path.join(output_dir, 'transform_fn')))

  # write ratings
  class CsvTupleCoder(beam.coders.Coder):
    def encode(self, value):
      return ",".join(map(str,value))
  csv_coder = CsvTupleCoder()
  _ = (transformed_data
       | 'map_ratings' >> beam.Map(lambda x : (x['userId'], x['itemId'], x['rating']))
       | 'WriteTransformedData' >> textio.WriteToText(os.path.join(output_dir, 'ratings.csv'),
            coder=csv_coder, num_shards=1, shard_name_template=''))

  return transformed_data

def write_count(a, outdir, basename):
  filename = os.path.join(outdir, basename)
  (a
       | '{}_1'.format(basename) >> beam.Map(lambda x: (1, 1))
       | '{}_2'.format(basename) >> beam.combiners.Count.PerKey()
       | '{}_3'.format(basename) >> beam.Map(lambda (k, v): v)
       | '{}_write'.format(basename) >> beam.io.WriteToText(file_path_prefix=filename, num_shards=1))

def to_tfrecord(key_vlist, indexCol):
  (key, vlist) = key_vlist
  return {
    'key': [key],
    'indices': [value[indexCol] for value in vlist],
    'values':  [value['rating'] for value in vlist]
  }

def create_wals_estimator_inputs(output_dir, transformed_data):
  # do a group-by to create users_for_item and items_for_user
  users_for_item = (transformed_data
                    | 'map_items' >> beam.Map(lambda x : (x['itemId'], x))
                    | 'group_items' >> beam.GroupByKey()
                    | 'totfr_items' >> beam.Map(lambda item_userlist : to_tfrecord(item_userlist, 'userId')))
  items_for_user = (transformed_data
                    | 'map_users' >> beam.Map(lambda x : (x['userId'], x))
                    | 'group_users' >> beam.GroupByKey()
                    | 'totfr_users' >> beam.Map(lambda item_userlist : to_tfrecord(item_userlist, 'itemId')))

  output_schema = {
    'key' : dataset_schema.ColumnSchema(tf.int64, [1], dataset_schema.FixedColumnRepresentation()),
    'indices': dataset_schema.ColumnSchema(tf.int64, [], dataset_schema.ListColumnRepresentation()),
    'values': dataset_schema.ColumnSchema(tf.float32, [], dataset_schema.ListColumnRepresentation())
  }

  _ = users_for_item | 'users_for_item' >> tfrecordio.WriteToTFRecord(
      os.path.join(output_dir, 'users_for_item'),
      coder=example_proto_coder.ExampleProtoCoder(
          dataset_schema.Schema(output_schema)))
  _ = items_for_user | 'items_for_user' >> tfrecordio.WriteToTFRecord(
      os.path.join(output_dir, 'items_for_user'),
      coder=example_proto_coder.ExampleProtoCoder(
          dataset_schema.Schema(output_schema)))

  write_count(users_for_item, output_dir, 'nitems')
  write_count(items_for_user, output_dir, 'nusers')


def preprocess(query, in_test_mode):

  job_name = 'preprocess-wals-features' + '-' + datetime.datetime.now().strftime('%y%m%d-%H%M%S')
  if in_test_mode:
    import shutil
    print 'Launching local Beam job ...'
    output_dir = './preproc_tft'
    shutil.rmtree(output_dir, ignore_errors=True)
  else:
    print 'Launching Dataflow job {} ... '.format(job_name)
    output_dir = 'gs://{0}/wals/preproc_tft/'.format(BUCKET)
    import subprocess
    subprocess.call('gsutil rm -r {}'.format(output_dir).split())

  options = {
    'staging_location': os.path.join(output_dir, 'tmp', 'staging'),
    'temp_location': os.path.join(output_dir, 'tmp'),
    'job_name': job_name,
    'project': PROJECT_ID,
    'max_num_workers': 24,
    'teardown_policy': 'TEARDOWN_ALWAYS',
    'save_main_session': False,
    'requirements_file': 'requirements.txt'
  }
  opts = beam.pipeline.PipelineOptions(flags=[], **options)
  if in_test_mode:
    RUNNER = 'DirectRunner'
  else:
    RUNNER = 'DataflowRunner'

  # set up metadata
  raw_data_schema = {
    colname : dataset_schema.ColumnSchema(tf.string, [], dataset_schema.FixedColumnRepresentation())
                   for colname in 'clientId,contentId'.split(',')
  }
  raw_data_schema.update({
    'timeOnPage' : dataset_schema.ColumnSchema(tf.int64, [], dataset_schema.FixedColumnRepresentation())
  })

  #raw_data_schema.update({
  #    colname : dataset_schema.ColumnSchema(tf.float32, [], dataset_schema.FixedColumnRepresentation())
  #                 for colname in 'timeOnPage'.split(',')
  #  })
  raw_data_metadata = dataset_metadata.DatasetMetadata(dataset_schema.Schema(raw_data_schema))

  # run Beam
  with beam.Pipeline(RUNNER, options=opts) as p:
    with beam_impl.Context(temp_dir=os.path.join(output_dir, 'tmp')):
      # read raw data
      raw_data = (p
                  | 'read' >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=False)))

      # create vocab files
      transformed_data = create_vocab_and_rating_files(output_dir, raw_data, raw_data_metadata)

      # do a group-by to create users_for_item and items_for_user
      #create_wals_estimator_inputs(output_dir, transformed_data)


def parse_arguments():
  """Parse job arguments."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--test-mode',
      default=False,
      action='store_true',
      help='Set test-mode on, run locally.'
  )

  args = parser.parse_args()

  return args


def main(args):
  query = QUERY.format(ARTICLE_CUSTOM_DIMENSION, PROJECT_ID, DATASET, TABLE_NAME)
  preprocess(query, args.test_mode)


if __name__ == '__main__':
  args = parse_arguments()
  main(args)
