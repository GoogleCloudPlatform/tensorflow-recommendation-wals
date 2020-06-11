# Copyright 2017 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


usage () {
  echo "usage: mltrain.sh [local | train | tune] [gs://]job_and_data_dir [path_to/]<input_file>.csv
                  [--data-type ratings|web_views]
                  [--delimiter <delim>]
                  [--use-optimized]
                  [--headers]

Use 'local' to train locally with a local data file, and 'train' and 'tune' to
run on ML Engine.  For ML Engine cloud jobs the data_dir must be prefixed with
gs:// and point to an existing bucket, and the input file must reside on GCS.

Optional args:
  --data-type:      Default to 'ratings', meaning MovieLens ratings from 0-5.
                    Set to 'web_views' for Google Analytics data.
  --delimiter:      CSV delimiter, default to '\t'.
  --use-optimized:  Use optimized hyperparamters, default False.
  --headers:        Default False for 'ratings', True for 'web_views'.

Examples:

# train locally with unoptimized hyperparams on included web views data set
./mltrain.sh local ../data recommendation_events.csv --data-type web_views

# train on ML Engine with optimized hyperparams
./mltrain.sh train gs://rec_serve data/recommendation_events.csv --data-type web_views --use-optimized

# tune hyperparams on ML Engine:
./mltrain.sh tune gs://rec_serve data/recommendation_events.csv --data-type web_views
"

}

date

TIME=`date +"%Y%m%d_%H%M%S"`

# change to your preferred region
REGION=us-central1

if [[ $# < 3 ]]; then
  usage
  exit 1
fi

# set job vars
TRAIN_JOB="$1"
BUCKET="$2"
DATA_FILE="$3"
JOB_NAME=wals_ml_${TRAIN_JOB}_${TIME}

# add additional args
shift; shift; shift

if [[ ${TRAIN_JOB} == "local" ]]; then

  ARGS="--train-file $BUCKET/${DATA_FILE} --verbose-logging $@"

  mkdir -p jobs/${JOB_NAME}

  gcloud ai-platform local train \
    --module-name trainer.task \
    --package-path trainer \
    -- \
    --job-dir jobs/${JOB_NAME} \
    ${ARGS}

elif [[ ${TRAIN_JOB} == "train" ]]; then

  ARGS="--gcs-bucket $BUCKET --train-file ${DATA_FILE} --verbose-logging $@"

  gcloud ai-platform jobs submit training ${JOB_NAME} \
    --region $REGION \
    --scale-tier=CUSTOM \
    --job-dir ${BUCKET}/jobs/${JOB_NAME} \
    --module-name trainer.task \
    --package-path trainer \
    --config trainer/config/config_train.json \
    -- \
    ${ARGS}

elif [[ $TRAIN_JOB == "tune" ]]; then

  ARGS="--gcs-bucket $BUCKET --train-file ${DATA_FILE} --verbose-logging $@"

  # set configuration for tuning
  CONFIG_TUNE="trainer/config/config_tune.json"
  for i in $ARGS ; do
    if [[ "$i" == "web_views" ]]; then
      CONFIG_TUNE="trainer/config/config_tune_web.json"
      break
    fi
  done

  gcloud ai-platform jobs submit training ${JOB_NAME} \
    --region ${REGION} \
    --scale-tier=CUSTOM \
    --job-dir ${BUCKET}/jobs/${JOB_NAME} \
    --module-name trainer.task \
    --package-path trainer \
    --config ${CONFIG_TUNE} \
    -- \
    --hypertune \
    ${ARGS}

else
  usage
fi

date
