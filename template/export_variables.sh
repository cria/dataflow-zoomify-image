#! /bin/bash
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PROJECT=your-project
REGION=us-central1
INPUT_BUCKET=your-input-bucket
INPUT_FOLDER=your/input/folder/with/original/images
OUTPUT_BUCKET=your-output-bucket
OUTPUT_FOLDER=your/output/folder/where/tiles/will/be/stored
BIGQUERY_PROJECT=your-bigquery-project-name
BIGQUERY_DATASET=your-bigquery-dataset
BIGQUERY_TABLE=your-bigquery-table-with-image-metadata (barcode, imagecode, filename, path, width, height, md5)
FINAL_BUCKET=your-final-bucket-to-move-original-input-images
FILE_EXTENSIONS=jpg,png,tif
NUM_WORKERS=10
STAGING_BUCKET=your-staging-bucket
STAGING_FOLDER=your/staging/folder
TEMP_BUCKET=your-temp-bucket
TEMP_FOLDER=your/temp/folder
GOOGLE_APPLICATION_CREDENTIALS=/your/key/path.json
JOB_NAME=job-name
