#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Example Airflow DAG that shows how to FTP a file from Google Cloud Storage.
"""

from airflow import DAG
from airflow.utils import dates
from orchestra.google.cloud.operators.gcp_gcs_operator import (
  GoogleCloudStorageToFTPOperator
)


BUCKET = "test-ftp-bucket"
FILE = "test_file.csv"

default_args = {"start_date": dates.days_ago(1)}

with DAG(
    "example_gcs_to_ftp",
    default_args=default_args,
    schedule_interval=None
) as dag:
  GoogleCloudStorageToFTPOperator(
      task_id="ftp_file",
      gcs_source_bucket=BUCKET,
      gcs_source_object=FILE,
      ftp_destination_path=FILE
  )
