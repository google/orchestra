###########################################################################
#
#  Copyright 2018 Google Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################


from gcs_hook import GoogleCloudStorageHook
from airflow import models

import tempfile
import os
import logging
import time
import requests

logger = logging.getLogger(__name__)


class DV360DownloadReportByFilePathOperator(models.BaseOperator):
  """Download report file, this is the same as the CM report file downloader
  but doesn't require profile id.

  **Note that files are called reports in DV360 and reports are called queries.

  """

  def __init__(self,
               conn_id='google_cloud_default',
               list_report_task_id=None,
               gcs_bucket=None,
               query_id=1,
               report_type='',
               *args,
               **kwargs):
    super(DV360DownloadReportByFilePathOperator, self).__init__(*args, **kwargs)
    self.conn_id = conn_id
    self.service = None
    self.gcs_bucket = gcs_bucket
    self.query_id = query_id
    self.report_type = report_type

  def execute(self, context):
    url = context['task_instance'].xcom_pull(
        task_ids=None,
        dag_id=self.dag.dag_id,
        key='%s_dv360_report_file_path' % self.report_type)
    try:
      report_file = tempfile.NamedTemporaryFile(delete=False)
      file_download = requests.get(url, stream=True)
      for chunk in file_download.iter_content(chunk_size=1024 * 1024):
        report_file.write(chunk)
      report_file.close()
      headers = False
      with open(report_file.name, 'r') as f:
        for line in f:
          if not headers:
            headers = line
            headers = headers.strip().split(',')
            break

      context['task_instance'].xcom_push(
          '%s_cm_report_schema' % self.report_type, headers)
      logger.info('Report written to file: %s', report_file.name)
      gcs_hook = GoogleCloudStorageHook(
          google_cloud_storage_conn_id='google_cloud_default')
      filename = 'report_%d.csv' % (time.time() * 1e+9)
      logger.info('GCS_bucket = %s', self.gcs_bucket)
      gcs_hook.upload(
          self.gcs_bucket, filename, report_file.name, multipart=True)
      context['task_instance'].xcom_push(
          '%s_gcs_file_uris' % self.report_type,
          ['gs://%s/%s' % (self.gcs_bucket, filename)])
    finally:
      if report_file:
        report_file.close()
        os.unlink(report_file.name)
