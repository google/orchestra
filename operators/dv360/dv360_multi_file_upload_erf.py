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

import logging
from airflow import models
from bq_hook import BigQueryBaseCursor
from bq_hook import BigQueryHook
from utils.download_and_transform_erf import download_and_transform_erf
from gcs_hook import GoogleCloudStorageHook


logger = logging.getLogger(__name__)

class DV360MultiERFUploadBqOperator(models.BaseOperator):
  """Upload Multiple Entity Read Files to specified big query dataset.
  """

  def __init__(self,
               conn_id='google_cloud_default',
               profile_id=-1,
               report_body=None,
               yesterday=False,
               entity_type=None,
               file_creation_date=None,
               cloud_project_id=None,
               bq_table=None,
               schema=None,
               gcs_bucket=None,
               erf_bucket=None,
               *args,
               **kwargs):
    super(DV360MultiERFUploadBqOperator, self).__init__(*args, **kwargs)
    self.conn_id = conn_id
    self.service = None
    self.report_body = report_body
    self.erf_bucket = erf_bucket
    self.yesterday = yesterday
    self.cloud_project_id = cloud_project_id
    self.bq_table = bq_table
    self.gcs_bucket = gcs_bucket
    self.schema = schema
    self.entity_type = entity_type
    self.erf_object = 'entity/%s.0.%s.json' % (file_creation_date, entity_type)
    self.file_creation_date = file_creation_date

  def execute(self, context):
    gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.conn_id)
    partner_ids = models.Variable.get('partner_ids').split(',')
    for i, partner_id in enumerate(partner_ids):
      filename = download_and_transform_erf(self, partner_id)
      entity_read_file_ndj = 'gs://%s/%s' % (self.gcs_bucket, filename)
      hook = BigQueryHook(bigquery_conn_id=self.conn_id)
      self.service = hook.get_service()
      if i == 0:
        write_disposition = 'WRITE_TRUNCATE'
      else:
        write_disposition = 'WRITE_APPEND'

      bq_base_cursor = BigQueryBaseCursor(self.service, self.cloud_project_id)
      bq_base_cursor.run_load(
          self.bq_table,
          self.schema, [entity_read_file_ndj],
          source_format='NEWLINE_DELIMITED_JSON',
          write_disposition=write_disposition)
      gcs_hook.delete(self.gcs_bucket, filename)
