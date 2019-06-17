###########################################################################
#
#  Copyright 2019 Google Inc.
#
#  Licensed under the Apache License, Version 2.0 (the 'License');
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an 'AS IS' BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################

import logging
import os
from random import randint
import tempfile
import time
from airflow import models
from hooks.bq_hook import BigQueryBaseCursor
from hooks.bq_hook import BigQueryHook
from hooks.dv360_hook import DV360Hook
from hooks.gcs_hook import GoogleCloudStorageHook
from schema.sdf import SDF_VERSIONED_SCHEMA_TYPES
logger = logging.getLogger(__name__)

DEFAULT_SDF_TABLE_NAMES = {
    'LINE_ITEM': 'SDFLineItem',
    'AD_GROUP': 'SDFAdGroup',
    'AD': 'SDFAd',
    'INSERTION_ORDER': 'SDFInsertionOrder',
    'CAMPAIGN': 'SDFCampaign'
}

SDF_API_RESPONSE_KEYS = {
    'LINE_ITEM': 'lineItems',
    'AD_GROUP': 'adGroups',
    'AD': 'ads',
    'INSERTION_ORDER': 'insertionOrders',
    'CAMPAIGN': 'campaigns'
}


class DV360SDFToBQOperator(models.BaseOperator):
  """Make a request to SDF API and upload the data to BQ."""

  def __init__(self,
               conn_id='google_cloud_default',
               gcs_bucket=None,
               schema=None,
               bq_dataset=None,
               write_disposition=None,
               cloud_project_id=None,
               file_types=None,
               filter_ids=None,
               api_version=None,
               filter_type=None,
               table_names=DEFAULT_SDF_TABLE_NAMES,
               sdf_api_response_keys=SDF_API_RESPONSE_KEYS,
               *args,
               **kwargs):
    super(DV360SDFToBQOperator, self).__init__(*args, **kwargs)
    self.conn_id = conn_id
    self.service = None
    self.gcs_bucket = gcs_bucket
    self.schema = schema
    self.bq_dataset = bq_dataset
    self.write_disposition = write_disposition
    self.cloud_project_id = cloud_project_id
    self.file_types = file_types
    self.filter_ids = filter_ids
    self.api_version = api_version
    self.filter_type = filter_type
    self.table_names = table_names
    self.sdf_api_response_keys = sdf_api_response_keys

  def execute(self, context):
    if (self.service == None):
      hook = DV360Hook(dv360_conn_id=self.conn_id)
      self.service = hook.get_service()

    request_body = {
        'fileTypes': [],
        'filterType': '',
        'filterIds': [],
        'version': ''
    }
    request_body['fileTypes'] = self.file_types
    request_body['filterIds'] = self.filter_ids
    request_body['version'] = self.api_version
    request_body['filterType'] = self.filter_type
    logger.info('Request body: %s ' % request_body)
    request = self.service.sdf().download(body=request_body)
    response = request.execute()
    gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.conn_id)

    for file_type in self.file_types:
      temp_file = None
      try:
        logger.info('Uploading SDF to GCS')
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        response_key = self.sdf_api_response_keys.get(file_type)
        temp_file.write(response[response_key].encode('utf-8'))
        temp_file.close()
        filename = '%d_%s_%s_%s.json' % (time.time() * 1e+9, randint(
            1, 1000000), response_key, 'sdf')
        gcs_hook.upload(self.gcs_bucket, filename, temp_file.name)
        logger.info('SDF upload to GCS complete')
      finally:
        if temp_file:
          temp_file.close()
        os.unlink(temp_file.name)

      sdf_file = 'gs://%s/%s' % (self.gcs_bucket, filename)

      hook = BigQueryHook(bigquery_conn_id=self.conn_id)
      self.service = hook.get_service()
      bq_table = self.table_names.get(file_type)
      bq_table = '%s.%s' % (self.bq_dataset, bq_table)
      schema = SDF_VERSIONED_SCHEMA_TYPES.get(self.api_version).get(file_type)
      try:
        bq_base_cursor = BigQueryBaseCursor(self.service, self.cloud_project_id)
        logger.info('Uploading SDF to BigQuery')
        bq_base_cursor.run_load(
            bq_table,
            schema, [sdf_file],
            source_format='CSV',
            skip_leading_rows=1,
            write_disposition=self.write_disposition)
      finally:
        logger.info('Deleting SDF from GCS')
        gcs_hook.delete(self.gcs_bucket, filename)
