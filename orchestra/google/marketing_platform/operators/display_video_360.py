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
import logging
import json
import csv
import os
from random import randint
import tempfile
import time
from urllib.parse import urlparse

import requests
from airflow import models
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.bigquery_hook import BigQueryBaseCursor
from airflow.models import BaseOperator

from orchestra.google.marketing_platform.hooks.display_video_360 import (
    GoogleDisplayVideo360Hook
)
from orchestra.google.marketing_platform.utils import erf_utils

from orchestra.google.marketing_platform.utils.schema.sdf import (
    SDF_VERSIONED_SCHEMA_TYPES
)

logger = logging.getLogger(__name__)


class GoogleDisplayVideo360CreateReportOperator(BaseOperator):
    """Creates and runs a new Display & Video 360 query.

    Attributes:
      report: The query body to create the report from. (templated)
          Can receive a json string representing the report or reference to a
          template file. Template references are recognized by a string ending in
          '.json'.
      api_version: The DV360 API version.
      gcp_conn_id: The connection ID to use when fetching connection info.
      delegate_to: The account to impersonate, if any.

    XComs:
      query_id: The query ID for the report created.
    """

    template_fields = ['params', 'report']
    template_ext = ['.json']

    def __init__(self,
                 report,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleDisplayVideo360CreateReportOperator, self).__init__(*args, **kwargs)
        self.report = report
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = GoogleDisplayVideo360Hook(
                api_version=self.api_version,
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to)

        report_body = json.loads(self.report)
        request = self.hook.get_service().queries().createquery(body=report_body)
        response = request.execute()

        context['task_instance'].xcom_push('query_id', response['queryId'])


class GoogleDisplayVideo360RunReportOperator(BaseOperator):
    """Runs a stored query to generate a report.

    Attributes:
      api_version: The DV360 API version.
      query_id: The ID of the query to run. (templated)
      gcp_conn_id: The connection ID to use when fetching connection info.
      delegate_to: The account to impersonate, if any.
    """

    template_fields = ['query_id']

    def __init__(self,
                 query_id,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleDisplayVideo360RunReportOperator, self).__init__(*args, **kwargs)
        self.api_version = api_version
        self.conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.service = None
        self.query_id = query_id

    def execute(self, context):
        if self.service is None:
            hook = GoogleDisplayVideo360Hook(
                api_version=self.api_version,
                gcp_conn_id=self.conn_id,
                delegate_to=self.delegate_to
            )
            self.service = hook.get_service()

        request = self.service.queries().runquery(
            queryId=self.query_id, body={})
        request.execute()


class GoogleDisplayVideo360DownloadReportOperator(BaseOperator):
    """Downloads a Display & Video 360 report into Google Cloud Storage.

    Attributes:
      report_url: The Google Cloud Storage url where the latest report is stored.
          (templated)
      destination_bucket: The destination Google cloud storage bucket where the
          report should be written to. (templated)
      destination_object: The destination name of the object in the destination
          Google cloud storage bucket. (templated)
          If the destination points to an existing folder, the report will be
          written under the specified folder.
      gcp_conn_id: The connection ID to use when fetching connection info.
      delegate_to: The account to impersonate, if any.

    XComs:
      destination_bucket: The Google cloud storage bucket the report was written
          to.
      destination_object: The Google cloud storage URI for the report.
    """

    template_fields = ['report_url', 'destination_bucket', 'destination_object']

    def __init__(self,
                 report_url,
                 destination_bucket,
                 destination_object=None,
                 chunk_size=5 * 1024 * 1024,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleDisplayVideo360DownloadReportOperator, self).__init__(*args, **kwargs)
        self.report_url = report_url
        self.destination_bucket = destination_bucket
        self.destination_object = destination_object
        self.chunk_size = chunk_size
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.hook = None

    @staticmethod
    def _download_report(source_url, destination_file, chunk_size):
        response = requests.head(source_url)
        content_length = int(response.headers['Content-Length'])

        start_byte = 0
        while start_byte < content_length:
            end_byte = start_byte + chunk_size - 1
            if end_byte >= content_length:
                end_byte = content_length - 1

            headers = {'Range': 'bytes=%s-%s' % (start_byte, end_byte)}
            response = requests.get(source_url, stream=True, headers=headers)
            chunk = response.raw.read()
            destination_file.write(chunk)
            start_byte = end_byte + 1
        destination_file.close()

    @staticmethod
    def _get_destination_uri(destination_object, report_url):
        report_file_name = urlparse(report_url).path.split('/')[2]

        if destination_object is None:
            return report_file_name

        if destination_object.endswith('/'):
            return destination_object + report_file_name

        return destination_object

    def execute(self, context):
        if self.hook is None:
            self.hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to)

        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            # TODO(efolgar): Directly stream to storage instead of temp file
            self._download_report(self.report_url, temp_file, self.chunk_size)
            destination_object_name = self._get_destination_uri(
                self.destination_object, self.report_url)
            self.hook.upload(
                bucket=self.destination_bucket,
                object=destination_object_name,
                filename=temp_file.name,
                multipart=True)

            context['task_instance'].xcom_push(
                'destination_bucket', self.destination_bucket)
            context['task_instance'].xcom_push(
                'destination_object', destination_object_name)
        finally:
            temp_file.close()
            os.unlink(temp_file.name)


class GoogleDisplayVideo360DeleteReportOperator(BaseOperator):
    """Deletes Display & Video 360 queries and any associated reports.

    Attributes:
      api_version: The DV360 API version.
      query_id: The DV360 query id to delete. (templated)
      query_title: The DV360 query title to delete. (templated)
          Any query with a matching title will be deleted.
      ignore_if_missing: If True, return success even if the query is missing.
      gcp_conn_id: The connection ID to use when fetching connection info.
      delegate_to: The account to impersonate, if any.
    """

    template_fields = ['query_id', 'query_title']
    ui_color = '#ffd1dc'

    def __init__(self,
                 api_version='v1',
                 query_id=None,
                 query_title=None,
                 ignore_if_missing=False,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleDisplayVideo360DeleteReportOperator, self).__init__(*args, **kwargs)
        self.api_version = api_version
        self.query_id = query_id
        self.query_title = query_title
        self.ignore_if_missing = ignore_if_missing
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = GoogleDisplayVideo360Hook(
                gcp_conn_id=self.gcp_conn_id,
                api_version=self.api_version,
                delegate_to=self.delegate_to)

        if self.query_id is not None:
            self.hook.deletequery(
                self.query_id,
                ignore_if_missing=self.ignore_if_missing)

        if self.query_title is not None:
            self.hook.deletequeries(
                self.query_title,
                ignore_if_missing=self.ignore_if_missing)


class GoogleDisplayVideo360ERFToBigQueryOperator(BaseOperator):
    """Upload Multiple Entity Read Files to specified big query dataset.
    """

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 report_body=None,
                 yesterday=False,
                 entity_type=None,
                 file_creation_date=None,
                 cloud_project_id=None,
                 bq_table=None,
                 schema=None,
                 gcs_bucket=None,
                 erf_bucket=None,
                 partner_ids=[],
                 write_disposition='WRITE_TRUNCATE',
                 *args,
                 **kwargs):
        super(GoogleDisplayVideo360ERFToBigQueryOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.service = None
        self.bq_hook = None
        self.gcs_hook = None
        self.report_body = report_body
        self.erf_bucket = erf_bucket
        self.yesterday = yesterday
        self.cloud_project_id = cloud_project_id
        self.bq_table = bq_table
        self.gcs_bucket = gcs_bucket
        self.schema = schema
        self.entity_type = entity_type
        self.erf_object = 'entity/%s.0.%s.json' % (file_creation_date, entity_type)
        self.partner_ids = partner_ids
        self.write_disposition = write_disposition
        self.file_creation_date = file_creation_date

    def execute(self, context):
        if self.gcs_hook is None:
            self.gcs_hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.gcp_conn_id)
        if self.bq_hook is None:
            self.bq_hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id)

        for i, partner_id in enumerate(self.partner_ids):
            filename = erf_utils.download_and_transform_erf(self, partner_id)
            entity_read_file_ndj = 'gs://%s/%s' % (self.gcs_bucket, filename)
            if i > 0:
                self.write_disposition = 'WRITE_APPEND'

            bq_base_cursor = self.bq_hook.get_conn().cursor()
            bq_base_cursor.run_load(
                destination_project_dataset_table=self.bq_table,
                schema_fields=self.schema,
                source_uris=[entity_read_file_ndj],
                source_format='NEWLINE_DELIMITED_JSON',
                write_disposition=self.write_disposition)
            self.gcs_hook.delete(self.gcs_bucket, filename)


class GoogleDisplayVideo360SDFToBigQueryOperator(BaseOperator):
    """Make a request to SDF API and upload the data to BQ."""

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

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
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
        super(GoogleDisplayVideo360SDFToBigQueryOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.service = None
        self.hook = None
        self.bq_hook = None
        self.gcs_hook = None
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
        if self.hook is None:
            self.hook = GoogleDisplayVideo360Hook(gcp_conn_id=self.gcp_conn_id)
        if self.bq_hook is None:
            self.bq_hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id)
        if self.gcs_hook is None:
            self.gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcp_conn_id)

        request_body = {'fileTypes': self.file_types, 'filterType': self.filter_type, 'filterIds': self.filter_ids,
                        'version': self.api_version}

        logger.info('Request body: %s ' % request_body)
        request = self.hook.get_service().sdf().download(body=request_body)
        response = request.execute()

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
                self.gcs_hook.upload(self.gcs_bucket, filename, temp_file.name)
                logger.info('SDF upload to GCS complete')
            finally:
                if temp_file:
                    temp_file.close()
                os.unlink(temp_file.name)

            sdf_file = 'gs://%s/%s' % (self.gcs_bucket, filename)

            bq_table = self.table_names.get(file_type)
            bq_table = '%s.%s' % (self.bq_dataset, bq_table)
            schema = SDF_VERSIONED_SCHEMA_TYPES.get(self.api_version).get(file_type)
            try:
                bq_base_cursor = self.bq_hook.get_conn().cursor()
                logger.info('Uploading SDF to BigQuery')
                bq_base_cursor.run_load(
                    destination_project_dataset_table=bq_table,
                    schema_fields=schema,
                    source_uris=[sdf_file],
                    source_format='CSV',
                    skip_leading_rows=1,
                    write_disposition=self.write_disposition)
            finally:
                logger.info('Deleting SDF from GCS')
                self.gcs_hook.delete(self.gcs_bucket, filename)


class GoogleDisplayVideo360RecordSDFAdvertiserOperator(BaseOperator):
    """
    Get Partner and Advertiser Ids from a report and populate an airflow variable.
    """

    template_fields = ['report_url', 'variable_name']

    def __init__(self,
                 report_url,
                 variable_name,
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):
        super(GoogleDisplayVideo360RecordSDFAdvertiserOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.service = None
        self.report_url = report_url
        self.variable_name = variable_name

    def execute(self, context):
        try:
            report_file = tempfile.NamedTemporaryFile(delete=False)
            file_download = requests.get(self.report_url, stream=True)
            for chunk in file_download.iter_content(chunk_size=1024 * 1024):
                report_file.write(chunk)
            report_file.close()
            advertisers = {}
            with open(report_file.name, 'r') as f:
                csv_reader = csv.DictReader(f)

                for line in csv_reader:
                    advertiser_id = line["Advertiser ID"]
                    partner_id = line["Partner ID"]
                    if advertiser_id.strip():
                        try:
                            advertisers[partner_id].append(advertiser_id)
                            message = 'ADDING to key %s new advertiser %s' % (
                                partner_id, advertiser_id)
                            logger.info(message)
                        except KeyError:
                            advertisers[partner_id] = [advertiser_id]
                            message = 'CREATING new key %s with advertiser %s' % (
                                partner_id, advertiser_id)
                            logger.info(message)
                    else:
                        break
            models.Variable.set(self.variable_name, json.dumps(advertisers))
        finally:
            if report_file:
                report_file.close()
                os.unlink(report_file.name)
