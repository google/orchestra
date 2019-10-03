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
import json
import os
import tempfile
from urllib.parse import urlparse

import requests
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from google.gmp.hooks.gmp_dv360_hook import DisplayVideo360Hook
from google.gmp.operators.gmp_base_operator import GoogleMarketingPlatformBaseOperator


class DisplayVideo360CreateReportOperator(GoogleMarketingPlatformBaseOperator):
    """Creates and runs a new Display & Video 360 query.

    Attributes:
      report: The query body to create the report from. (templated)
          Can receive a json string representing the report or reference to a
          template file. Template references are recognized by a string ending in
          '.json'.
      gcp_conn_id: The connection ID to use when fetching connection info.
      delegate_to: The account to impersonate, if any.

    XComs:
      query_id: The query ID for the report created.
    """

    template_fields = ['params', 'report']
    template_ext = ['.json']

    def __init__(self,
                 report,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(DisplayVideo360CreateReportOperator, self).__init__(*args, **kwargs)
        self.report = report
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = DisplayVideo360Hook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to)

        report_body = json.loads(self.report)
        request = self.hook.get_service().queries().createquery(body=report_body)
        response = request.execute()

        context['task_instance'].xcom_push('query_id', response['queryId'])


class DisplayVideo360DownloadReportOperator(GoogleMarketingPlatformBaseOperator):
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
        super(DisplayVideo360DownloadReportOperator, self).__init__(*args, **kwargs)
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


class DisplayVideo360DeleteReportOperator(GoogleMarketingPlatformBaseOperator):
    """Deletes Display & Video 360 queries and any associated reports.

    Attributes:
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
                 query_id=None,
                 query_title=None,
                 ignore_if_missing=False,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(DisplayVideo360DeleteReportOperator, self).__init__(*args, **kwargs)
        self.query_id = query_id
        self.query_title = query_title
        self.ignore_if_missing = ignore_if_missing
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = DisplayVideo360Hook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to)

        if self.query_id is not None:
            self.hook.deletequery(
                self.query_id,
                ignore_if_missing=self.ignore_if_missing)

        if self.query_title is not None:
            self.hook.deletequeries(
                self.query_title,
                ignore_if_missing=self.ignore_if_missing)
