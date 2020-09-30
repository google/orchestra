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

"""Collection of SA360 reporting operators.

Set of operators that allow developers to manage Queries and Reports within
Search Ads 360.
"""

import json
import os
import tempfile
from pathlib import Path

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from orchestra.google.marketing_platform.hooks.search_ads_360 import (
  GoogleSearchAds360Hook
)


class GoogleSearchAds360InsertReportOperator(BaseOperator):
  """Creates and runs a new Search Ads 360 report.

  Attributes:
    report: The report body to create the report from. (templated)
        Can receive a json string representing the report or reference to a
        template file. Template references are recognized by a string ending in
        '.json'.
    gcp_conn_id: The connection ID to use when fetching connection info.
    delegate_to: The account to impersonate, if any.

  XComs:
    report_id: The ID for the report created.
  """

  template_fields = ['params', 'report']
  template_ext = ['.json']

  def __init__(self,
      report,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      *args,
      **kwargs):
    super(GoogleSearchAds360InsertReportOperator, self).__init__(*args, **kwargs)
    self.report = report
    self.gcp_conn_id = gcp_conn_id
    self.delegate_to = delegate_to
    self.hook = None

  def execute(self, context):
    if self.hook is None:
      self.hook = GoogleSearchAds360Hook(
          gcp_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)

    report_body = json.loads(self.report)
    request = self.hook.get_service().reports().request(body=report_body)
    response = request.execute()

    context['task_instance'].xcom_push('report_id', response['id'])


class GoogleSearchAds360DownloadReportOperator(BaseOperator):
  """Downloads a Search Ads 360 report into Google Cloud Storage.

  Attributes:
    report_id: The ID of the report to download. (templated)
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

  template_fields = ['report_id', 'destination_bucket', 'destination_object']

  def __init__(self,
      report_id,
      destination_bucket,
      destination_object=None,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      *args,
      **kwargs):
    super(GoogleSearchAds360DownloadReportOperator, self).__init__(*args, **kwargs)
    self.report_id = report_id
    self.destination_bucket = destination_bucket
    self.destination_object = destination_object
    self.gcp_conn_id = gcp_conn_id
    self.delegate_to = delegate_to
    self.sa360_hook = None
    self.gcs_hook = None

  def _download_report(self, report_id, destination_file, fragment_count):
    for i in range(fragment_count):
      request = self.sa360_hook.get_service().reports().getFile(
          reportId=report_id, reportFragment=i)
      fragment = request.execute()
      if i > 0:
        fragment_records = fragment.split('\n', 1)
        if len(fragment_records) > 1:
          fragment = fragment_records[1]
        else:
          fragment = ''
      destination_file.write(fragment)

  def _get_destination_uri(self, destination_object, destination_file):
    report_file_name = destination_file.name

    if destination_object is None:
      return report_file_name

    if destination_object.endswith('/'):
      return destination_object + report_file_name

    return destination_object

  def execute(self, context):
    if self.sa360_hook is None:
      self.sa360_hook = GoogleSearchAds360Hook(
          gcp_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)
    if self.gcs_hook is None:
      self.gcs_hook = GoogleCloudStorageHook(
          google_cloud_storage_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)

    request = self.sa360_hook.get_service().reports().get(
        reportId=self.report_id)
    response = request.execute()

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
      self._download_report(self.report_id, temp_file, len(response['files']))
      destination_object_name = self._get_destination_uri(
          self.destination_object, temp_file)
      self.gcs_hook.upload(
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

class GoogleSearchAds360InsertConversionOperator(BaseOperator):
  """Insert conversions in  Search Ads 360.

  Attributes:
    conversions_file: path to json file with conversions to be inserted (templated)
        If the destination points to an existing folder, the report will be
        written under the specified folder.
    gcp_conn_id: The connection ID to use when fetching connection info.
    delegate_to: The account to impersonate, if any.

  XComs:
    destination_bucket: The Google cloud storage bucket the report was written
        to.
    destination_object: The Google cloud storage URI for the report.
  """

  template_fields = ['conversions_file']
  hook = None

  @apply_defaults
  def __init__(self,
      *args,
      conversions_file,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      **kwargs):
    super(GoogleSearchAds360InsertConversionOperator, self).__init__(*args, **kwargs)
    self.conversions_file = conversions_file
    self.gcp_conn_id = gcp_conn_id
    self.delegate_to = delegate_to

  def execute(self, context):
    file = Path(self.conversions_file)
    if not file.is_file():
      raise AirflowException(
        f'conversions_file {self.conversions_file} not found'
      )

    if self.hook is None:
      self.hook = GoogleSearchAds360Hook(
        gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
      )

    conversions = json.loads(file.read_text())
    if not conversions:
      self.log.info('No conversions to insert')
      return

    request = (
      self.hook.get_service()
      .conversion()
      .insert(body={'conversion': conversions})
    )
    request.execute()
