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

"""Collection of DCM reporting operators.

Set of operators that allow developers to create and manage reports within
Campaign Manager.
"""
import json
import logging
import os
import tempfile

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator

from orchestra.google.marketing_platform.hooks.campaign_manager import (
  GoogleCampaignManagerHook
)
from googleapiclient import http


logger = logging.getLogger(__name__)


class GoogleCampaignManagerInsertReportOperator(BaseOperator):
  """Creates and runs a new Display & Video 360 query.

  Attributes:
    report: The query body to create the report from. (templated)
        Can receive a json string representing the report or reference to a
        template file. Template references are recognized by a string ending in
        '.json'.
    profile_id: DCM profile ID used when making API requests. (templated)
    gcp_conn_id: The connection ID to use when fetching connection info.
    delegate_to: The account to impersonate, if any.

  XComs:
    report_id: The ID associated with the created report.
    file_id: The ID associated with created report file.
  """
  template_fields = ['params', 'report', 'profile_id']
  template_ext = ['.json']

  def __init__(self,
      report,
      profile_id,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      *args,
      **kwargs):
    super(GoogleCampaignManagerInsertReportOperator, self).__init__(*args, **kwargs)
    self.report = report
    self.profile_id = profile_id
    self.gcp_conn_id = gcp_conn_id
    self.delegate_to = delegate_to
    self.hook = None

  def _create_report(self, profile_id, report):
    report_body = json.loads(report)
    req = self.hook.get_service().reports().insert(
        profileId=profile_id,
        body=report_body)
    res = req.execute()
    return res['id']

  def _run_report(self, profile_id, report_id):
    req = self.hook.get_service().reports().run(
        profileId=profile_id,
        reportId=report_id)
    res = req.execute()
    return res['id']

  def execute(self, context):
    if self.hook is None:
      self.hook = GoogleCampaignManagerHook(
          gcp_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)

    report_id = self._create_report(self.profile_id, self.report)

    file_id = self._run_report(self.profile_id, report_id)
    context['task_instance'].xcom_push(
        'report_id', report_id)
    context['task_instance'].xcom_push(
        'file_id', file_id)
    msg = 'Report Created report_id = %s and file_id = %s'%(report_id, file_id)
    logger.info(msg)


class GoogleCampaignManagerDeleteReportOperator(BaseOperator):
  """Deletes Campaign Manager reports.

  Attributes:
    report_id: The DCM report ID to delete. (templated)
    report_name: The DCM report name to delete. (templated)
    profile_id: DCM profile ID to validate against when interacting
                with the API. (templated)
    ignore_if_missing: If True, return success even if the report is missing.
    gcp_conn_id: The connection ID to use when fetching connection info.
    delegate_to: The account to impersonate, if any.
  """

  template_fields = ['profile_id', 'report_id', 'report_name']

  def __init__(self,
      profile_id,
      report_id=None,
      report_name=None,
      ignore_if_missing=False,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      *args,
      **kwargs):
    super(GoogleCampaignManagerDeleteReportOperator, self).__init__(*args, **kwargs)
    self.report_id = report_id
    self.report_name = report_name
    self.profile_id = profile_id
    self.ignore_if_missing = ignore_if_missing
    self.gcp_conn_id = gcp_conn_id
    self.delegate_to = delegate_to
    self.hook = None

  def execute(self, context):
    if self.hook is None:
      self.hook = GoogleCampaignManagerHook(
          gcp_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)

    if self.report_id is not None:
      self.hook.delete_report(
          self.profile_id, self.report_id, self.ignore_if_missing)

    if self.report_name is not None:
      self.hook.delete_report_by_name(
          self.profile_id, self.report_name, self.ignore_if_missing)


class GoogleCampaignManagerDownloadReportOperator(BaseOperator):
  """Downloads a Campaign Manager report into Google Cloud Storage.

  Attributes:
    report_id: The DCM report ID with which the report file is associated with.
        (templated)
    file_id: The DCM file ID of the report file to download. (templated)
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

  template_fields = [
      'report_id',
      'file_id',
      'destination_bucket',
      'destination_object']

  def __init__(self,
      report_id,
      file_id,
      destination_bucket,
      destination_object=None,
      gcp_conn_id='google_cloud_default',
      chunk_size=5 * 1024 * 1024,
      delegate_to=None,
      *args,
      **kwargs):
    super(GoogleCampaignManagerDownloadReportOperator, self).__init__(*args, **kwargs)
    self.file_id = file_id
    self.report_id = report_id
    self.destination_bucket = destination_bucket
    self.destination_object = destination_object
    self.chunk_size = chunk_size
    self.gcp_conn_id = gcp_conn_id
    self.delegate_to = delegate_to
    self.gcs_hook = None
    self.cm_hook = None

  def _download_report(self,
      report_id,
      file_id,
      destination_file,
      chunk_size):
    file_metadata = self.cm_hook.get_service().files().get(
        reportId=report_id, fileId=file_id).execute()

    if file_metadata['status'] != 'REPORT_AVAILABLE':
      msg = 'File with ID = %s and Report ID = %s not available, status = %s.'%(
          file_id, report_id, file_metadata['status'])
      raise Exception(msg)

    request = self.cm_hook.get_service().files().get_media(
        reportId=report_id, fileId=file_id)

    downloader = http.MediaIoBaseDownload(
        destination_file, request, chunksize=chunk_size)

    download_finished = False
    while not download_finished:
      _, download_finished = downloader.next_chunk()

    return file_metadata['fileName']

  def _get_destination_uri(self, destination_object, report_file_name):
    report_file_name = '%s.csv.gz' % report_file_name

    if destination_object is None:
      return report_file_name

    if destination_object.endswith('/'):
      return destination_object + report_file_name

    return destination_object

  def execute(self, context):
    if self.gcs_hook is None:
      self.gcs_hook = GoogleCloudStorageHook(
          google_cloud_storage_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)
    if self.cm_hook is None:
      self.cm_hook = GoogleCampaignManagerHook(
          gcp_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
      report_file_name = self._download_report(
          self.report_id, self.file_id, temp_file, self.chunk_size)

      destination_object_name = self._get_destination_uri(
          self.destination_object, report_file_name)

      self.gcs_hook.upload(
          bucket=self.destination_bucket,
          object=destination_object_name,
          filename=temp_file.name,
          gzip=True,
          multipart=True)

      context['task_instance'].xcom_push(
          'destination_bucket', self.destination_bucket)
      context['task_instance'].xcom_push(
          'destination_object', destination_object_name)
    finally:
      temp_file.close()
      os.unlink(temp_file.name)
