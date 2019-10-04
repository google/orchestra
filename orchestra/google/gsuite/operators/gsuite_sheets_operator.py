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

import csv
import logging
import os
import tempfile
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.bash_operator import BaseOperator
from orchestra.google.gsuite.hooks.gsuite_sheets_hook import GoogleSheetsHook

logger = logging.getLogger(__name__)


class GoogleSheetsToCloudStorageOperator(BaseOperator):
  """Writes Google Sheet data into Google Cloud Storage.

  Attributes:
    spreadsheet_id: (templated) Required
    sheet_filter: Default to None, if provided, Should be an array of the sheet
    titles to pull from.
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
    destination_objects: The Google cloud storage URI array for the objects
    created by the operator (destination folder + file name).
  """

  template_fields = [
      'spreadsheet_id',
      'destination_bucket',
      'destination_object']

  def __init__(
      self,
      spreadsheet_id,
      destination_bucket,
      sheet_filter=[],
      destination_object=None,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      *args,
      **kwargs):
    super(GoogleSheetsToCloudStorageOperator, self).__init__(*args, **kwargs)
    self.gcp_conn_id = gcp_conn_id
    self.spreadsheet_id = spreadsheet_id
    self.sheet_filter = sheet_filter
    self.destination_bucket = destination_bucket
    self.destination_object = destination_object
    self.delegate_to = delegate_to
    self.sheets_hook = None
    self.gcs_hook = None

  def _pullSheetData(self):
    logger.info('GoogleSheetsToCloudStorageOperator::_pullSheetData')
    sheets = self.sheets_hook.getSheetTitles(self.spreadsheet_id,
                                             self.sheet_filter)
    data = {}
    for sheet_title in sheets:
      sheet_values = self.sheets_hook.getSheetValues(self.spreadsheet_id,
                                                     sheet_title)
      data[sheet_title] = sheet_values
    return data

  def _storeOutputFile(self, sheet_name, sheet_values):
    logger.info('GoogleSheetsToCloudStorageOperator::_storeOutputFile')
    # Construct destianation file path
    spread_sheet = self.sheets_hook.getSpreadsheet(self.spreadsheet_id)
    destination_file_name = spread_sheet['properties'][
                              'title'] + '_' + sheet_name
    destination_file_name = destination_file_name.replace(' ', '_')

    destination_object_name = self._get_destination_uri(
        self.destination_object,
        destination_file_name)
    temp_file = tempfile.NamedTemporaryFile(delete=False)

    try:
      with open(temp_file.name, 'w') as t:
        writer = csv.writer(t)
        writer.writerows(sheet_values)
      temp_file.close()
      self.gcs_hook.upload(
          bucket=self.destination_bucket,
          object=destination_object_name,
          filename=temp_file.name)
    finally:
      temp_file.close()
      os.unlink(temp_file.name)
    return destination_object_name

  def _get_destination_uri(self, destination_object, file_name):
    logger.info('GoogleSheetsToCloudStorageOperator::_get_destination_uri')
    dest_file_name = file_name + '.csv'
    if destination_object is None:
      return dest_file_name

    if destination_object.endswith('/'):
      return destination_object + dest_file_name
    return destination_object

  def execute(self, context):
    # init sheets api hook
    if self.sheets_hook is None:
      self.sheets_hook = GoogleSheetsHook(
          gcp_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)

    if self.gcs_hook is None:
      self.gcs_hook = GoogleCloudStorageHook(
          google_cloud_storage_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)

    if self.spreadsheet_id is not None:
      sheet_data = self._pullSheetData()
      sheet_titles = list(sheet_data.keys())
      destination_array = []
      for key in sheet_titles:
        destination = self._storeOutputFile(key, sheet_data.get(key))
        if destination:
          destination_array.append(destination)

      context['task_instance'].xcom_push(
          'destination_bucket', self.destination_bucket)
      context['task_instance'].xcom_push(
          'destination_objects', destination_array)
