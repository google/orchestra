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

"""Collection of Google Cloud Storage operators.
"""


import os
import tempfile
from airflow.contrib.hooks.ftp_hook import FTPSHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator


class GoogleCloudStorageToFTPOperator(BaseOperator):
  """Operator for uploading a file from Google Cloud Storage to FTP.

  Attributes:
    gcs_conn_id: The connection ID used to connect to Google Cloud Storage.
    delegate_to: The account to impersonate, if any.
    ftp_conn_id: The connection ID used to establish the FTP connection.
    gcs_source_bucket: The Google Cloud Storage bucket containing the object to
        upload. (templated)
    gcs_source_object: The Google Cloud Storage object to be uploaded.
        (templated)
    ftp_destination_path: The destination path for the file being uploaded.
        (templated)
  """

  template_fields = [
      'gcs_source_bucket', 'gcs_source_object', 'ftp_destination_path'
  ]

  def __init__(self,
      gcs_conn_id='google_cloud_default',
      delegate_to=None,
      ftp_conn_id='ftp_default',
      gcs_source_bucket=None,
      gcs_source_object=None,
      ftp_destination_path=None,
      *args,
      **kwargs):
    super(GoogleCloudStorageToFTPOperator, self).__init__(*args, **kwargs)
    self.gcs_conn_id = gcs_conn_id
    self.delegate_to = delegate_to
    self.ftp_conn_id = ftp_conn_id

    self.gcs_source_bucket = gcs_source_bucket
    self.gcs_source_object = gcs_source_object
    self.ftp_destination_path = ftp_destination_path

    self.gcs_hook = None
    self.ftp_hook = None

  def _get_gcs_hook(self):
    if self.gcs_hook is None:
      self.gcs_hook = GoogleCloudStorageHook(
          google_cloud_storage_conn_id=self.gcs_conn_id,
          delegate_to=self.delegate_to)
    return self.gcs_hook

  def _get_ftp_hook(self):
    if self.ftp_hook is None:
      self.ftp_hook = FTPSHook(ftp_conn_id=self.ftp_conn_id)
    return self.ftp_hook

  def execute(self, context):
    gcs_hook = self._get_gcs_hook()
    ftp_hook = self._get_ftp_hook()

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
      gcs_hook.download(
          bucket=self.gcs_source_bucket,
          object=self.gcs_source_object,
          filename=temp_file.name)

      ftp_hook.store_file(
          remote_full_path=self.ftp_destination_path,
          local_full_path_or_buffer=temp_file.name)
    finally:
      temp_file.close()
      os.unlink(temp_file.name)
