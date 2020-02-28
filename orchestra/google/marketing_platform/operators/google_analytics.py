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
"""GA operators.

Operator to interact with GA

"""
import csv
import logging
import os
import tempfile
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from orchestra.google.marketing_platform.hooks.google_analytics import GoogleAnalyticsManagementHook

logger = logging.getLogger(__name__)


class GoogleAnalyticsDataImportUploadOperator(BaseOperator):
    """Take a file from Cloud Storage and uploads it to GA via data import API.

    :param storage_bucket: The Google cloud storage bucket where the file is
          stored. (templated)
    :type storage_bucket: str
    :param storage_name_object: The name of the object in the desired Google cloud
          storage bucket. (templated) If the destination points to an existing
          folder, the file will be taken from the specified folder.
    :type storage_name_object: str
    :param account_id: The GA account Id (long) to which the data upload belongs
    :type account_id: str
    :param web_property_id: The web property UA-string associated with the upload
    :type web_property_id: str
    :param custom_data_source_id: The id to which the data import belongs
    :type custom_data_source_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param mime_type: Label to identify the type of data stored for the HTTP
           request
    :type mime_type: str
    :param delegate_to: The account to impersonate, if any.
    :type delegate_to: str
    :param api_name: The name of the GA API.
    :type api_name: str
    :param api_version: The version of the GA API.
    :type api_version: str

    """

    template_fields = [
        'storage_bucket',
        'storage_name_object']

    def __init__(self,
                 storage_bucket,
                 storage_name_object,
                 account_id,
                 web_property_id,
                 custom_data_source_id,
                 gcp_conn_id='google_cloud_default',
                 mime_type=None,
                 delegate_to=None,
                 api_version=None,
                 api_name=None,
                 *args,
                 **kwargs):
        super(GoogleAnalyticsDataImportUploadOperator, self).__init__(*args, **kwargs)
        self.storage_bucket = storage_bucket
        self.storage_name_object = storage_name_object
        self.account_id = account_id
        self.web_property_id = web_property_id
        self.custom_data_source_id = custom_data_source_id
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.api_version = api_version
        self.api_name = api_name
        self.gcs_hook = None
        self.ga_hook = None

    @staticmethod
    def _get_file_from_cloud_storage(gcs_hook, storage_bucket, storage_object,
                                     temp_ga_upload_file):

        logger.info('Downloading file from GCS: %s/%s ',
                    storage_bucket, storage_object)
        gcs_hook.download(storage_bucket, storage_object, temp_ga_upload_file.name)

    def execute(self, context):
        if self.gcs_hook is None:
            self.gcs_hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to)

        if self.ga_hook is None:
            self.ga_hook = GoogleAnalyticsManagementHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
            )

        try:
            temp_ga_upload_file = tempfile.NamedTemporaryFile(delete=False)
            self._get_file_from_cloud_storage(self.gcs_hook,
                                              self.storage_bucket,
                                              self.storage_name_object,
                                              temp_ga_upload_file)

            self.ga_hook.upload_file(temp_ga_upload_file.name,
                                     self.account_id,
                                     self.web_property_id,
                                     self.custom_data_source_id,
                                     self.mime_type)

        finally:
            temp_ga_upload_file.close()
            os.unlink(temp_ga_upload_file.name)


class GoogleAnalyticsDeletePreviousDataUploadsOperator(BaseOperator):
    """Deletes previous GA uploads to leave the latest file to control the size
    of the Data Set Quota

    :param account_id: The GA account Id (long) to which the data upload belongs
    :type account_id: str
    :param web_property_id: The web property UA-string associated with the upload
    :type web_property_id: str
    :param custom_data_source_id: The id to which the data import belongs
    :type custom_data_source_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
    :type delegate_to: str
    :param api_name: The name of the GA API.
    :type api_name: str
    :param api_version: The version of the GA API.
    :type api_version: str


    Attributes:
      account_id: The GA account Id (long) to which the data upload belongs.
      web_property_id: The web property UA-string associated with the upload
      custom_data_source_id: The id to which the data import belongs
      gcp_conn_id: The connection ID to use when fetching connection info.
    """

    def __init__(self,
                 account_id,
                 web_property_id,
                 custom_data_source_id,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 api_version=None,
                 api_name=None,
                 *args,
                 **kwargs):
        super(GoogleAnalyticsDeletePreviousDataUploadsOperator, self).__init__(*args, **kwargs)
        self.account_id = account_id
        self.web_property_id = web_property_id
        self.custom_data_source_id = custom_data_source_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.api_version = api_version
        self.api_name = api_name
        self.ga_hook = None

    def execute(self, context):
        if self.ga_hook is None:
            self.ga_hook = GoogleAnalyticsManagementHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to
            )

        response = self.ga_hook.get_list_of_uploads(self.account_id,
                                                    self.web_property_id,
                                                    self.custom_data_source_id)

        uploads = response['items']
        cids = [upload['id'] for upload in uploads[1:]]
        delete_request_body = {'customDataImportUids': cids}

        self.ga_hook.delete_upload_data(self.account_id,
                                        self.web_property_id,
                                        self.custom_data_source_id,
                                        delete_request_body)


class GoogleAnalyticsModifyFileHeadersDataImportOperator(BaseOperator):
    """GA has a very particular naming convention for Data Import. Ability to
    prefix "ga:" to all column headers and also a dict to rename columns to
    match the custom dimension ID in GA i.e clientId : dimensionX.


    :param storage_bucket: The Google cloud storage bucket where the file is
          stored. (templated)
    :type storage_bucket: str
    :param storage_name_object: The name of the object in the desired Google cloud
          storage bucket. (templated) If the destination points to an existing
          folder, the file will be taken from the specified folder.
    :type storage_name_object: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param custom_dimension_header_mapping: Dictionary to handle when uploading
          custom dimensions which have generic IDs ie. 'dimensionX' which are
          set by GA. Dictionary maps the current CSV header to GA ID which will
          be the new header for the CSV to upload to GA eg clientId : dimension1
    :type custom_dimension_header_mapping: dict
    :param delegate_to: The account to impersonate, if any.
    :type delegate_to: str
    """

    template_fields = ['storage_bucket', 'storage_name_object']

    def __init__(self,
                 storage_bucket,
                 storage_name_object,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 custom_dimension_header_mapping=None,
                 *args,
                 **kwargs):
        super(GoogleAnalyticsModifyFileHeadersDataImportOperator, self).__init__(*args, **kwargs)
        self.storage_bucket = storage_bucket
        self.storage_name_object = storage_name_object
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.custom_dimension_header_mapping = custom_dimension_header_mapping
        self.gcs_hook = None

    @staticmethod
    def _get_file_from_cloud_storage(gcs_hook, storage_bucket, storage_object,
                                     temp_ga_upload_file):

        logger.info('Downloading file from GCS: %s/%s ',
                    storage_bucket, storage_object)

        # TODO(): future improvement check file size before downloading,
        #  to check for local space availability
        gcs_hook.download(storage_bucket, storage_object,
                          temp_ga_upload_file.name)

    @staticmethod
    def _upload_file_to_cloud_storage(gcs_hook, storage_bucket, storage_object,
                                      temp_ga_upload_file_name):

        logger.info('Uploading file to GCS: %s/%s ',
                    storage_bucket, storage_object)
        gcs_hook.upload(storage_bucket, storage_object,
                        temp_ga_upload_file_name)

    @staticmethod
    def _modify_column_headers(tmp_file_location,
                               custom_dimension_header_mapping):
        logger.info('Modifying column headers to be compatible for data upload')
        with open(tmp_file_location, 'r') as check_header_file:
            has_header = csv.Sniffer().has_header(check_header_file.read(1024))
            if has_header:
                with open(tmp_file_location, 'r') as read_file:
                    reader = csv.reader(read_file)
                    headers = next(reader)
                    new_headers = []
                    for header in headers:
                        if header in custom_dimension_header_mapping:
                            header = custom_dimension_header_mapping.get(header)
                        new_header = 'ga:' + header
                        new_headers.append(new_header)
                    all_data = read_file.readlines()
                    final_headers = ','.join(new_headers) + '\n'
                    all_data.insert(0, final_headers)
                    with open(tmp_file_location, 'w') as write_file:
                        write_file.writelines(all_data)
            else:
                raise NameError('CSV does not contain headers, please add them '
                                'to use the modify column headers functionality')

    def execute(self, context):
        if self.gcs_hook is None:
            self.gcs_hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to)
        try:
            temp_ga_file_to_modify = tempfile.NamedTemporaryFile(delete=False)
            self._get_file_from_cloud_storage(self.gcs_hook,
                                              self.storage_bucket,
                                              self.storage_name_object,
                                              temp_ga_file_to_modify)

            self._modify_column_headers(temp_ga_file_to_modify.name,
                                        self.custom_dimension_header_mapping)

            # upload newly formatted file to cloud storage
            self._upload_file_to_cloud_storage(self.gcs_hook,
                                               self.storage_bucket,
                                               self.storage_name_object,
                                               temp_ga_file_to_modify.name)

        finally:
            temp_ga_file_to_modify.close()
            os.unlink(temp_ga_file_to_modify.name)
