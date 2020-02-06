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

"""Hook for accessing the API.

This hook handles authentication and provides a lightweight wrapper around the
Google Analytics API.
"""

import logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


logger = logging.getLogger(__name__)


class GoogleAnalyticsManagementHook(GoogleCloudBaseHook):
    """Hook for connecting to the Google Analytics Management API.
    """

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 api_name='analytics',
                 api_version='v3'):
        """Constructor.

        :param gcp_conn_id: The connection ID to use when fetching connection info.
        :type gcp_conn_id: str
        :param delegate_to: The account to impersonate, if any.
        :type delegate_to: str
        :param api_name: The name of the GA API.
        :type api_name: str
        :param api_version: The version of the GA API.
        :type api_version: str
        """
        super(GoogleAnalyticsManagementHook, self).__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to)
        self.api_name = api_name
        self.api_version = api_version
        self._service = None

    def get_service(self):
        """Retrieves the GA service object.

        Returns:
          The GA service object.
        """
        if self._service is None:
            http_authorized = self._authorize()
            self._service = build(
                self.api_name, self.api_version, http=http_authorized)
        return self._service

    def upload_file(self,
                    file_location,
                    account_id,
                    web_property_id,
                    custom_data_source_id,
                    mime_type='application/octet-stream',
                    resumable_upload=False):

        """Uploads file to GA via the Data Import API

        :param file_location: The path and name of the file to upload.
        :type file_location: str
        :param account_id: The GA account Id to which the data upload belongs.
        :type account_id: str
        :param web_property_id: UA-string associated with the upload.
        :type web_property_id: str
        :param custom_data_source_id: Custom Data Source Id to which this data
                                      import belongs.
        :type custom_data_source_id: str
        :param mime_type: Label to identify the type of data in the HTTP request
        :type mime_type: str
        :param resumable_upload: flag to upload the file in a resumable fashion,
                                 using a series of at least two requests
        :type resumable_upload: bool
        """

        media = MediaFileUpload(file_location,
                                mimetype=mime_type,
                                resumable=resumable_upload)

        logger.info('Uploading file to GA file for accountId:%s,'
                    'webPropertyId:%s'
                    'and customDataSourceId:%s ',
                    account_id, web_property_id, custom_data_source_id)

        # TODO(): handle scenario where upload fails
        self.get_service().management().uploads().uploadData(
            accountId=account_id,
            webPropertyId=web_property_id,
            customDataSourceId=custom_data_source_id,
            media_body=media).execute()

    def delete_upload_data(self,
                           account_id,
                           web_property_id,
                           custom_data_source_id,
                           delete_request_body
    ):
        """Deletes the uploaded data for a given account/property/dataset

        :param account_id: The GA account Id to which the data upload belongs.
        :type account_id: str
        :param web_property_id: UA-string associated with the upload.
        :type web_property_id: str
        :param custom_data_source_id: Custom Data Source Id to which this data
                                      import belongs.
        :type custom_data_source_id: str
        :param delete_request_body: Dict of customDataImportUids to delete
        :type delete_request_body: dict
        """

        logger.info('Deleting previous uploads to GA file for accountId:%s,'
                    'webPropertyId:%s'
                    'and customDataSourceId:%s ',
                    account_id, web_property_id, custom_data_source_id)

        # TODO(): handle scenario where deletion fails
        self.get_service().management().uploads().deleteUploadData(
            accountId=account_id,
            webPropertyId=web_property_id,
            customDataSourceId=custom_data_source_id,
            body=delete_request_body).execute()

    def get_list_of_uploads(self,
                            account_id,
                            web_property_id,
                            custom_data_source_id):
        """Get list of data upload from GA

        :param account_id: The GA account Id to which the data upload belongs.
        :type account_id: str
        :param web_property_id: UA-string associated with the upload.
        :type web_property_id: str
        :param custom_data_source_id: Custom Data Source Id to which this data
                                      import belongs.
        :type custom_data_source_id: str
        """
        logger.info('Getting list of uploads for accountId:%s,'
                    'webPropertyId:%s'
                    'and customDataSourceId:%s ',
                    account_id, web_property_id, custom_data_source_id)

        # TODO(): handle scenario where request fails
        response = self.get_service().management().uploads().list(
            accountId=account_id,
            webPropertyId=web_property_id,
            customDataSourceId=custom_data_source_id).execute()

        return response
