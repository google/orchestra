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

"""Airflow hook for accessing DV360 Reporting API.

This hook provides access to DV360 Reporting API. It makes use of
GoogleCloudBaseHook to provide an authenticated api client.
"""

from airflow.contrib.hooks import gcp_api_base_hook
from apiclient import discovery


class DV360Hook(gcp_api_base_hook.GoogleCloudBaseHook):
  """Airflow Hook to connect to DV360 Reporting API.

  This hook utilizes GoogleCloudBaseHook connection to generate the service
  object to DV360 reporting API.

  Once the Hook is instantiated you can invoke get_service() to obtain a
  service object that you can use to invoke the API.
  """
  conn_name_attr = 'dv360_conn_id'

  def __init__(self,
               dv360_conn_id='dv360_default',
               api_name='doubleclickbidmanager',
               api_version='v1',
               delegate_to=None):
    """Constructor.

    Args:
      dv360_conn_id: Airflow connection ID to be used for accessing DV360 API
        and authenticating requests. Default is dv360_default
      api_name: DV360 API name to be used. Default is doubleclickbidmanager.
      api_version: DV360 API Version. Default is v1.
      delegate_to: The account to impersonate, if any. For this to work, the
        service account making the request must have domain-wide delegation
        enabled.
    """
    super(DV360Hook, self).__init__(
        conn_id=dv360_conn_id, delegate_to=delegate_to)

    self.api_name = api_name
    self.api_version = api_version

  def get_service(self):
    """Gets API service object.

    This is called by Airflow whenever an API client service object is needed.
    Returned service object is already authenticated using the Airflow
    connection data provided in the dv360_conn_id constructor parameter.

    Returns:
      Google API client service object.
    """
    http_authorized = self._authorize()
    return discovery.build(
        self.api_name, self.api_version, http=http_authorized)
