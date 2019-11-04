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

"""Hook for accessing SA360 API.

This hook handles authentication and provides a lightweight wrapper around the
Search Ads 360 API.
"""

import logging
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


class GoogleSearchAds360Hook(GoogleCloudBaseHook):
  """Airflow Hook to connect to Search Ads 360 API.
  """

  def __init__(self,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      api_name='doubleclicksearch',
      api_version='v2'):
    """Constructor.

    Args:
      gcp_conn_id: The connection ID to use for accessing SA360 API and
          authenticating requests.
      delegate_to: The account to impersonate, if any.
      api_name: The name of the SA360 API.
      api_version: The version of the SA360 API.
    """
    super(GoogleSearchAds360Hook, self).__init__(
        gcp_conn_id=gcp_conn_id,
        delegate_to=delegate_to)
    self.api_name = api_name
    self.api_version = api_version
    self._service = None

  def get_service(self):
    """Retrieves the SA360 service object.

    For a detailed reference of the service API see:
    https://developers.google.com/search-ads/v2/reference/

    Returns:
      The SA360 service object.
    """
    if self._service is None:
      http_authorized = self._authorize()
      self._service = build(
          self.api_name, self.api_version, http=http_authorized)
    return self._service
