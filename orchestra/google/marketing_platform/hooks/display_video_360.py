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

"""Hook for accessing the DV360 API.

This hook handles authentication and provides a lightweight wrapper around the
Display & Video 360 API.
"""

import logging
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


class GoogleDisplayVideo360Hook(GoogleCloudBaseHook):
  """Hook for connecting to the Display & Video 360 API.
  """

  def __init__(self,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      api_name='doubleclickbidmanager',
      api_version='v1'):
    """Constructor.

    Args:
      gcp_conn_id: The connection ID to use when fetching connection info.
      delegate_to: The account to impersonate, if any.
      api_name: The name of the DV360 API.
      api_version: The version of the DV360 API.
    """
    super(GoogleDisplayVideo360Hook, self).__init__(
        gcp_conn_id=gcp_conn_id,
        delegate_to=delegate_to)
    self.api_name = api_name
    self.api_version = api_version
    self._service = None

  def get_service(self):
    """Retrieves the DV360 service object.

    For a detailed reference of the service API see:
    https://developers.google.com/bid-manager/release-notes

    Returns:
      The DV360 service object.
    """
    if self._service is None:
      http_authorized = self._authorize()
      self._service = build(
          self.api_name, self.api_version, http=http_authorized)
    return self._service

  def listqueries(self, title):
    """Retrieves the list of existing queries matching the given title.

    Args:
      title: The query title.

    Returns:
      An array of query resources that matched the title, or an empty array if
      none exist.
    """
    queries = []

    request = self.get_service().queries().listqueries()
    response = request.execute()

    for query in response.get('queries', []):
      if query['metadata']['title'] == title:
        queries.append(query)

    return queries

  def deletequery(self, query_id, ignore_if_missing=False):
    """Delete the query with the given ID.

    Args:
      query_id: The ID of the query to delete.
      ignore_if_missing: If True, return success even if the query is missing.
    """
    logger.info('Deleting DV360 query with id: %s', query_id)
    request = self.get_service().queries().deletequery(
        queryId=query_id)
    response = request.execute()
    # TODO(efolgar): check the response for successful execution

  def deletequeries(self, title, ignore_if_missing=False):
    """Deletes any existing queries that match the given title.

    Args:
      title: The query title.
      ignore_if_missing: If True, return success even if the query is missing.

    Raises:
      Exception: When ignore_if_missing is False and no queries were found
          matching the given title.
    """
    queries = self.listqueries(title)

    if not queries and not ignore_if_missing:
      raise Exception('Table deletion failed. Table does not exist.')

    for query in queries:
      self.deletequery(query['queryId'], ignore_if_missing=ignore_if_missing)
