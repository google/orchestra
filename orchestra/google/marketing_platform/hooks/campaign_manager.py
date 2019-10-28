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

"""Hook for accessing the DCM DFA Reporting API.

This hook handles authentication and provides a lightweight wrapper around the
DCM DFA Reporting API.
"""

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient.discovery import build


class GoogleCampaignManagerHook(GoogleCloudBaseHook):
  """Hook for connecting to the Doubleclick Campaign Manager DFA Reporting API.
  """

  def __init__(self,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      api_name='dfareporting',
      api_version='v3.3'):
    """Constructor.

    Args:
      gcp_conn_id: The connection ID to use when fetching connection info.
      delegate_to: The account to impersonate, if any.
      api_name: The name of the DCM API.
      api_version: The version of the DCM API.
    """
    super(GoogleCampaignManagerHook, self).__init__(
        gcp_conn_id=gcp_conn_id,
        delegate_to=delegate_to)
    self.api_name = api_name
    self.api_version = api_version
    self._service = None

  def delete_report(self, profile_id, report_id, ignore_if_missing=False):
    """Deletes DCM report with the given ID.

    Args:
      profile_id: DCM profile ID to validate against API.
      report_id: ID of report to delete.
      ignore_if_missing: flag to determine whether or not to raise an exception
      if a matching report is not found.
    """
    req = self.get_service().reports().delete(
        profileId=profile_id,
        reportId=report_id)
    req.execute()

  def delete_report_by_name(self,
      profile_id,
      report_title,
      ignore_if_missing=False):
    """Deletes DCM report(s) that match the given title.

    Args:
      profile_id: DCM profile ID to validate against API.
      report_title: Title of report to delete
      ignore_if_missing: flag to determine whether or not to raise an exception
      if a matching report is not found.

    Raises:
      Exception: When ignore_if_missing is False and no reports were found
          matching the given title.
    """
    reports = self.list_reports_by_name(profile_id, report_title)
    if not reports and not ignore_if_missing:
      raise Exception('Report deletion failed, report does not exist')

    #  loop over report list to find matching report name
    for report in reports:
      self.delete_report(profile_id, report['id'])

  def list_reports_by_name(self, profile_id, report_title):
    """List all DCM reports that match the given title.

    Args:
      profile_id: DCM profile ID to validate against API.
      report_title: Title of report to delete

    Returns:
      Array of reports that match the given title. Array may be empty if no
      matches are found.
    """
    reports = []
    report_list_response = self.list_reports(profile_id)
    page_token = report_list_response.get('nextPageToken', None)
    for report in report_list_response['items']:
      if report['name'] == report_title:
        reports.append(report)

    while page_token:
      report_list_response = self.list_reports(profile_id, page_token)
      page_token = report_list_response.get('nextPageToken', None)
      for report in report_list_response['items']:
        if report['name'] == report_title:
          reports.append(report)
    return reports

  def list_reports(self, profile_id, page_token=None):
    """List all DCM reports that match the given title.

    Args:
      profile_id: DCM profile ID to validate against API.
      page_token: page token from preovious request to fetch next page of
      results.

    Returns:
      Response object for reports().list() API call.
    """
    request = None
    if page_token:
      request = self.get_service().reports().list(
          profileId=profile_id, pageToken=page_token)
    else:
      request = self.get_service().reports().list(
          profileId=profile_id)
    response = request.execute()
    return response

  def get_service(self):
    """Retrieves the DCM DFA Reporting service object.

    For a detailed reference of the service API see:
    https://developers.google.com/doubleclick-advertisers/v3.3/

    Returns:
      The DFA Reporting service object.
    """
    if self._service is None:
      http_authorized = self._authorize()
      self._service = build(
          self.api_name, self.api_version, http=http_authorized)
    return self._service
