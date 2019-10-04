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

import logging
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


class GoogleSheetsHook(GoogleCloudBaseHook):
  """Hook for connecting to the Sheets API.
  """

  def __init__(self,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      api_name='sheets',
      api_version='v4'):
    """Constructor.

    Args:
      gcp_conn_id: The connection ID to use when fetching connection info.
      delegate_to: The account to impersonate, if any.
      api_name: The name of the Sheets API.
      api_version: The version of the Sheets API.
    """
    super(GoogleSheetsHook, self).__init__(
        gcp_conn_id=gcp_conn_id, delegate_to=delegate_to)
    self.api_name = api_name
    self.api_version = api_version
    self._service = None

  def get_service(self):
    """Retrieves the Sheets service object.

    For a detailed reference of the service API see:
    https://developers.google.com/sheets/api/reference/rest/

    Returns:
      The Sheets service object.
    """
    logger.info('GoogleSheetsHook::get_service')
    if self._service is None:
      http_authorized = self._authorize()
      self._service = build(
          self.api_name, self.api_version, http=http_authorized)
    return self._service

  def getSheetValues(self, spreadsheet_id, sheet_range):
    """Retrieves the values from a spreadsheet matching the given id and sheet range.

    Args:
      spreadsheet_id: The spreadsheet id.
      sheet_range: string which dictates range of values to retrieve
                   from sheet (a1 notation).

    Returns:
      An array of sheet values from the specified sheet.
    """
    logger.info('GoogleSheetsHook::getSheetValues')
    req = self.get_service().spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=sheet_range)
    res = req.execute()
    values = res.get('values')
    logger.info('Retrieved sheet values: %d' % len(values))
    return values

  def getSheetTitles(self, spreadsheet_id, sheet_filter):
    """Retrieves the sheet titles from a spreadsheet matching the given id and sheet filter.

    Args:
      spreadsheet_id: The spreadsheet id.
      sheet_filter: array of sheet title to retrieve from sheet.

    Returns:
      An array of sheet titles from the specified sheet that match
      the sheet filter.
    """
    logger.info('GoogleSheetsHook::getSheetTitles')
    req = self.get_service().spreadsheets().get(
        spreadsheetId=spreadsheet_id)
    res = req.execute()
    sheets = []
    for sh in res.get('sheets'):
      title = sh['properties']['title']
      if len(sheet_filter) > 0:
        if sheet_filter.count(title) > 0:
          sheets.append(title)
      else:
        sheets.append(title)

    return sheets

  def getSpreadsheet(self, spreadsheet_id):
    """Retrieves spreadsheet matching the given id.

    Args:
      spreadsheet_id: The spreadsheet id.

    Returns:
      An spreadsheet that matches the sheet filter.
    """
    logger.info('GoogleSheetsHook::getSpreadsheet')
    req = self.get_service().spreadsheets().get(
        spreadsheetId=spreadsheet_id)
    res = req.execute()
    if res:
      return res
    else:
      return None
