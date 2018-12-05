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

from airflow.contrib.hooks import GoogleCloudBaseHook
from apiclient import discovery

class CMReportingHook(gcp_api_base_hook.GoogleCloudBaseHook):
  """
  This hook utilizes GoogleCloudBaseHook connection to generate the service
  object to CM reporting API
  """
  conn_name_attr = 'cm_report_conn_id'

  def __init__(self,
               cm_report_conn_id='cm_report_default',
               api_name='dfareporting',
               api_version='v3.0',
               delegate_to=None):
    super(CMReportingHook, self).__init__(
        conn_id=cm_report_conn_id,
        delegate_to=delegate_to)

    self.api_name = api_name
    self.api_version = api_version

  def get_service(self):
    http_authorized = self._authorize()
    return discovery.build(self.api_name, self.api_version, http=http_authorized)
