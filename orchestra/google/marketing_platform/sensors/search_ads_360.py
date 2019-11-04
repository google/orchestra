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

"""Sensor for detecting the completion of SA360 reports.
"""

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from orchestra.google.marketing_platform.hooks.search_ads_360 import (
  GoogleSearchAds360Hook
)


class GoogleSearchAds360ReportSensor(BaseSensorOperator):
  """Sensor for detecting the completion of SA360 reports.

  Waits for a Search Ads 360 report to complete.

  Attributes:
    report_id: The ID of the report to poll. (templated)
    gcp_conn_id: The connection ID to use when fetching connection info.
    delegate_to: The account to impersonate, if any.
    poke_interval: Time, in seconds, that the job should wait in between tries.
    timeout: Time, in seconds, before the task times out and fails.
    mode: Whether the sensor should poke or reschedule the task when the
        criteria is not met.
  """

  template_fields = ['report_id']

  def __init__(self,
      report_id,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      poke_interval=60 * 5,
      timeout=60 * 60 * 24,
      mode='reschedule',
      *args,
      **kwargs):
    super(GoogleSearchAds360ReportSensor, self).__init__(
        poke_interval=poke_interval,
        timeout=timeout,
        mode=mode,
        *args,
        **kwargs)
    self.gcp_conn_id = gcp_conn_id
    self.report_id = report_id
    self.delegate_to = delegate_to
    self.hook = None

  def poke(self, context):
    if self.hook is None:
      self.hook = GoogleSearchAds360Hook(
          gcp_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)

    request = self.hook.get_service().reports().get(reportId=self.report_id)
    response = request.execute()

    if response:
      return response.get('isReportReady', False)

    return False
