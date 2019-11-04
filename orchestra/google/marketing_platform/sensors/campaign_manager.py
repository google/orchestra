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

"""Sensor for detecting the completion of DCM reports.
"""
import logging


from airflow.sensors.base_sensor_operator import BaseSensorOperator
from orchestra.google.marketing_platform.hooks.campaign_manager import (
  GoogleCampaignManagerHook
)

logger = logging.getLogger(__name__)


class GoogleCampaignManagerReportSensor(BaseSensorOperator):
  """Sensor for detecting the completion of DCM reports.

  Waits for a Campaign Manger report to complete.

  Attributes:
    report_id: The ID of the report to poll. (templated)
    file_id: The ID of the file associated with the report. (templated)
    profile_id: DCM profile ID used when making API requests. (templated)
    gcp_conn_id: The connection ID to use when fetching connection info.
    delegate_to: The account to impersonate, if any.
    poke_interval: Time, in seconds, that the job should wait in between tries.
    timeout: Time, in seconds, before the task times out and fails.
    mode: Whether the sensor should poke or reschedule the task when the
        criteria is not met.
  """

  template_fields = ['report_id', 'file_id', 'profile_id']

  def __init__(self,
      report_id,
      file_id,
      profile_id,
      gcp_conn_id='google_cloud_default',
      delegate_to=None,
      poke_interval=60 * 5,
      timeout=60 * 60 * 24,
      mode='reschedule',
      *args,
      **kwargs):
    super(GoogleCampaignManagerReportSensor, self).__init__(
        poke_interval=poke_interval,
        timeout=timeout,
        mode=mode,
        *args,
        **kwargs)
    self.gcp_conn_id = gcp_conn_id
    self.profile_id = profile_id
    self.file_id = file_id
    self.report_id = report_id
    self.delegate_to = delegate_to
    self.hook = None

  def poke(self, context):
    if self.hook is None:
      self.hook = GoogleCampaignManagerHook(
          gcp_conn_id=self.gcp_conn_id,
          delegate_to=self.delegate_to)
    logger.info(self.gcp_conn_id)
    logger.info(self.report_id)
    logger.info(self.file_id)
    request = self.hook.get_service().reports().files().get(
        profileId=self.profile_id,
        reportId=self.report_id,
        fileId=self.file_id)
    response = request.execute()
    logger.info(response)
    if response:
      if response['status'] != 'PROCESSING':
        return True
    return False
