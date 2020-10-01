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

"""Sensor for detecting the completion of DV360 reports.
"""

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from orchestra.google.marketing_platform.hooks.display_video_360 import (
    GoogleDisplayVideo360Hook
)


class GoogleDisplayVideo360ReportSensor(BaseSensorOperator):
    """Sensor for detecting the completion of DV360 reports.

    Waits for a Display & Video 360 query to complete and returns the latest
    report path as XCom variable.

    Attributes:
      query_id: The ID of the query to poll. (templated)
      gcp_conn_id: The connection ID to use when fetching connection info.
      delegate_to: The account to impersonate, if any.
      poke_interval: Time, in seconds, that the job should wait in between tries.
      timeout: Time, in seconds, before the task times out and fails.
      mode: Whether the sensor should poke or reschedule the task when the
          criteria is not met.

    XComs:
      report_url: The Google Cloud Storage url where the latest report is stored.
    """

    template_fields = ['query_id']

    def __init__(self,
                 query_id,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 poke_interval=60 * 5,
                 timeout=60 * 60 * 24,
                 mode='reschedule',
                 *args,
                 **kwargs):
        super(GoogleDisplayVideo360ReportSensor, self).__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            mode=mode,
            *args,
            **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.query_id = query_id
        self.delegate_to = delegate_to
        self.hook = None

    def poke(self, context):
        if self.hook is None:
            self.hook = GoogleDisplayVideo360Hook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to)

        request = self.hook.get_service().queries().getquery(queryId=self.query_id)
        response = request.execute()

        if response:
            metadata = response.get('metadata')
            if metadata and not metadata.get('running'):
                report_url = metadata.get(u'googleCloudStoragePathForLatestReport')
                if report_url:
                    context['task_instance'].xcom_push('report_url', report_url)
                return True

        return False


class GoogleDisplayVideo360SDFdownloadTaskOperationGetSensor(BaseSensorOperator):
    """Sensor for detecting the completion of DV360 SDF File Download.

    Waits for a Display & Video 360 SDF download to complete 
    
    """

    template_fields = ['operation_name']

    def __init__(self,
                 operation_name=None,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 poke_interval=30,
                 timeout=60 * 60 * 24,
                 mode='reschedule',
                 api_version='v1',
                 api_name='displayvideo',
                 *args,
                 **kwargs):
        super(GoogleDisplayVideo360SDFdownloadTaskOperationGetSensor, self).__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            mode=mode,
            *args,
            **kwargs)
        
        self.gcp_conn_id = gcp_conn_id
        self.operation_name = operation_name
        self.delegate_to = delegate_to
        self.api_version = api_version
        self.api_name = api_name
        self.hook = None

    def poke(self, context):
        if self.hook is None:
            self.hook = GoogleDisplayVideo360Hook(
                api_version=self.api_version,
                api_name=self.api_name,
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to)

        request = self.hook.get_service().sdfdownloadtasks().operations().get(name=self.operation_name)
        response = request.execute()
        if response:
            response_body = response.get('response')
            if response_body and response.get('done') == True:
                resourceName = response_body.get(u'resourceName')
                if resourceName:
                    context['task_instance'].xcom_push('resourceName', resourceName)
                return True

        return False