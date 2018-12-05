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

from airflow.operators import sensors
from hooks.cm_reporting_hook import DcmReportingHook

import pprint

class DV360ReportFileAvailableSensor(sensors.BaseSensorOperator):
  def __init__(self,
               conn_id='google_cloud_default',
               report_id=1,
               query_id=1,
               *args,
               **kwargs):
    super(DV360ReportFileAvailableSensor, self).__init__(*args, **kwargs)
    self.conn_id = conn_id
    self.query_id = query_id
    self.service = None

  def poke(self, context):
    if(self.service == None):
      hook = DcmReportingHook(dcm_report_conn_id=self.conn_id)
      self.service = hook.get_service()

    reportFileId = context['task_instance'].xcom_pull(
        task_ids=None,
        dag_id=self.dag.dag_id,
        key='dbm_report_file_id')
    reportId = self.query_id
    if(reportId is not None and reportFileId is not None):
      request = self.service.files().get(reportId=reportId, fileId=reportFileId)
      response = request.execute()
      pprint.pprint(response)
      if(response['status'] == 'REPORT_AVAILABLE'):
        return True

    return False
