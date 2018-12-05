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

from hooks.dv360_hook import DV360Hook
from airflow.models import BaseOperator
import logging
logger = logging.getLogger(__name__)

class DV360GetReportFileOperator(BaseOperator):
  """
  Get the file for the DBM Report
  """
  def __init__(self,
               conn_id='google_cloud_default',
               query_id=1,
               report_type='',
               *args,
               **kwargs):
    super(DV360GetReportFileOperator, self).__init__(*args, **kwargs)
    self.conn_id = conn_id
    self.query_id = query_id
    self.service = None
    self.report_type = report_type

  def execute(self, context):
    if self.service is None:
      hook = DV360Hook(dv360_conn_id=self.conn_id)
      self.service = hook.get_service()
    request = self.service.reports().listreports(queryId=self.query_id)
    logger.info(dir(request))
    response = request.execute()
    logger.info(response)
    reports = response.get('reports')
    for report in reversed(reports):
      try:
        if report['metadata']['googleCloudStoragePath'] != '':
          report_file_id = report['metadata']['googleCloudStoragePath']
          task_instance = context['task_instance']
          task_instance.xcom_push('%s_dv360_report_file_path' % self.report_type, report_file_id)
          break
      except KeyError:
        pass
