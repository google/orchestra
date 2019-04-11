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

"""Creates a DBM Report.

Uses the DBM API to create a report (named a "query" in the tool); outputs the
ID of the created report to both xcom and an airflow variable.
"""

import json
import logging
from airflow import models
from hooks.dv360_hook import DV360Hook

logger = logging.getLogger(__name__)


class DV360CreateQueryOperator(models.BaseOperator):
  """
  Create a query for DV360.
  API REFERENCE: https://developers.google.com/bid-manager/v1/queries#resource

  Args:
    conn_id {string}: The ID of the airflow connection.
    body {string}: (Optional) The report body. A json-formatted string. Will
      default to loading the value in the airflow variable named
      'dv360_report_body' if no value is provided for this argument.
    output_var {string}: (Optional) The name of the output variable and xcom.
      Will default to 'dv360_create_report_id' but can be overwritten.
  """
  def __init__(self,
              conn_id=None,
              body=None,
              output_var='dv360_create_report_id',
              *args,
              **kwargs):
    super(DV360CreateQueryOperator, self).__init__(*args, **kwargs)
    self.dv360_conn_id = conn_id
    body = body if body else models.Variable.get('dv360_report_body')
    self.body = json.loads(body)
    self.output_var = output_var


  def execute(self, context):
    service = DV360Hook(dv360_conn_id=self.dv360_conn_id)
    service = service.get_service()

    logger.info(self.body)

    request = service.queries().createquery(body=self.body)
    response = request.execute()
    logger.info(response)
    if response:
      context['task_instance'].xcom_push(self.output_var, response['queryId'])
      models.Variable.set(self.output_var, response['queryId'])
