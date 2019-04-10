"""Creates a DBM Report.

Uses the DBM API to create a query, then outputting the ID of the created
report to both xcom and an airflow variable.
"""
import json
import logging
from airflow import models
from hooks.dv360_hook import DV360Hook

logger = logging.getLogger(__name__)


class DV360CreateQueryOperator(models.BaseOperator):
  """Create a query for DV360.
  API REFERENCE: https://developers.google.com/bid-manager/v1/queries#resource
  """
  def __init__(self,
        conn_id=None,
        body=None,
        output_var='dv360_create_report_id',
        *args,
        **kwargs
      ):
    super(DV360CreateQueryOperator, self).__init__(*args, **kwargs)
    self.dv360_conn_id = conn_id
    if body:
      self.body = json.loads(body)
    else:
      self.body = json.loads(models.Variable.get('dv360_report_body'))
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
