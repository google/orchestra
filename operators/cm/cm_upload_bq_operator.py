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

from airflow.models import BaseOperator
from bq_hook import BigQueryHook
from bq_hook import BigQueryBaseCursor
from header_to_schema import report_schema

import pprint
import logging

logger = logging.getLogger(__name__)

class CMUploadBqOperator(BaseOperator):
  """
  Upload report to bigquery dataset table.

  Args:
    cloud_project_id: Cloud project ID. E.g.: google.com:project-id
    bq_table: Big query table name, in the form: dataset.table
  """
  def __init__(self,
               conn_id='google_cloud_default',
               cloud_project_id=None,
               bq_table=None,
               schema=None,
               report_id=None,
               report_type='',
               write_disposition='WRITE_TRUNCATE',
               *args,
               **kwargs):
    super(CMUploadBqOperator, self).__init__(*args, **kwargs)
    self.conn_id = conn_id
    self.service = None
    self.cloud_project_id = cloud_project_id
    self.bq_table = bq_table
    self.schema = schema
    self.report_id = report_id
    self.report_type = report_type
    self.write_disposition = write_disposition

  def execute(self, context):

    hook = BigQueryHook(bigquery_conn_id='google_cloud_default')
    self.service = hook.get_service()


    bq_base_cursor = BigQueryBaseCursor(self.service, self.cloud_project_id)

    input_source_uris = context['task_instance'].xcom_pull(task_ids = None,
                                                      key = '%s_gcs_file_uris' % self.report_type)

    pprint.pprint(context['task_instance'])
    if self.schema is None:
      self.schema = context['task_instance'].xcom_pull(task_ids = None, key = '%s_cm_report_schema' % self.report_type)
      self.schema = report_schema(self.schema)


    logger.info("schema: ", self.schema)

    bq_base_cursor.run_load(self.bq_table,
                            self.schema,
                            input_source_uris,
                            skip_leading_rows=1,
                            max_bad_records=10,
                            write_disposition=self.write_disposition)
