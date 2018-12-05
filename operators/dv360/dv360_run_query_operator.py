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

from hooks.dbm_hook import DV360Hook
from airflow import models


class DV360RunQueryOperator(models.BaseOperator):
  """
  Take the specified query_id and generate the file
  """
  def __init__(self,
               conn_id='google_cloud_default',
               query_id=1,
               *args,
               **kwargs):
    super(DV360RunQueryOperator, self).__init__(*args, **kwargs)
    self.conn_id = conn_id
    self.service = None
    self.query_id = query_id

  def execute(self, context):
    if(self.service == None):
      hook = DV360Hook(dbm_conn_id=self.conn_id)
      self.service = hook.get_service()

    request = self.service.queries().runquery(queryId=self.query_id, body={})
    request.execute()
