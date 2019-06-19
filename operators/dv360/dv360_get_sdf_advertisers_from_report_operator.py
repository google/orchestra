###########################################################################
#
#  Copyright 2019 Google Inc.
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

import json
import logging
import os
import tempfile
from airflow import models
import requests

logger = logging.getLogger(__name__)


class DV360GetSDFAdvertisersFromReportOperator(models.BaseOperator):
  """
  Get Partner and Advertiser Ids from a report and populate an airflow variable.
  """

  def __init__(self,
               conn_id='google_cloud_default',
               report_type='',
               *args,
               **kwargs):
    super(DV360GetSDFAdvertisersFromReportOperator, self).__init__(*args, **kwargs)
    self.conn_id = conn_id
    self.service = None
    self.report_type = report_type

  def execute(self, context):
    url = context['task_instance'].xcom_pull(
        task_ids=None,
        dag_id=self.dag.dag_id,
        key='%s_dv360_report_file_path' % self.report_type)
    try:
      report_file = tempfile.NamedTemporaryFile(delete=False)
      file_download = requests.get(url, stream=True)
      for chunk in file_download.iter_content(chunk_size=1024 * 1024):
        report_file.write(chunk)
      report_file.close()
      advertisers = {}
      headers = False
      with open(report_file.name, 'r') as f:
        for line in f:
          if not headers:
            headers = line
            headers = headers.strip().split(',')
            advertiser_id_index = headers.index('Advertiser ID')
            partner_id_index = headers.index('Partner ID')
          elif line.strip():
            line = line.strip().split(',')
            advertiser_id = line[advertiser_id_index]
            partner_id = line[partner_id_index]
            if advertiser_id.strip():
              try:
                advertisers[partner_id].append(advertiser_id)
                message = 'ADDING to key %s new advertiser %s' % (
                    partner_id, advertiser_id)
                logger.info(message)
              except KeyError:
                advertisers[partner_id] = [advertiser_id]
                message = 'CREATING new key %s with advertiser %s' % (
                    partner_id, advertiser_id)
                logger.info(message)
          else:
            break
      models.Variable.set('dv360_sdf_advertisers', json.dumps(advertisers))
    finally:
      if report_file:
        report_file.close()
        os.unlink(report_file.name)
