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

"""Example DAG with tasks for listing DCM reports and report file
"""
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import models
from schema import Entity_Schema_Lookup
from dv360_multi_file_upload_erf import DV360MultiERFUploadBqOperator

def yesterday():
  return datetime.today() - timedelta(days=1)


default_args = {
  'owner': 'airflow',
  'start_date': yesterday(),
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(seconds=10),
}

private_entity_types =  [
 'Advertiser', 'Campaign', 'Creative', 'CustomAffinity', 'InsertionOrder',
 'InventorySource', 'LineItem', 'Partner', 'Pixel',
 'UniversalChannel'
]

conn_id = 'dt'
cloud_project_id = models.Variable.get('cloud_project_id')
bq_dataset = models.Variable.get('erf_bq_dataset')

gcs_bucket = models.Variable.get('gcs_bucket')
file_creation_date = yesterday()
file_creation_date = file_creation_date.strftime('%Y%m%d')
dag = DAG(
    'multi_erf_to_bq', default_args=default_args, schedule_interval=timedelta(1))


for entity_type in private_entity_types:
  schema = Entity_Schema_Lookup[entity_type]
  local_bq_table = '%s.%s' % (bq_dataset, entity_type)

  task_id = 'multi_%s_to_bq' % (entity_type)
  multi = DV360MultiERFUploadBqOperator(
  task_id=task_id,
  entity_type=entity_type,
  file_creation_date=file_creation_date,
  depends_on_past=False,
  bq_table=local_bq_table,
  gcs_bucket=gcs_bucket,
  cloud_project_id=cloud_project_id,
  schema=schema,
  dag=dag)
